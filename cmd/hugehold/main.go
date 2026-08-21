/*
 * Copyright The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// hugehold acquires or releases 1Gi hugepages on the node on behalf of the
// dra-example-kubeletplugin. It stands in for device buffers a real DRA
// driver would allocate at NodePrepareResources and free only at
// NodeUnprepareResources: pages held in a hugetlbfs file persist after this
// process exits and are freed when the file is unlinked.
//
// It must run in the node's mount and cgroup namespaces
// (nsenter -t 1 -m -C -- hugehold ...).
//
// Usage:
//
//	hugehold acquire <name> <pages>
//	hugehold release <name>
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const (
	mountDir = "/dev/hugepages-demo-1g"
	pageSize = 1 << 30
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "hugehold: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: hugehold acquire <name> <pages> | hugehold release <name>")
	}
	name := filepath.Base(os.Args[2])
	switch os.Args[1] {
	case "acquire":
		if len(os.Args) != 4 {
			return fmt.Errorf("acquire needs <name> <pages>")
		}
		pages, err := strconv.Atoi(os.Args[3])
		if err != nil || pages <= 0 {
			return fmt.Errorf("bad page count %q", os.Args[3])
		}
		return acquire(name, pages)
	case "release":
		err := os.Remove(filepath.Join(mountDir, name))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		fmt.Printf("released hugepages file %s\n", name)
		return nil
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func acquire(name string, pages int) error {
	// The plugin pod's cgroup has a zero hugetlb limit (it requests no
	// hugepages), so join init.scope, a leaf cgroup with no hugetlb limit,
	// before faulting pages. Pages stay charged there until released.
	if err := os.WriteFile("/sys/fs/cgroup/init.scope/cgroup.procs",
		[]byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return fmt.Errorf("joining init.scope cgroup: %w", err)
	}
	if err := os.MkdirAll(mountDir, 0755); err != nil {
		return err
	}
	if !mounted(mountDir) {
		if err := syscall.Mount("nodev", mountDir, "hugetlbfs", 0, "pagesize=1G"); err != nil {
			return fmt.Errorf("mounting hugetlbfs: %w", err)
		}
	}
	path := filepath.Join(mountDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	size := int64(pages) * pageSize
	if err := f.Truncate(size); err != nil {
		return err
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		os.Remove(path)
		return fmt.Errorf("mmap of %d hugepages failed (pool exhausted?): %w", pages, err)
	}
	for off := int64(0); off < size; off += pageSize {
		data[off] = 1
	}
	if err := syscall.Munmap(data); err != nil {
		return err
	}
	fmt.Printf("holding %d x 1Gi hugepages in %s\n", pages, path)
	return nil
}

func mounted(dir string) bool {
	b, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 2 && fields[1] == dir && fields[2] == "hugetlbfs" {
			return true
		}
	}
	return false
}
