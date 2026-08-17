/*
 * Copyright 2025 The Kubernetes Authors.
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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/dra-example-driver/internal/profiles/cpu"
	"sigs.k8s.io/dra-example-driver/internal/profiles/gpu"
)

var (
	testShareId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
)

func TestPreparedDevicesGetDevices(t *testing.T) {
	tests := map[string]struct {
		preparedDevices PreparedDevices
		expected        []*drapbv1.Device
	}{
		"nil PreparedDevices": {
			preparedDevices: nil,
			expected:        nil,
		},
		"several PreparedDevices": {
			preparedDevices: PreparedDevices{
				{Device: drapbv1.Device{DeviceName: "dev1"}},
				{Device: drapbv1.Device{DeviceName: "dev2"}},
				{Device: drapbv1.Device{DeviceName: "dev3"}},
			},
			expected: []*drapbv1.Device{
				{DeviceName: "dev1"},
				{DeviceName: "dev2"},
				{DeviceName: "dev3"},
			},
		},
		"preparedDevice with shareId": {
			preparedDevices: PreparedDevices{
				{Device: drapbv1.Device{DeviceName: "dev1", ShareId: &testShareId}},
				{Device: drapbv1.Device{DeviceName: "dev2", ShareId: &testShareId}},
			},
			expected: []*drapbv1.Device{
				{DeviceName: "dev1", ShareId: &testShareId},
				{DeviceName: "dev2", ShareId: &testShareId},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			devices := test.preparedDevices.GetDevices()
			assert.Equal(t, test.expected, devices)
		})
	}
}

// TestComputeDeviceConfigShareID verifies that the ShareID the scheduler
// assigns to each allocation result is carried through to the prepared device,
// which the kubelet plugin then forwards to the kubelet. This guards the
// consumable-capacity sharing path: two allocations of the same device must
// keep their distinct ShareIDs.
func TestComputeDeviceConfigShareID(t *testing.T) {
	const (
		nodeName   = "test-node"
		driverName = "cpu.example.com"
	)

	flags := &Flags{
		cdiRoot:         t.TempDir(),
		driverName:      driverName,
		profile:         "cpu",
		nodeName:        nodeName,
		cpuNUMANodes:    1,
		cpusPerNUMANode: 4,
	}
	state, err := NewDeviceState(&Config{
		flags:   flags,
		profile: cpu.NewProfile(nodeName, driverName, flags.cpuNUMANodes, flags.cpusPerNUMANode),
	})
	require.NoError(t, err)

	result := func(request string, shareID *types.UID) resourceapi.DeviceRequestAllocationResult {
		return resourceapi.DeviceRequestAllocationResult{
			Request: request,
			Driver:  driverName,
			Pool:    nodeName,
			Device:  "numa-0",
			ShareID: shareID,
		}
	}

	tests := map[string]struct {
		results          []resourceapi.DeviceRequestAllocationResult
		expectedShareIDs []*types.UID
		// expectedCDISuffixes are CDI device-ID suffixes that must each appear
		// on some prepared device, proving the ShareID is woven into the CDI
		// device name so shares of one device stay distinct.
		expectedCDISuffixes []string
	}{
		"no ShareID": {
			results:             []resourceapi.DeviceRequestAllocationResult{result("cpus", nil)},
			expectedShareIDs:    []*types.UID{nil},
			expectedCDISuffixes: []string{"claim-uid-numa-0"},
		},
		"distinct ShareIDs sharing one device": {
			results: []resourceapi.DeviceRequestAllocationResult{
				result("cpus-0", ptr.To(types.UID("share-0"))),
				result("cpus-1", ptr.To(types.UID("share-1"))),
			},
			expectedShareIDs:    []*types.UID{ptr.To(types.UID("share-0")), ptr.To(types.UID("share-1"))},
			expectedCDISuffixes: []string{"claim-uid-numa-0-share-0", "claim-uid-numa-0-share-1"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			claim := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{Results: test.results},
					},
				},
			}

			prepared, err := state.computeDeviceConfig(claim)
			require.NoError(t, err)
			require.Len(t, prepared, len(test.expectedShareIDs))

			var shareIDs []*types.UID
			var cdiDeviceIDs []string
			for _, device := range prepared {
				shareIDs = append(shareIDs, device.ShareID)
				cdiDeviceIDs = append(cdiDeviceIDs, device.CdiDeviceIds...)
			}
			assert.ElementsMatch(t, test.expectedShareIDs, shareIDs)

			for _, suffix := range test.expectedCDISuffixes {
				matched := slices.ContainsFunc(cdiDeviceIDs, func(id string) bool {
					return strings.HasSuffix(id, suffix)
				})
				assert.True(t, matched, "expected a CDI device ID ending with %q in %v", suffix, cdiDeviceIDs)
			}
		})
	}
}

// TestComputeDeviceConfigSharedDeviceContainerEdits verifies that when one
// device is allocated multiple times in a claim (consumable capacity), each
// share keeps its own container edits keyed by the share-aware device id rather
// than collapsing onto a single entry for the bare device name.
func TestComputeDeviceConfigSharedDeviceContainerEdits(t *testing.T) {
	const (
		nodeName   = "test-node"
		driverName = "cpu.example.com"
	)

	flags := &Flags{
		cdiRoot:         t.TempDir(),
		driverName:      driverName,
		profile:         "cpu",
		nodeName:        nodeName,
		cpuNUMANodes:    1,
		cpusPerNUMANode: 4,
	}
	state, err := NewDeviceState(&Config{
		flags:   flags,
		profile: cpu.NewProfile(nodeName, driverName, flags.cpuNUMANodes, flags.cpusPerNUMANode),
	})
	require.NoError(t, err)

	capacityKey := resourceapi.QualifiedName(driverName + "/cpu")
	result := func(request string, shareID types.UID, consumed string) resourceapi.DeviceRequestAllocationResult {
		return resourceapi.DeviceRequestAllocationResult{
			Request: request,
			Driver:  driverName,
			Pool:    nodeName,
			Device:  "numa-0",
			ShareID: ptr.To(shareID),
			ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{
				capacityKey: resource.MustParse(consumed),
			},
		}
	}

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{Results: []resourceapi.DeviceRequestAllocationResult{
					result("cpu0", "share-0", "1"),
					result("cpu1", "share-1", "3"),
				}},
			},
		},
	}

	prepared, err := state.computeDeviceConfig(claim)
	require.NoError(t, err)
	require.Len(t, prepared, 2)

	consumedByShare := map[types.UID]string{}
	for _, device := range prepared {
		require.NotNil(t, device.ShareID)
		require.NotNil(t, device.ContainerEdits)
		require.NotNil(t, device.ContainerEdits.ContainerEdits)
		for _, env := range device.ContainerEdits.Env {
			if consumed, ok := strings.CutPrefix(env, "CPU_DEVICE_0_CONSUMED_CPU="); ok {
				consumedByShare[*device.ShareID] = consumed
			}
		}
	}
	assert.Equal(t, "1", consumedByShare["share-0"], "share-0 should keep its own consumed CPU edit")
	assert.Equal(t, "3", consumedByShare["share-1"], "share-1 should keep its own consumed CPU edit")
}

// TestConfigMatchesRequest verifies the request-scoping rules for opaque device
// configs, in particular that a config naming a parent request also applies to
// results allocated via one of its prioritized-list subrequests (KEP-4816
// firstAvailable), where the scheduler records the result's request as
// "<request>/<subrequest>".
func TestConfigMatchesRequest(t *testing.T) {
	tests := map[string]struct {
		configRequests []string
		requestRef     string
		expected       bool
	}{
		"empty config requests match everything": {
			configRequests: nil,
			requestRef:     "gpu",
			expected:       true,
		},
		"exact match on plain request": {
			configRequests: []string{"gpu"},
			requestRef:     "gpu",
			expected:       true,
		},
		"parent request matches subrequest result": {
			configRequests: []string{"gpu"},
			requestRef:     "gpu/older-gpu",
			expected:       true,
		},
		"exact subrequest reference matches": {
			configRequests: []string{"gpu/older-gpu"},
			requestRef:     "gpu/older-gpu",
			expected:       true,
		},
		"subrequest reference does not match sibling subrequest": {
			configRequests: []string{"gpu/latest-gpu"},
			requestRef:     "gpu/older-gpu",
			expected:       false,
		},
		"subrequest reference does not match bare parent request": {
			configRequests: []string{"gpu/older-gpu"},
			requestRef:     "gpu",
			expected:       false,
		},
		"unrelated request does not match": {
			configRequests: []string{"other"},
			requestRef:     "gpu/older-gpu",
			expected:       false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, configMatchesRequest(test.configRequests, test.requestRef))
		})
	}
}

// TestComputeDeviceConfigSubrequestConfigMatch verifies end to end through
// computeDeviceConfig that an opaque config attached to a claim is applied to
// a device allocated via a prioritized-list subrequest (KEP-4816
// firstAvailable): a config scoped to the parent request name or to the full
// "<request>/<subrequest>" reference must apply, while configs scoped to a
// sibling subrequest or an unrelated request must fall back to the default.
func TestComputeDeviceConfigSubrequestConfigMatch(t *testing.T) {
	const (
		nodeName   = "test-node"
		driverName = "gpu.example.com"
	)

	flags := &Flags{
		cdiRoot:    t.TempDir(),
		driverName: driverName,
		profile:    "gpu",
		nodeName:   nodeName,
		numDevices: 1,
	}
	state, err := NewDeviceState(&Config{
		flags:   flags,
		profile: gpu.NewProfile(nodeName, flags.numDevices, 0, false, false, false),
	})
	require.NoError(t, err)

	claimConfig := func(interval string, requests ...string) resourceapi.DeviceAllocationConfiguration {
		params := fmt.Sprintf(`{
			"apiVersion": "gpu.resource.example.com/v1alpha1",
			"kind": "GpuConfig",
			"sharing": {
				"strategy": "TimeSlicing",
				"timeSlicingConfig": {"interval": %q}
			}
		}`, interval)
		return resourceapi.DeviceAllocationConfiguration{
			Source:   resourceapi.AllocationConfigSourceClaim,
			Requests: requests,
			DeviceConfiguration: resourceapi.DeviceConfiguration{
				Opaque: &resourceapi.OpaqueDeviceConfiguration{
					Driver:     driverName,
					Parameters: runtime.RawExtension{Raw: []byte(params)},
				},
			},
		}
	}

	tests := map[string]struct {
		configs []resourceapi.DeviceAllocationConfiguration
		// expectedInterval is the TIMESLICE_INTERVAL env expected on the
		// prepared device; "Default" means no config matched and the default
		// GpuConfig was applied.
		expectedInterval string
	}{
		"config scoped to parent request applies to subrequest result": {
			configs:          []resourceapi.DeviceAllocationConfiguration{claimConfig("Long", "gpu")},
			expectedInterval: "Long",
		},
		"config scoped to full subrequest reference applies": {
			configs:          []resourceapi.DeviceAllocationConfiguration{claimConfig("Long", "gpu/older-gpu")},
			expectedInterval: "Long",
		},
		"config scoped to sibling subrequest does not apply": {
			configs:          []resourceapi.DeviceAllocationConfiguration{claimConfig("Long", "gpu/latest-gpu")},
			expectedInterval: "Default",
		},
		"config scoped to unrelated request does not apply": {
			configs:          []resourceapi.DeviceAllocationConfiguration{claimConfig("Long", "other")},
			expectedInterval: "Default",
		},
		// Matching carries no specificity ranking: the last matching config
		// in precedence order wins, whether it names the parent request or
		// the full subrequest reference.
		"subrequest-scoped config listed later shadows parent-scoped config": {
			configs: []resourceapi.DeviceAllocationConfiguration{
				claimConfig("Long", "gpu"),
				claimConfig("Short", "gpu/older-gpu"),
			},
			expectedInterval: "Short",
		},
		"parent-scoped config listed later shadows subrequest-scoped config": {
			configs: []resourceapi.DeviceAllocationConfiguration{
				claimConfig("Short", "gpu/older-gpu"),
				claimConfig("Long", "gpu"),
			},
			expectedInterval: "Long",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			claim := &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{UID: "claim-uid"},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{{
								Request: "gpu/older-gpu",
								Driver:  driverName,
								Pool:    nodeName,
								Device:  "gpu-0",
							}},
							Config: test.configs,
						},
					},
				},
			}

			prepared, err := state.computeDeviceConfig(claim)
			require.NoError(t, err)
			require.Len(t, prepared, 1)
			require.NotNil(t, prepared[0].ContainerEdits)
			assert.Contains(t, prepared[0].ContainerEdits.Env,
				"GPU_DEVICE_0_TIMESLICE_INTERVAL="+test.expectedInterval)
		})
	}
}

// TestUnprepareReturnsErrorOnUnreadableCheckpoint verifies that Unprepare returns an error
// (rather than panicking) when checkpoint.json exists but cannot be decoded.
// The fix returns the error immediately without touching the checkpoint, letting
// the kubelet retry once the operator has corrected the file.
func TestUnprepareReturnsErrorOnUnreadableCheckpoint(t *testing.T) {
	const (
		nodeName   = "test-node"
		driverName = "cpu.example.com"
		claimUID   = "some-claim-uid"
	)

	// t.TempDir() is used for both CDI files and the checkpoint directory so
	// the test stays fully self-contained and cleans up automatically.
	tmpDir := t.TempDir()

	flags := &Flags{
		cdiRoot:                     tmpDir,
		driverName:                  driverName,
		profile:                     "cpu",
		nodeName:                    nodeName,
		cpuNUMANodes:                1,
		cpusPerNUMANode:             4,
		kubeletPluginsDirectoryPath: tmpDir,
	}

	state, err := NewDeviceState(&Config{
		flags:   flags,
		profile: cpu.NewProfile(nodeName, driverName, flags.cpuNUMANodes, flags.cpusPerNUMANode),
	})
	require.NoError(t, err)

	// The plugin directory must exist before writing files (NewDeviceState does
	// not create it; that is done by RunPlugin at startup).
	pluginDir := filepath.Join(tmpDir, driverName)
	require.NoError(t, os.MkdirAll(pluginDir, 0750))

	// create a CDI spec file for the claim, simulating a claim that was fully
	// prepared before the checkpoint was corrupted. After Unprepare returns an
	// error the file must still be present — nothing should have been released.
	cdiSpecPath := filepath.Join(tmpDir, "k8s.cpu.example.com-cpu-"+claimUID+"-0.yaml")
	require.NoError(t, os.WriteFile(cdiSpecPath, []byte("placeholder"), 0600))

	// Write garbage bytes to the checkpoint file so that readCheckpoint returns
	// (nil, err) — the exact precondition that triggered the nil dereference.
	corruptData := []byte("THIS IS NOT VALID CHECKPOINT JSON")
	require.NoError(t, os.WriteFile(
		filepath.Join(pluginDir, DriverPluginCheckpointFile),
		corruptData,
		0600,
	))

	// 1. Unprepare must return an error, not panic.
	err = state.Unprepare(types.UID(claimUID))
	require.Error(t, err, "Unprepare must return an error on an unreadable checkpoint")
	assert.Contains(t, err.Error(), "unable to read checkpoint")

	// 2. The corrupt file must be left exactly as it was — the driver must not
	// overwrite it, preserving evidence and avoiding silent data loss.
	data, readErr := os.ReadFile(state.checkpointPath)
	require.NoError(t, readErr)
	assert.Equal(t, corruptData, data, "corrupt checkpoint file must not be modified by Unprepare")

	// 3. The CDI spec file for the claim must still be present — Unprepare
	// must not release anything when it cannot read the checkpoint.
	_, statErr := os.Stat(cdiSpecPath)
	assert.NoError(t, statErr, "CDI spec file must still be present after Unprepare returns an error")
}
