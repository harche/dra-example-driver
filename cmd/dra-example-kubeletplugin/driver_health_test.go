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

package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/klog/v2"
)

// newTestDriver builds just enough of a driver to exercise the device-health
// logic without starting the kubeletplugin helper or a Kubernetes client.
func newTestDriver(deviceNames []string) *driver {
	d := &driver{
		poolName:        "test-node",
		deviceHealth:    true,
		lastHealth:      make(map[string]deviceHealthSnapshot),
		healthOverrides: make(map[string]string),
		subscribers:     make(map[chan struct{}]struct{}),
		stopHealthCh:    make(chan struct{}),
	}
	d.devices = append([]string(nil), deviceNames...)
	sort.Strings(d.devices)
	d.simulator = NewHealthSimulator(d.devices, false)
	return d
}

func healthOf(r kubeletplugin.DeviceHealthReport, name string) kubeletplugin.HealthStatus {
	for _, dh := range r.Devices {
		if dh.DeviceName == name {
			return dh.Health
		}
	}
	return ""
}

func TestBuildHealthReport(t *testing.T) {
	d := newTestDriver([]string{"gpu-1", "gpu-0"})
	report := d.buildHealthReport()

	require.Len(t, report.Devices, 2)
	// devices are iterated in sorted order.
	assert.Equal(t, "gpu-0", report.Devices[0].DeviceName)
	assert.Equal(t, "gpu-1", report.Devices[1].DeviceName)
	for _, dh := range report.Devices {
		assert.Equal(t, "test-node", dh.PoolName)
		assert.Equal(t, kubeletplugin.HealthStatusHealthy, dh.Health)
		assert.Equal(t, 60*time.Second, dh.HealthCheckTimeout)
		assert.NotEmpty(t, dh.Message)
	}
}

func TestApplyHealthOverridesLifecycle(t *testing.T) {
	d := newTestDriver([]string{"gpu-0", "gpu-1"})
	logger := klog.Background()

	// Apply "unhealthy" to gpu-0 only.
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "unhealthy"})
	assert.Equal(t, kubeletplugin.HealthStatusUnhealthy, healthOf(d.buildHealthReport(), "gpu-0"))
	assert.Equal(t, kubeletplugin.HealthStatusHealthy, healthOf(d.buildHealthReport(), "gpu-1"))
	assert.Equal(t, "unhealthy", d.healthOverrides["gpu-0"])

	// Change gpu-0 to "unknown".
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "unknown"})
	assert.Equal(t, kubeletplugin.HealthStatusUnknown, healthOf(d.buildHealthReport(), "gpu-0"))

	// Unknown/unrelated annotation keys are ignored; gpu-0 keeps its override.
	d.applyHealthOverrides(logger, map[string]string{
		"health.example.com/gpu-0":          "unknown",
		"health.example.com/does-not-exist": "unhealthy",
		"unrelated/annotation":              "x",
	})
	assert.Equal(t, kubeletplugin.HealthStatusUnknown, healthOf(d.buildHealthReport(), "gpu-0"))

	// Removing all overrides returns the device to healthy simulation.
	d.applyHealthOverrides(logger, map[string]string{})
	assert.Equal(t, kubeletplugin.HealthStatusHealthy, healthOf(d.buildHealthReport(), "gpu-0"))
	_, ok := d.healthOverrides["gpu-0"]
	assert.False(t, ok, "override bookkeeping should be cleared")
}

func TestApplyHealthOverridesIgnoresInvalidValues(t *testing.T) {
	d := newTestDriver([]string{"gpu-0"})
	logger := klog.Background()

	// A typo must NOT silently force the device healthy or create an override.
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "degraded"})
	assert.Equal(t, kubeletplugin.HealthStatusHealthy, healthOf(d.buildHealthReport(), "gpu-0"))
	_, ok := d.healthOverrides["gpu-0"]
	assert.False(t, ok, "invalid value must not create an override")

	// Case and surrounding whitespace are normalized for mapping and storage.
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "  UNHEALTHY "})
	assert.Equal(t, kubeletplugin.HealthStatusUnhealthy, healthOf(d.buildHealthReport(), "gpu-0"))
	assert.Equal(t, "unhealthy", d.healthOverrides["gpu-0"])

	// Replacing a valid override with garbage returns the device to simulation.
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "nonsense"})
	assert.Equal(t, kubeletplugin.HealthStatusHealthy, healthOf(d.buildHealthReport(), "gpu-0"))
	_, ok = d.healthOverrides["gpu-0"]
	assert.False(t, ok, "override must be cleared when replaced by an invalid value")
}

func TestWatchHealthStatusDisabledReturnsErrHealthNotSupported(t *testing.T) {
	d := newTestDriver([]string{"gpu-0"})
	d.deviceHealth = false // simulate --device-health=false

	reports := make(chan kubeletplugin.DeviceHealthReport, 1)
	err := d.WatchHealthStatus(context.Background(), reports)

	require.ErrorIs(t, err, kubeletplugin.ErrHealthNotSupported)
	assert.Empty(t, reports, "no report should be sent when device health is disabled")
}

func TestNotifySubscribersCoalesces(t *testing.T) {
	d := newTestDriver([]string{"gpu-0"})
	sig := make(chan struct{}, 1)
	d.subscribers[sig] = struct{}{}

	// Two wakes with no reader in between must coalesce into one pending signal
	// and never block.
	d.notifySubscribers()
	d.notifySubscribers()
	require.Len(t, sig, 1)
}

func TestWatchHealthStatusStreamsUpdates(t *testing.T) {
	d := newTestDriver([]string{"gpu-0", "gpu-1"})
	logger := klog.Background()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reports := make(chan kubeletplugin.DeviceHealthReport)
	done := make(chan error, 1)
	go func() { done <- d.WatchHealthStatus(ctx, reports) }()

	// Reading the initial snapshot also guarantees the subscriber is registered.
	initial := receiveReport(t, reports)
	assert.Equal(t, kubeletplugin.HealthStatusHealthy, healthOf(initial, "gpu-0"))

	// Flip gpu-0 unhealthy; the subscriber must wake and resend.
	d.applyHealthOverrides(logger, map[string]string{"health.example.com/gpu-0": "unhealthy"})
	eventuallyHealth(t, reports, "gpu-0", kubeletplugin.HealthStatusUnhealthy)

	// Cancelling the context ends the stream and deregisters the subscriber.
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WatchHealthStatus did not return after context cancel")
	}
	d.clientsMu.RLock()
	assert.Empty(t, d.subscribers, "subscriber must be deregistered on exit")
	d.clientsMu.RUnlock()
}

// TestConcurrentHealthAccess runs subscribers, overrides, and polls concurrently
// so `go test -race` can flag any data race across the health code paths.
func TestConcurrentHealthAccess(t *testing.T) {
	d := newTestDriver([]string{"gpu-0", "gpu-1", "gpu-2"})
	logger := klog.Background()
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	// Subscribers that continuously drain reports until the context is cancelled.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reports := make(chan kubeletplugin.DeviceHealthReport)
			sub := make(chan error, 1)
			go func() { sub <- d.WatchHealthStatus(ctx, reports) }()
			for {
				select {
				case <-reports:
				case <-sub:
					return
				}
			}
		}()
	}

	// Concurrent override churn, which drives notifySubscribers via pollDeviceHealth.
	values := []string{"healthy", "unhealthy", "unknown"}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				d.applyHealthOverrides(logger, map[string]string{
					fmt.Sprintf("health.example.com/gpu-%d", i%3): values[j%len(values)],
				})
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	d.clientsMu.RLock()
	assert.Empty(t, d.subscribers, "all subscribers must be deregistered after shutdown")
	d.clientsMu.RUnlock()
}

func receiveReport(t *testing.T, reports <-chan kubeletplugin.DeviceHealthReport) kubeletplugin.DeviceHealthReport {
	t.Helper()
	select {
	case r := <-reports:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for report")
		return kubeletplugin.DeviceHealthReport{}
	}
}

func eventuallyHealth(t *testing.T, reports <-chan kubeletplugin.DeviceHealthReport, name string, want kubeletplugin.HealthStatus) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case r := <-reports:
			if healthOf(r, name) == want {
				return
			}
		case <-deadline:
			t.Fatalf("did not observe %s = %s in time", name, want)
		}
	}
}
