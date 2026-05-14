// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package system

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

func TestCPU(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := stopper.NewStopper("test")
	defer st.Stop()
	Run(st)
	mcpu := NumCPU()
	time.Sleep(2 * time.Second)
	acpu := CPUAvailable()
	require.Equal(t, true, float64(mcpu) >= acpu)
}

func TestMemory(t *testing.T) {
	totalMemory := MemoryTotal()
	availableMemory := MemoryAvailable()
	require.Equal(t, true, totalMemory >= availableMemory)
}

func TestWorkingSet(t *testing.T) {
	ws := WorkingSet()
	used := MemoryUsed()
	// WorkingSet <= MemoryUsed (inactive_file subtracted)
	require.LessOrEqual(t, ws, used)
	// WorkingSet > 0 (we're running, so some memory is in use)
	require.Greater(t, ws, uint64(0))
}

func TestReadStatKey(t *testing.T) {
	t.Run("nonexistent file returns 0", func(t *testing.T) {
		v := readStatKey("/nonexistent/path", "inactive_file")
		require.Equal(t, uint64(0), v)
	})

	t.Run("reads cgroup v2 memory.stat if available", func(t *testing.T) {
		v := readStatKey("/sys/fs/cgroup/memory.stat", "inactive_file")
		// On systems with cgroups v2, this should be > 0
		// On systems without, it will be 0 (file doesn't exist)
		t.Logf("inactive_file from cgroups v2: %d", v)
	})
}

// Benchmark_GoRutinues
// goos: darwin
// goarch: arm64
// pkg: github.com/matrixorigin/matrixone/pkg/common/system
// cpu: Apple M1 Pro
// Benchmark_GoRutinues
// Benchmark_GoRutinues/Atomic
// Benchmark_GoRutinues/Atomic-10         	1000000000	         0.5446 ns/op
// Benchmark_GoRutinues/GoMaxProcs
// Benchmark_GoRutinues/GoMaxProcs-10     	87136477	        14.06 ns/op
// Benchmark_GoRutinues/NumGoroutine
// Benchmark_GoRutinues/NumGoroutine-10   	249281432	         4.795 ns/op
func Benchmark_GoRutinues(b *testing.B) {
	var v atomic.Int32
	b.Logf("v: %d, runtime.GOMAXPROCS(0): %d, runtime.NumGoroutine: %d",
		v.Load(), runtime.GOMAXPROCS(0), runtime.NumGoroutine())
	b.Run("Atomic", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v.Load()
		}
	})
	b.Run("GoMaxProcs", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.GOMAXPROCS(0)
		}
	})
	b.Run("NumGoroutine", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runtime.NumGoroutine() // running go routines, not eq GOMAXPROCS
		}
	})
}

// TestSetGoMaxProcs
// ut for https://github.com/matrixorigin/MO-Cloud/issues/4486
func TestSetGoMaxProcs(t *testing.T) {
	// init
	initMaxProcs := runtime.GOMAXPROCS(0)
	type args struct {
		n int
	}
	tests := []struct {
		name    string
		args    args
		wantRet int
		wantGet int
	}{
		{
			name: "normal",
			args: args{
				n: 5,
			},
			wantRet: initMaxProcs,
			wantGet: 5,
		},
		{
			name: "zero",
			args: args{
				n: 0,
			},
			wantRet: 5,
			wantGet: 5,
		},
		{
			name: "nagetive",
			args: args{
				n: -1,
			},
			wantRet: 5,
			wantGet: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SetGoMaxProcs(tt.args.n)
			require.Equal(t, tt.wantRet, got)
			gotQuery := GoMaxProcs()
			require.Equal(t, tt.wantGet, gotQuery)
		})
	}
}

// ============================================================================
// Tests for quota refresh debouncing (Issue #20964)
// ============================================================================

func TestShouldRefreshQuotaConfig(t *testing.T) {
	// Reset state
	lastQuotaRefreshTime.Store(0)

	t.Run("first call should allow refresh", func(t *testing.T) {
		if !shouldRefreshQuotaConfig() {
			t.Error("first call should return true")
		}
	})

	t.Run("immediate second call should be debounced", func(t *testing.T) {
		// Simulate a refresh
		lastQuotaRefreshTime.Store(time.Now().UnixNano())

		// Immediate call should be blocked
		if shouldRefreshQuotaConfig() {
			t.Error("immediate second call should return false (debounced)")
		}
	})

	t.Run("call after debounce period should allow refresh", func(t *testing.T) {
		// Set last refresh to 2 seconds ago
		lastQuotaRefreshTime.Store(time.Now().UnixNano() - 2*int64(time.Second))

		if !shouldRefreshQuotaConfig() {
			t.Error("call after debounce period should return true")
		}
	})

	t.Run("call exactly at debounce boundary should allow refresh", func(t *testing.T) {
		// Set last refresh to exactly quotaRefreshDebounceSeconds ago
		lastQuotaRefreshTime.Store(time.Now().UnixNano() - int64(quotaRefreshDebounceSeconds)*int64(time.Second))

		if !shouldRefreshQuotaConfig() {
			t.Error("call at debounce boundary should return true")
		}
	})
}

func TestRefreshQuotaConfigDebounce(t *testing.T) {
	// Reset state
	lastQuotaRefreshTime.Store(0)

	t.Run("refreshQuotaConfig updates timestamp", func(t *testing.T) {
		before := time.Now().UnixNano()
		refreshQuotaConfig()
		after := lastQuotaRefreshTime.Load()

		if after < before {
			t.Errorf("timestamp not updated: got %d, want >= %d", after, before)
		}
	})

	t.Run("multiple rapid refreshes are debounced", func(t *testing.T) {
		lastQuotaRefreshTime.Store(0)

		// First refresh should succeed
		if !shouldRefreshQuotaConfig() {
			t.Fatal("first refresh should be allowed")
		}
		refreshQuotaConfig()

		// Rapid subsequent calls should be blocked
		blocked := 0
		for i := 0; i < 5; i++ {
			if !shouldRefreshQuotaConfig() {
				blocked++
			}
			time.Sleep(100 * time.Millisecond)
		}

		if blocked == 0 {
			t.Error("expected some calls to be debounced")
		}
	})
}

func TestDebounceSimulatesK8sScaling(t *testing.T) {
	// Simulate k8s vertical scaling scenario where kubelet updates
	// both cpu.max and memory.max in quick succession

	lastQuotaRefreshTime.Store(0)

	// First event (cpu.max changed)
	if !shouldRefreshQuotaConfig() {
		t.Fatal("first event should trigger refresh")
	}
	refreshQuotaConfig()
	firstRefreshTime := lastQuotaRefreshTime.Load()

	// Second event (memory.max changed) - happens immediately after
	time.Sleep(10 * time.Millisecond)
	if shouldRefreshQuotaConfig() {
		t.Error("second event should be debounced (< 1s)")
	}

	// Verify timestamp wasn't updated by the debounced call
	if lastQuotaRefreshTime.Load() != firstRefreshTime {
		t.Error("debounced call should not update timestamp")
	}

	// After debounce period, next event should trigger refresh
	time.Sleep(time.Duration(quotaRefreshDebounceSeconds) * time.Second)
	if !shouldRefreshQuotaConfig() {
		t.Error("event after debounce period should trigger refresh")
	}
}

func BenchmarkShouldRefreshQuotaConfig(b *testing.B) {
	lastQuotaRefreshTime.Store(time.Now().UnixNano() - 10*int64(time.Second))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shouldRefreshQuotaConfig()
	}
}

func TestConcurrentDebounce(t *testing.T) {
	// Test concurrent access to debounce mechanism
	lastQuotaRefreshTime.Store(0)

	done := make(chan bool, 10)

	// Simulate 10 concurrent goroutines calling shouldRefreshQuotaConfig
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				shouldRefreshQuotaConfig()
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should complete without panic or race condition
	t.Log("concurrent debounce test passed")
}

func TestHasCgroupMemLimit(t *testing.T) {
	// Just verify the function runs without panic and returns a bool.
	_ = HasCgroupMemLimit()
}

func TestMallocTrimIfTight(t *testing.T) {
	// Reset the rate-limit so the call is not silently skipped.
	lastMallocTrim.Store(0)
	// On a normal system total > 0 and used < 75%, so the trim branch is skipped.
	// The test just verifies no panic and correct early-return path.
	MallocTrimIfTight()
}

func TestStartMallocTrimLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	StartMallocTrimLoop(ctx)
	// Allow the goroutine to start and then cancel.
	time.Sleep(10 * time.Millisecond)
	cancel()
	// Give the goroutine time to observe the cancellation.
	time.Sleep(20 * time.Millisecond)
}

func TestRefreshCgroupInactiveFile(t *testing.T) {
	// refreshCgroupInactiveFile reads from /sys/fs/cgroup; on most CI machines the
	// files don't exist so it stores 0. Either way it must not panic.
	refreshCgroupInactiveFile()
	// cgroupInactiveFile() returns whatever was stored.
	_ = cgroupInactiveFile()
}

func TestReadStatKeyMissingFile(t *testing.T) {
	// A path that does not exist should return 0.
	v := readStatKey("/nonexistent/path/memory.stat", "inactive_file")
	require.Equal(t, uint64(0), v)
}
