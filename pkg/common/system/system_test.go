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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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

func TestMemoryStatsFromPages(t *testing.T) {
	const total = uint64(16 << 30)
	for _, pageSize := range []uint64{4096, 16384} {
		for _, pages := range [][2]uint64{{0, 0}, {1, 0}, {32768, 65536}} {
			t.Run(fmt.Sprintf("page-%d/free-%d/inactive-%d", pageSize, pages[0], pages[1]), func(t *testing.T) {
				mem, err := memoryStatsFromPages(total, pages[0], pages[1], pageSize)
				require.NoError(t, err)
				require.Equal(t, pages[0]*pageSize, mem.Free)
				require.Equal(t, (pages[0]+pages[1])*pageSize, mem.ActualFree)
				require.Equal(t, total, mem.Used+mem.Free)
				require.Equal(t, total, mem.ActualUsed+mem.ActualFree)
			})
		}
	}
	for _, input := range [][4]uint64{
		{total, 0, 0, 0},
		{total, total/4096 + 1, 0, 4096},
		{total, total / 4096, 1, 4096},
		{total, ^uint64(0), 0, 16384},
	} {
		_, err := memoryStatsFromPages(input[0], input[1], input[2], input[3])
		require.Error(t, err)
	}
}

func TestHostMemoryStats(t *testing.T) {
	mem, err := hostMemoryStats()
	require.NoError(t, err)
	require.Positive(t, mem.Total)
	require.LessOrEqual(t, mem.Free, mem.ActualFree)
	require.LessOrEqual(t, mem.ActualFree, mem.Total)
	require.Equal(t, mem.Total, mem.Used+mem.Free)
	require.Equal(t, mem.Total, mem.ActualUsed+mem.ActualFree)
}

func TestMinHierarchicalCgroupLimit(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "tenant")
	child := filepath.Join(parent, "query")
	require.NoError(t, os.MkdirAll(child, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "memory.max"), []byte("max\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "memory.max"), []byte("2147483648\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(child, "memory.max"), []byte("max\n"), 0o600))
	require.Equal(t, uint64(2<<30), minHierarchicalLimit(child, root, "memory.max"))

	// A non-PID-1 process may remain in the same nested cgroup while its
	// ancestor limit is lowered. Re-reading the hierarchy must observe the
	// lower limit instead of retaining a process-start snapshot.
	require.NoError(t, os.WriteFile(filepath.Join(parent, "memory.max"), []byte("1073741824\n"), 0o600))
	require.Equal(t, uint64(1<<30), minHierarchicalLimit(child, root, "memory.max"))

	dir, ok := cgroupDirectory(root, "/tenant", "/tenant/query")
	require.True(t, ok)
	require.Equal(t, filepath.Join(root, "query"), dir)
	_, ok = cgroupDirectory(root, "/tenant", "/other/query")
	require.False(t, ok)
	dir, ok = cgroupDirectory(root, "/", "/tenant/query")
	require.True(t, ok)
	require.Equal(t, filepath.Join(root, "tenant", "query"), dir)
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

func TestEffectiveGoMaxProcs(t *testing.T) {
	tests := []struct {
		name            string
		availableCPUs   int
		currentMaxProcs int
		expected        int
	}{
		{
			name:            "available CPUs unavailable",
			availableCPUs:   0,
			currentMaxProcs: 8,
			expected:        8,
		},
		{
			name:            "scheduler limit unavailable",
			availableCPUs:   24,
			currentMaxProcs: 0,
			expected:        24,
		},
		{
			name:            "scheduler limit below available CPUs",
			availableCPUs:   24,
			currentMaxProcs: 8,
			expected:        8,
		},
		{
			name:            "scheduler limit matches available CPUs",
			availableCPUs:   24,
			currentMaxProcs: 24,
			expected:        24,
		},
		{
			name:            "scheduler limit above available CPUs",
			availableCPUs:   24,
			currentMaxProcs: 32,
			expected:        24,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, effectiveGoMaxProcs(
				test.availableCPUs,
				test.currentMaxProcs,
			))
		})
	}
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
	lastQuotaRefreshTime.Store(time.Now().UnixNano() -
		int64(quotaRefreshDebounceSeconds)*int64(time.Second) - 1)
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

// TestMinHierarchicalHeadroom is the counterexample for taking the minimum
// ancestor LIMIT and then subtracting only the leaf's usage: a constrained
// parent has far less headroom than that arithmetic reports.
func TestMinHierarchicalHeadroom(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "tenant")
	child := filepath.Join(parent, "query")
	require.NoError(t, os.MkdirAll(child, 0o700))

	write := func(dir, name, val string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(val+"\n"), 0o600))
	}

	// Parent: 8 GiB cap with 7 GiB already charged (siblings) -> 1 GiB headroom.
	// Child:  4 GiB cap with 1 GiB charged to us              -> 3 GiB headroom.
	// The binding constraint is the PARENT's 1 GiB. Limit-minimum minus
	// leaf-usage would instead report 4 GiB - 1 GiB = 3 GiB, i.e. 3x too much.
	write(root, "memory.max", "max")
	write(root, "memory.current", "0")
	write(parent, "memory.max", strconv.FormatUint(8<<30, 10))
	write(parent, "memory.current", strconv.FormatUint(7<<30, 10))
	write(child, "memory.max", strconv.FormatUint(4<<30, 10))
	write(child, "memory.current", strconv.FormatUint(1<<30, 10))

	got, ok := minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(1<<30), got, "must report the parent's headroom, not the leaf's")

	t.Run("exhausted level reports zero, still measured", func(t *testing.T) {
		write(parent, "memory.current", strconv.FormatUint(9<<30, 10)) // over its cap
		got, ok := minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
		require.True(t, ok, "an exhausted cgroup is MEASURED, not unmeasured")
		require.Equal(t, uint64(0), got)
		write(parent, "memory.current", strconv.FormatUint(7<<30, 10))
	})

	t.Run("limit without readable usage is unmeasured", func(t *testing.T) {
		// Skipping such a level would resurrect the overstatement this prevents.
		require.NoError(t, os.Remove(filepath.Join(parent, "memory.current")))
		_, ok := minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
		require.False(t, ok)
		write(parent, "memory.current", strconv.FormatUint(7<<30, 10))
	})

	t.Run("no limit anywhere is unmeasured", func(t *testing.T) {
		write(parent, "memory.max", "max")
		write(child, "memory.max", "max")
		_, ok := minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
		require.False(t, ok, "unlimited hierarchy must fall back to the host reading")
	})
}

func TestMemoryAvailableIncludingCacheReportsStatus(t *testing.T) {
	// The contract that matters to callers: a zero value must be distinguishable
	// from an unavailable measurement, because they demand opposite responses.
	avail, measured := MemoryAvailableIncludingCache()
	if measured {
		require.GreaterOrEqual(t, avail, uint64(0))
	} else {
		require.Equal(t, uint64(0), avail, "unmeasured must report zero")
	}
}

// cgroup v1 has no "max" string: when no memory limit is set it writes
// PAGE_COUNTER_MAX into memory.limit_in_bytes, which parses as a perfectly
// good integer. Treating it as a real limit reported ~9.2 EB of headroom as a
// MEASURED figure, and a measured figure is exactly what callers size bulk
// allocations from -- so an unlimited v1 host silently lost its memory bound
// instead of falling back to the host reading.
func TestMinHierarchicalHeadroom_V1UnlimitedSentinel(t *testing.T) {
	const v1Unlimited = uint64(0x7FFFFFFFFFFFF000) // PAGE_COUNTER_MAX
	root := t.TempDir()
	child := filepath.Join(root, "child")
	require.NoError(t, os.MkdirAll(child, 0o755))
	write := func(dir, name string, val uint64) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name),
			[]byte(strconv.FormatUint(val, 10)+"\n"), 0o600))
	}

	write(root, "memory.limit_in_bytes", v1Unlimited)
	write(root, "memory.usage_in_bytes", 1<<30)
	write(child, "memory.limit_in_bytes", v1Unlimited)
	write(child, "memory.usage_in_bytes", 1<<30)

	_, ok := minHierarchicalHeadroom(child, root, "memory.limit_in_bytes", "memory.usage_in_bytes", "memory.stat")
	require.False(t, ok, "an unlimited v1 hierarchy must fall back to the host reading")

	// A bare LONG_MAX is larger than the sentinel and must also read as unlimited.
	write(child, "memory.limit_in_bytes", uint64(1<<63-1))
	_, ok = minHierarchicalHeadroom(child, root, "memory.limit_in_bytes", "memory.usage_in_bytes", "memory.stat")
	require.False(t, ok, "LONG_MAX must also read as unlimited")

	// A REAL v1 limit still binds: 4 GiB cap, 1 GiB used -> 3 GiB headroom.
	write(child, "memory.limit_in_bytes", 4<<30)
	got, ok := minHierarchicalHeadroom(child, root, "memory.limit_in_bytes", "memory.usage_in_bytes", "memory.stat")
	require.True(t, ok, "a real limit must still be measured")
	require.Equal(t, uint64(3<<30), got)

	// ...and the same sentinel must not be mistaken for a limit by the limit walk,
	// which feeds CgroupMemoryLimit and thence the second tier of
	// MemoryAvailableIncludingCache.
	write(child, "memory.limit_in_bytes", v1Unlimited)
	require.Zero(t, minHierarchicalLimit(child, root, "memory.limit_in_bytes"),
		"unlimited v1 must not surface as a colossal limit")
}

// The hierarchy walk is not the only way the v1 sentinel reaches a caller:
// CgroupMemoryLimit falls back to gosigar, which returns the sentinel verbatim
// (its own tests assert 9223372036854771712). Filtering it in only one of the
// two paths left MemoryAvailableIncludingCache's second tier still deriving a
// budget from ~9.2 EB.
func TestNormalizeCgroupLimit(t *testing.T) {
	require.Zero(t, normalizeCgroupLimit(0), "no limit")
	require.Zero(t, normalizeCgroupLimit(-1), "error sentinel")
	require.Zero(t, normalizeCgroupLimit(int64(cgroupV1Unlimited)), "v1 PAGE_COUNTER_MAX")
	require.Zero(t, normalizeCgroupLimit(1<<63-1), "bare LONG_MAX")
	require.Equal(t, uint64(4<<30), normalizeCgroupLimit(4<<30), "a real limit survives")
	require.Equal(t, uint64(cgroupV1Unlimited-1), normalizeCgroupLimit(int64(cgroupV1Unlimited)-1),
		"just below the sentinel is still a real limit")
}

func TestNormalizeMemoryCapacity(t *testing.T) {
	require.Zero(t, NormalizeMemoryCapacity(0), "unknown capacity")
	require.Zero(t, NormalizeMemoryCapacity(cgroupV1Unlimited), "v1 PAGE_COUNTER_MAX")
	require.Zero(t, NormalizeMemoryCapacity(^uint64(0)), "invalid oversized capacity")
	require.Equal(t, uint64(4<<30), NormalizeMemoryCapacity(4<<30), "a real capacity survives")
}

func TestEffectiveContainerMemoryTotal(t *testing.T) {
	require.Equal(t, uint64(4<<30), effectiveContainerMemoryTotal(4<<30, 256<<30),
		"a finite cgroup limit wins over the host")
	require.Equal(t, uint64(256<<30), effectiveContainerMemoryTotal(int64(cgroupV1Unlimited), 256<<30),
		"an unlimited v1 value falls back to host capacity")
	require.Equal(t, uint64(256<<30), effectiveContainerMemoryTotal(-1, 256<<30),
		"an unavailable cgroup value falls back to host capacity")
}

// Clean filesystem cache remains available regardless of its active/inactive
// LRU state. Anonymous and dirty memory, including siblings, remain charged.
func TestHierarchicalHeadroomDoesNotChargeReclaimableCache(t *testing.T) {
	const (
		limit = 10 << 30
		anon  = 2 << 30
		cache = 6 << 30
	)
	root := t.TempDir()
	child := filepath.Join(root, "leaf")
	require.NoError(t, os.MkdirAll(child, 0o755))
	write := func(dir, name, content string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}
	write(child, "memory.max", strconv.Itoa(limit))
	write(child, "memory.current", strconv.Itoa(anon+cache))

	got, ok := minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(limit-anon-cache), got, "unreadable cache leaves full usage charged")

	for _, active := range []uint64{0, cache} {
		write(child, "memory.stat", fmt.Sprintf("file %d\nshmem 0\ninactive_file %d\nactive_file %d\nfile_dirty 0\nfile_writeback 0\n",
			uint64(cache), cache-active, active))
		got, ok = minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
		require.True(t, ok)
		require.Equal(t, uint64(limit-anon), got, "cache recency must not collapse admission headroom")
	}

	// The parent's sibling memory still binds, even with abundant clean cache
	// in the leaf. Then give the parent its own clean-cache credit, net of dirty
	// and writeback pages; it must use the parent's counters, not the leaf's.
	write(root, "memory.max", strconv.Itoa(8<<30))
	write(root, "memory.current", strconv.Itoa(7<<30))
	got, ok = minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(1<<30), got)
	write(root, "memory.stat", "file 5368709120\nshmem 0\ninactive_file 0\nactive_file 5368709120\nfile_dirty 1073741824\nfile_writeback 1073741824\n")
	got, ok = minHierarchicalHeadroom(child, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(4<<30), got)

	// An inconsistent stat must not make usage subtraction wrap.
	write(child, "memory.current", "1")
	got, ok = minHierarchicalHeadroom(child, child, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(limit), got)
}

func TestReclaimableCgroupCache(t *testing.T) {
	for _, v1 := range []bool{false, true} {
		version := "v2"
		keys := [6]string{"file", "shmem", "inactive_file", "active_file", "file_dirty", "file_writeback"}
		if v1 {
			version = "v1"
			keys = [6]string{"total_cache", "total_shmem", "total_inactive_file", "total_active_file", "total_dirty", "total_writeback"}
		}
		t.Run(version, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				values   [6]uint64
				extra    string
				want     uint64
				measured bool
			}{
				{name: "active clean cache", values: [6]uint64{100, 0, 0, 100, 0, 0}, want: 100, measured: true},
				{name: "inactive clean cache", values: [6]uint64{100, 0, 100, 0, 0, 0}, want: 100, measured: true},
				{name: "dirty and writeback stay charged", values: [6]uint64{100, 0, 20, 80, 7, 3}, want: 90, measured: true},
				{name: "shmem is not disk cache", values: [6]uint64{100, 40, 20, 80, 0, 0}, want: 60, measured: true},
				{name: "unevictable is outside file LRUs", values: [6]uint64{110, 0, 20, 80, 0, 0}, extra: "unevictable 10\n", want: 100, measured: true},
				{name: "all dirty", values: [6]uint64{100, 0, 20, 80, 100, 0}, measured: true},
				{name: "writeback exceeds remaining cache", values: [6]uint64{100, 0, 20, 80, 60, 60}, measured: true},
				{name: "shmem exceeds file", values: [6]uint64{100, 110, 20, 80, 0, 0}, measured: true},
				{name: "LRU overflow", values: [6]uint64{100, 0, 1, ^uint64(0), 0, 0}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					stat := filepath.Join(t.TempDir(), "memory.stat")
					var data strings.Builder
					for i, key := range keys {
						fmt.Fprintf(&data, "%s\t%d\n", key, tc.values[i])
					}
					if v1 {
						// Local counters deliberately contradict the hierarchical ones.
						data.WriteString("cache 1\nshmem 0\ninactive_file 1\nactive_file 0\ndirty 999\nwriteback 999\n")
					}
					if v1 {
						data.WriteString(strings.ReplaceAll(tc.extra, "unevictable", "total_unevictable"))
					} else {
						data.WriteString(tc.extra)
					}
					require.NoError(t, os.WriteFile(stat, []byte(data.String()), 0o600))
					got, ok := reclaimableCgroupCache(stat)
					require.Equal(t, tc.measured, ok)
					require.Equal(t, tc.want, got)
				})
			}

			// Missing/malformed safety counters must not be replaced by zeros
			// or, on v1, by readable but non-hierarchical local counters.
			for _, value := range []string{"", "not-a-number"} {
				stat := filepath.Join(t.TempDir(), "memory.stat")
				data := ""
				for i, key := range keys {
					if i != 4 {
						data += fmt.Sprintf("%s %d\n", key, [6]uint64{100, 0, 20, 80, 0, 0}[i])
					}
				}
				if value != "" {
					data += keys[4] + " " + value + "\n"
				}
				if v1 {
					data += "dirty 0\n"
				}
				require.NoError(t, os.WriteFile(stat, []byte(data), 0o600))
				_, ok := reclaimableCgroupCache(stat)
				require.False(t, ok)
			}
		})
	}
	_, ok := reclaimableCgroupCache(filepath.Join(t.TempDir(), "absent.stat"))
	require.False(t, ok)
}
