// Copyright 2021 Matrix Origin
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

package memory

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func freeGetter(free map[int]uint64) func(int) (uint64, error) {
	return func(d int) (uint64, error) { return free[d], nil }
}

// reservedSnapshot reads the package-global in-flight totals. Tests must leave
// this empty: the map outlives any one test, and -count=N reruns share the
// process, so a leaked claim would silently change a later run's verdict.
func reservedSnapshot() map[int]uint64 {
	loadReservations.mu.Lock()
	defer loadReservations.mu.Unlock()
	out := make(map[int]uint64, len(loadReservations.reserved))
	for k, v := range loadReservations.reserved {
		out[k] = v
	}
	return out
}

// requireNoReservations asserts the package-global claim map is empty. Called at
// the START of a test so a leak from an earlier test is attributed to the test
// that leaked, rather than silently shrinking this one's budget.
func requireNoReservations(t *testing.T) {
	t.Helper()
	require.Empty(t, reservedSnapshot(), "a previous test leaked a device reservation")
}

func TestDeviceLoadBytes(t *testing.T) {
	t.Run("sharded splits across resolved devices", func(t *testing.T) {
		got := DeviceLoadBytes(vectorindex.DistributionMode_SHARDED, []int{0, 1, 2, 3}, 4, 400)
		require.Equal(t, map[int]uint64{0: 100, 1: 100, 2: 100, 3: 100}, got)
	})

	t.Run("sharded under simulation aggregates onto the aliased device", func(t *testing.T) {
		// gpu_multi_simulation resolves every logical shard onto physical 0.
		// Charging per-entry would understate that card by 4x; the map keys by
		// device id, so the four shards correctly sum back to the full tar.
		got := DeviceLoadBytes(vectorindex.DistributionMode_SHARDED, []int{0, 0, 0, 0}, 4, 400)
		require.Equal(t, map[int]uint64{0: 400}, got)
	})

	t.Run("sharded with no manifest shard count charges conservatively", func(t *testing.T) {
		// Manifest disagrees with the configured mode: over-estimate rather
		// than under-estimate, because under-estimating OOMs at first query.
		got := DeviceLoadBytes(vectorindex.DistributionMode_SHARDED, []int{0, 1}, 0, 400)
		require.Equal(t, map[int]uint64{0: 400, 1: 400}, got)
	})

	t.Run("replicated charges a full copy per device", func(t *testing.T) {
		got := DeviceLoadBytes(vectorindex.DistributionMode_REPLICATED, []int{0, 1}, 0, 400)
		require.Equal(t, map[int]uint64{0: 400, 1: 400}, got)
	})

	t.Run("single charges only the first device", func(t *testing.T) {
		got := DeviceLoadBytes(vectorindex.DistributionMode_SINGLE_GPU, []int{2, 3}, 0, 400)
		require.Equal(t, map[int]uint64{2: 400}, got)
	})

	t.Run("degenerate inputs", func(t *testing.T) {
		require.Empty(t, DeviceLoadBytes(vectorindex.DistributionMode_SHARDED, nil, 2, 400))
		require.Empty(t, DeviceLoadBytes(vectorindex.DistributionMode_SHARDED, []int{0}, 2, 0))
	})
}

// TestDeviceReserveLoadRejectsConcurrentDoubleSpend is the counterexample that
// motivated DeviceReserveLoad. DeviceAdmitLoad only SAMPLES free VRAM,
// and the index cache deduplicates loads by cache key alone, so two different
// cold indexes can both observe the same free bytes and both pass -- then
// together overcommit the card. The first sub-test documents that gap; the
// second proves reservation closes it.
func TestDeviceReserveLoadRejectsConcurrentDoubleSpend(t *testing.T) {
	requireNoReservations(t)
	free := map[int]uint64{0: 10 << 30} // budget = 60% = 6 GiB
	want := map[int]uint64{0: 4 << 30}  // two of these = 8 GiB > 6 GiB

	t.Run("check-only admits both (the defect)", func(t *testing.T) {
		require.NoError(t, DeviceAdmitLoad(want, freeGetter(free), "A"))
		require.NoError(t, DeviceAdmitLoad(want, freeGetter(free), "B"))
	})

	t.Run("reservation admits exactly one", func(t *testing.T) {
		const n = 2
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			releases []func()
			okCount  int
		)
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				release, err := DeviceReserveLoad(want, freeGetter(free), "concurrent")
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					okCount++
					releases = append(releases, release)
				}
			}()
		}
		wg.Wait()
		// Register the safety net BEFORE asserting: require.* aborts via
		// runtime.Goexit, so any release left below an assertion would never run
		// and would leak a claim into every later test and -count iteration.
		// release is idempotent, so the explicit call below is still meaningful.
		for _, r := range releases {
			t.Cleanup(r)
		}
		require.Equal(t, 1, okCount, "exactly one of two 4GiB loads may hold a 6GiB budget")

		for _, r := range releases {
			r()
		}
		require.Empty(t, reservedSnapshot(), "every claim must be released")
	})

	t.Run("headroom is reusable once released", func(t *testing.T) {
		release, err := DeviceReserveLoad(want, freeGetter(free), "first")
		require.NoError(t, err)
		t.Cleanup(release) // safety net if an assertion below aborts the test
		_, err = DeviceReserveLoad(want, freeGetter(free), "second")
		require.Error(t, err, "second must be refused while the first is in flight")
		release()
		release() // idempotent: safe alongside a deferred call
		require.Empty(t, reservedSnapshot())

		release2, err := DeviceReserveLoad(want, freeGetter(free), "third")
		require.NoError(t, err, "released headroom must be reusable")
		t.Cleanup(release2)
		release2()
		require.Empty(t, reservedSnapshot())
	})

	t.Run("partial failure reserves nothing", func(t *testing.T) {
		// Device 1 cannot fit; device 0 was claimed first in map order. Whatever
		// the iteration order, a rejected admission must leave no residue.
		twoDev := map[int]uint64{0: 1 << 30, 1: 100 << 30}
		_, err := DeviceReserveLoad(twoDev, freeGetter(map[int]uint64{0: 10 << 30, 1: 10 << 30}), "partial")
		require.Error(t, err)
		require.Empty(t, reservedSnapshot(), "a refused admission must not leak a claim")
	})

	t.Run("getter error reserves nothing", func(t *testing.T) {
		_, err := DeviceReserveLoad(map[int]uint64{0: 1}, func(int) (uint64, error) {
			return 0, errors.New("cuda query failed")
		}, "boom")
		require.Error(t, err)
		require.Empty(t, reservedSnapshot())
	})
}
