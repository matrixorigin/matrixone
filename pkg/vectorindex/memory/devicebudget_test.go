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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func TestDeviceDistinct(t *testing.T) {
	cases := []struct {
		name string
		in   []int
		want []int
	}{
		{"nil", nil, nil},
		{"empty", []int{}, nil},
		{"already distinct keeps order", []int{2, 0, 1}, []int{2, 0, 1}},
		{"simulation aliases collapse", []int{0, 0, 0, 0}, []int{0}},
		{"mixed keeps first-seen order", []int{1, 0, 1, 2, 0}, []int{1, 0, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DeviceDistinct(tc.in))
		})
	}
}

// TestDeviceBuildBytes covers the attribution the build-side VRAM claim depends
// on. The predecessor this replaced (DeviceLoadBytes) had equivalent coverage;
// losing it when the Go ledger was retired would have left the SHARDED split and
// the simulation-aliasing case unexercised.
func TestDeviceBuildBytes(t *testing.T) {
	t.Run("sharded splits across devices", func(t *testing.T) {
		got := DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, []int{0, 1, 2, 3}, 400)
		require.Equal(t, map[int]uint64{0: 100, 1: 100, 2: 100, 3: 100}, got)
	})

	t.Run("simulation aliases accumulate onto the one physical card", func(t *testing.T) {
		// gpu_multi_simulation resolves every logical shard onto physical 0.
		// Charging per-entry would understate that card 4x; keying the map by
		// device id sums the shards back to the full demand.
		got := DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, []int{0, 0, 0, 0}, 400)
		require.Equal(t, map[int]uint64{0: 400}, got)
	})

	t.Run("replicated charges a full copy per device", func(t *testing.T) {
		got := DeviceBuildBytes(vectorindex.DistributionMode_REPLICATED, []int{0, 1}, 400)
		require.Equal(t, map[int]uint64{0: 400, 1: 400}, got)
	})

	t.Run("single charges only the first device", func(t *testing.T) {
		got := DeviceBuildBytes(vectorindex.DistributionMode_SINGLE_GPU, []int{2, 3}, 400)
		require.Equal(t, map[int]uint64{2: 400}, got)
	})

	t.Run("a demand smaller than the device count still claims", func(t *testing.T) {
		// Without the guard the per-device share rounds to 0, ReserveBuildMemory
		// skips zero entries, and the build would take NO claim at all -- silently
		// losing the admission it is supposed to get.
		got := DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, []int{0, 1, 2, 3}, 3)
		require.Equal(t, map[int]uint64{0: 3, 1: 3, 2: 3, 3: 3}, got)
	})

	t.Run("degenerate inputs claim nothing", func(t *testing.T) {
		require.Empty(t, DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, nil, 400))
		require.Empty(t, DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, []int{0}, 0))
	})
}

// fakeRowsFitting mirrors cuvs.RowsFittingFreeMem EXACTLY, including the two
// details that matter: the budget is 60% of free, and the result is CLAMPED to a
// minimum of 1 (helper.cpp rows_fitting_gpu_mem). A fake without that clamp lets
// a "did anything fit?" predicate pass in tests while being always-true against
// real hardware -- which is precisely the bug this fake now reproduces.
func fakeRowsFitting(free uint64) DeviceRowsFittingFunc {
	return func(device int, perRow uint64) (int64, uint64, error) {
		if perRow == 0 {
			return 0, free, nil
		}
		rows := int64(free / 10 * 6 / perRow)
		if rows < 1 {
			rows = 1
		}
		return rows, free, nil
	}
}

// TestDeviceLoadFits is the review's counterexample: a build rotated into
// sub-indexes that each load fine but cannot be resident together. The refusal
// must happen BEFORE the first load, and must name the aggregate.
func TestDeviceLoadFits(t *testing.T) {
	devices := []int{0}

	// 1000 free -> a 600-byte budget.
	require.NoError(t, DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 500, fakeRowsFitting(1000)))

	// Aggregate does not fit, even though each individual sub-index would.
	err := DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 3000, fakeRowsFitting(1000))
	require.Error(t, err)
	require.Contains(t, err.Error(), "resident on device 0")
	require.Contains(t, err.Error(), "built successfully",
		"the message must say the build was fine, or the operator looks in the wrong place")

	// Exactly at the budget is admitted; one byte over is not.
	require.NoError(t, DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 600, fakeRowsFitting(1000)))
	require.Error(t, DeviceLoadFits(
		vectorindex.DistributionMode_SINGLE_GPU, devices, 601, fakeRowsFitting(1000)))
}

func TestDeviceLoadFitsAttribution(t *testing.T) {
	devices := []int{0, 1}
	// Budget is 600 per device. SHARDED halves the aggregate, so 1000 total is 500
	// per device and fits; REPLICATED charges each device the whole 1000 and does not.
	require.NoError(t, DeviceLoadFits(
		vectorindex.DistributionMode_SHARDED, devices, 1000, fakeRowsFitting(1000)))
	require.Error(t, DeviceLoadFits(
		vectorindex.DistributionMode_REPLICATED, devices, 1000, fakeRowsFitting(1000)))
}

func TestDeviceLoadFitsDegenerateInputs(t *testing.T) {
	devices := []int{0}
	// Nothing to load, no devices, or no measuring function: not this gate's call
	// to refuse. A zero total in particular means the sizes were unknown, and the
	// per-load claims remain the real admission.
	require.NoError(t, DeviceLoadFits(vectorindex.DistributionMode_SINGLE_GPU, devices, 0, fakeRowsFitting(10)))
	require.NoError(t, DeviceLoadFits(vectorindex.DistributionMode_SINGLE_GPU, nil, 100, fakeRowsFitting(10)))
	require.NoError(t, DeviceLoadFits(vectorindex.DistributionMode_SINGLE_GPU, devices, 100, nil))

	// A device that cannot be measured must fail loudly: admitting an unmeasured
	// load is what produced the partial-load failure in the first place.
	boom := func(device int, perRow uint64) (int64, uint64, error) {
		return 0, 0, moerr.NewInternalErrorNoCtx("cudaMemGetInfo failed")
	}
	err := DeviceLoadFits(vectorindex.DistributionMode_SINGLE_GPU, devices, 100, boom)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot measure device 0")
}

// fakeTotalMem builds a DeviceTotalMemFunc over a per-device capacity map. This
// is the seam that lets the hardware rule be tested without a GPU; production
// passes cuvs.DeviceTotalMem, the only part that needs real hardware.
func fakeMaxAdmissible(total map[int]uint64) DeviceMaxAdmissibleFunc {
	return func(dev int) (uint64, error) {
		t, ok := total[dev]
		if !ok {
			return 0, errors.New("no such device")
		}
		return t, nil
	}
}

// TestDeviceAggregateFitsHardware covers the CREATE-time refusal. Unlike
// DeviceLoadFits, which admits against a fraction of currently-FREE memory and so
// refuses situationally, this compares against the card's TOTAL capacity: a
// refusal here is permanent, which is what makes it safe to fail CREATE on rather
// than deferring to the first query.
func TestDeviceAggregateFitsHardware(t *testing.T) {
	t.Run("fits on the hardware", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsHardware(
			[]int{0}, 4<<30, fakeMaxAdmissible(map[int]uint64{0: 8 << 30})))
	})

	t.Run("exceeding the admissible budget is refused and names both figures", func(t *testing.T) {
		err := DeviceAggregateFitsHardware(
			[]int{0}, 9<<30, fakeMaxAdmissible(map[int]uint64{0: 8 << 30}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "9216 MB") // what one device must hold
		require.Contains(t, err.Error(), "8192 MB") // what the card has
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("exactly the budget is admitted", func(t *testing.T) {
		// The refusal test is >, not >=: an index that exactly fills the admissible
		// budget is not provably unusable, and this gate only refuses the provable
		// case -- one that no free-memory level could ever satisfy.
		require.NoError(t, DeviceAggregateFitsHardware(
			[]int{0}, 8<<30, fakeMaxAdmissible(map[int]uint64{0: 8 << 30})))
	})

	t.Run("one byte over is refused", func(t *testing.T) {
		require.Error(t, DeviceAggregateFitsHardware(
			[]int{0}, (8<<30)+1, fakeMaxAdmissible(map[int]uint64{0: 8 << 30})))
	})

	t.Run("the tightest participating device decides", func(t *testing.T) {
		// Heterogeneous devices: fitting the roomiest is not enough, because under
		// SINGLE_GPU/REPLICATED every device must hold the whole per-device share.
		err := DeviceAggregateFitsHardware(
			[]int{0, 1}, 12<<30, fakeMaxAdmissible(map[int]uint64{0: 24 << 30, 1: 8 << 30}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("simulation aliases are checked once, not N times", func(t *testing.T) {
		// gpu_multi_simulation resolves every logical rank onto physical 0. The
		// demand is per-device already, so the aliases must collapse rather than
		// multiply.
		require.NoError(t, DeviceAggregateFitsHardware(
			[]int{0, 0, 0, 0}, 4<<30, fakeMaxAdmissible(map[int]uint64{0: 8 << 30})))
	})

	t.Run("an unreadable device refuses rather than guesses", func(t *testing.T) {
		// Assuming it fits is exactly the failure this gate exists to prevent.
		err := DeviceAggregateFitsHardware(
			[]int{7}, 1<<30, fakeMaxAdmissible(map[int]uint64{0: 8 << 30}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot read the admissible VRAM")
	})

	t.Run("degenerate inputs are no-ops", func(t *testing.T) {
		fn := fakeMaxAdmissible(map[int]uint64{0: 1})
		require.NoError(t, DeviceAggregateFitsHardware([]int{0}, 0, fn), "nothing to admit")
		require.NoError(t, DeviceAggregateFitsHardware(nil, 1<<30, fn), "no devices")
		require.NoError(t, DeviceAggregateFitsHardware([]int{0}, 1<<30, nil), "no measurement source")
	})
}

func TestShardRank(t *testing.T) {
	require.Equal(t, 0, ShardRank("shard_0.bin"))
	require.Equal(t, 7, ShardRank("shard_7.bin"))
	require.Equal(t, 12, ShardRank("shard_12.bin"))
	require.Equal(t, -1, ShardRank("index.bin"), "not a shard")
	require.Equal(t, -1, ShardRank("shard_.bin"), "no rank")
	require.Equal(t, -1, ShardRank("shard_x.bin"), "non-numeric rank")
	require.Equal(t, -1, ShardRank("shard_-1.bin"), "negative rank")
	require.Equal(t, -1, ShardRank("shard_0.tar"), "wrong suffix")
}

// PeakDeviceBytes must reduce by PHYSICAL device, not by a max over component
// sizes. The reduction depends on the device list: distinct cards each hold one
// shard, but gpu_multi_simulation aliases every rank onto one card, which then
// holds all of them. A premature max under-states the aliased case by the shard
// count -- the same aliasing DeviceBuildBytes handles by accumulating.
func TestPeakDeviceBytes(t *testing.T) {
	t.Run("sharded across distinct cards charges each its own shard", func(t *testing.T) {
		got := PeakDeviceBytes([]int{0, 1}, []map[string]int64{
			{"shard_0.bin": 100, "shard_1.bin": 140},
		})
		require.Equal(t, int64(140), got, "the busiest card holds the biggest shard")
	})

	t.Run("SIMULATION aliases put every shard on one card", func(t *testing.T) {
		// The bug this function exists to prevent: a max would answer 140 here,
		// when physical device 0 really holds 100+140.
		got := PeakDeviceBytes([]int{0, 0}, []map[string]int64{
			{"shard_0.bin": 100, "shard_1.bin": 140},
		})
		require.Equal(t, int64(240), got, "one physical card holds both shards")
	})

	t.Run("partial aliasing groups by physical device", func(t *testing.T) {
		// ranks 0,2 -> card 0 (10+30=40); ranks 1,3 -> card 1 (20+50=70)
		got := PeakDeviceBytes([]int{0, 1, 0, 1}, []map[string]int64{
			{"shard_0.bin": 10, "shard_1.bin": 20, "shard_2.bin": 30, "shard_3.bin": 50},
		})
		require.Equal(t, int64(70), got)
	})

	t.Run("sub-indexes sum, because a query reads them all", func(t *testing.T) {
		got := PeakDeviceBytes([]int{0, 1}, []map[string]int64{
			{"shard_0.bin": 100, "shard_1.bin": 140},
			{"shard_0.bin": 100, "shard_1.bin": 140},
		})
		require.Equal(t, int64(280), got)
	})

	t.Run("index.bin is held in full by every participating device", func(t *testing.T) {
		got := PeakDeviceBytes([]int{0, 1}, []map[string]int64{{"index.bin": 500}})
		require.Equal(t, int64(500), got)
		// Aliased devices must not double-charge the same card for one component.
		got = PeakDeviceBytes([]int{0, 0, 0}, []map[string]int64{{"index.bin": 500}})
		require.Equal(t, int64(500), got, "one component, one card, charged once")
	})

	t.Run("a shard rank beyond the device list is charged, not dropped", func(t *testing.T) {
		// Dropping it would under-state demand, which admits an index that cannot
		// load; charging devices[0] over-states, which merely over-refuses.
		got := PeakDeviceBytes([]int{0}, []map[string]int64{
			{"shard_0.bin": 10, "shard_9.bin": 70},
		})
		require.Equal(t, int64(80), got)
	})

	t.Run("degenerate inputs", func(t *testing.T) {
		require.Zero(t, PeakDeviceBytes(nil, []map[string]int64{{"index.bin": 1}}))
		require.Zero(t, PeakDeviceBytes([]int{0}, nil))
		require.Zero(t, PeakDeviceBytes([]int{0}, []map[string]int64{{"index.bin": 0}}))
	})
}
