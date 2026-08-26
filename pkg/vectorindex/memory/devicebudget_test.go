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

// fakeBudget implements DeviceBudget for the CPU tests. A missing bound returns an
// error rather than zero: a gate that silently treated an absent ceiling as 0
// would refuse everything, and one that treated it as unlimited would admit
// everything -- both are worse than a loud failure.
type fakeBudget struct {
	maxAdm func(dev int) (uint64, error)
	rows   DeviceRowsFittingFunc
}

func (f fakeBudget) MaxAdmissible(dev int) (uint64, error) {
	if f.maxAdm == nil {
		return 0, errors.New("fakeBudget: MaxAdmissible not configured")
	}
	return f.maxAdm(dev)
}

func (f fakeBudget) RowsFitting(dev int, perRow uint64) (int64, uint64, error) {
	if f.rows == nil {
		return 0, 0, errors.New("fakeBudget: RowsFitting not configured")
	}
	return f.rows(dev, perRow)
}

// fakeRowsFitting stands in for cuvs.RowsFittingFreeMem. It reproduces the one
// detail that matters to the code under test: the result is CLAMPED to a minimum
// of 1 (helper.cpp rows_fitting_gpu_mem). A fake without that clamp lets a "did
// anything fit?" predicate pass in tests while being always-true against real
// hardware -- which is precisely the bug this fake now reproduces.
//
// The 60% here is an arbitrary stand-in, NOT the production fraction. That is per
// index (index_cost.hpp: 75% default, 65% for IVF-PQ) and is chosen by the caller,
// so pinning a number here would only test the fake.
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

// fakeTotalMem builds a DeviceTotalMemFunc over a per-device capacity map. This
// is the seam that lets the hardware rule be tested without a GPU; production
// passes cuvs.DeviceTotalMem, the only part that needs real hardware.
func fakeMaxAdmissible(total map[int]uint64) func(dev int) (uint64, error) {
	return func(dev int) (uint64, error) {
		t, ok := total[dev]
		if !ok {
			return 0, errors.New("no such device")
		}
		return t, nil
	}
}

// TestDeviceAggregateFitsHardware covers the CREATE-time refusal. Unlike
// DeviceAggregateFitsFree, which admits against a fraction of currently-FREE
// memory and so refuses situationally, this compares against the card's TOTAL
// capacity: a refusal here is permanent, which is what makes it safe to fail
// CREATE on rather than deferring to the first query.
func TestDeviceAggregateFitsHardware(t *testing.T) {
	t.Run("fits on the hardware", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsHardware([]int{0}, 4<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("exceeding the admissible budget is refused and names both figures", func(t *testing.T) {
		err := DeviceAggregateFitsHardware([]int{0}, 9<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "9216 MB") // what one device must hold
		require.Contains(t, err.Error(), "8192 MB") // what the card has
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("exactly the budget is admitted", func(t *testing.T) {
		// The refusal test is >, not >=: an index that exactly fills the admissible
		// budget is not provably unusable, and this gate only refuses the provable
		// case -- one that no free-memory level could ever satisfy.
		require.NoError(t, DeviceAggregateFitsHardware([]int{0}, 8<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("one byte over is refused", func(t *testing.T) {
		require.Error(t, DeviceAggregateFitsHardware([]int{0}, (8<<30)+1, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("the tightest participating device decides", func(t *testing.T) {
		// Heterogeneous devices: fitting the roomiest is not enough, because under
		// SINGLE_GPU/REPLICATED every device must hold the whole per-device share.
		err := DeviceAggregateFitsHardware([]int{0, 1}, 12<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 24 << 30, 1: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("simulation aliases are checked once, not N times", func(t *testing.T) {
		// gpu_multi_simulation resolves every logical rank onto physical 0. The
		// demand is per-device already, so the aliases must collapse rather than
		// multiply.
		require.NoError(t, DeviceAggregateFitsHardware([]int{0, 0, 0, 0}, 4<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("an unreadable device refuses rather than guesses", func(t *testing.T) {
		// Assuming it fits is exactly the failure this gate exists to prevent.
		err := DeviceAggregateFitsHardware([]int{7}, 1<<30, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot read the admissible VRAM")
	})

	t.Run("degenerate inputs are no-ops", func(t *testing.T) {
		fn := fakeMaxAdmissible(map[int]uint64{0: 1})
		require.NoError(t, DeviceAggregateFitsHardware([]int{0}, 0, fakeBudget{maxAdm: fn}), "nothing to admit")
		require.NoError(t, DeviceAggregateFitsHardware(nil, 1<<30, fakeBudget{maxAdm: fn}), "no devices")
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

// DeviceAggregateFitsFree is the situational twin of DeviceAggregateFitsHardware
// and must be fed the SAME quantity -- per-device device-resident bytes -- so the
// two gates can only disagree about timing, never about what an index costs. If
// this were sized from the whole tar again, CREATE would commit artifacts refused
// here at every free level.
func TestDeviceAggregateFitsFree(t *testing.T) {
	// 1000 free -> 600 admissible at the fake's stand-in fraction, clamp included:
	// a fake without that clamp models a contract the real rows_fitting_gpu_mem
	// does not have, which is how an always-true predicate shipped here once
	// before. Production passes cuvs.BudgetFor, which carries the index's own
	// fraction; what is under test here is the comparison, not the fraction.
	fn := fakeRowsFitting(1000)

	t.Run("under the budget is admitted, over is refused", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsFree([]int{0}, 600, 1, 1, fakeBudget{rows: fn}))
		require.Error(t, DeviceAggregateFitsFree([]int{0}, 601, 1, 1, fakeBudget{rows: fn}))
	})

	t.Run("the refusal is situational, naming free rather than the card", func(t *testing.T) {
		err := DeviceAggregateFitsFree([]int{0}, 900, 1, 1, fakeBudget{rows: fn})
		require.Error(t, err)
		require.Contains(t, err.Error(), "right now")
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("the busiest device decides", func(t *testing.T) {
		perDev := func(free map[int]uint64) DeviceRowsFittingFunc {
			return func(dev int, perRow uint64) (int64, uint64, error) {
				f, ok := free[dev]
				if !ok {
					return 0, 0, errors.New("no such device")
				}
				rows := int64(f / 10 * 6 / perRow)
				if rows < 1 {
					rows = 1
				}
				return rows, f, nil
			}
		}
		err := DeviceAggregateFitsFree([]int{0, 1}, 700, 2, 2, fakeBudget{rows: perDev(map[int]uint64{0: 100000, 1: 1000})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("an unmeasurable device refuses rather than guesses", func(t *testing.T) {
		boom := func(int, uint64) (int64, uint64, error) { return 0, 0, errors.New("unreadable") }
		err := DeviceAggregateFitsFree([]int{0}, 10, 1, 1, fakeBudget{rows: boom})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot measure device")
	})

	t.Run("degenerate inputs are no-ops", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsFree([]int{0}, 0, 1, 1, fakeBudget{rows: fn}))
		require.NoError(t, DeviceAggregateFitsFree(nil, 600, 1, 1, fakeBudget{rows: fn}))
		require.NoError(t, DeviceAggregateFitsFree([]int{0}, 600, 1, 1, nil))
	})
}

// The loader refuses as soon as the RUNNING total is over budget, without
// downloading the sub-indexes it has not measured. Two things have to hold for
// that to be sound, and both are asserted here.
func TestDeviceAggregateFitsFreePartial(t *testing.T) {
	fn := fakeRowsFitting(1000) // budget 600

	t.Run("a partial refusal does not present its figure as the whole index", func(t *testing.T) {
		// Sizing a fix from "needs 700 MB" when only 2 of 5 sub-indexes were
		// measured sends the operator after a target that is too small.
		err := DeviceAggregateFitsFree([]int{0}, 700, 2, 5, fakeBudget{rows: fn})
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least")
		require.Contains(t, err.Error(), "2 of 5 sub-indexes measured")

		// The complete aggregate states the figure plainly, with no hedge.
		err = DeviceAggregateFitsFree([]int{0}, 700, 5, 5, fakeBudget{rows: fn})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "at least")
		require.NotContains(t, err.Error(), "sub-indexes measured")
	})

	t.Run("a partial total under budget is still admitted", func(t *testing.T) {
		// Fail-fast must not become fail-early: the loop has to keep going while
		// the running total still fits, or a large index could never load.
		require.NoError(t, DeviceAggregateFitsFree([]int{0}, 300, 1, 5, fakeBudget{rows: fn}))
	})

	t.Run("PeakDeviceBytes is monotone, which is what makes early refusal sound", func(t *testing.T) {
		// If adding a sub-index could LOWER the peak, a running total over budget
		// would not imply the finished one is, and the loader would refuse indexes
		// that fit. Checked across both attribution shapes: whole-index components
		// charged to every device, and shards charged to one each.
		devices := []int{0, 1}
		subs := []map[string]int64{
			{"index.bin": 100},
			{"shard_0.bin": 40, "shard_1.bin": 90},
			{"index.bin": 7},
			{"shard_0.bin": 300, "shard_1.bin": 1},
		}
		prev := int64(0)
		for i := 1; i <= len(subs); i++ {
			cur := PeakDeviceBytes(devices, subs[:i])
			require.GreaterOrEqual(t, cur, prev,
				"peak fell when sub-index %d was added: %d -> %d", i-1, prev, cur)
			prev = cur
		}

		// Same with the aliased device list gpu_multi_simulation produces, where
		// every shard lands back on the one physical card.
		prev = 0
		for i := 1; i <= len(subs); i++ {
			cur := PeakDeviceBytes([]int{0, 0}, subs[:i])
			require.GreaterOrEqual(t, cur, prev)
			prev = cur
		}
	})
}

// The refusal text is what an operator sizes a fix from, so a non-zero demand
// must never print as "0 MB". It did: n>>20 on a sub-megabyte figure produced
// "needs 0 MB but only 0 MB may be claimed (0 MB free)", which reads as a broken
// gate rather than a statement about the index.
func TestRefusalNeverPrintsZeroForNonZeroBytes(t *testing.T) {
	require.Equal(t, "0 bytes", mib(0))
	require.Equal(t, "1 bytes", mib(1))
	require.Equal(t, "1048575 bytes", mib(1<<20-1), "one byte short of a MB is still not 0 MB")
	require.Equal(t, "1 MB", mib(1<<20))
	require.Equal(t, "2048 MB", mib(2<<30))

	// End to end through the gate a small index actually hits.
	err := DeviceAggregateFitsFree([]int{0}, 900, 1, 1, fakeBudget{rows: fakeRowsFitting(1000)})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "0 MB",
		"a sub-megabyte refusal must not report zeros")
	require.Contains(t, err.Error(), "900 bytes")

	err = DeviceAggregateFitsHardware([]int{0}, 900,
		fakeBudget{maxAdm: func(int) (uint64, error) { return 600, nil }})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "0 MB")
	require.Contains(t, err.Error(), "900 bytes")
}

// A SINGLE_GPU index occupies devices[0] alone, so the cards it never touches
// must not be able to refuse it. The counterexample is small on purpose: cards
// [0,1] with ceilings {0:1000, 1:100} and a 500-byte demand. The native loader
// would only ever touch device 0, where it fits.
//
// This matters most on the HARDWARE gate, which is permanent: before the fix a
// smaller second card rejected such a build for good, and the operator's only
// escape was to shrink an index that already fit the card it would run on.
func TestDeviceParticipants_SingleGpuIgnoresBystanderCards(t *testing.T) {
	devices := []int{0, 1}
	ceilings := map[int]uint64{0: 1000, 1: 100}
	budget := fakeBudget{
		maxAdm: func(dev int) (uint64, error) { return ceilings[dev], nil },
		rows: func(dev int, perRow uint64) (int64, uint64, error) {
			return int64(ceilings[dev]), ceilings[dev], nil
		},
	}

	single := DeviceParticipants(devices, true)
	require.Equal(t, []int{0}, single, "SINGLE_GPU runs on devices[0] alone")

	require.NoError(t, DeviceAggregateFitsHardware(single, 500, budget),
		"device 1 holds none of a SINGLE_GPU index and must not veto it")
	require.NoError(t, DeviceAggregateFitsFree(single, 500, 1, 1, budget),
		"the situational gate must narrow the same way")

	// The narrowing must not become a way to smuggle an oversized index past the
	// gate: device 0's own ceiling still binds.
	require.Error(t, DeviceAggregateFitsHardware(single, 1001, budget),
		"the participating device's own ceiling must still refuse")

	// REPLICATED really does put the whole index on every card, and SHARDED
	// spreads ranks across them, so neither narrows -- device 1 must still refuse.
	both := DeviceParticipants(devices, false)
	require.Equal(t, devices, both)
	require.Error(t, DeviceAggregateFitsHardware(both, 500, budget),
		"a non-SINGLE index does occupy device 1, which cannot hold it")
}

// PeakDeviceBytes attributes a non-shard component to every device it is given,
// so it has to be fed the same narrowed list as the gate. Feeding it the full
// list is what turned a bystander card into a veto.
func TestPeakDeviceBytes_NarrowedListDropsBystanders(t *testing.T) {
	comps := []map[string]int64{{"index.bin": 500}}
	require.Equal(t, int64(500), PeakDeviceBytes([]int{0, 1}, comps),
		"a whole-index component is charged to each device given")
	require.Equal(t, int64(500), PeakDeviceBytes(DeviceParticipants([]int{0, 1}, true), comps),
		"narrowing changes who is charged, not how much the one card holds")
}
