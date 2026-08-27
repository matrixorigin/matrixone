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
// uniform is the shape these gates used to take: one demand applied to every
// device. They now take per-device demand, and "the same on each" is still a
// legitimate input -- it is what REPLICATED and SINGLE_GPU produce -- so the
// cases below keep their original meaning.
func uniform(devices []int, n int64) map[int]int64 {
	d := make(map[int]int64, len(devices))
	for _, dev := range DeviceDistinct(devices) {
		d[dev] = n
	}
	return d
}

func TestDeviceAggregateFitsHardware(t *testing.T) {
	t.Run("fits on the hardware", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsHardware(uniform([]int{0}, 4<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("exceeding the admissible budget is refused and names both figures", func(t *testing.T) {
		err := DeviceAggregateFitsHardware(uniform([]int{0}, 9<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "9216 MB") // what one device must hold
		require.Contains(t, err.Error(), "8192 MB") // what the card has
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("exactly the budget is admitted", func(t *testing.T) {
		// The refusal test is >, not >=: an index that exactly fills the admissible
		// budget is not provably unusable, and this gate only refuses the provable
		// case -- one that no free-memory level could ever satisfy.
		require.NoError(t, DeviceAggregateFitsHardware(uniform([]int{0}, 8<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("one byte over is refused", func(t *testing.T) {
		require.Error(t, DeviceAggregateFitsHardware(uniform([]int{0}, (8<<30)+1), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("the tightest participating device decides", func(t *testing.T) {
		// Heterogeneous devices: fitting the roomiest is not enough, because under
		// SINGLE_GPU/REPLICATED every device must hold the whole per-device share.
		err := DeviceAggregateFitsHardware(uniform([]int{0, 1}, 12<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 24 << 30, 1: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("simulation aliases are checked once, not N times", func(t *testing.T) {
		// gpu_multi_simulation resolves every logical rank onto physical 0. The
		// demand is per-device already, so the aliases must collapse rather than
		// multiply.
		require.NoError(t, DeviceAggregateFitsHardware(uniform([]int{0, 0, 0, 0}, 4<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})}))
	})

	t.Run("an unreadable device refuses rather than guesses", func(t *testing.T) {
		// Assuming it fits is exactly the failure this gate exists to prevent.
		err := DeviceAggregateFitsHardware(uniform([]int{7}, 1<<30), 1, true, fakeBudget{maxAdm: fakeMaxAdmissible(map[int]uint64{0: 8 << 30})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot read the admissible VRAM")
	})

	t.Run("degenerate inputs are no-ops", func(t *testing.T) {
		fn := fakeMaxAdmissible(map[int]uint64{0: 1})
		require.NoError(t, DeviceAggregateFitsHardware(uniform([]int{0}, 0), 1, true, fakeBudget{maxAdm: fn}), "nothing to admit")
		require.NoError(t, DeviceAggregateFitsHardware(uniform(nil, 1<<30), 1, true, fakeBudget{maxAdm: fn}), "no devices")
		require.NoError(t, DeviceAggregateFitsHardware(uniform([]int{0}, 1<<30), 1, true, nil), "no measurement source")
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
		require.NoError(t, DeviceAggregateFitsFree(uniform([]int{0}, 600), 1, 1, fakeBudget{rows: fn}))
		require.Error(t, DeviceAggregateFitsFree(uniform([]int{0}, 601), 1, 1, fakeBudget{rows: fn}))
	})

	t.Run("the refusal is situational, naming free rather than the card", func(t *testing.T) {
		err := DeviceAggregateFitsFree(uniform([]int{0}, 900), 1, 1, fakeBudget{rows: fn})
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
		err := DeviceAggregateFitsFree(uniform([]int{0, 1}, 700), 2, 2, fakeBudget{rows: perDev(map[int]uint64{0: 100000, 1: 1000})})
		require.Error(t, err)
		require.Contains(t, err.Error(), "device 1")
	})

	t.Run("an unmeasurable device refuses rather than guesses", func(t *testing.T) {
		boom := func(int, uint64) (int64, uint64, error) { return 0, 0, errors.New("unreadable") }
		err := DeviceAggregateFitsFree(uniform([]int{0}, 10), 1, 1, fakeBudget{rows: boom})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot measure device")
	})

	t.Run("degenerate inputs are no-ops", func(t *testing.T) {
		require.NoError(t, DeviceAggregateFitsFree(uniform([]int{0}, 0), 1, 1, fakeBudget{rows: fn}))
		require.NoError(t, DeviceAggregateFitsFree(uniform(nil, 600), 1, 1, fakeBudget{rows: fn}))
		require.NoError(t, DeviceAggregateFitsFree(uniform([]int{0}, 600), 1, 1, nil))
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
		err := DeviceAggregateFitsFree(uniform([]int{0}, 700), 2, 5, fakeBudget{rows: fn})
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least")
		require.Contains(t, err.Error(), "2 of 5 sub-indexes measured")

		// The complete aggregate states the figure plainly, with no hedge.
		err = DeviceAggregateFitsFree(uniform([]int{0}, 700), 5, 5, fakeBudget{rows: fn})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "at least")
		require.NotContains(t, err.Error(), "sub-indexes measured")
	})

	t.Run("a partial total under budget is still admitted", func(t *testing.T) {
		// Fail-fast must not become fail-early: the loop has to keep going while
		// the running total still fits, or a large index could never load.
		require.NoError(t, DeviceAggregateFitsFree(uniform([]int{0}, 300), 1, 5, fakeBudget{rows: fn}))
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
	err := DeviceAggregateFitsFree(uniform([]int{0}, 900), 1, 1, fakeBudget{rows: fakeRowsFitting(1000)})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "0 MB",
		"a sub-megabyte refusal must not report zeros")
	require.Contains(t, err.Error(), "900 bytes")

	err = DeviceAggregateFitsHardware(uniform([]int{0}, 900), 1, true,
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

	require.NoError(t, DeviceAggregateFitsHardware(uniform(single, 500), 1, true, budget),
		"device 1 holds none of a SINGLE_GPU index and must not veto it")
	require.NoError(t, DeviceAggregateFitsFree(uniform(single, 500), 1, 1, budget),
		"the situational gate must narrow the same way")

	// The narrowing must not become a way to smuggle an oversized index past the
	// gate: device 0's own ceiling still binds.
	require.Error(t, DeviceAggregateFitsHardware(uniform(single, 1001), 1, true, budget),
		"the participating device's own ceiling must still refuse")

	// REPLICATED really does put the whole index on every card, and SHARDED
	// spreads ranks across them, so neither narrows -- device 1 must still refuse.
	both := DeviceParticipants(devices, false)
	require.Equal(t, devices, both)
	require.Error(t, DeviceAggregateFitsHardware(uniform(both, 500), 1, true, budget),
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

// Each device is judged on ITS OWN share. The old reduction collapsed demand to
// the busiest card's number and tested that against every ceiling, which refuses
// a card for bytes a different card holds.
//
// Two cards, uneven shards and uneven ceilings: shard_0 is 900 on a 1000 card,
// shard_1 is 100 on a 200 card. Both fit what they were given. Testing 900 (the
// peak) against device 1's 200 refuses an index that loads fine.
func TestDeviceAggregate_JudgesEachDeviceOnItsOwnShare(t *testing.T) {
	devices := []int{0, 1}
	comps := []map[string]int64{{"shard_0.bin": 900, "shard_1.bin": 100}}
	ceilings := map[int]uint64{0: 1000, 1: 200}
	budget := fakeBudget{
		maxAdm: func(dev int) (uint64, error) { return ceilings[dev], nil },
		rows: func(dev int, _ uint64) (int64, uint64, error) {
			return int64(ceilings[dev]), ceilings[dev], nil
		},
	}

	demand := PerDeviceDemand(devices, comps)
	require.Equal(t, map[int]int64{0: 900, 1: 100}, demand, "each shard lands on its own rank")
	require.Equal(t, int64(900), PeakDeviceBytes(devices, comps), "the peak is still the biggest shard")

	require.NoError(t, DeviceAggregateFitsHardware(demand, 1, true, budget),
		"each card holds its own shard, so nothing should be refused")
	require.NoError(t, DeviceAggregateFitsFree(demand, 1, 1, budget))

	// The peak applied to every card is what used to happen, and it refuses.
	require.Error(t, DeviceAggregateFitsHardware(uniform(devices, 900), 1, true, budget),
		"guards the regression: the peak against every ceiling refuses device 1")

	// A card that genuinely cannot hold its own shard is still refused, and the
	// refusal names that card and ITS demand -- not the peak.
	tight := fakeBudget{maxAdm: func(dev int) (uint64, error) {
		if dev == 1 {
			return 50, nil // less than shard_1's 100
		}
		return 1000, nil
	}}
	err := DeviceAggregateFitsHardware(demand, 1, true, tight)
	require.Error(t, err)
	require.Contains(t, err.Error(), "device 1")
	require.Contains(t, err.Error(), "100 bytes", "must quote device 1's own share, not the 900 peak")
}

// The build checks its RUNNING aggregate after each sub-index is packed, so an
// index that can never be queried is refused after a handful of sub-indexes
// rather than after all of them. PerDeviceDemand is monotone -- a sub-index only
// adds bytes to a device -- so a running total already over the ceiling
// guarantees the finished one is.
func TestDeviceAggregateFitsHardware_PartialAggregateRefusesEarly(t *testing.T) {
	budget := fakeBudget{maxAdm: func(int) (uint64, error) { return 10 << 30, nil }}

	// Four sub-indexes of 3 GiB each land on one card: 12 GiB against a 10 GiB
	// ceiling. The running total crosses it at the fourth.
	sub := []map[string]int64{}
	for i := 0; i < 4; i++ {
		sub = append(sub, map[string]int64{"index.bin": 3 << 30})
		demand := PerDeviceDemand([]int{0}, sub)
		err := DeviceAggregateFitsHardware(demand, len(sub), false, budget)
		if len(sub) < 4 {
			require.NoError(t, err, "%d sub-indexes are still under the ceiling", len(sub))
			continue
		}
		require.Error(t, err, "the running total must be refused as soon as it is over")
		// A partial refusal must not present its figure as the whole index: the
		// operator would size a fix from a number that is still rising.
		require.Contains(t, err.Error(), "at least")
		require.Contains(t, err.Error(), "sub-index(es)")
	}

	// The completed check words it as final, with no "at least".
	full := PerDeviceDemand([]int{0}, sub)
	err := DeviceAggregateFitsHardware(full, len(sub), true, budget)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "at least")
}

// SHARDED is always ONE sub-index -- planCapacity refuses to combine it with a
// split, since each sub-index would be packed as a sharded index and could not
// be reloaded. So the incremental check never fires for it: there is no
// rotation, and the completed check at the end is the only one that runs.
//
// What still has to hold is the attribution, in both device layouts.
func TestDeviceAggregate_ShardedLayouts(t *testing.T) {
	// One sharded index, four shards of 3 GiB.
	comps := []map[string]int64{{
		"shard_0.bin": 3 << 30, "shard_1.bin": 3 << 30,
		"shard_2.bin": 3 << 30, "shard_3.bin": 3 << 30,
	}}
	budget := fakeBudget{maxAdm: func(int) (uint64, error) { return 10 << 30, nil }}

	t.Run("distinct cards each hold one shard", func(t *testing.T) {
		demand := PerDeviceDemand([]int{0, 1, 2, 3}, comps)
		require.Equal(t, map[int]int64{0: 3 << 30, 1: 3 << 30, 2: 3 << 30, 3: 3 << 30}, demand,
			"rank i lands on devices[i]")
		require.NoError(t, DeviceAggregateFitsHardware(demand, 1, true, budget),
			"3 GiB per card is well under the 10 GiB ceiling")
	})

	t.Run("gpu_multi_simulation aliases every rank onto one card", func(t *testing.T) {
		// [0,0,0,0] is one PHYSICAL card pretending to be four. It therefore holds
		// ALL the shards, and must be judged on their sum -- 12 GiB, over the
		// ceiling. Attributing per rank here would admit an index that only fits
		// because the simulation claims cards that do not exist.
		demand := PerDeviceDemand([]int{0, 0, 0, 0}, comps)
		require.Equal(t, map[int]int64{0: 12 << 30}, demand, "aliased ranks accumulate")

		err := DeviceAggregateFitsHardware(demand, 1, true, budget)
		require.Error(t, err, "one card cannot hold what four shards need")
		require.Contains(t, err.Error(), "device 0")
	})

	t.Run("one sub-index makes the partial and complete checks agree", func(t *testing.T) {
		// Nothing rotates, so the running aggregate IS the final one. The only
		// difference is the wording, which must not claim the figure is still
		// rising when it cannot.
		demand := PerDeviceDemand([]int{0, 0, 0, 0}, comps)
		partial := DeviceAggregateFitsHardware(demand, 1, false, budget)
		complete := DeviceAggregateFitsHardware(demand, 1, true, budget)
		require.Error(t, partial)
		require.Error(t, complete)
		require.Contains(t, partial.Error(), "at least")
		require.NotContains(t, complete.Error(), "at least")
	})
}
