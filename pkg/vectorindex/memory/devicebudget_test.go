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

// rowsFittingFrom builds a DeviceRowsFittingFunc over a per-device free-bytes
// map. This is the seam that lets the sizing POLICY be tested without a GPU:
// production passes cuvs.RowsFittingFreeMem, which is the only part that needs
// real hardware.
func rowsFittingFrom(free map[int]uint64) DeviceRowsFittingFunc {
	return func(dev int, perRowBytes uint64) (int64, uint64, error) {
		f, ok := free[dev]
		if !ok {
			return 0, 0, errors.New("no such device")
		}
		if perRowBytes == 0 {
			return 0, f, errors.New("per-row size is 0")
		}
		return int64(f / perRowBytes), f, nil
	}
}

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

// TestDeviceMinRowsFittingHeterogeneous is the counterexample the fix exists
// for: SHARDED cuts EQUAL shards, so sizing from devices[0] on a mixed pair
// hands the small card a shard built for the big one.
func TestDeviceMinRowsFittingHeterogeneous(t *testing.T) {
	const perRow = 1 << 20 // 1 MiB/row, so rows == GiB free
	free := map[int]uint64{
		0: 40 << 30, // 40 GiB -> 40960 rows
		1: 8 << 30,  //  8 GiB ->  8192 rows
	}

	t.Run("sizes from the smallest card regardless of order", func(t *testing.T) {
		for _, order := range [][]int{{0, 1}, {1, 0}} {
			rows, dev, freeBytes, err := DeviceMinRowsFitting(order, perRow, rowsFittingFrom(free))
			require.NoError(t, err)
			require.Equal(t, int64(8192), rows,
				"must size from the 8 GiB card; sampling devices[0]=%d would give the wrong answer", order[0])
			require.Equal(t, 1, dev, "must name the binding device")
			require.Equal(t, uint64(8<<30), freeBytes)
		}
	})

	t.Run("the pre-fix behaviour would have overcommitted", func(t *testing.T) {
		// What sampling only devices[0] used to yield, kept as the contrast:
		// 40960 rows per shard against a card that holds 8192.
		firstOnly, _, _, err := DeviceMinRowsFitting([]int{0}, perRow, rowsFittingFrom(free))
		require.NoError(t, err)
		require.Equal(t, int64(40960), firstOnly)
		require.Greater(t, firstOnly, int64(8192),
			"this is the overcommit: an equal shard sized here does not fit device 1")
	})

	t.Run("homogeneous is unaffected", func(t *testing.T) {
		rows, _, _, err := DeviceMinRowsFitting([]int{0, 1}, perRow,
			rowsFittingFrom(map[int]uint64{0: 16 << 30, 1: 16 << 30}))
		require.NoError(t, err)
		require.Equal(t, int64(16384), rows)
	})

	t.Run("simulation aliases query the card once", func(t *testing.T) {
		calls := 0
		counting := func(dev int, perRowBytes uint64) (int64, uint64, error) {
			calls++
			return rowsFittingFrom(free)(dev, perRowBytes)
		}
		rows, dev, _, err := DeviceMinRowsFitting([]int{0, 0, 0, 0}, perRow, counting)
		require.NoError(t, err)
		require.Equal(t, 1, calls, "gpu_multi_simulation must not query one card four times")
		require.Equal(t, 0, dev)
		require.Equal(t, int64(40960), rows)
	})

	t.Run("no devices is unmeasured, not zero capacity", func(t *testing.T) {
		rows, _, _, err := DeviceMinRowsFitting(nil, perRow, rowsFittingFrom(free))
		require.NoError(t, err, "an absent GPU reading must fall back, not fail")
		require.Zero(t, rows)
	})

	t.Run("a failing device fails the whole sizing", func(t *testing.T) {
		// Never guess: assuming the table fits is the failure being prevented.
		_, dev, _, err := DeviceMinRowsFitting([]int{0, 7}, perRow, rowsFittingFrom(free))
		require.Error(t, err)
		require.Equal(t, 7, dev, "the error must name the device that could not be read")
		require.Contains(t, err.Error(), "device 7")
	})

	t.Run("nil getter is rejected", func(t *testing.T) {
		_, _, _, err := DeviceMinRowsFitting([]int{0}, perRow, nil)
		require.Error(t, err)
	})
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

// TestDeviceBuildPeakBytes pins the governor policy: a build claims the peak of
// its non-overlapping phases. The training-dominant case is the regression --
// claiming resident-only there under-claims by exactly the gap a concurrent
// allocation can then take.
func TestDeviceBuildPeakBytes(t *testing.T) {
	// Resident-dominant: narrow vector, small train fraction.
	require.Equal(t, uint64(1000), DeviceBuildPeakBytes(1000, 200))
	// Training-dominant: wide vector, generous kmeans_train_percent, narrow PQ
	// codes. Claiming 1000 here is the defect; the build really peaks at 5000.
	require.Equal(t, uint64(5000), DeviceBuildPeakBytes(1000, 5000))
	// Equal phases.
	require.Equal(t, uint64(700), DeviceBuildPeakBytes(700, 700))
	// A missing trainset figure must not erase the resident claim.
	require.Equal(t, uint64(900), DeviceBuildPeakBytes(900, 0))
	// Nor the reverse: a build that streams everything still claims its trainset.
	require.Equal(t, uint64(400), DeviceBuildPeakBytes(0, 400))
	require.Zero(t, DeviceBuildPeakBytes(0, 0))
}

// TestDeviceBuildPeakBytesFlowsThroughDistribution checks the peak is attributed
// per device the same way the resident figure is: divided when sharded, charged
// in full to every device when replicated.
func TestDeviceBuildPeakBytesFlowsThroughDistribution(t *testing.T) {
	devices := []int{0, 1}
	peak := DeviceBuildPeakBytes(1000, 4000) // training dominates

	sharded := DeviceBuildBytes(vectorindex.DistributionMode_SHARDED, devices, peak)
	require.Equal(t, uint64(2000), sharded[0])
	require.Equal(t, uint64(2000), sharded[1])

	replicated := DeviceBuildBytes(vectorindex.DistributionMode_REPLICATED, devices, peak)
	require.Equal(t, uint64(4000), replicated[0])
	require.Equal(t, uint64(4000), replicated[1])
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
