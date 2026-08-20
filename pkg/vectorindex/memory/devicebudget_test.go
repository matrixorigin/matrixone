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
