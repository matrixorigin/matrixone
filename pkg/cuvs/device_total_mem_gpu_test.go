// Copyright 2026 Matrix Origin
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

//go:build gpu

package cuvs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// DeviceTotalMem backs the CREATE-time hardware gate, so its blast radius is every
// CREATE INDEX: DeviceAggregateFitsHardware turns any error from it into a build
// failure, and a silent (0, nil) would make `need > total` true for every index and
// refuse them all. It is new cgo, so it needs a test that actually calls it rather
// than one that only proves it compiles.
func TestDeviceTotalMem(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU")
	}

	t.Run("reports a plausible capacity", func(t *testing.T) {
		total, err := DeviceTotalMem(devices[0])
		require.NoError(t, err)
		require.Positive(t, total, "a zero total would refuse every CREATE")
		// Sanity-bound it rather than hard-coding this card: anything under 256 MB
		// is not a GPU we build indexes on, and 1 PB means we misread the value.
		require.Greater(t, total, uint64(256)<<20)
		require.Less(t, total, uint64(1)<<50)
	})

	t.Run("total is at least free", func(t *testing.T) {
		// The two come from the same cudaMemGetInfo call, so a total below free
		// would mean the out-params are transposed.
		_, free, err := RowsFittingFreeMem(devices[0], 1)
		require.NoError(t, err)
		total, err := DeviceTotalMem(devices[0])
		require.NoError(t, err)
		require.GreaterOrEqual(t, total, free, "total VRAM cannot be below free VRAM")
	})

	t.Run("an invalid device errors rather than reporting zero", func(t *testing.T) {
		// (0, nil) here would be the dangerous outcome: the gate would compare
		// against a zero capacity and refuse every build with a confusing message.
		_, err := DeviceTotalMem(9999)
		require.Error(t, err)
	})

	t.Run("leaves the caller's current device alone", func(t *testing.T) {
		// It binds the requested device to read cudaMemGetInfo; a stray current
		// device would leak into whatever the calling goroutine does next.
		before, err := DeviceTotalMem(devices[0])
		require.NoError(t, err)
		_, _ = DeviceTotalMem(9999) // failed rebind must still restore
		after, err := DeviceTotalMem(devices[0])
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}

// DeviceMaxAdmissible is the threshold the CREATE gate refuses on, so it must be
// the budget fraction of TOTAL -- not total, and not a fraction of free. If it
// drifted to total, indexes in the band above the budget would commit and then be
// refused by every query; if it tracked free, a refusal would be situational
// rather than permanent and CREATE would reject buildable indexes.
func TestDeviceMaxAdmissible(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU")
	}
	total, err := DeviceTotalMem(devices[0])
	require.NoError(t, err)
	maxAdm, err := DeviceMaxAdmissible(devices[0])
	require.NoError(t, err)

	require.Positive(t, maxAdm)
	require.Less(t, maxAdm, total, "the admissible bound must be strictly below total")
	// Pin it to the governor's fraction rather than a literal, so raising the
	// budget moves both the admission path and this gate together.
	require.Equal(t, total/4*3, maxAdm, "must be the 75%% budget fraction of TOTAL")

	// It must NOT track free memory: free moves, and a refusal derived from it
	// would be situational rather than permanent.
	_, free, err := RowsFittingFreeMem(devices[0], 1)
	require.NoError(t, err)
	require.NotEqual(t, free/4*3, maxAdm, "must be a fraction of total, not of free")
}
