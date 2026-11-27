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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// Allocated counts the pool memory an owned null bitmap holds: CleanOnlyData
// keeps it for reuse, so size-limited buffers must see it.
func TestAllocatedCountsOwnedNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	v := NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, v.PreExtendNulls(4096, mp))
	require.GreaterOrEqual(t, v.Allocated(), 4096/8)
	v.CleanOnlyData()
	require.GreaterOrEqual(t, v.Allocated(), 4096/8, "the kept bitmap still counts")

	// A window references its source's bitmap and owns none of it.
	src := NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 8; i++ {
		require.NoError(t, AppendFixed(src, int64(i), i%3 == 0, mp))
	}
	w, err := src.Window(1, 8)
	require.NoError(t, err)
	require.Equal(t, 0, w.Allocated()-cap(w.data)-cap(w.area))
	v.Free(mp)
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}

// A window starting at a row that is not 64-aligned has a shifted null
// bitmap, which C cannot read in place: FillRawPtrLen hands over a clean copy.
func TestFillRawPtrLenShiftedWindowNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	src := NewVec(types.T_float32.ToType(), mp)
	for i := 0; i < 200; i++ {
		require.NoError(t, AppendFixed(src, float32(i), i%7 == 0, mp))
	}
	w, err := src.Window(3, 150)
	require.NoError(t, err)

	var dest [6]uintptr
	keep := w.FillRawPtrLen(dest[:])
	require.Len(t, keep, (147+63)/64)
	require.NotZero(t, dest[0])
	require.Equal(t, uintptr(147), dest[1])
	for row := 0; row < 147; row++ {
		got := keep[row/64]&(uint64(1)<<(row%64)) != 0
		require.Equal(t, (row+3)%7 == 0, got, "row %d", row)
	}

	// An owned bitmap is passed in place.
	keep = src.FillRawPtrLen(dest[:])
	require.Nil(t, keep)
	require.NotZero(t, dest[0])
	require.Equal(t, uintptr(src.GetNulls().Len()), dest[1])
	src.Free(mp)
	require.Zero(t, mp.CurrNB())
}
