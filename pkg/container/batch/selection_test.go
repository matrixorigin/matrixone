// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBatchSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := int64(0); i < 6; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], i*10, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte('a' + i)}, i == 4, mp))
	}
	bat.SetRowCount(6)
	require.False(t, bat.HasSelection())
	require.Nil(t, bat.SelectionMask(nil))

	var selection bitmap.Bitmap

	selection.Init(bitmapTestMP)
	mustBitmapOK(selection.InitWithSize(6, bitmapTestMP))
	selection.AddMany([]uint64{1, 4, 5})
	bat.SetSelection(6, &selection)
	require.Equal(t, []int64{1, 4, 5}, bat.SelectionRows(nil))
	require.True(t, bat.HasSelection())
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, 6, bat.PhysicalRowCount())
	require.Equal(t, []bool{false, true, false, false, true, true}, bat.SelectionMask(nil))

	var view Batch
	bat.PhysicalView(&view)
	require.False(t, view.HasSelection())
	require.Equal(t, 6, view.RowCount())

	// A selected batch never reaches a codec.
	var w bytes.Buffer
	require.Error(t, bat.MarshalBinaryTo(&w))
	_, err := bat.MarshalBinarySize()
	require.Error(t, err)

	compact := NewWithSize(2)
	compact.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	compact.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	// Compacting twice gives the same rows: the destination is reset and
	// refilled, and no row list is built.
	for range 2 {
		require.NoError(t, bat.CompactInto(compact, mp))
		require.False(t, compact.HasSelection())
		require.Equal(t, 3, compact.RowCount())
		require.Equal(t, []int64{10, 40, 50}, vector.MustFixedColNoTypeCheck[int64](compact.Vecs[0]))
		require.Equal(t, "b", compact.Vecs[1].GetStringAt(0))
		require.True(t, compact.Vecs[1].IsNull(1))
		require.Equal(t, "f", compact.Vecs[1].GetStringAt(2))
	}

	// Any explicit row count replaces the selection.
	bat.ClearSelection()
	require.Equal(t, 6, bat.RowCount())
	bat.SetSelection(6, &selection)
	bat.SetRowCount(6)
	require.False(t, bat.HasSelection())

	compact.Clean(mp)
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}
