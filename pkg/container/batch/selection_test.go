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
	selection.InitWithSize(6)
	selection.AddMany([]uint64{1, 4, 5})
	bat.SetSelection(&selection)
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
	// Compacting without and with a row scratch gives the same rows; the
	// scratch holds the selected rows and is reused by the next call.
	var rows []int64
	for _, scratch := range []*[]int64{nil, &rows, &rows} {
		require.NoError(t, bat.CompactInto(compact, mp, scratch))
		require.False(t, compact.HasSelection())
		require.Equal(t, 3, compact.RowCount())
		require.Equal(t, []int64{10, 40, 50}, vector.MustFixedColNoTypeCheck[int64](compact.Vecs[0]))
		require.Equal(t, "b", compact.Vecs[1].GetStringAt(0))
		require.True(t, compact.Vecs[1].IsNull(1))
		require.Equal(t, "f", compact.Vecs[1].GetStringAt(2))
	}
	require.Equal(t, bat.SelectionRows(nil), rows)
	reused := &rows[0]
	require.NoError(t, bat.CompactInto(compact, mp, &rows))
	require.Same(t, reused, &rows[0], "the row scratch is reused")

	// Any explicit row count replaces the selection.
	bat.ClearSelection()
	require.Equal(t, 6, bat.RowCount())
	bat.SetSelection(&selection)
	bat.SetRowCount(6)
	require.False(t, bat.HasSelection())

	compact.Clean(mp)
	bat.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

// A batch's selection is owned or borrowed, like its vectors: OwnSelection
// fills the batch's own storage, a forwarded or viewed selection borrows the
// input's bitmap, and Clean releases only the batch's own storage.
func TestBatchSelectionOwnedOrBorrowed(t *testing.T) {
	producer := NewWithSize(0)
	own := producer.OwnSelection(6)
	require.Same(t, &producer.ownSel, own)
	require.Equal(t, int64(6), own.Len())
	require.Zero(t, own.Count())
	own.AddMany([]uint64{0, 3})
	producer.SetSelection(own)
	require.Same(t, &producer.ownSel, producer.Selection())
	require.Equal(t, 2, producer.RowCount())
	require.Equal(t, 6, producer.PhysicalRowCount())

	// A forwarder borrows: no copy, the same bitmap.
	forwarder := NewWithSize(0)
	forwarder.SetSelection(producer.Selection())
	require.Same(t, producer.Selection(), forwarder.Selection())
	require.NotSame(t, &forwarder.ownSel, forwarder.Selection())

	// A view borrows the parent's selection; a physical view has none.
	var view Batch
	producer.ViewInto(&view)
	require.Same(t, producer.Selection(), view.Selection())
	var physical Batch
	producer.PhysicalView(&physical)
	require.False(t, physical.HasSelection())

	// A narrower copies into its own storage and never touches the input's.
	narrower := NewWithSize(0)
	narrowed := narrower.OwnSelection(0)
	narrowed.InitWith(forwarder.Selection())
	narrowed.Remove(3)
	narrower.SetSelection(narrowed)
	require.Equal(t, []int64{0}, narrower.SelectionRows(nil))
	require.Equal(t, []int64{0, 3}, producer.SelectionRows(nil))

	// Re-initializing the own storage for the next batch drops the published
	// selection first.
	again := producer.OwnSelection(4)
	require.False(t, producer.HasSelection())
	require.Equal(t, 6, producer.RowCount(), "the physical row count comes back")
	require.Equal(t, int64(4), again.Len())

	// Cleaning a borrower leaves the lender's selection alone; cleaning the
	// owner releases its storage.
	mp := mpool.MustNewZero()
	forwarder.Clean(mp)
	require.Equal(t, int64(4), producer.ownSel.Len())
	producer.Clean(mp)
	require.False(t, producer.HasSelection())
	require.Zero(t, producer.ownSel.Len())
	narrower.Clean(mp)
}
