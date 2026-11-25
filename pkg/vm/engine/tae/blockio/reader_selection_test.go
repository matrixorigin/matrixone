// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// The kept rows arrive as a row list or as a mask; both shrink the same way.
func TestPublishReaderSelectionShrinksInPlace(t *testing.T) {
	for _, tc := range []struct {
		rows   []int64
		asMask bool
	}{{[]int64{1, 3}, false}, {nil, false}, {[]int64{1, 3}, true}, {nil, true}} {
		rows := tc.rows
		mp := mpool.MustNewZero()
		output := batch.NewWithSize(3)
		for i := range output.Vecs {
			output.Vecs[i] = vector.NewVec(types.T_int32.ToType(), mp)
			require.NoError(t, vector.AppendFixedList(output.Vecs[i], []int32{10, 20, 30, 40}, nil, mp))
		}
		output.SetRowCount(4)
		owned, unused := output.Vecs[0], output.Vecs[2]
		request := &engine.ReaderFilterRequest{ColumnUses: []engine.ReaderColumnUse{engine.ReaderColumnOwned, engine.ReaderColumnOwned, engine.ReaderColumnUnused}}
		res := engine.ReaderFilterResult{Sels: rows}
		var mask bitmap.Bitmap
		if tc.asMask {
			require.NoError(t, mask.InitWithRows(4, rows, mp))
			res = engine.ReaderFilterResult{Mask: &mask}
		}
		require.NoError(t, publishReaderSelection(output, []int{0, 2}, res, request, mp))
		require.Equal(t, len(rows), output.RowCount())
		require.Same(t, owned, output.Vecs[0])
		require.Same(t, unused, output.Vecs[2])
		require.Zero(t, unused.Length())
		if len(rows) > 0 {
			require.Equal(t, []int32{20, 40}, vector.MustFixedColNoTypeCheck[int32](owned))
		} else {
			require.Zero(t, owned.Length())
		}
		require.Equal(t, 4, output.Vecs[1].Length(), "columns outside positions are untouched")
		output.Clean(mp)
		mask.Free()
		require.Zero(t, mp.CurrNB())
	}
}

// maskResult is the filter result that keeps rows of an input of n rows, as
// a mask; the bitmap is freed when the test ends.
func maskResult(t testing.TB, rows []int64, n int) engine.ReaderFilterResult {
	mp := mpool.MustNewZero()
	mask := new(bitmap.Bitmap)
	require.NoError(t, mask.InitWithRows(int64(n), rows, mp))
	t.Cleanup(func() {
		mask.Free()
		mpool.DeleteMPool(mp)
	})
	return engine.ReaderFilterResult{Mask: mask}
}

// A mask result must cover exactly the input rows, keep the reported count,
// and not come with a list.
func TestValidateReaderFilterResultMask(t *testing.T) {
	ok := maskResult(t, []int64{1, 3}, 5)
	require.NoError(t, validateReaderFilterResult(ok, 5, 2))
	require.Error(t, validateReaderFilterResult(ok, 6, 2), "length differs from the input rows")
	require.Error(t, validateReaderFilterResult(ok, 5, 3), "count differs from the output rows")
	both := ok
	both.Sels = []int64{1, 3}
	require.Error(t, validateReaderFilterResult(both, 5, 2))
	require.Equal(t, 2, ok.Count(5))
	require.Equal(t, []int64{1, 3}, ok.RowList(nil))
	var request engine.ReaderFilterRequest
	require.Equal(t, []int64{1, 3}, request.RowList(ok))
	list := engine.ReaderFilterResult{Sels: []int64{4}}
	require.Equal(t, []int64{4}, request.RowList(list))
	require.Equal(t, 1, list.Count(5))
	require.Equal(t, 5, engine.ReaderFilterResult{All: true}.Count(5))
}
