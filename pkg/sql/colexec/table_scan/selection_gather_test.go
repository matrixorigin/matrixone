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

package table_scan

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// compactInto gathers a selection of several columns through one decoded row
// list, and of one column by mask; both match Union over the selected rows.
func TestCompactIntoGathersThroughRowList(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(3))
	const n = 300
	ints := vector.NewVec(types.T_int64.ToType(), mp)
	strs := vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < n; i++ {
		require.NoError(t, vector.AppendFixed(ints, int64(i), i%7 == 0, mp))
		s := fmt.Sprintf("s%d", i)
		if i%3 == 0 {
			s = strings.Repeat(s, 12)
		}
		require.NoError(t, vector.AppendBytes(strs, []byte(s), i%11 == 0, mp))
	}
	empty := vector.NewVec(types.T_int32.ToType(), mp)
	sel := new(bitmap.Bitmap)
	require.NoError(t, sel.InitWithSize(n, mp))
	var rows []int64
	for i := 0; i < n; i++ {
		if rng.Intn(10) < 4 {
			sel.Add(uint64(i))
			rows = append(rows, int64(i))
		}
	}

	check := func(vecs []*vector.Vector) {
		out := batch.NewWithSize(len(vecs))
		copy(out.Vecs, vecs)
		out.SetRowCount(n)
		out.SetSelection(n, sel)
		var s selectionState
		compact, err := s.compactInto(out, mp)
		require.NoError(t, err)
		require.Equal(t, len(rows), compact.RowCount())
		for i, vec := range vecs {
			switch {
			case vec == nil:
				require.Nil(t, compact.Vecs[i])
			case vec.Length() == 0:
				require.Zero(t, compact.Vecs[i].Length())
			default:
				want := vector.NewVec(*vec.GetType(), mp)
				require.NoError(t, want.Union(vec, rows, mp))
				require.Equal(t, want.Length(), compact.Vecs[i].Length())
				for r := 0; r < want.Length(); r++ {
					require.Equal(t, want.IsNull(uint64(r)), compact.Vecs[i].IsNull(uint64(r)))
					if !want.IsNull(uint64(r)) {
						require.Equal(t, want.GetRawBytesAt(r), compact.Vecs[i].GetRawBytesAt(r), "col %d row %d", i, r)
					}
				}
				want.Free(mp)
			}
		}
		s.free(mp)
	}
	check([]*vector.Vector{ints, strs, nil, empty}) // row list
	check([]*vector.Vector{strs})                   // by mask

	ints.Free(mp)
	strs.Free(mp)
	empty.Free(mp)
	sel.Free()
	require.Zero(t, mp.CurrNB())
}

func TestSelectionGatherRows(t *testing.T) {
	mp := mpool.MustNewZero()
	var s selectionState
	sel := new(bitmap.Bitmap)
	require.NoError(t, sel.InitWithSize(200, mp))
	for _, r := range []uint64{1, 64, 130, 199} {
		sel.Add(r)
	}
	require.Equal(t, []int64{1, 64, 130, 199}, s.gatherRows(sel, 200))
	// A row past the source: the caller gathers by mask.
	require.Nil(t, s.gatherRows(sel, 150))
	// Bits past the source that are clear are fine.
	sel.Remove(199)
	require.Equal(t, []int64{1, 64, 130}, s.gatherRows(sel, 150))
	s.free(mp)
	sel.Free()
	require.Zero(t, mp.CurrNB())
}
