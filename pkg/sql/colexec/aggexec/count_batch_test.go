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

package aggexec

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCountBatchDenseState(t *testing.T) {
	for _, mode := range []string{"star", "column", "nullable", "constant", "null"} {
		t.Run(mode, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			typ := types.T_int64.ToType()
			var exec AggFuncExec
			if mode == "star" {
				exec = newCountStarExec(mp, AggIdOfCountStar, false, []types.Type{typ})
			} else {
				exec = newCountColumnExec(mp, AggIdOfCountColumn, false, []types.Type{typ})
			}
			t.Cleanup(exec.Free)
			count := 2*AggBatchSize + 1
			require.NoError(t, exec.GroupGrow(count))
			groups := []uint64{0, 1, AggBatchSize, AggBatchSize + 1, 1, AggBatchSize + 1, uint64(count)}
			for group := uint64(2); group <= 257; group++ {
				groups = append(groups, group)
			}
			// Revisit cached groups after the 255th local group, including a
			// NULL/skip in the fallback and another destination chunk.
			groups = append(groups, 0, 1, AggBatchSize+1, 1)
			rows := len(groups) + 2
			var vec *vector.Vector
			if mode == "null" {
				vec = vector.NewConstNull(typ, rows, mp)
			} else {
				vec = vector.NewVec(typ)
			}
			t.Cleanup(func() { vec.Free(mp) })
			if mode != "null" {
				require.NoError(t, vector.AppendFixedList(vec, make([]int64, rows), nil, mp))
				if mode == "nullable" {
					nulls.Add(vec.GetNulls(), 3, uint64(rows-2))
				}
				if mode == "constant" {
					vec.ToConst()
					vec.SetLength(rows)
				}
			}
			// Preserve existing int64 addition at the wrap boundary as well as
			// chunk/offset/skip handling. Expected counts do not use BatchFill.
			var state *aggExec
			switch typed := exec.(type) {
			case *countStarExec:
				state = &typed.aggExec
			case *countColumnExec:
				state = &typed.aggExec
			}
			chunkArr[int64](state.state[0].vecs[0])[0] = math.MaxInt64
			want := make([]int64, count)
			want[0] = math.MaxInt64
			for range 2 {
				require.NoError(t, exec.BatchFill(2, nil, []*vector.Vector{vec}))
				require.NoError(t, exec.BatchFill(2, groups, []*vector.Vector{vec}))
				for i, group := range groups {
					if group != 0 && (mode == "star" || !vec.IsNull(uint64(i+2))) {
						want[group-1]++
					}
				}
			}
			results, err := exec.Flush()
			for _, result := range results {
				t.Cleanup(func() { result.Free(mp) })
			}
			require.NoError(t, err)
			start := 0
			for _, result := range results {
				got := vector.MustFixedColNoTypeCheck[int64](result)
				require.Equal(t, want[start:start+len(got)], got)
				require.False(t, result.HasNull())
				start += len(got)
			}
			require.Equal(t, count, start)
		})
	}
}
