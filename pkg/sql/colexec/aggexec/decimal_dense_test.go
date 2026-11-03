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
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestDecimalDenseGroupBoundary(t *testing.T) {
	for _, param := range []types.Type{
		types.New(types.T_decimal64, 15, 2),
		types.New(types.T_decimal128, 38, 2),
		types.New(types.T_decimal256, 50, 2),
	} {
		for _, count := range []int{4, 255, 256, 257} {
			for _, isSum := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/groups=%d/sum=%t", param, count, isSum), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					vec := vector.NewVec(param)
					t.Cleanup(func() { vec.Free(mp) })
					groups := make([]uint64, count*2+3)
					for row := 0; row <= len(groups); row++ {
						value := int64(row%7 - 3)
						var arg any
						switch param.Oid {
						case types.T_decimal64:
							arg = types.Decimal64(value)
						case types.T_decimal128:
							arg = types.Decimal128FromInt64(value)
						default:
							arg = types.Decimal256FromInt64(value)
						}
						require.NoError(t, vector.AppendAny(vec, arg, row%5 == 0, mp))
						if row < len(groups) && row%11 != 0 {
							groups[row] = uint64(row%count + 1)
						}
					}
					id := AggIdOfAvg
					if isSum {
						id = AggIdOfSum
					}
					scalar := makeSumAvgExec(mp, isSum, id, false, param)
					t.Cleanup(scalar.Free)
					batch := makeSumAvgExec(mp, isSum, id, false, param)
					t.Cleanup(batch.Free)
					require.NoError(t, scalar.GroupGrow(count))
					require.NoError(t, batch.GroupGrow(count))
					require.NoError(t, batch.BatchFill(1, groups, []*vector.Vector{vec}))
					for row, group := range groups {
						if group != GroupNotMatched {
							require.NoError(t, scalar.Fill(int(group-1), row+1, []*vector.Vector{vec}))
						}
					}
					var want, got bytes.Buffer
					require.NoError(t, scalar.SaveIntermediateResultOfChunk(0, &want))
					require.NoError(t, batch.SaveIntermediateResultOfChunk(0, &got))
					require.Equal(t, want.Bytes(), got.Bytes())
				})
			}
		}
	}
}
