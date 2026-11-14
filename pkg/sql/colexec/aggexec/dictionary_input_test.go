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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryDecimalAggregateReads(t *testing.T) {
	for _, param := range []types.Type{types.New(types.T_decimal64, 15, 2), types.New(types.T_decimal128, 38, 2), types.New(types.T_decimal256, 50, 2)} {
		for _, sum := range []bool{false, true} {
			for _, groupCount := range []int{4, 300} {
				t.Run(fmt.Sprintf("%s/sum=%t/groups=%d", param, sum, groupCount), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					source := vector.NewVec(param)
					t.Cleanup(func() { source.Free(mp) })
					for i, value := range []int64{-3, 2, 7, 0, 9, -4} {
						var raw any
						switch param.Oid {
						case types.T_decimal64:
							raw = types.Decimal64(value)
						case types.T_decimal128:
							raw = types.Decimal128FromInt64(value)
						default:
							raw = types.Decimal256FromInt64(value)
						}
						require.NoError(t, vector.AppendAny(source, raw, i == 3, mp))
					}
					mapping := []int64{5, 1, 5, 0, 3, 4, 2, 1, 0, 4, 2, 3}
					base, err := vector.FreezeVector(source, mp, nil)
					require.NoError(t, err)
					t.Cleanup(base.Release)
					rows, err := vector.NewRowSelection(mapping, source.Length(), mp, nil)
					require.NoError(t, err)
					t.Cleanup(rows.Release)
					selected, err := vector.NewDictionaryVector(base, rows, mp, nil)
					require.NoError(t, err)
					t.Cleanup(func() { selected.Free(mp) })
					id := AggIdOfAvg
					if sum {
						id = AggIdOfSum
					}
					want := makeSumAvgExec(mp, sum, id, false, param)
					t.Cleanup(want.Free)
					got := makeSumAvgExec(mp, sum, id, false, param)
					t.Cleanup(got.Free)
					require.NoError(t, want.GroupGrow(groupCount))
					require.NoError(t, got.GroupGrow(groupCount))
					groups := []uint64{1, 2, 3, 4, 0, 4, 1, 2, 3, 1}
					require.NoError(t, got.BatchFill(1, groups, []*vector.Vector{selected}))
					for i, group := range groups {
						if group != GroupNotMatched {
							require.NoError(t, want.Fill(int(group-1), int(mapping[i+1]), []*vector.Vector{source}))
						}
					}
					var expected, actual bytes.Buffer
					require.NoError(t, want.SaveIntermediateResultOfChunk(0, &expected))
					require.NoError(t, got.SaveIntermediateResultOfChunk(0, &actual))
					require.Equal(t, expected.Bytes(), actual.Bytes())
					// Bulk uses the same logical order, including repeated and NULL rows.
					want.Free()
					got.Free()
					require.NoError(t, want.GroupGrow(1))
					require.NoError(t, got.GroupGrow(1))
					require.NoError(t, got.BulkFill(0, []*vector.Vector{selected}))
					for _, physical := range mapping {
						require.NoError(t, want.Fill(0, int(physical), []*vector.Vector{source}))
					}
					expected.Reset()
					actual.Reset()
					require.NoError(t, want.SaveIntermediateResultOfChunk(0, &expected))
					require.NoError(t, got.SaveIntermediateResultOfChunk(0, &actual))
					require.Equal(t, expected.Bytes(), actual.Bytes())
				})
			}
		}
	}
}
