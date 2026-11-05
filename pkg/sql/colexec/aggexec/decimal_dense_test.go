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
	"strings"
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

func TestDecimalDenseCheckedOverflow(t *testing.T) {
	param := types.New(types.T_decimal256, 76, 0)
	text := "9" + strings.Repeat("0", 75)
	positive, err := types.ParseDecimal256(text, param.Width, param.Scale)
	require.NoError(t, err)
	negative, err := types.ParseDecimal256("-"+text, param.Width, param.Scale)
	require.NoError(t, err)
	for _, count := range []int{256, 257} {
		for _, isSum := range []bool{true, false} {
			for _, test := range []struct {
				name    string
				seed    types.Decimal256
				value   types.Decimal256
				wantErr bool
			}{
				{name: "cancelling positive", seed: negative, value: positive},
				{name: "cancelling negative", seed: positive, value: negative},
				{name: "positive overflow", seed: positive, value: positive, wantErr: true},
				{name: "negative overflow", seed: negative, value: negative, wantErr: true},
			} {
				t.Run(fmt.Sprintf("groups=%d/sum=%t/%s", count, isSum, test.name), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					vec := vector.NewVec(param)
					t.Cleanup(func() { vec.Free(mp) })
					seed := vector.NewVec(param)
					t.Cleanup(func() { seed.Free(mp) })
					for range 6 {
						require.NoError(t, vector.AppendFixed(seed, test.seed, false, mp))
					}
					// The legacy combiner holds the first 255 distinct groups.
					// It adds later groups directly to their existing state.
					groups := make([]uint64, 255+7)
					require.NoError(t, vector.AppendFixed(vec, test.value, false, mp)) // skipped by offset
					for row := range groups {
						value := types.Decimal256{}
						if row < 255 {
							groups[row] = uint64(row + 1)
						} else {
							groups[row] = uint64(count)
							value = test.value
						}
						require.NoError(t, vector.AppendFixed(vec, value, false, mp))
					}
					id := AggIdOfAvg
					if isSum {
						id = AggIdOfSum
					}
					scalar := makeSumAvgExec(mp, isSum, id, false, param)
					t.Cleanup(scalar.Free)
					batch := makeSumAvgExec(mp, isSum, id, false, param)
					t.Cleanup(batch.Free)
					for _, exec := range []AggFuncExec{scalar, batch} {
						require.NoError(t, exec.GroupGrow(count))
						for row := 0; row < seed.Length(); row++ {
							require.NoError(t, exec.Fill(count-1, row, []*vector.Vector{seed}))
						}
					}
					var scalarErr error
					for row, group := range groups {
						if scalarErr = scalar.Fill(int(group-1), row+1, []*vector.Vector{vec}); scalarErr != nil {
							break
						}
					}
					batchErr := batch.BatchFill(1, groups, []*vector.Vector{vec})
					if test.wantErr {
						require.ErrorContains(t, scalarErr, "overflow")
						require.ErrorContains(t, batchErr, "overflow")
						return
					}
					require.NoError(t, scalarErr)
					require.NoError(t, batchErr)
					var want, got bytes.Buffer
					require.NoError(t, scalar.SaveIntermediateResultOfChunk(0, &want))
					require.NoError(t, batch.SaveIntermediateResultOfChunk(0, &got))
					require.Equal(t, want.Bytes(), got.Bytes(), "SUM state and AVG count must match scalar Fill")
				})
			}
		}
	}
}
