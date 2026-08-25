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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSourcePreservingAggregatePreflightsMixedSidecars(t *testing.T) {
	for _, mode := range []string{"fill", "merge"} {
		for _, kind := range []string{"any", "fixed-min", "bytes-min", "max-by-equal"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				mp := mpool.MustNewZero()
				registry, account, allocation := newTestAggregateAllocation(t)
				makeExec := func(id int64, params ...types.Type) GroupAggFuncExec {
					exec, err := MakeGroupAgg(mp, id, false, allocation, nil, params...)
					require.NoError(t, err)
					SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
					require.NoError(t, exec.GroupGrow(2))
					return exec
				}
				makeText := func(values []string, sources []types.StringSource) *vector.Vector {
					vec := vector.NewVec(types.T_text.ToType())
					for _, value := range values {
						require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
					}
					require.NoError(t, vec.SetStringSourcesWithMP(sources, mp))
					return vec
				}
				makeInt := func(values []int64) *vector.Vector {
					vec := vector.NewVec(types.T_int64.ToType())
					for _, value := range values {
						require.NoError(t, vector.AppendFixed(vec, value, false, mp))
					}
					return vec
				}

				var left, right GroupAggFuncExec
				var candidate []*vector.Vector
				wantFirst := types.StringSourceExpression
				switch kind {
				case "any":
					left = makeExec(AggIdOfAny, types.T_text.ToType())
					left.(aggregateBaseCarrier).aggregateBase().state[0].vecs[0].SetStringSource(
						types.StringSourceLiteral)
					candidate = []*vector.Vector{makeText(
						[]string{"winner", "winner"},
						[]types.StringSource{types.StringSourceCOMStmt, types.StringSourceLiteral})}
					wantFirst = types.StringSourceCOMStmt
				case "fixed-min":
					left = makeExec(AggIdOfMin, types.T_int64.ToType())
					seed := makeInt([]int64{5, 5})
					require.NoError(t, seed.SetStringSource(types.StringSourceLiteral))
					require.NoError(t, left.BatchFill(0, []uint64{1, 2}, []*vector.Vector{seed}))
					seed.Free(mp)
					values := makeInt([]int64{5, 5})
					require.NoError(t, values.SetStringSourcesWithMP([]types.StringSource{
						types.StringSourceCOMStmt, types.StringSourceLiteral}, mp))
					candidate = []*vector.Vector{values}
				case "bytes-min":
					left = makeExec(AggIdOfMin, types.T_text.ToType())
					seed := makeText([]string{"5", "5"}, []types.StringSource{
						types.StringSourceLiteral, types.StringSourceLiteral})
					require.NoError(t, left.BatchFill(0, []uint64{1, 2}, []*vector.Vector{seed}))
					seed.Free(mp)
					candidate = []*vector.Vector{makeText([]string{"5", "5"}, []types.StringSource{
						types.StringSourceCOMStmt, types.StringSourceLiteral})}
				case "max-by-equal":
					left = makeExec(AggIdOfMaxBy,
						types.T_text.ToType(), types.T_int64.ToType(), types.T_int64.ToType())
					seed := []*vector.Vector{
						makeText([]string{"value", "value"}, []types.StringSource{
							types.StringSourceLiteral, types.StringSourceLiteral}),
						makeInt([]int64{1, 1}), makeInt([]int64{1, 1}),
					}
					require.NoError(t, left.BatchFill(0, []uint64{1, 2}, seed))
					for _, vec := range seed {
						vec.Free(mp)
					}
					candidate = []*vector.Vector{
						makeText([]string{"value", "value"}, []types.StringSource{
							types.StringSourceCOMStmt, types.StringSourceLiteral}),
						makeInt([]int64{1, 1}), makeInt([]int64{1, 1}),
					}
				}

				if mode == "merge" {
					argTypes, _ := left.TypesInfo()
					right = makeExec(left.AggID(), argTypes...)
					require.NoError(t, right.BatchFill(0, []uint64{1, 2}, candidate))
				}
				state := left.(aggregateBaseCarrier).aggregateBase().state[0].vecs[0]
				require.Empty(t, state.GetStringSources())
				var err error
				if mode == "fill" {
					err = left.PreflightBatchFill(0, []uint64{1, 2}, candidate)
				} else {
					err = left.PreflightBatchMerge(right, 0, []uint64{1, 2})
				}
				require.NoError(t, err)
				require.Equal(t, []types.StringSource{
					types.StringSourceLiteral, types.StringSourceLiteral,
				}, state.GetStringSources(), "preflight must reserve without publishing metadata")
				admitted := mp.CurrNB()
				if mode == "fill" {
					require.NoError(t, left.BatchFill(0, []uint64{1, 2}, candidate))
				} else {
					require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))
				}
				require.Equal(t, admitted, mp.CurrNB(), "runtime source publication must use preflighted capacity")
				require.Equal(t, wantFirst, state.GetStringSourceAt(0))
				require.Equal(t, types.StringSourceLiteral, state.GetStringSourceAt(1))

				for _, vec := range candidate {
					vec.Free(mp)
				}
				left.Free()
				require.NoError(t, left.ClearAllocationAccount(allocation))
				if right != nil {
					right.Free()
					require.NoError(t, right.ClearAllocationAccount(allocation))
				}
				finishTestAggregateAllocation(t, registry, account)
				require.Zero(t, mp.CurrNB())
			})
		}
	}
}
