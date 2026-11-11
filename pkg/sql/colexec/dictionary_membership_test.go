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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFixedMembershipScopedProbeLifecycle(t *testing.T) {
	for _, kind := range []string{"integer-map", "integer-bitmap", "string"} {
		for _, name := range []string{"in", "not_in"} {
			for _, listNull := range []bool{false, true} {
				t.Run(kind+"/"+name+map[bool]string{false: "/non-null-list", true: "/null-list"}[listNull], func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(nil))
					registry, err := mpool.NewAllocationAccountRegistry(1, 32)
					require.NoError(t, err)
					account, err := registry.Open(1 << 20)
					require.NoError(t, err)
					t.Cleanup(func() {
						require.Zero(t, account.Snapshot().Used)
						_, _, err := registry.CompleteTerminal(account)
						require.NoError(t, err)
					})
					allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
					require.NoError(t, err)
					typ := types.T_int64.ToType()
					if kind == "string" {
						typ = types.T_varchar.ToType()
					}
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(typ)
					t.Cleanup(func() { input.Clean(mp) })
					strings := []string{"outside", "twenty as a non-inline string", "missing", "forty as a non-inline string", "null probe", "outside tail"}
					fillProbe := func(v *vector.Vector) {
						if kind == "string" {
							for _, s := range strings {
								require.NoError(t, vector.AppendBytes(v, []byte(s), false, mp))
							}
						} else {
							require.NoError(t, vector.AppendFixedList(v, []int64{10, 20, 30, 40, 50, 60}, nil, mp))
						}
						v.GetNulls().Add(4)
					}
					fillProbe(input.Vecs[0])
					input.SetRowCount(6)
					var data []byte
					var listLength int
					func() {
						payload := vector.NewVec(typ)
						defer payload.Free(mp)
						if kind == "string" {
							for _, s := range []string{strings[1], strings[3]} {
								require.NoError(t, vector.AppendBytes(payload, []byte(s), false, mp))
							}
							if listNull {
								require.NoError(t, vector.AppendBytes(payload, nil, true, mp))
							}
						} else {
							members := []int64{20, 40}
							if kind == "integer-bitmap" {
								for i := int64(101); i <= 114; i++ {
									members = append(members, i)
								}
							}
							require.NoError(t, vector.AppendFixedList(payload, members, nil, mp))
							payload.SetSorted(kind == "integer-bitmap")
							if listNull {
								require.NoError(t, vector.AppendFixed(payload, int64(0), true, mp))
							}
						}
						data, err = payload.MarshalBinary()
						require.NoError(t, err)
						listLength = payload.Length()
					}()
					column := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
					list := &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: int32(listLength), Data: data}}}
					executor, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, name, column, list), allocation)
					require.NoError(t, err)
					t.Cleanup(executor.Free)
					root := executor.(*FunctionExpressionExecutor)
					require.True(t, predicateBorrowsInputs(root))
					rhs := root.parameterExecutor[1].(*FixedVectorExpressionExecutor).resultVector
					frame := &DictionaryInputFrame{}
					t.Cleanup(func() { frame.Free(mp) })
					frame.Begin(input, allocation, mp)
					evaluate := func(rows []int64) *vector.Vector {
						t.Helper()
						selected, borrowed, err := frame.predicateBatch(proc, input, nil, root, rows, allocation)
						require.NoError(t, err)
						require.True(t, borrowed)
						require.True(t, selected.Vecs[0].IsDictionary())
						frame.Begin(selected, allocation, mp)
						frame.Bind([]ExpressionExecutor{root})
						result, err := executor.Eval(proc, []*batch.Batch{selected}, nil)
						require.NoError(t, err)
						require.Equal(t, listLength, rhs.Length(), "list row count is independent of probe selection")
						require.Zero(t, frame.Gathers, "membership must not gather the scoped probe")
						require.Empty(t, frame.predicateVectors)
						return result
					}
					// Reordered and repeated rows; the source NULL is outside this domain.
					rows := []int64{3, 1, 3, 2}
					result := evaluate(rows)
					for i := range rows {
						missing := i == 3
						require.Equal(t, listNull && missing, result.IsNull(uint64(i)))
						if !result.IsNull(uint64(i)) {
							require.Equal(t, (!missing) != (name == "not_in"), vector.GetFixedAtNoTypeCheck[bool](result, i))
						}
					}
					if !listNull {
						require.Zero(t, result.GetNulls().GetBitmap().ExternalStorageCapacity())
					}
					frame.End()
					require.Nil(t, root.parameterResults[0])
					// The result owns its values after the source and scoped headers retire.
					input.Vecs[0].Free(mp)
					input.Vecs[0] = vector.NewVec(typ)
					fillProbe(input.Vecs[0])
					require.Equal(t, name == "in", vector.GetFixedAtNoTypeCheck[bool](result, 0))
					result = evaluate([]int64{4, 2, 3})
					require.True(t, result.IsNull(0))
					require.Equal(t, listNull, result.IsNull(1))
					require.False(t, result.IsNull(2))
					require.Equal(t, name == "in", vector.GetFixedAtNoTypeCheck[bool](result, 2))
					frame.End()
					// A new query generation may rebind the fixed list. Reset must retire
					// both membership values and its cached hasNull before NULL-free prepare.
					executor.ResetForNextQuery()
					rhs.CleanOnlyData()
					if kind == "string" {
						require.NoError(t, vector.AppendBytes(rhs, []byte(strings[2]), false, mp))
					} else {
						require.NoError(t, vector.AppendFixed(rhs, int64(30), false, mp))
					}
					listLength = 1
					result = evaluate(rows)
					require.False(t, result.HasNull())
					for i := range rows {
						require.Equal(t, (i == 3) != (name == "not_in"), vector.GetFixedAtNoTypeCheck[bool](result, i))
					}
					frame.End()
					// Exercise the real ordered predicate chain. The first
					// comparison removes a row before membership sees it.
					var excluded *plan.Expr
					if kind == "string" {
						excluded = &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: strings[0]}}}}
					} else {
						excluded = &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 10}}}}
					}
					guard, err := NewExpressionExecutorWithAllocation(proc, bindTestFunction(t, proc, "<>", column, excluded), allocation)
					require.NoError(t, err)
					t.Cleanup(guard.Free)
					var scratch []int64
					var parameter vector.FunctionParameterWrapper[bool]
					kept, all, err := SelectPredicateRows(proc, input, nil, []ExpressionExecutor{guard, root}, allocation, &scratch, &parameter, nil, nil, frame)
					require.NoError(t, err)
					require.False(t, all)
					if name == "in" {
						require.Equal(t, []int64{2}, kept)
					} else {
						require.Equal(t, []int64{1, 3, 5}, kept)
					}
					require.Zero(t, frame.Gathers)
					require.Equal(t, 1, rhs.Length())
					// A batch-sized fixed executor is not an independent list.
					listExecutor := root.parameterExecutor[1].(*FixedVectorExpressionExecutor)
					listExecutor.noNeedToSetLength = false
					require.False(t, predicateBorrowsInputs(root))
					require.Nil(t, root.fixedMembershipList())
					fallback, borrowed, err := frame.predicateBatch(proc, input, nil, root, rows, allocation)
					require.NoError(t, err)
					require.True(t, borrowed)
					require.False(t, fallback.HasDictionary())
					require.Positive(t, frame.Gathers, "unproved lists retain the owned input bank")
					frame.End()
					listExecutor.noNeedToSetLength = true
					executor.ResetForNextQuery()
					rhs.CleanOnlyData()
					listLength = 0
					// Empty runtime-filter lists still have an independent
					// zero-row domain; non-NULL probes never match them.
					selected, borrowed, err := frame.predicateBatch(proc, input, nil, root, rows, allocation)
					require.NoError(t, err)
					require.True(t, borrowed)
					require.True(t, selected.HasDictionary())
					frame.Begin(selected, allocation, mp)
					frame.Bind([]ExpressionExecutor{root})
					result, err = executor.Eval(proc, []*batch.Batch{selected}, nil)
					require.NoError(t, err)
					require.False(t, result.HasNull())
					for i := range rows {
						require.Equal(t, name == "not_in", vector.GetFixedAtNoTypeCheck[bool](result, i))
					}
					require.Zero(t, rhs.Length())
					frame.End()
				})
			}
		}
	}
}
