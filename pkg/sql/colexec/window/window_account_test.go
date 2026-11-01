// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestWindowResetReleasesInheritedAccountedBuffers(t *testing.T) {
	for _, tc := range []struct {
		name                                             string
		partitioned, accounted, cancelBuffered, freeOnly bool
	}{
		{name: "partitioned row_number", partitioned: true, accounted: true},
		{name: "unpartitioned cumulative sum", accounted: true},
		{name: "canceled buffered input", accounted: true, cancelBuffered: true},
		{name: "direct free", accounted: true, freeOnly: true},
		{name: "unaccounted reuse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			child := colexec.NewMockOperator()
			spec := makeWindowSpec()
			spec.GetW().Name = "sum"
			spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(1)}}
			spec.GetW().Frame = &plan.FrameClause{
				Type:  plan.FrameClause_ROWS,
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
				End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			}
			aggregate := newAggExprAt(2)
			if tc.partitioned {
				spec.GetW().Name, spec.GetW().WindowFunc = "row_number", newFunExpr("row_number")
				spec.GetW().PartitionBy = []*plan.Expr{newColExpr(0)}
				spec.GetW().OrderBy = append([]*plan.OrderBySpec{{Expr: newColExpr(0)}}, spec.GetW().OrderBy...)
				aggregate = newRowNumberAggExpr(t)
			}
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{aggregate}}
			arg.AppendChild(child)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, mp.CurrNB())
			})
			registry, err := mpool.NewAllocationAccountRegistry(1, 32)
			require.NoError(t, err)
			for generation := range 2 {
				ctx, cancel := context.WithCancel(t.Context())
				proc.Ctx = ctx
				t.Cleanup(cancel)
				var account *mpool.AllocationAccount
				var selection *vector.AllocationAccountSelection
				if tc.accounted {
					account, err = registry.Open(1 << 20)
					require.NoError(t, err)
					selection, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
					require.NoError(t, err)
				}
				columns := [][]int32{{1, 1, 1}, {3, 1, 2}, {30, 10, 20}}
				want := []int64{10, 30, 60}
				if generation == 1 {
					columns, want = [][]int32{{1, 1}, {2, 1}, {200, 100}}, []int64{100, 300}
				}
				input := batch.NewOffHeapWithSize(3)
				child.WithBatchs([]*batch.Batch{input})
				for col, values := range columns {
					input.Vecs[col] = vector.NewOffHeapVecWithType(types.T_int32.ToType())
					require.NoError(t, input.Vecs[col].SetAllocationAccount(selection))
					require.NoError(t, vector.AppendFixedList(input.Vecs[col], values, nil, mp))
				}
				input.SetRowCount(len(want))
				canceled := tc.cancelBuffered && generation == 0
				if canceled {
					// receiveAll has already copied the input when it observes EOF.
					child.WithEndOfDataCallback(cancel)
				}
				require.NoError(t, arg.Prepare(proc))
				result, callErr := vm.Exec(arg, proc)
				if canceled {
					require.ErrorIs(t, callErr, context.Canceled)
				} else {
					require.NoError(t, callErr)
					require.NotNil(t, result.Batch)
					require.Equal(t, len(want), result.Batch.RowCount())
					for row, sum := range want {
						require.Equal(t, int32(row+1), vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[1], row))
						if tc.partitioned {
							sum = int64(row + 1)
						}
						require.Equal(t, sum, vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[3], row))
					}
				}
				require.NotNil(t, arg.ctr.bat)
				for _, vec := range arg.ctr.bat.Vecs {
					require.Same(t, selection, vec.AllocationAccountSelection())
				}
				if tc.accounted && !canceled {
					require.NotEmpty(t, arg.ctr.orderVecs)
					require.Len(t, arg.ctr.aggVecs, 1)
					if tc.partitioned {
						require.Empty(t, arg.ctr.aggVecs[0].Vec, "row_number has no argument copies")
					} else {
						require.Len(t, arg.ctr.aggVecs[0].Vec, 1)
					}
					for _, expressions := range [][]colexec.ExprEvalVector{arg.ctr.orderVecs, arg.ctr.aggVecs} {
						for _, expression := range expressions {
							for _, vec := range expression.Vec {
								require.NotNil(t, vec)
								require.Same(t, selection, vec.AllocationAccountSelection(), "expression copies must belong to the current generation")
							}
						}
					}
				}
				// Child-first cleanup leaves only Window's independently owned copies.
				child.Free(proc, false, nil)
				if tc.accounted {
					require.Positive(t, account.Snapshot().Used)
				}
				buffered := arg.ctr.bat
				allocated := buffered.Vecs[0].Allocated()
				if tc.freeOnly {
					arg.Free(proc, false, nil)
				} else {
					arg.Reset(proc, callErr != nil, callErr)
					if !tc.accounted {
						require.Same(t, buffered, arg.ctr.bat)
						require.Equal(t, allocated, arg.ctr.bat.Vecs[0].Allocated())
						require.Zero(t, arg.ctr.bat.RowCount())
					}
				}
				if tc.accounted {
					for _, expressions := range [][]colexec.ExprEvalVector{arg.ctr.orderVecs, arg.ctr.aggVecs} {
						for _, expression := range expressions {
							for _, vec := range expression.Vec {
								require.Nil(t, vec, "freed expression slots must not be reused without accounting")
							}
						}
					}
					require.Zero(t, account.Snapshot().Used, "Reset must release the ending generation before Free")
					terminal, first, err := registry.CompleteTerminal(account)
					require.NoError(t, err)
					require.True(t, first)
					require.Zero(t, terminal.LiveAllocations)
				}
				if arg.ctr.rBat != nil {
					require.Zero(t, arg.ctr.rBat.RowCount())
					for _, vec := range arg.ctr.rBat.Vecs {
						require.Nil(t, vec)
					}
				}
				cancel()
				if tc.freeOnly {
					break
				}
			}
			// A valid terminal must leave admission usable for a different account.
			next, err := registry.Open(1 << 20)
			require.NoError(t, err)
			_, _, err = registry.CompleteTerminal(next)
			require.NoError(t, err)
		})
	}
}

// Window owns each buffered copy even when VALUE_SCAN supplies ordinary,
// unaccounted vectors or the producer uses a different allocation account.
func TestWindowBufferedInputAccountAdmission(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		sourceAccounted, varlen, legacy bool
	}{
		{name: "unaccounted fixed input"},
		{name: "accounted fixed input", sourceAccounted: true},
		{name: "unaccounted variable input", varlen: true},
		{name: "accounted variable input", sourceAccounted: true, varlen: true},
		{name: "unaccounted buffer enters accounted execution", legacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			child := colexec.NewMockOperator()
			spec := makeWindowSpec()
			spec.GetW().Name = "row_number"
			spec.GetW().WindowFunc = newFunExpr("row_number")
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)}}
			arg.AppendChild(child)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, mp.CurrNB())
			})
			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			require.NoError(t, err)
			sourceRegistry, err := mpool.NewAllocationAccountRegistry(1, 512)
			require.NoError(t, err)
			limits := []uint64{1024, 3072, 1 << 20}
			if tc.legacy {
				limits = append([]uint64{0}, limits...)
			}
			for _, limit := range limits {
				func() {
					var account, sourceAccount *mpool.AllocationAccount
					var selection *vector.AllocationAccountSelection
					if limit > 0 {
						account, err = registry.Open(limit)
						require.NoError(t, err)
						require.NoError(t, arg.SetAllocationAccount(account))
						defer func() {
							arg.Free(proc, false, nil)
							require.Zero(t, account.Snapshot().Used)
							require.NoError(t, arg.ClearAllocationAccount(account))
							terminal, _, err := registry.CompleteTerminal(account)
							require.NoError(t, err)
							require.Zero(t, terminal.LiveAllocations)
						}()
					}
					if tc.sourceAccounted {
						sourceAccount, err = sourceRegistry.Open(1 << 20)
						require.NoError(t, err)
						selection, err = vector.NewAllocationAccountSelection(sourceAccount, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
						require.NoError(t, err)
						defer func() {
							arg.Free(proc, false, nil)
							child.Free(proc, false, nil)
							require.Zero(t, sourceAccount.Snapshot().Used)
							terminal, _, err := sourceRegistry.CompleteTerminal(sourceAccount)
							require.NoError(t, err)
							require.Zero(t, terminal.LiveAllocations)
						}()
					} else {
						defer child.Free(proc, false, nil)
					}
					width := 64
					typ := types.T_int64.ToType()
					if tc.varlen {
						width = 1
						typ = types.T_varchar.ToType()
					}
					for batchIdx := range 2 {
						input := batch.NewWithSize(width)
						child.WithBatchs([]*batch.Batch{input})
						for col := range width {
							var v *vector.Vector
							if selection != nil {
								v, err = vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
								require.NoError(t, err)
							} else {
								v = vector.NewVec(typ)
							}
							input.Vecs[col] = v
							for row := range 4 {
								if tc.varlen {
									require.NoError(t, vector.AppendBytes(v, []byte(strings.Repeat("x", 512)), row == 0, mp))
								} else {
									require.NoError(t, vector.AppendFixed(v, int64(batchIdx*4+row+col), false, mp))
								}
							}
						}
						input.SetRowCount(4)
					}
					sourceUsed := uint64(0)
					if sourceAccount != nil {
						sourceUsed = sourceAccount.Snapshot().Used
					}
					require.NoError(t, arg.Prepare(proc))
					result, callErr := vm.Exec(arg, proc)
					if limit > 0 && limit < 1<<20 {

						retained := 0
						if arg.ctr.bat != nil {
							for _, v := range arg.ctr.bat.Vecs {
								if v != nil {
									retained += v.Allocated()
								}
							}
						}
						t.Logf("%d-byte limit: retained=%d bytes, Window account=%d bytes, error=%v", limit, retained, account.Snapshot().Used, callErr)
						require.ErrorIs(t, callErr, mpool.ErrAllocationAccountCapacity, "retained input must not bypass Window admission")
					} else {
						require.NoError(t, callErr)
						require.NotNil(t, result.Batch)
						require.Equal(t, 8, result.Batch.RowCount())
						for row := range 8 {
							require.Equal(t, uint64(row+1), vector.GetFixedAtNoTypeCheck[uint64](result.Batch.Vecs[width], row))
						}
						if account != nil {
							retained := 0
							for _, v := range arg.ctr.bat.Vecs {
								require.Same(t, arg.ctr.allocation.Vector, v.AllocationAccountSelection())
								retained += v.Allocated()
							}
							require.GreaterOrEqual(t, retained, 3072)
							require.GreaterOrEqual(t, account.Snapshot().Used, uint64(retained))
							t.Logf("retained input=%d bytes, full Window account=%d bytes", retained, account.Snapshot().Used)
						}
					}
					if sourceAccount != nil {
						require.Equal(t, sourceUsed, sourceAccount.Snapshot().Used, "Window copies must not charge the producer")
					}
					arg.Reset(proc, callErr != nil, callErr)
					if account != nil {
						require.Zero(t, account.Snapshot().Used)
					} else {
						require.NotNil(t, arg.ctr.bat, "legacy unaccounted capacity is reusable")
					}
				}()
			}
		})
	}
}
