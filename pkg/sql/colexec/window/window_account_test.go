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

	"github.com/RoaringBitmap/roaring/v2"
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

type rejectBitmapWindowGrowth struct {
	reject func() bool
}

func (r *rejectBitmapWindowGrowth) AcquireAllocationCapacity(uint64) error {
	if r.reject != nil && r.reject() {
		return mpool.ErrAllocationAccountCapacity
	}
	return nil
}

func (*rejectBitmapWindowGrowth) ReleaseAllocationCapacity(uint64) {}

func checkBitmapWindowRow(t *testing.T, out *vector.Vector, row int, want []uint32) {
	t.Helper()
	if want == nil {
		require.True(t, out.IsNull(uint64(row)), "row %d", row)
		return
	}
	require.False(t, out.IsNull(uint64(row)), "row %d", row)
	bm := roaring.New()
	require.NoError(t, bm.UnmarshalBinary(out.GetBytesAt(row)), "row %d", row)
	require.Equal(t, want, bm.ToArray(), "row %d", row)
}

func TestAccountedBitmapWindow(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "sliding"
		if full {
			name = "full partitions"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			t.Cleanup(func() { proc.Free(); require.Zero(t, mp.CurrNB()) })
			registry, err := mpool.NewAllocationAccountRegistry(1, 512)
			require.NoError(t, err)
			controller := &rejectBitmapWindowGrowth{}
			account, err := registry.OpenWithController(1<<20, controller)
			require.NoError(t, err)
			t.Cleanup(func() {
				terminal, _, err := registry.CompleteTerminal(account)
				require.NoError(t, err)
				require.Zero(t, terminal.Used)
				require.Zero(t, terminal.LiveAllocations)
			})
			typ := types.T_varbinary.ToType()
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(typ, mp)
			t.Cleanup(func() { input.Clean(mp) })
			for _, values := range [][]uint32{{99}, {1, 2}, {2, 3}, {4, 5, 6}, {8}, {8, 9}, nil, nil, nil} {
				if values == nil {
					require.NoError(t, vector.AppendBytes(input.Vecs[0], nil, true, mp))
					continue
				}
				data, err := roaring.BitmapOf(values...).ToBytes()
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(input.Vecs[0], data, false, mp))
			}
			input.SetRowCount(9)
			spec := makeAggWindowSpec("bitmap_or_agg")
			spec.GetW().Frame = &plan.FrameClause{
				Type:  plan.FrameClause_ROWS,
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}}},
				End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			}
			if full {
				spec.GetW().Frame = makeFullFrame()
				spec.GetW().Frame.Type = plan.FrameClause_RANGE
			}
			expression := newColExprWithType(0, typ)
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{
				aggexec.MakeAggFunctionExpression(aggexec.AggIdOfBitmapOr, false, []*plan.Expr{expression}, nil),
			}}
			require.NoError(t, arg.SetAllocationAccount(account))
			t.Cleanup(func() { arg.Free(proc, false, nil) })
			ctr := &arg.ctr
			evaluate := func(start, end int) (*vector.Vector, error) {
				// Borrow prepartitioned input so independently owned output vectors can
				// be kept across range calls (unlike the borrowed Call result batch).
				ctr.bat, ctr.ps = input, []int64{0, 1, 4, 7}
				ctr.aggVecs = []colexec.ExprEvalVector{{Vec: input.Vecs}}
				defer func() { ctr.bat, ctr.aggVecs = nil, nil }()
				return ctr.processAggregateFuncRange(0, arg, proc, start, end)
			}
			check := func() {
				if !full {
					out, err := evaluate(2, 6)
					require.NoError(t, err)
					defer out.Free(out.MPool())
					require.Nil(t, ctr.fullPartitionResults)
					want := [][]uint32{{1, 2, 3}, {2, 3, 4, 5, 6}, {8}, {8, 9}}
					require.Equal(t, len(want), out.Length())
					for row, expected := range want {
						checkBitmapWindowRow(t, out, row, expected)
					}
					return
				}
				first, err := evaluate(0, 3)
				require.NoError(t, err)
				defer func() {
					if first != nil {
						first.Free(first.MPool())
					}
				}()
				require.NotNil(t, ctr.fullPartitionResults, "partition crosses the range boundary")
				second, err := evaluate(3, 9)
				require.NoError(t, err)
				defer second.Free(second.MPool())
				require.Nil(t, ctr.fullPartitionResults)
				union := []uint32{1, 2, 3, 4, 5, 6}
				require.Equal(t, 3, first.Length())
				require.Equal(t, 6, second.Length())
				checkBitmapWindowRow(t, first, 0, []uint32{99})
				require.Greater(t, len(first.GetBytesAt(1)), types.VarlenaInlineSize)
				checkBitmapWindowRow(t, first, 1, union)
				checkBitmapWindowRow(t, first, 2, union)
				replacement, err := roaring.BitmapOf(77, 78, 79, 80, 81, 82).ToBytes()
				require.NoError(t, err)
				// Use the public row replacement API, not writes through a borrowed byte view.
				require.NoError(t, vector.SetBytesAt(first, 1, replacement, first.MPool()))
				checkBitmapWindowRow(t, first, 1, []uint32{77, 78, 79, 80, 81, 82})
				checkBitmapWindowRow(t, first, 2, union)
				first.Free(first.MPool())
				first = nil
				for row, expected := range [][]uint32{union, {8, 9}, {8, 9}, {8, 9}, nil, nil} {
					checkBitmapWindowRow(t, second, row, expected)
				}
			}
			start, end := 2, 6
			if full {
				start, end = 0, 3
			}
			inputBytes := mp.CurrNB()
			rejectAndReset := func(reject func() bool) {
				t.Helper()
				controller.reject = reject
				out, err := evaluate(start, end)
				controller.reject = nil
				if out != nil {
					defer out.Free(out.MPool())
				}
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, out)
				require.Nil(t, ctr.batAggs)
				require.Nil(t, ctr.fullPartitionResults)
				require.Zero(t, account.Snapshot().Used)
				arg.Reset(proc, true, err)
				require.Equal(t, inputBytes, mp.CurrNB(), "only borrowed input should remain")
			}
			// The executor is published after GroupGrow. Reject growth during Fill,
			// without pinning an allocation ordinal or byte size.
			rejectAndReset(func() bool { return len(ctr.batAggs) > 0 && ctr.batAggs[0] != nil })
			if full {
				// Reject output allocation with a completed partition cache.
				rejectAndReset(func() bool { return ctr.fullPartitionResults != nil })
			}
			check()
			require.Zero(t, account.Snapshot().Used)
			if full {
				// A bad later partition must not prevent publication of earlier ranges.
				require.NoError(t, vector.SetBytesAt(input.Vecs[0], 4, []byte{0, 0, 0, 0}, mp))
				first, err := evaluate(0, 3)
				require.NoError(t, err)
				defer first.Free(first.MPool())
				out, err := evaluate(3, 9)
				if out != nil {
					defer out.Free(out.MPool())
				}
				require.ErrorContains(t, err, "invalid roaring bitmap cookie")
				require.Nil(t, out)
				require.Nil(t, ctr.batAggs)
				require.Nil(t, ctr.fullPartitionResults)
				checkBitmapWindowRow(t, first, 2, []uint32{1, 2, 3, 4, 5, 6})
			}
		})
	}
}

func TestWindowResetReleasesAccountedBuffers(t *testing.T) {
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
				colexec.FreeMockChildren(arg)
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
				if tc.accounted {
					account, err = registry.Open(1 << 20)
					require.NoError(t, err)
					require.NoError(t, arg.SetAllocationAccount(account))
				}
				columns := [][]int32{{1, 1, 1}, {3, 1, 2}, {30, 10, 20}}
				want := []int64{10, 30, 60}
				if generation == 1 {
					columns, want = [][]int32{{1, 1}, {2, 1}, {200, 100}}, []int64{100, 300}
				}
				input := batch.NewWithSize(3)
				child.WithBatchs([]*batch.Batch{input})
				for col, values := range columns {
					input.Vecs[col] = vector.NewVec(types.T_int32.ToType(), mp)
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
						if tc.partitioned {
							require.Equal(t, sum, vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[3], row))
						} else {
							require.Equal(t, types.Decimal128FromInt64(sum), vector.GetFixedAtNoTypeCheck[types.Decimal128](result.Batch.Vecs[3], row))
						}
					}
				}
				require.NotNil(t, arg.ctr.bat)
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
							}
						}
					}
				}
				// Child-first cleanup leaves only Window's independently owned copies.
				child.Free(proc, false, nil)
				colexec.FreeMockChildren(child)
				if tc.accounted {
					require.Positive(t, account.Snapshot().Used)
				}
				buffered := arg.ctr.bat
				allocated := buffered.Vecs[0].Allocated()
				if tc.freeOnly {
					arg.Free(proc, false, nil)
					colexec.FreeMockChildren(arg)
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
					require.NoError(t, arg.ClearAllocationAccount(account))
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
			spec.GetW().Name = "count"
			spec.GetW().WindowFunc = newFunExpr("starcount")
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, nil, nil)}}
			arg.AppendChild(child)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				colexec.FreeMockChildren(arg)
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
					var selection *mpool.MPool
					if limit > 0 {
						account, err = registry.Open(limit)
						require.NoError(t, err)
						require.NoError(t, arg.SetAllocationAccount(account))
						defer func() {
							arg.Free(proc, false, nil)
							colexec.FreeMockChildren(arg)
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
						selection, err = mp.WithAccount(sourceAccount, mpool.AllocationOwnerOrder, 1, mpool.AllocationCapacityClassDefault)
						require.NoError(t, err)
						defer func() {
							arg.Free(proc, false, nil)
							colexec.FreeMockChildren(arg)
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
							inputMP := mp
							if selection != nil {
								inputMP = selection
							}
							v := vector.NewVec(typ, inputMP)
							input.Vecs[col] = v
							for row := range 4 {
								if tc.varlen {
									require.NoError(t, vector.AppendBytes(v, []byte(strings.Repeat("x", 512)), row == 0, inputMP))
								} else {
									require.NoError(t, vector.AppendFixed(v, int64(batchIdx*4+row+col), false, inputMP))
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
							require.Equal(t, int64(8), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[width], row))
						}
						if account != nil {
							retained := 0
							for _, v := range arg.ctr.bat.Vecs {
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
