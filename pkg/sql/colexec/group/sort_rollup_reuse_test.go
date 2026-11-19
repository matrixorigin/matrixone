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

package group

import (
	"context"
	"fmt"
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

func TestSortRollupLaterDecimalFinalizerFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	child := colexec.NewMockOperator()
	typ := types.New(types.T_decimal256, 65, 0)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{
		countStarAgg(), aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{colExprWithType(1, typ)}, nil),
	})
	g.SortRollup = true
	g.AppendChild(child)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	defer func() {
		g.Free(proc, true, nil)
		child.Free(proc, true, nil)
		finalizeGroupTestAllocation(t, g, allocation)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	max, err := types.ParseDecimal256(strings.Repeat("9", 65), 65, 0)
	require.NoError(t, err)
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	input.Vecs[1] = vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed(input.Vecs[1], max, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal256FromInt64(1), false, proc.Mp()))
	input.SetRowCount(2)
	child.WithBatchs([]*batch.Batch{input})
	require.NoError(t, g.Prepare(proc))
	require.True(t, g.ctr.sortRollup.reuseSingleGroup)
	result, err := vm.Exec(g, proc)
	require.ErrorContains(t, err, "beyond the range")
	require.Nil(t, result.Batch)
	g.Reset(proc, true, err)
	require.Zero(t, allocation.account.Snapshot().Used)
}

func TestSortRollupSingleGroupReuseAndMixedFallback(t *testing.T) {
	for _, name := range []string{"reuse", "mixed-distinct", "mixed-min", "mixed-decimal"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			child := colexec.NewMockOperator()
			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
				[]aggexec.AggFuncExecExpression{countStarLiteralAgg(), sumAgg(1), avgAgg(1)})
			g.SortRollup = true
			g.AppendChild(child)
			t.Cleanup(func() {
				g.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			switch name {
			case "mixed-distinct":
				g.Aggs = append(g.Aggs, countDistinctAgg(1))
			case "mixed-min":
				g.Aggs = append(g.Aggs, aggexec.MakeAggFunctionExpression(
					aggexec.AggIdOfMin, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil))
			case "mixed-decimal":
				for _, id := range []int64{aggexec.AggIdOfSum, aggexec.AggIdOfAvg, aggexec.AggIdOfMin, aggexec.AggIdOfMax} {
					g.Aggs = append(g.Aggs, aggexec.MakeAggFunctionExpression(id, false, []*plan.Expr{colExpr(2, types.T_int64)}, nil))
				}
			}
			// The first group crosses a batch boundary; the next groups exercise
			// non-NULL -> all NULL -> non-NULL reset and an offset run. The last
			// value repeats across detail groups, so DISTINCT's total must dedup.
			first := batch.NewWithSize(3)
			first.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
			first.Vecs[1] = testutil.MakeInt32Vector([]int32{2}, nil, proc.Mp())
			first.Vecs[2] = testutil.MakeInt64Vector([]int64{2}, nil, proc.Mp())
			first.SetRowCount(1)
			second := batch.NewWithSize(3)
			second.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
			second.Vecs[1] = testutil.MakeInt32Vector([]int32{4, 0, 2}, []uint64{1}, proc.Mp())
			second.Vecs[2] = testutil.MakeInt64Vector([]int64{4, 0, 2}, []uint64{1}, proc.Mp())
			second.SetRowCount(3)
			child.WithBatchs([]*batch.Batch{first, second})
			require.NoError(t, g.Prepare(proc))
			require.Equal(t, name == "reuse" || name == "mixed-min" || name == "mixed-decimal", g.ctr.sortRollup.reuseSingleGroup)
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			out := result.Batch
			require.Equal(t, 4, out.RowCount())
			require.Equal(t, []int32{1, 2, 3, 0}, vector.MustFixedColNoTypeCheck[int32](out.Vecs[0]))
			require.Equal(t, []int64{2, 1, 1, 4}, vector.MustFixedColNoTypeCheck[int64](out.Vecs[1]))
			for row, sum := range []int64{6, 0, 2, 8} {
				require.Equal(t, row == 3, out.Vecs[0].GetGrouping().Contains(uint64(row)))
				require.Equal(t, row == 1, out.Vecs[2].IsNull(uint64(row)))
				require.Equal(t, row == 1, out.Vecs[3].IsNull(uint64(row)))
				if row != 1 {
					require.Equal(t, sum, vector.GetFixedAtNoTypeCheck[int64](out.Vecs[2], row))
					avg := vector.GetFixedAtNoTypeCheck[types.Decimal128](out.Vecs[3], row)
					require.Equal(t, []string{"3.0000", "", "2.0000", "2.6667"}[row], avg.Format(out.Vecs[3].GetType().Scale))
				}
				for _, vec := range out.Vecs[1:4] {
					if !vec.IsNull(uint64(row)) {
						require.True(t, vec.HasPrepareParamKind())
						require.Equal(t, vector.PrepareParamNone, vec.GetPrepareParamKindAt(row))
					}
				}
				switch name {
				case "mixed-distinct":
					require.Equal(t, []int64{2, 0, 1, 2}[row], vector.GetFixedAtNoTypeCheck[int64](out.Vecs[4], row))
				case "mixed-min":
					require.Equal(t, row == 1, out.Vecs[4].IsNull(uint64(row)))
					if row != 1 {
						require.Equal(t, int32(2), vector.GetFixedAtNoTypeCheck[int32](out.Vecs[4], row))
					}
				case "mixed-decimal":
					require.Equal(t, row == 1, out.Vecs[4].IsNull(uint64(row)))
					if row != 1 {
						require.Equal(t, types.Decimal128FromInt64(sum), vector.GetFixedAtNoTypeCheck[types.Decimal128](out.Vecs[4], row))
						require.Equal(t, []string{"3.0000", "", "2.0000", "2.6667"}[row], vector.GetFixedAtNoTypeCheck[types.Decimal128](out.Vecs[5], row).Format(out.Vecs[5].GetType().Scale))
						require.Equal(t, []int64{2, 0, 2, 2}[row], vector.GetFixedAtNoTypeCheck[int64](out.Vecs[6], row))
						require.Equal(t, []int64{4, 0, 2, 4}[row], vector.GetFixedAtNoTypeCheck[int64](out.Vecs[7], row))
					}
				}
			}
			result, err = vm.Exec(g, proc)
			require.NoError(t, err)
			require.Nil(t, result.Batch)
		})
	}
}

func TestSortRollupAppendFailureAbortsAndReprepares(t *testing.T) {
	for _, target := range []int{2, 3} {
		for _, growth := range []bool{false, true} {
			name := fmt.Sprintf("column-%d/new-result", target)
			if growth {
				name = fmt.Sprintf("column-%d/result-growth", target)
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				child := colexec.NewMockOperator()
				g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
					[]aggexec.AggFuncExecExpression{countStarAgg(),
						aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{colExpr(1, types.T_int64)}, nil),
						aggexec.MakeAggFunctionExpression(aggexec.AggIdOfAvg, false, []*plan.Expr{colExpr(1, types.T_int64)}, nil)})
				g.SortRollup = true
				g.AppendChild(child)
				var allocation groupTestAllocation
				defer func() {
					g.Free(proc, true, nil)
					child.Free(proc, true, nil)
					if allocation.account != nil {
						finalizeGroupTestAllocation(t, g, allocation)
					}
					proc.Free()
					require.Zero(t, proc.Mp().CurrNB())
				}()
				generation := installGroupTestResourceBudget(t, proc)
				registry, err := mpool.NewAllocationAccountRegistry(1, 1<<16)
				require.NoError(t, err)
				controller := &rejectNextGroupAllocationController{}
				account, err := registry.OpenWithController(64<<20, controller)
				require.NoError(t, err)
				require.NoError(t, g.ctr.setAllocationAccount(account))
				allocation = groupTestAllocation{generation: generation, registry: registry, account: account}
				rows := 2
				if growth {
					rows = aggBatchSize + 1
				}
				values := make([]int32, rows)
				for i := range values {
					values[i] = int32(i + 1)
				}
				input := batch.NewWithSize(2)
				input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				bigints := make([]int64, rows)
				for i := range bigints {
					bigints[i] = int64(values[i])
				}
				input.Vecs[1] = testutil.MakeInt64Vector(bigints, nil, proc.Mp())
				input.SetRowCount(rows)
				child.WithBatchs([]*batch.Batch{input})
				require.NoError(t, g.Prepare(proc))
				require.True(t, g.ctr.sortRollup.reuseSingleGroup)
				controller.rejectWhen = func() bool {
					s := g.ctr.sortRollup
					if s.output == nil {
						return false
					}
					if !growth {
						return len(s.output.Vecs) == target
					}
					return len(s.output.Vecs) == 4 && s.outputRows > 0 &&
						s.output.Vecs[target-1].Length() == s.outputRows+1 &&
						s.output.Vecs[target].Length() == s.outputRows
				}
				result, err := vm.Exec(g, proc)
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, result.Batch, "a later column failure must not expose any buffered row")
				s := g.ctr.sortRollup
				require.Equal(t, s.outputRows, s.output.RowCount())
				require.Equal(t, s.outputRows+1, s.output.Vecs[1].Length())
				if growth {
					require.Positive(t, s.outputRows)
					require.Equal(t, s.outputRows, s.output.Vecs[target].Length())
				} else {
					require.Zero(t, s.outputRows)
				}
				oldSelection := s.output.Vecs[1].AllocationAccountSelection()
				_, rejected := controller.snapshot()
				require.True(t, rejected)
				g.Reset(proc, true, err)
				require.Nil(t, g.ctr.sortRollup)
				require.Zero(t, account.Snapshot().Used)
				used, _ := controller.snapshot()
				require.Zero(t, used)
				finalizeGroupTestAllocation(t, g, allocation)
				allocation = groupTestAllocation{}
				child.Free(proc, true, err)
				g.Children = nil

				// A fresh account and Prepare must not retain the failed generation.
				child = colexec.NewMockOperator()
				fresh := batch.NewWithSize(2)
				fresh.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
				fresh.Vecs[1] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
				fresh.SetRowCount(1)
				child.WithBatchs([]*batch.Batch{fresh})
				g.AppendChild(child)
				allocation = installGroupTestAllocation(t, g, proc, 64<<20, generation)
				require.NoError(t, g.Prepare(proc))
				result, err = vm.Exec(g, proc)
				require.NoError(t, err)
				require.NotNil(t, result.Batch)
				require.False(t, vector.AllocationAccountSelectionsEqual(oldSelection, result.Batch.Vecs[1].AllocationAccountSelection()))
				require.Equal(t, []int64{1, 1}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]))
				for row := 0; row < 2; row++ {
					require.Equal(t, types.Decimal128FromInt64(7), vector.GetFixedAtNoTypeCheck[types.Decimal128](result.Batch.Vecs[2], row))
					avg := vector.GetFixedAtNoTypeCheck[types.Decimal128](result.Batch.Vecs[3], row)
					require.Equal(t, "7.0000", avg.Format(result.Batch.Vecs[3].GetType().Scale))
				}
			})
		}
	}
}

func TestSortRollupCancellationRetainsPublishedOutputForCleanup(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx, cancel := context.WithCancel(proc.Ctx)
	defer cancel()
	proc.Ctx = ctx
	child := colexec.NewMockOperator()
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)},
		[]aggexec.AggFuncExecExpression{countStarAgg(),
			aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{colExpr(1, types.T_int64)}, nil),
			aggexec.MakeAggFunctionExpression(aggexec.AggIdOfAvg, false, []*plan.Expr{colExpr(1, types.T_int64)}, nil)})
	g.SortRollup = true
	g.AppendChild(child)
	var allocation groupTestAllocation
	defer func() {
		g.Free(proc, true, context.Canceled)
		child.Free(proc, true, context.Canceled)
		if allocation.account != nil {
			finalizeGroupTestAllocation(t, g, allocation)
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	allocation = installGroupTestAllocation(t, g, proc, 64<<20)
	values := make([]int32, aggBatchSize+1)
	for i := range values {
		values[i] = int32(i)
	}
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bigints := make([]int64, len(values))
	for i := range bigints {
		bigints[i] = int64(values[i])
	}
	input.Vecs[1] = testutil.MakeInt64Vector(bigints, nil, proc.Mp())
	input.SetRowCount(len(values))
	child.WithBatchs([]*batch.Batch{input})
	require.NoError(t, g.Prepare(proc))
	result, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	borrowed := result.Batch
	require.Equal(t, aggBatchSize, borrowed.RowCount())
	require.Same(t, borrowed, g.ctr.sortRollup.lastOutput)
	for row := 0; row < borrowed.RowCount(); row++ {
		require.Equal(t, int32(row), vector.GetFixedAtNoTypeCheck[int32](borrowed.Vecs[0], row))
		require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](borrowed.Vecs[1], row))
		require.Equal(t, types.Decimal128FromInt64(int64(row)), vector.GetFixedAtNoTypeCheck[types.Decimal128](borrowed.Vecs[2], row))
		avg := vector.GetFixedAtNoTypeCheck[types.Decimal128](borrowed.Vecs[3], row)
		require.Equal(t, fmt.Sprintf("%d.0000", row), avg.Format(borrowed.Vecs[3].GetType().Scale))
	}
	cancel()
	result, err = vm.Exec(g, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	// Entry cancellation can precede reclamation. The operator must retain
	// its cleanup reference to the borrowed batch until Reset/Free.
	require.Same(t, borrowed, g.ctr.sortRollup.lastOutput)
	g.Reset(proc, true, err)
	require.Nil(t, g.ctr.sortRollup)
	require.Zero(t, allocation.account.Snapshot().Used)
	generation := allocation.generation
	finalizeGroupTestAllocation(t, g, allocation)
	allocation = groupTestAllocation{}
	child.Free(proc, true, err)
	g.Children = nil
	proc.Ctx = context.Background()
	child = colexec.NewMockOperator()
	fresh := batch.NewWithSize(2)
	fresh.Vecs[0] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
	fresh.Vecs[1] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
	fresh.SetRowCount(1)
	child.WithBatchs([]*batch.Batch{fresh})
	g.AppendChild(child)
	allocation = installGroupTestAllocation(t, g, proc, 64<<20, generation)
	require.NoError(t, g.Prepare(proc))
	result, err = vm.Exec(g, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{1, 1}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1]))
	for row := 0; row < 2; row++ {
		require.Equal(t, types.Decimal128FromInt64(7), vector.GetFixedAtNoTypeCheck[types.Decimal128](result.Batch.Vecs[2], row))
		avg := vector.GetFixedAtNoTypeCheck[types.Decimal128](result.Batch.Vecs[3], row)
		require.Equal(t, "7.0000", avg.Format(result.Batch.Vecs[3].GetType().Scale))
	}
}

func TestSortRollupMinMetadataDowngradesBeforeClosingPreviousBatch(t *testing.T) {
	for _, sameGroup := range []bool{false, true} {
		for _, forceFallback := range []bool{false, true} {
			t.Run(fmt.Sprintf("same=%t/fallback=%t", sameGroup, forceFallback), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				child := colexec.NewMockOperator()
				minAgg := aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil)
				g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{minAgg, aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{colExpr(2, types.T_int64)}, nil), aggexec.MakeAggFunctionExpression(aggexec.AggIdOfAvg, false, []*plan.Expr{colExpr(2, types.T_int64)}, nil)})
				g.SortRollup = true
				g.AppendChild(child)
				t.Cleanup(func() {
					g.Free(proc, false, nil)
					child.Free(proc, false, nil)
					proc.Free()
					require.Zero(t, proc.Mp().CurrNB())
				})
				first := batch.NewWithSize(3)
				first.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil, proc.Mp())
				first.Vecs[1] = testutil.MakeInt32Vector([]int32{7}, nil, proc.Mp())
				first.Vecs[2] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
				first.SetRowCount(1)
				key := int32(2)
				if sameGroup {
					key = 1
				}
				second := batch.NewWithSize(3)
				second.Vecs[0] = testutil.MakeInt32Vector([]int32{key}, nil, proc.Mp())
				second.Vecs[1] = testutil.MakeInt32Vector([]int32{3}, nil, proc.Mp())
				second.Vecs[1].SetPrepareParamKind(vector.PrepareParamFloat)
				second.Vecs[2] = testutil.MakeInt64Vector([]int64{3}, nil, proc.Mp())
				second.SetRowCount(1)
				child.WithBatchs([]*batch.Batch{first, second})
				require.NoError(t, g.Prepare(proc))
				require.True(t, g.ctr.sortRollup.reuseSingleGroup)
				if forceFallback {
					g.ctr.sortRollup.reuseSingleGroup = false
				}
				result, err := vm.Exec(g, proc)
				require.NoError(t, err)
				require.False(t, g.ctr.sortRollup.reuseSingleGroup)
				out := result.Batch
				if sameGroup {
					require.Equal(t, 2, out.RowCount())
					require.Equal(t, []int32{3, 3}, vector.MustFixedColNoTypeCheck[int32](out.Vecs[1]))
					require.Equal(t, []types.Decimal128{types.Decimal128FromInt64(10), types.Decimal128FromInt64(10)}, vector.MustFixedColNoTypeCheck[types.Decimal128](out.Vecs[2]))
					for row := 0; row < out.RowCount(); row++ {
						require.Equal(t, "5.0000", vector.GetFixedAtNoTypeCheck[types.Decimal128](out.Vecs[3], row).Format(out.Vecs[3].GetType().Scale))
					}
					require.Equal(t, vector.PrepareParamFloat, out.Vecs[1].GetPrepareParamKindAt(0))
					require.Equal(t, vector.PrepareParamFloat, out.Vecs[1].GetPrepareParamKindAt(1))
				} else {
					require.Equal(t, 3, out.RowCount())
					require.Equal(t, []int32{7, 3, 3}, vector.MustFixedColNoTypeCheck[int32](out.Vecs[1]))
					require.Equal(t, []types.Decimal128{types.Decimal128FromInt64(7), types.Decimal128FromInt64(3), types.Decimal128FromInt64(10)}, vector.MustFixedColNoTypeCheck[types.Decimal128](out.Vecs[2]))
					for row, want := range []string{"7.0000", "3.0000", "5.0000"} {
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](out.Vecs[3], row).Format(out.Vecs[3].GetType().Scale))
					}
					require.Equal(t, vector.PrepareParamNone, out.Vecs[1].GetPrepareParamKindAt(0))
					require.Equal(t, vector.PrepareParamFloat, out.Vecs[1].GetPrepareParamKindAt(1))
					require.Equal(t, vector.PrepareParamFloat, out.Vecs[1].GetPrepareParamKindAt(2))
				}
			})
		}
	}
}

func TestSortRollupMinDowngradeAtFullOutputBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)
	child := colexec.NewMockOperator()
	minAgg := aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMin, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{minAgg, aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{colExpr(2, types.T_int64)}, nil), aggexec.MakeAggFunctionExpression(aggexec.AggIdOfAvg, false, []*plan.Expr{colExpr(2, types.T_int64)}, nil)})
	g.SortRollup = true
	g.AppendChild(child)
	t.Cleanup(func() {
		g.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	keys := make([]int32, aggBatchSize)
	for i := range keys {
		keys[i] = int32(i)
	}
	first := batch.NewWithSize(3)
	first.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
	bigkeys := make([]int64, len(keys))
	for i := range keys {
		bigkeys[i] = int64(keys[i])
	}
	first.Vecs[2] = testutil.MakeInt64Vector(bigkeys, nil, proc.Mp())
	first.SetRowCount(len(keys))
	second := batch.NewWithSize(3)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{int32(aggBatchSize)}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{-1}, nil, proc.Mp())
	second.Vecs[1].SetPrepareParamKind(vector.PrepareParamFloat)
	second.Vecs[2] = testutil.MakeInt64Vector([]int64{-1}, nil, proc.Mp())
	second.SetRowCount(1)
	child.WithBatchs([]*batch.Batch{first, second})
	require.NoError(t, g.Prepare(proc))
	res, err := vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, aggBatchSize, res.Batch.RowCount())
	for i := range keys {
		require.Equal(t, int32(i), vector.GetFixedAtNoTypeCheck[int32](res.Batch.Vecs[1], i))
		require.Equal(t, types.Decimal128FromInt64(int64(i)), vector.GetFixedAtNoTypeCheck[types.Decimal128](res.Batch.Vecs[2], i))
		require.Equal(t, vector.PrepareParamNone, res.Batch.Vecs[1].GetPrepareParamKindAt(i))
	}
	require.False(t, g.ctr.sortRollup.reuseSingleGroup)
	res, err = vm.Exec(g, proc)
	require.NoError(t, err)
	require.Equal(t, 2, res.Batch.RowCount())
	require.Equal(t, []int32{-1, -1}, vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1]))
	require.Equal(t, types.Decimal128FromInt64(-1), vector.GetFixedAtNoTypeCheck[types.Decimal128](res.Batch.Vecs[2], 0))
	require.Equal(t, types.Decimal128FromInt64(int64(aggBatchSize*(aggBatchSize-1)/2-1)), vector.GetFixedAtNoTypeCheck[types.Decimal128](res.Batch.Vecs[2], 1))
	require.Equal(t, vector.PrepareParamFloat, res.Batch.Vecs[1].GetPrepareParamKindAt(0))
	res, err = vm.Exec(g, proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
}
