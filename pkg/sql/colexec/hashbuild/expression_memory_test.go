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

package hashbuild

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeExpressionLeaseTestExpr(t *testing.T, proc *process.Process) *plan.Expr {
	t.Helper()
	col := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"%",
		[]*plan.Expr{col, plan2.MakePlan2Int32ConstExprWithType(2)},
	)
	require.NoError(t, err)
	return expr
}

func makeExpressionLeaseTestBatch(proc *process.Process, rows int) *batch.Batch {
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)
	return bat
}

func makeIssue26454ConcatKey(t testing.TB, proc *process.Process) *plan.Expr {
	t.Helper()
	cast := func(colPos int32) *plan.Expr {
		col := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos}},
		}
		targetType := plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}
		expr, err := plan2.BindFuncExprImplByPlanExpr(
			proc.Ctx,
			"cast",
			[]*plan.Expr{
				col,
				{
					Typ:  targetType,
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		)
		require.NoError(t, err)
		return expr
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"concat",
		[]*plan.Expr{
			cast(0),
			plan2.MakePlan2StringConstExprWithType("-"),
			cast(1),
		},
	)
	require.NoError(t, err)
	return expr
}

func makeIssue26454CaseKey(t testing.TB, proc *process.Process) *plan.Expr {
	t.Helper()
	column := &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	condition, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"=",
		[]*plan.Expr{
			column,
			plan2.MakePlan2StringConstExprWithType("ATM_CON"),
		},
	)
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"case",
		[]*plan.Expr{
			condition,
			plan2.MakePlan2StringConstExprWithType("CON_CONTRACT_HEADERS"),
			plan2.MakePlan2StringConstExprWithType("CON_CONTRACT_DOC"),
		},
	)
	require.NoError(t, err)
	return expr
}

func TestAllocationAccountedExpressionIssue26454AndOneByteShort(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454ConcatKey(t, proc)
	require.True(t, expressionSetAllocationClosed([]*plan.Expr{expr}))
	require.False(t, expressionSetAllocationClosed(
		[]*plan.Expr{makeExpressionLeaseTestExpr(t, proc)},
	))
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{3, 4}, nil, proc.Mp())
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	run := func(limit uint64, verify bool) (uint64, error) {
		budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 64)
		require.NoError(t, err)
		account, err := registry.OpenWithController(limit, generation)
		require.NoError(t, err)
		allocation, err := colexec.NewExpressionAllocationAccount(
			account,
			HashBuildAllocationOwner,
		)
		require.NoError(t, err)
		executors, runErr := NewAllocationAccountedExpressionExecutors(
			proc,
			[]*plan.Expr{expr},
			allocation,
		)
		if runErr == nil {
			var result *vector.Vector
			result, runErr = executors[0].Eval(
				proc,
				[]*batch.Batch{input},
				nil,
			)
			if runErr == nil && verify {
				require.Equal(t, []string{"1-3", "2-4"},
					vector.InefficientMustStrCol(result))
			}
		}
		peak := account.Snapshot().Peak
		freeExpressionLeaseTestExecutors(executors)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, generation.Used())
		_, _, terminalErr := registry.CompleteTerminal(account)
		require.NoError(t, terminalErr)
		return peak, runErr
	}

	peak, err := run(1<<20, true)
	require.NoError(t, err)
	require.Positive(t, peak)
	_, err = run(peak-1, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Contains(t, err.Error(), "allocation owner=1 site=")
}

func TestAllocationAccountedExpressionIssue26454CaseKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454CaseKey(t, proc)
	require.True(t, expressionSetAllocationClosed([]*plan.Expr{expr}))
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(
		[]string{"ATM_CON", "OTHER"},
		nil,
		proc.Mp(),
	)
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, generation)
	require.NoError(t, err)
	allocation, err := colexec.NewExpressionAllocationAccount(
		account,
		HashBuildAllocationOwner,
	)
	require.NoError(t, err)
	executors, err := NewAllocationAccountedExpressionExecutors(
		proc,
		[]*plan.Expr{expr},
		allocation,
	)
	require.NoError(t, err)
	result, err := executors[0].Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t,
		[]string{"CON_CONTRACT_HEADERS", "CON_CONTRACT_DOC"},
		vector.InefficientMustStrCol(result),
	)
	require.Positive(t, account.Snapshot().Used)
	freeExpressionLeaseTestExecutors(executors)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestAllocationAccountedExpressionRealValueOverCapRollsBack(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	column := &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"concat",
		[]*plan.Expr{column, plan2.MakePlan2StringConstExprWithType("-suffix")},
	)
	require.NoError(t, err)
	require.True(t, expressionSetAllocationClosed([]*plan.Expr{expr}))

	const capBytes = uint64(4 << 10)
	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.OpenWithController(capBytes, generation)
	require.NoError(t, err)
	allocation, err := colexec.NewExpressionAllocationAccount(
		account,
		HashBuildAllocationOwner,
	)
	require.NoError(t, err)
	executors, err := NewAllocationAccountedExpressionExecutors(
		proc,
		[]*plan.Expr{expr},
		allocation,
	)
	require.NoError(t, err)

	eval := func(value string) (*vector.Vector, error) {
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeVarcharVector([]string{value}, nil, proc.Mp())
		input.SetRowCount(1)
		defer input.Clean(proc.Mp())
		return executors[0].Eval(proc, []*batch.Batch{input}, nil)
	}
	_, err = eval(strings.Repeat("x", 8<<10))
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Contains(t, err.Error(), "allocation owner=1 site=")
	result, err := eval("ok")
	require.NoError(t, err)
	require.Equal(t, []string{"ok-suffix"}, vector.InefficientMustStrCol(result))

	freeExpressionLeaseTestExecutors(executors)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestHashmapBuilderFallsBackOnlyForUnclosedExpressionScratch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	for _, tc := range []struct {
		name      string
		expr      *plan.Expr
		accounted bool
	}{
		{name: "closed concat cast", expr: makeIssue26454ConcatKey(t, proc), accounted: true},
		{name: "closed case equality", expr: makeIssue26454CaseKey(t, proc), accounted: true},
		{name: "unclosed modulo", expr: makeExpressionLeaseTestExpr(t, proc)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := process.MustNewHashBuildBudget(16<<20, 16<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			account, err := registry.OpenWithController(16<<20, generation)
			require.NoError(t, err)
			var op HashBuild
			op.NeedHashMap = true
			require.NoError(t, op.SetAllocationAccount(account))
			hb := &op.ctr.hashmapBuilder
			hb.setBudget(generation)
			require.NoError(t, hb.Prepare(
				[]*plan.Expr{tc.expr},
				-1,
				-1,
				nil,
				proc,
			))
			if tc.accounted {
				require.Nil(t, hb.expressionLease)
				require.Equal(t, generation.Used(), generation.Snapshot().AllocationUsed)
			} else {
				require.NotNil(t, hb.expressionLease)
			}
			hb.FreeExecutors()
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, generation.Used())
			require.NoError(t, op.ClearAllocationAccount(account))
			_, _, err = registry.CompleteTerminal(account)
			require.NoError(t, err)
		})
	}
}

func BenchmarkIssue26454ExpressionAccounting(b *testing.B) {
	const capBytes = uint64(8 << 30)
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454ConcatKey(b, proc)
	input := testutil.NewBatch(
		[]types.Type{types.T_int32.ToType(), types.T_int32.ToType()},
		true,
		colexec.DefaultBatchSize,
		proc.Mp(),
	)
	defer input.Clean(proc.Mp())

	b.Run("legacy", func(b *testing.B) {
		budget := process.MustNewHashBuildBudget(capBytes, capBytes)
		generation, err := budget.OpenGeneration(1)
		if err != nil {
			b.Fatal(err)
		}
		executors, lease, err := NewBudgetedExpressionExecutors(
			proc,
			generation,
			[]*plan.Expr{expr},
			false,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err = lease.Eval(
				proc,
				[]*batch.Batch{input},
				input.RowCount(),
				func(_ int, _ *vector.Vector) error { return nil },
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		freeExpressionLeaseTestExecutors(executors)
		lease.Release()
		if generation.Used() != 0 {
			b.Fatalf("generation used = %d", generation.Used())
		}
	})

	b.Run("accounted", func(b *testing.B) {
		budget := process.MustNewHashBuildBudget(capBytes, capBytes)
		generation, err := budget.OpenGeneration(1)
		if err != nil {
			b.Fatal(err)
		}
		registry, err := mpool.NewAllocationAccountRegistry(1, 64)
		if err != nil {
			b.Fatal(err)
		}
		account, err := registry.OpenWithController(capBytes, generation)
		if err != nil {
			b.Fatal(err)
		}
		allocation, err := colexec.NewExpressionAllocationAccount(
			account,
			HashBuildAllocationOwner,
		)
		if err != nil {
			b.Fatal(err)
		}
		executors, err := NewAllocationAccountedExpressionExecutors(
			proc,
			[]*plan.Expr{expr},
			allocation,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err = executors[0].Eval(
				proc,
				[]*batch.Batch{input},
				nil,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		freeExpressionLeaseTestExecutors(executors)
		if account.Snapshot().Used != 0 || generation.Used() != 0 {
			b.Fatalf(
				"live account=%d generation=%d",
				account.Snapshot().Used,
				generation.Used(),
			)
		}
		if _, _, err = registry.CompleteTerminal(account); err != nil {
			b.Fatal(err)
		}
	})
}

func BenchmarkIssue26454CaseExpressionAccounting(b *testing.B) {
	const capBytes = uint64(64 << 20)
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454CaseKey(b, proc)
	values := make([]string, colexec.DefaultBatchSize)
	for i := range values {
		if i%2 == 0 {
			values[i] = "ATM_CON"
		} else {
			values[i] = "OTHER"
		}
	}
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.MakeVarcharVector(values, nil, proc.Mp())
	input.SetRowCount(len(values))
	defer input.Clean(proc.Mp())

	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	if err != nil {
		b.Fatal(err)
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	if err != nil {
		b.Fatal(err)
	}
	account, err := registry.OpenWithController(capBytes, generation)
	if err != nil {
		b.Fatal(err)
	}
	allocation, err := colexec.NewExpressionAllocationAccount(
		account,
		HashBuildAllocationOwner,
	)
	if err != nil {
		b.Fatal(err)
	}
	executors, err := NewAllocationAccountedExpressionExecutors(
		proc,
		[]*plan.Expr{expr},
		allocation,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err = executors[0].Eval(
			proc,
			[]*batch.Batch{input},
			nil,
		); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	freeExpressionLeaseTestExecutors(executors)
	if account.Snapshot().Used != 0 || generation.Used() != 0 {
		b.Fatalf(
			"live account=%d generation=%d",
			account.Snapshot().Used,
			generation.Used(),
		)
	}
	if _, _, err = registry.CompleteTerminal(account); err != nil {
		b.Fatal(err)
	}
}

func makeMaxArrayLeaseTestVector[T types.ArrayElement](
	t *testing.T,
	proc *process.Process,
	oid types.T,
) *vector.Vector {
	t.Helper()
	typ := types.New(oid, types.MaxArrayDimension, 0)
	vec := vector.NewVec(typ)
	values := make([]T, types.MaxArrayDimension)
	require.NoError(t, vector.AppendArrayList(
		vec,
		[][]T{values, values},
		nil,
		proc.Mp(),
	))
	return vec
}

func evalExpressionLeaseTestExecutors(
	proc *process.Process,
	executors []colexec.ExpressionExecutor,
	bat *batch.Batch,
) error {
	for _, executor := range executors {
		if _, err := executor.Eval(proc, []*batch.Batch{bat}, nil); err != nil {
			return err
		}
	}
	return nil
}

func freeExpressionLeaseTestExecutors(executors []colexec.ExpressionExecutor) {
	for _, executor := range executors {
		executor.Free()
	}
}

func TestExpressionMemoryLeaseReusesRetainedHighWater(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)

	initialRetained, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
	require.True(t, ok)
	require.Positive(t, initialRetained)
	largePeak, err := expressionVectorPeak(proc, expr, colexec.DefaultBatchSize, false)
	require.NoError(t, err)
	budgetCap := initialRetained + largePeak
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)
	require.Equal(t, initialRetained, generation.Used())

	large := makeExpressionLeaseTestBatch(proc, colexec.DefaultBatchSize)
	defer large.Clean(proc.Mp())
	require.NoError(t, lease.Run(proc, large.RowCount(), func(_ int) error {
		return evalExpressionLeaseTestExecutors(proc, executors, large)
	}))
	require.Equal(t, largePeak, generation.Used())
	reservesAfterLarge := generation.ReserveCount()

	retainedAfterLarge, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
	require.True(t, ok)
	require.Greater(t, retainedAfterLarge, initialRetained)
	leaseRetained, ok := lease.Retained()
	require.True(t, ok)
	require.Equal(t, retainedAfterLarge, leaseRetained)
	require.GreaterOrEqual(t, lease.Reserved(), leaseRetained)

	small := makeExpressionLeaseTestBatch(proc, 1)
	defer small.Clean(proc.Mp())
	require.NoError(t, lease.Run(proc, small.RowCount(), func(_ int) error {
		return evalExpressionLeaseTestExecutors(proc, executors, small)
	}))
	require.Equal(t, reservesAfterLarge, generation.ReserveCount())
	require.Equal(t, largePeak, generation.Used())

	for _, executor := range executors {
		executor.ResetForNextQuery()
	}
	retainedAfterReset, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
	require.True(t, ok)
	require.Equal(t, retainedAfterLarge, retainedAfterReset)
	require.NoError(t, lease.Run(proc, large.RowCount(), func(_ int) error {
		return evalExpressionLeaseTestExecutors(proc, executors, large)
	}))
	require.Equal(t, reservesAfterLarge, generation.ReserveCount())

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestExpressionTypePeakUsesArrayElementWidth(t *testing.T) {
	for _, tc := range []struct {
		oid          types.T
		elementWidth uint64
	}{
		{oid: types.T_array_float64, elementWidth: 8},
		{oid: types.T_array_float32, elementWidth: 4},
		{oid: types.T_array_bf16, elementWidth: 2},
		{oid: types.T_array_float16, elementWidth: 2},
		{oid: types.T_array_int8, elementWidth: 1},
		{oid: types.T_array_uint8, elementWidth: 1},
	} {
		t.Run(tc.oid.String(), func(t *testing.T) {
			peak, err := expressionTypePeak(plan.Type{
				Id:    int32(tc.oid),
				Width: types.MaxArrayDimension,
			}, 1)
			require.NoError(t, err)
			require.Equal(
				t,
				uint64(types.MaxArrayDimension)*tc.elementWidth+32+(64<<10),
				peak,
			)
		})
	}
}

func TestExpressionMemoryLeaseCoversMaxNarrowArrayPayload(t *testing.T) {
	for _, tc := range []struct {
		oid       types.T
		makeInput func(*testing.T, *process.Process) *vector.Vector
	}{
		{
			oid: types.T_array_bf16,
			makeInput: func(t *testing.T, proc *process.Process) *vector.Vector {
				return makeMaxArrayLeaseTestVector[types.BF16](
					t, proc, types.T_array_bf16)
			},
		},
		{
			oid: types.T_array_float16,
			makeInput: func(t *testing.T, proc *process.Process) *vector.Vector {
				return makeMaxArrayLeaseTestVector[types.Float16](
					t, proc, types.T_array_float16)
			},
		},
	} {
		t.Run(tc.oid.String(), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			arrayType := plan.Type{
				Id:    int32(tc.oid),
				Width: types.MaxArrayDimension,
			}
			condition := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			}
			left := &plan.Expr{
				Typ:  arrayType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
			}
			right := &plan.Expr{
				Typ:  arrayType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}},
			}
			expr, err := plan2.BindFuncExprImplByPlanExpr(
				proc.Ctx,
				"iff",
				[]*plan.Expr{condition, left, right},
			)
			require.NoError(t, err)

			peak, err := expressionVectorPeak(proc, expr, 2, false)
			require.NoError(t, err)
			budget := process.MustNewHashBuildBudget(2*peak, 2*peak)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			executors, lease, err := NewBudgetedExpressionExecutors(
				proc,
				generation,
				[]*plan.Expr{expr},
				false,
			)
			require.NoError(t, err)

			input := batch.NewWithSize(3)
			input.Vecs[0] = testutil.MakeBoolVector(
				[]bool{true, false}, nil, proc.Mp())
			input.Vecs[1] = tc.makeInput(t, proc)
			input.Vecs[2] = tc.makeInput(t, proc)
			input.SetRowCount(2)
			require.NoError(t, lease.Eval(
				proc,
				[]*batch.Batch{input},
				input.RowCount(),
				func(_ int, _ *vector.Vector) error { return nil },
			))
			retained, ok := lease.Retained()
			require.True(t, ok)
			require.LessOrEqual(t, retained, lease.Reserved(),
				"retained max-width array payload must remain within admission")

			input.Clean(proc.Mp())
			freeExpressionLeaseTestExecutors(executors)
			lease.Release()
			require.Zero(t, generation.Used())
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestExpressionMemoryLeaseGrowthRequiresReplacementPeak(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	smallPeak, err := expressionVectorPeak(proc, expr, 1, false)
	require.NoError(t, err)
	largePeak, err := expressionVectorPeak(proc, expr, colexec.DefaultBatchSize, false)
	require.NoError(t, err)

	t.Run("reject before evaluation", func(t *testing.T) {
		executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
		require.NoError(t, err)
		retained, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
		require.True(t, ok)
		require.LessOrEqual(t, retained, smallPeak)
		budgetCap := retained + largePeak - 1
		budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
		generation, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		lease, err := NewExpressionMemoryLease(generation, []*plan.Expr{expr}, executors, false)
		require.NoError(t, err)

		require.NoError(t, lease.Run(proc, 1, func(_ int) error { return nil }))
		require.Equal(t, smallPeak, generation.Used())
		evaluated := false
		err = lease.Run(proc, colexec.DefaultBatchSize, func(_ int) error {
			evaluated = true
			return nil
		})
		require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
		require.False(t, evaluated)
		require.Equal(t, smallPeak, generation.Used())

		freeExpressionLeaseTestExecutors(executors)
		lease.Release()
		require.Zero(t, generation.Used())
	})

	t.Run("commit exact replacement peak", func(t *testing.T) {
		executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
		require.NoError(t, err)
		retained, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
		require.True(t, ok)
		require.LessOrEqual(t, retained, smallPeak)
		budgetCap := retained + largePeak
		budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
		generation, err := budget.OpenGeneration(2)
		require.NoError(t, err)
		lease, err := NewExpressionMemoryLease(generation, []*plan.Expr{expr}, executors, false)
		require.NoError(t, err)

		require.NoError(t, lease.Run(proc, 1, func(_ int) error { return nil }))
		require.NoError(t, lease.Run(proc, colexec.DefaultBatchSize, func(_ int) error { return nil }))
		require.Equal(t, largePeak, generation.Used())

		freeExpressionLeaseTestExecutors(executors)
		lease.Release()
		require.Zero(t, generation.Used())
	})
}

func TestExpressionMemoryLeaseGrowsRootsIndependently(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	exprs := []*plan.Expr{expr, expr}
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, exprs)
	require.NoError(t, err)

	smallPeak, err := expressionVectorPeak(proc, expr, 1, false)
	require.NoError(t, err)
	largePeak, err := expressionVectorPeak(proc, expr, colexec.DefaultBatchSize, false)
	require.NoError(t, err)
	require.Greater(t, largePeak, smallPeak)
	for _, executor := range executors {
		retained, ok := colexec.ExpressionExecutorRetainedBytes(executor)
		require.True(t, ok)
		require.LessOrEqual(t, retained, smallPeak)
	}

	// Sequential root growth peaks at old(root 2) + new(root 1) +
	// new(root 2). An aggregate replacement would incorrectly also charge
	// old(root 1) and reject this exact-cap admission.
	budgetCap := smallPeak + 2*largePeak
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(generation, exprs, executors, false)
	require.NoError(t, err)

	small := makeExpressionLeaseTestBatch(proc, 1)
	defer small.Clean(proc.Mp())
	require.NoError(t, lease.Eval(
		proc,
		[]*batch.Batch{small},
		small.RowCount(),
		func(_ int, _ *vector.Vector) error { return nil },
	))
	require.Equal(t, 2*smallPeak, generation.Used())
	large := makeExpressionLeaseTestBatch(proc, colexec.DefaultBatchSize)
	defer large.Clean(proc.Mp())
	require.NoError(t, lease.Eval(
		proc,
		[]*batch.Batch{large},
		large.RowCount(),
		func(_ int, _ *vector.Vector) error { return nil },
	))
	require.Equal(t, 2*largePeak, generation.Used())
	require.LessOrEqual(t, generation.Peak(), budgetCap)

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestExpressionMemoryLeaseCoversVariableWidthReuseOverlap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	col := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "lower", []*plan.Expr{col})
	require.NoError(t, err)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	peak, err := expressionVectorPeak(proc, expr, 1, false)
	require.NoError(t, err)

	budget := process.MustNewHashBuildBudget(2*peak, 2*peak)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(
		generation,
		[]*plan.Expr{expr},
		executors,
		false,
	)
	require.NoError(t, err)

	eval := func(value string) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = testutil.MakeVarcharVector([]string{value}, nil, proc.Mp())
		bat.SetRowCount(1)
		defer bat.Clean(proc.Mp())
		require.NoError(t, lease.Eval(
			proc,
			[]*batch.Batch{bat},
			1,
			func(_ int, _ *vector.Vector) error { return nil },
		))
	}
	eval("a")
	require.Equal(t, peak, generation.Used())
	retained, ok := lease.Retained()
	require.True(t, ok)
	require.Positive(t, retained)
	reserves := generation.ReserveCount()

	eval(strings.Repeat("b", 64<<10))
	require.Equal(t, reserves+1, generation.ReserveCount(),
		"same-row variable-width growth needs a transient overlap reservation")
	require.Equal(t, peak, generation.Used(),
		"transient overlap must not inflate the retained high-water charge")
	require.GreaterOrEqual(t, generation.Peak(), peak+retained)

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestExpressionMemoryLeaseCoversFlowControlSelectedScratch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	condition := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	makeLower := func(colPos int32) *plan.Expr {
		col := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos}},
		}
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "lower", []*plan.Expr{col})
		require.NoError(t, err)
		return expr
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"iff",
		[]*plan.Expr{condition, makeLower(1), makeLower(2)},
	)
	require.NoError(t, err)
	peak, err := expressionVectorPeak(proc, expr, 4, false)
	require.NoError(t, err)
	rootOutput, err := expressionTypePeak(expr.Typ, 4)
	require.NoError(t, err)
	expectedPeak := rootOutput
	for _, branch := range expr.GetF().Args[1:] {
		branchOutput, branchErr := expressionTypePeak(branch.Typ, 4)
		require.NoError(t, branchErr)
		selectedParameter, parameterErr := expressionTypePeak(branch.GetF().Args[0].Typ, 4)
		require.NoError(t, parameterErr)
		expectedPeak += branchOutput + branchOutput + selectedParameter
	}
	require.Equal(t, expectedPeak, peak,
		"flow-control branches need ordinary output, selected result, and selected parameter capacity")
	budget := process.MustNewHashBuildBudget(2*peak, 2*peak)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	executors, lease, err := NewBudgetedExpressionExecutors(
		proc,
		generation,
		[]*plan.Expr{expr},
		false,
	)
	require.NoError(t, err)

	eval := func(width int) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.MakeBoolVector(
			[]bool{true, false, true, false},
			nil,
			proc.Mp(),
		)
		left := []string{
			strings.Repeat("A", width),
			strings.Repeat("B", width),
			strings.Repeat("C", width),
			strings.Repeat("D", width),
		}
		right := []string{
			strings.Repeat("E", width),
			strings.Repeat("F", width),
			strings.Repeat("G", width),
			strings.Repeat("H", width),
		}
		bat.Vecs[1] = testutil.MakeVarcharVector(left, nil, proc.Mp())
		bat.Vecs[2] = testutil.MakeVarcharVector(right, nil, proc.Mp())
		bat.SetRowCount(4)
		defer bat.Clean(proc.Mp())
		require.NoError(t, lease.Eval(
			proc,
			[]*batch.Batch{bat},
			bat.RowCount(),
			func(_ int, _ *vector.Vector) error { return nil },
		))
	}
	eval(8)
	retained, ok := lease.Retained()
	require.True(t, ok)
	require.LessOrEqual(t, retained, lease.Reserved(),
		"selected result and parameter scratch must be covered by the admitted peak")
	eval(4 << 10)
	retained, ok = lease.Retained()
	require.True(t, ok)
	require.LessOrEqual(t, retained, lease.Reserved())

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestBudgetedExpressionConstructionAdmitsBeforeMpoolAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer proc.Free()
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: strings.Repeat("x", 64<<10)},
		}},
	}
	initial, err := expressionInitialOwnedBytes(expr)
	require.NoError(t, err)
	require.Positive(t, initial)

	budget := process.MustNewHashBuildBudget(initial-1, initial-1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	epoch := mp.StartResourcePeakEpoch()
	executors, lease, err := NewBudgetedExpressionExecutors(
		proc,
		generation,
		[]*plan.Expr{expr},
		false,
	)
	peak, ok := mp.EndResourcePeakEpoch(epoch)
	require.True(t, ok)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetAdmission)
	require.Nil(t, executors)
	require.Nil(t, lease)
	require.Zero(t, peak, "budget rejection must happen before constructing the literal vector")
	require.Zero(t, generation.Used())
	require.Zero(t, mp.CurrNB())
}

func TestExpressionMemoryLeaseRetainsFailedEvaluationBound(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	retained, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
	require.True(t, ok)
	peak, err := expressionVectorPeak(proc, expr, 32, false)
	require.NoError(t, err)
	budgetCap := retained + peak
	budget := process.MustNewHashBuildBudget(budgetCap, budgetCap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)

	wantErr := errors.New("expression evaluation failed")
	require.ErrorIs(t, lease.Run(proc, 32, func(_ int) error {
		return wantErr
	}), wantErr)
	require.Equal(t, peak, generation.Used(),
		"a failed evaluator may retain partially grown buffers")

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestExpressionMemoryLeaseDoesNotShrinkAdoptedCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)

	large := makeExpressionLeaseTestBatch(proc, colexec.DefaultBatchSize*8)
	defer large.Clean(proc.Mp())
	require.NoError(t, evalExpressionLeaseTestExecutors(proc, executors, large))
	retained, ok := colexec.ExpressionExecutorsRetainedBytes(executors)
	require.True(t, ok)
	require.Positive(t, retained)

	smallPeak, err := expressionVectorPeak(proc, expr, 1, false)
	require.NoError(t, err)
	require.Less(t, smallPeak, retained)
	budget := process.MustNewHashBuildBudget(retained, retained)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(
		generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)
	reservesAfterAdoption := generation.ReserveCount()

	small := makeExpressionLeaseTestBatch(proc, 1)
	defer small.Clean(proc.Mp())
	require.NoError(t, lease.Run(proc, small.RowCount(), func(_ int) error {
		return evalExpressionLeaseTestExecutors(proc, executors, small)
	}))
	require.Equal(t, reservesAfterAdoption, generation.ReserveCount())
	require.Equal(t, retained, generation.Used())

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	require.Zero(t, generation.Used())
}

func TestExpressionMemoryLeaseRejectsUnknownExecutorOwnership(t *testing.T) {
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	_, err = NewExpressionMemoryLease(
		generation,
		[]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int32)}}},
		[]colexec.ExpressionExecutor{unknownExpressionLeaseExecutor{}},
		false,
	)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	require.Zero(t, generation.Used())
}

func TestExpressionMemoryLeaseRejectsInvalidCalls(t *testing.T) {
	_, err := NewExpressionMemoryLease(
		nil,
		[]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int32)}}},
		nil,
		false,
	)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)

	var nilLease *ExpressionMemoryLease
	require.ErrorIs(t,
		nilLease.Run(nil, 0, func(int) error { return nil }),
		process.ErrHashBuildBudgetInvalid,
	)
	require.ErrorIs(t,
		nilLease.Run(nil, 0, nil),
		process.ErrHashBuildBudgetInvalid,
	)
	require.ErrorIs(t,
		nilLease.Eval(nil, nil, 0, nil),
		process.ErrHashBuildBudgetInvalid,
	)
	require.Zero(t, nilLease.Reserved())
	require.Zero(t, nilLease.Len())
	retained, ok := nilLease.Retained()
	require.True(t, ok)
	require.Zero(t, retained)
	nilLease.Release()

	emptyLease := &ExpressionMemoryLease{}
	require.ErrorIs(t,
		emptyLease.Run(nil, -1, func(int) error { return nil }),
		process.ErrHashBuildBudgetInvalid,
	)
}

func TestExpressionMemoryAccountingHelperBoundaries(t *testing.T) {
	size, err := expressionInitialOwnedBytes(nil)
	require.ErrorIs(t, err, process.ErrHashBuildBudgetInvalid)
	require.Zero(t, size)

	size, err = literalInitialOwnedBytes(types.T_int32, &plan.Literal{})
	require.NoError(t, err)
	require.Zero(t, size)

	require.True(t, expressionExecutorMayGrowWithinBound(nil))
	require.True(t, expressionExecutorMayGrowWithinBound(&plan.Expr{}))

	size, err = initialAllocationCapacity(0)
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestExpressionMemoryLeaseEvalPropagatesExecutorError(t *testing.T) {
	expected := errors.New("expression evaluation failed")
	executor := failingExpressionLeaseExecutor{err: expected}
	lease, err := NewExpressionMemoryLease(
		nil,
		[]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int32)}}},
		[]colexec.ExpressionExecutor{executor},
		false,
	)
	require.NoError(t, err)
	defer lease.Release()

	consumed := false
	err = lease.Eval(nil, nil, 0, func(int, *vector.Vector) error {
		consumed = true
		return nil
	})
	require.ErrorIs(t, err, expected)
	require.False(t, consumed)
}

func TestExpressionMemoryLeaseReleaseIsTerminal(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := NewExpressionMemoryLease(
		generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)

	freeExpressionLeaseTestExecutors(executors)
	lease.Release()
	lease.Release()
	require.Zero(t, lease.Reserved())
	_, ok := lease.Retained()
	require.False(t, ok)
	require.Zero(t, generation.Used())

	called := false
	err = lease.Run(proc, 1, func(_ int) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, process.ErrHashBuildReservationInactive)
	require.False(t, called)
	require.Zero(t, generation.Used())

	executors, err = colexec.NewExpressionExecutorsFromPlanExpressions(
		proc, []*plan.Expr{expr})
	require.NoError(t, err)
	lease, err = NewExpressionMemoryLease(
		generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)
	require.NoError(t, lease.Run(proc, 1, func(_ int) error {
		freeExpressionLeaseTestExecutors(executors)
		lease.Release()
		return nil
	}))
	require.Zero(t, generation.Used(),
		"release during evaluation must also discard the pending replacement")
}

func TestExpressionMemoryLeaseCancellationAndRepeatedGenerations(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeExpressionLeaseTestExpr(t, proc)
	budget := process.MustNewHashBuildBudget(8<<20, 8<<20)

	runGeneration := func(id uint64, callbackErr error) {
		executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(
			proc, []*plan.Expr{expr})
		require.NoError(t, err)
		generation, err := budget.OpenGeneration(id)
		require.NoError(t, err)
		lease, err := NewExpressionMemoryLease(
			generation, []*plan.Expr{expr}, executors, false)
		require.NoError(t, err)
		input := makeExpressionLeaseTestBatch(proc, 128)

		err = lease.Run(proc, input.RowCount(), func(_ int) error {
			if evalErr := evalExpressionLeaseTestExecutors(
				proc, executors, input); evalErr != nil {
				return evalErr
			}
			return callbackErr
		})
		if callbackErr != nil {
			require.ErrorIs(t, err, callbackErr)
		} else {
			require.NoError(t, err)
		}
		require.Positive(t, generation.Used())

		input.Clean(proc.Mp())
		freeExpressionLeaseTestExecutors(executors)
		lease.Release()
		generation.Close()
		require.Zero(t, generation.Used())
		require.Zero(t, proc.Mp().CurrNB())
	}

	runGeneration(1, context.Canceled)
	runGeneration(2, nil)
}

type unknownExpressionLeaseExecutor struct{}

func (unknownExpressionLeaseExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}
func (unknownExpressionLeaseExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}
func (unknownExpressionLeaseExecutor) ResetForNextQuery() {}
func (unknownExpressionLeaseExecutor) Free()              {}
func (unknownExpressionLeaseExecutor) IsColumnExpr() bool { return false }
func (unknownExpressionLeaseExecutor) TypeName() string   { return "unknown" }

type failingExpressionLeaseExecutor struct {
	unknownExpressionLeaseExecutor
	err error
}

func (f failingExpressionLeaseExecutor) Eval(
	*process.Process,
	[]*batch.Batch,
	[]bool,
) (*vector.Vector, error) {
	return nil, f.err
}
