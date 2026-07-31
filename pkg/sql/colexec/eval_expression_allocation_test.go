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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testExpressionAllocationAccount struct {
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
	allocation *ExpressionAllocationAccount
}

func newTestExpressionAllocationAccount(
	t testing.TB,
	limit uint64,
	metadataSlots uint64,
) testExpressionAllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, metadataSlots)
	require.NoError(t, err)
	account, err := registry.Open(limit)
	require.NoError(t, err)
	allocation, err := NewExpressionAllocationAccount(account, 1)
	require.NoError(t, err)
	return testExpressionAllocationAccount{
		registry:   registry,
		account:    account,
		allocation: allocation,
	}
}

func finalizeTestExpressionAllocationAccount(
	t testing.TB,
	state testExpressionAllocationAccount,
) {
	t.Helper()
	snapshot := state.account.Seal()
	require.Zero(t, snapshot.Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	_, err := state.registry.Finalize(state.account)
	require.NoError(t, err)
}

func expressionAllocationColumn(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(typ.Oid),
			Width: typ.Width,
			Scale: typ.Scale,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: pos},
		},
	}
}

func expressionAllocationString(value string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_varchar),
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: value},
		}},
	}
}

func expressionAllocationFunction(
	t testing.TB,
	proc *process.Process,
	name string,
	args ...*plan.Expr,
) *plan.Expr {
	t.Helper()
	argTypes := make([]types.Type, len(args))
	for i := range args {
		argTypes[i] = types.New(
			types.T(args[i].Typ.Id),
			args[i].Typ.Width,
			args[i].Typ.Scale,
		)
	}
	fn, err := function.GetFunctionByName(proc.Ctx, name, argTypes)
	require.NoError(t, err)
	retType := fn.GetReturnType()
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(retType.Oid),
			Width: retType.Width,
			Scale: retType.Scale,
		},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{
				Obj:     fn.GetEncodedOverloadID(),
				ObjName: name,
			},
			Args: args,
		}},
	}
}

func expressionAllocationCast(
	t testing.TB,
	proc *process.Process,
	source *plan.Expr,
	targetType types.Type,
) *plan.Expr {
	t.Helper()
	target := &plan.Expr{
		Typ: plan.Type{
			Id:          int32(targetType.Oid),
			Width:       targetType.Width,
			Scale:       targetType.Scale,
			NotNullable: true,
		},
		Expr: &plan.Expr_T{T: &plan.TargetType{}},
	}
	return expressionAllocationFunction(t, proc, "cast", source, target)
}

func TestExpressionAllocationAccountNestedSelectedLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-lifecycle"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 64<<20, 128)

	input := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			4,
			types.T_bool.ToType(),
			proc.Mp(),
			false,
			[]bool{true, false, true, false},
		),
		testutil.NewVector(
			4,
			types.T_varchar.ToType(),
			proc.Mp(),
			false,
			[]string{"a", "b", "c", "d"},
		),
		testutil.NewVector(
			4,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{10, 20, 30, 40},
		),
	}, nil)
	defer input.Clean(proc.Mp())

	thenExpr := expressionAllocationFunction(
		t,
		proc,
		"concat",
		expressionAllocationCast(
			t,
			proc,
			expressionAllocationColumn(2, types.T_int64.ToType()),
			types.T_varchar.ToType(),
		),
		expressionAllocationString("-then-payload-longer-than-inline"),
	)
	elseExpr := expressionAllocationFunction(
		t,
		proc,
		"concat",
		expressionAllocationColumn(1, types.T_varchar.ToType()),
		expressionAllocationString("-else-payload-longer-than-inline"),
	)
	caseExpr := expressionAllocationFunction(
		t,
		proc,
		"case",
		expressionAllocationColumn(0, types.T_bool.ToType()),
		thenExpr,
		elseExpr,
	)
	executor, err := NewExpressionExecutorWithAllocation(
		proc,
		caseExpr,
		state.allocation,
	)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)

	result, err := executor.Eval(
		proc,
		[]*batch.Batch{input},
		[]bool{true, false, true, false},
	)
	require.NoError(t, err)
	require.NotNil(t, result.AllocationAccountSelection())
	require.Equal(t, "10-then-payload-longer-than-inline", result.GetStringAt(0))
	require.True(t, result.IsNull(1))
	require.Equal(t, "30-then-payload-longer-than-inline", result.GetStringAt(2))
	require.True(t, result.IsNull(3))

	root := executor.(*FunctionExpressionExecutor)
	require.GreaterOrEqual(t, cap(root.selectList1), input.RowCount())
	require.GreaterOrEqual(t, cap(root.selectList2), input.RowCount())
	require.GreaterOrEqual(t, cap(root.selectedRows), input.RowCount())
	require.NotNil(t, root.selectedResult)
	require.NotNil(t, root.selectedResult.GetResultVector())
	require.NotNil(
		t,
		root.selectedResult.GetResultVector().AllocationAccountSelection(),
	)
	usedAfterPartial := state.account.Snapshot().Used

	executor.ResetForNextQuery()
	result, err = executor.Eval(
		proc,
		[]*batch.Batch{input},
		[]bool{true, false, true, false},
	)
	require.NoError(t, err)
	require.Equal(t, usedAfterPartial, state.account.Snapshot().Used)

	transferred, err := executor.EvalWithoutResultReusing(
		proc,
		[]*batch.Batch{input},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, transferred.AllocationAccountSelection())
	executor.Free()
	require.Positive(t, state.account.Snapshot().Used)
	transferred.Free(proc.Mp())
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountConstantKinds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-constants"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 1<<20, 8)

	fixed := &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: 42},
		}},
	}
	null := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
	}
	executors, err :=
		NewExpressionExecutorsFromPlanExpressionsWithAllocation(
			proc,
			[]*plan.Expr{fixed, null},
			state.allocation,
		)
	require.NoError(t, err)
	require.Positive(t, state.account.Snapshot().Used)
	for _, executor := range executors {
		fixedExecutor := executor.(*FixedVectorExpressionExecutor)
		require.NotNil(
			t,
			fixedExecutor.resultVector.AllocationAccountSelection(),
		)
		_, err = executor.Eval(
			proc,
			[]*batch.Batch{batch.EmptyForConstFoldBatch},
			nil,
		)
		require.NoError(t, err)
	}
	for _, executor := range executors {
		executor.Free()
	}
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountDecodedVectorTransfer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-decoded-vector"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 1<<20, 8)

	source := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(proc.Mp())

	executor, err := NewExpressionExecutorWithAllocation(
		proc,
		&plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
				Len:  3,
				Data: data,
			}},
		},
		state.allocation,
	)
	require.NoError(t, err)
	fixed := executor.(*FixedVectorExpressionExecutor)
	require.NotNil(
		t,
		fixed.resultVector.AllocationAccountSelection(),
	)
	require.False(t, fixed.resultVector.NeedDup())
	require.Positive(t, state.account.Snapshot().Used)

	transferred, err := executor.EvalWithoutResultReusing(
		proc,
		[]*batch.Batch{batch.EmptyForConstFoldBatch},
		nil,
	)
	require.NoError(t, err)
	executor.Free()
	require.Positive(t, state.account.Snapshot().Used)
	transferred.Free(proc.Mp())
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountFoldedTransfer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-folded-transfer"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 1<<20, 16)

	expr := expressionAllocationFunction(
		t,
		proc,
		"concat",
		expressionAllocationString("folded-payload-longer-than-inline"),
		expressionAllocationString("-suffix"),
	)
	executor, err := NewExpressionExecutorWithAllocation(
		proc,
		expr,
		state.allocation,
	)
	require.NoError(t, err)

	transferred, err := executor.EvalWithoutResultReusing(
		proc,
		[]*batch.Batch{batch.EmptyForConstFoldBatch},
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		"folded-payload-longer-than-inline-suffix",
		transferred.GetStringAt(0),
	)
	require.NotNil(t, transferred.AllocationAccountSelection())
	executor.Free()
	require.Positive(t, state.account.Snapshot().Used)
	transferred.Free(proc.Mp())
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountConstructionRollback(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-construction"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 1<<20, 1)

	expr := expressionAllocationFunction(
		t,
		proc,
		"concat",
		expressionAllocationString("left"),
		expressionAllocationString("right"),
	)
	_, err := NewExpressionExecutorWithAllocation(
		proc,
		expr,
		state.allocation,
	)
	require.ErrorIs(t, err, mpool.ErrAllocationMetadataSlots)
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountScratchFailureCleanup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(
		t,
		"",
		mpool.MustNew("expression-allocation-scratch-failure"),
	)
	defer proc.Free()
	state := newTestExpressionAllocationAccount(t, 8, 8)

	input := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			8,
			types.T_bool.ToType(),
			proc.Mp(),
			false,
			[]bool{true, false, true, false, true, false, true, false},
		),
		testutil.NewVector(
			8,
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		),
	}, nil)
	defer input.Clean(proc.Mp())

	expr := expressionAllocationFunction(
		t,
		proc,
		"case",
		expressionAllocationColumn(0, types.T_bool.ToType()),
		expressionAllocationColumn(1, types.T_int64.ToType()),
		expressionAllocationColumn(1, types.T_int64.ToType()),
	)
	executor, err := NewExpressionExecutorWithAllocation(
		proc,
		expr,
		state.allocation,
	)
	require.NoError(t, err)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, uint64(8), state.account.Snapshot().Used)

	executor.Free()
	require.Zero(t, state.account.Snapshot().Used)
	require.Zero(t, state.registry.LiveAllocationMetadata())
	finalizeTestExpressionAllocationAccount(t, state)
}

func TestExpressionAllocationAccountZeroLengthScratchGrowth(t *testing.T) {
	mp := mpool.MustNew("expression-allocation-zero-length-scratch")
	defer mpool.DeleteMPool(mp)
	state := newTestExpressionAllocationAccount(t, 1<<20, 4)

	values, err := ensureExpressionSlice(
		[]int64(nil),
		4,
		mp,
		state.allocation,
		ExpressionAllocationSiteSelectedRows,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(32), state.account.Snapshot().Used)

	values = values[:0]
	values, err = ensureExpressionSlice(
		values,
		8,
		mp,
		state.allocation,
		ExpressionAllocationSiteSelectedRows,
	)
	require.NoError(t, err)
	require.Len(t, values, 8)
	require.Equal(t, uint64(64), state.account.Snapshot().Used)

	values = values[:0]
	freeExpressionSlice(values, mp, state.allocation)
	require.Zero(t, state.account.Snapshot().Used)
	finalizeTestExpressionAllocationAccount(t, state)
}
