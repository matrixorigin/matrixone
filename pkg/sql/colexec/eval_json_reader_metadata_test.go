// Copyright 2026 Matrix Origin
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

package colexec

import (
	"io"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONProjectionPreparationWithoutDemandDoesNotAllocate(t *testing.T) {
	leaf := &FunctionExpressionExecutor{}
	nodes := make([]*FunctionExpressionExecutor, 0, 81)
	nodes = append(nodes, leaf)
	var root ExpressionExecutor = leaf
	for range 80 {
		node := &FunctionExpressionExecutor{parameterExecutor: []ExpressionExecutor{root, root}}
		nodes = append(nodes, node)
		root = node
	}
	roots := []ExpressionExecutor{root}
	allocations := testing.AllocsPerRun(10, func() {
		inputs, err := PrepareJSONProjectionInputs(nil, roots)
		if err != nil || len(inputs) != 0 {
			panic("unrelated projection prepared JSON inputs")
		}
	})
	require.Zero(t, allocations)
	allocations = testing.AllocsPerRun(10, func() {
		if err := PrepareJSONProjectionExecutors(nil, roots, nil); err != nil {
			panic("unrelated projection prepared JSON state")
		}
	})
	require.Zero(t, allocations)
	for _, node := range nodes {
		require.False(t, node.jsonProjectionProbe)
	}
}

func TestJSONProjectionMetadataReleaseAndFailureRollback(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	column := &ColumnExpressionExecutor{typ: types.T_json.ToType(), relIndex: 0, colIndex: 0}
	jsonFn := &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.JSON_VALUE}, parameterExecutor: []ExpressionExecutor{column}}
	inputs, err := PrepareJSONProjectionInputs(proc, []ExpressionExecutor{jsonFn})
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Greater(t, budget.Snapshot().Used, before)
	require.IsType(t, &JSONReaderExpressionExecutor{}, jsonFn.parameterExecutor[0])
	inputs.Close()
	inputs.Close()
	require.Same(t, column, jsonFn.parameterExecutor[0])
	require.Equal(t, before, budget.Snapshot().Used)
	inputs, err = PrepareJSONProjectionInputs(proc, []ExpressionExecutor{jsonFn})
	require.NoError(t, err)
	inputs.Close()
	require.Equal(t, before, budget.Snapshot().Used)
	roots := []ExpressionExecutor{jsonFn}
	require.NoError(t, PrepareJSONProjectionExecutors(proc, roots, nil))
	require.NotSame(t, jsonFn, roots[0])
	require.Equal(t, []int{0}, JSONProjectionColumns(roots))
	require.Greater(t, budget.Snapshot().Used, before)
	CloseJSONProjectionExecutors(roots)
	CloseJSONProjectionExecutors(roots)
	require.Same(t, jsonFn, roots[0])
	require.Equal(t, before, budget.Snapshot().Used)

	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	capacity, err := vector.JSONReaderContainerMetadataCapacity(unsafe.Sizeof(JSONMetadataScope{}), 4)
	require.NoError(t, err)
	account, err := registry.Open(capacity)
	require.NoError(t, err)
	defer func() {
		require.Zero(t, account.Seal().Used)
		require.Zero(t, registry.LiveAllocationMetadata())
		_, e := registry.Finalize(account)
		require.NoError(t, e)
	}()
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	root := &FunctionExpressionExecutor{parameterExecutor: []ExpressionExecutor{jsonFn, &FunctionExpressionExecutor{}, &FunctionExpressionExecutor{}, &FunctionExpressionExecutor{}}}
	inputs, err = PrepareJSONProjectionInputs(proc, []ExpressionExecutor{root}, selection)
	require.Error(t, err)
	require.Nil(t, inputs)
	require.Same(t, column, jsonFn.parameterExecutor[0], "failed preparation must restore the original executor")
	require.Zero(t, account.Snapshot().Used)
}

func TestJSONProjectionFindsNestedConstructorInput(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	column := &ColumnExpressionExecutor{typ: types.T_json.ToType(), relIndex: 0, colIndex: 0}
	inner := &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.JSON_VALUE}, parameterExecutor: []ExpressionExecutor{column}}
	constructor := &jsonConstructorExpressionExecutor{FunctionExpressionExecutor: &FunctionExpressionExecutor{parameterExecutor: []ExpressionExecutor{inner}}}
	outer := &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.JSON_VALUE}, parameterExecutor: []ExpressionExecutor{constructor}}
	roots := []ExpressionExecutor{outer}
	require.NoError(t, PrepareJSONProjectionExecutors(proc, roots, nil))
	require.Equal(t, []int{0}, JSONProjectionColumns(roots))
	require.IsType(t, &JSONReaderExpressionExecutor{}, inner.parameterExecutor[0])
	CloseJSONProjectionExecutors(roots)
	require.Same(t, column, inner.parameterExecutor[0])
	require.Equal(t, before, budget.Snapshot().Used)
}

func TestJSONMetadataScopeGrowthAndClose(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	scope, err := NewJSONMetadataScope(proc, nil, 1)
	require.NoError(t, err)
	require.Greater(t, budget.Snapshot().Used, before)
	first := budget.Snapshot().Used
	require.NoError(t, scope.Ensure(5))
	require.Greater(t, budget.Snapshot().Used, first)
	scope.Close()
	scope.Close()
	require.Equal(t, before, budget.Snapshot().Used)
	require.ErrorIs(t, scope.Ensure(1), io.ErrClosedPipe)
}

func TestJSONLiteralExecutorMetadataLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	doc, err := types.ParseStringToByteJson(`{"keep":5}`)
	require.NoError(t, err)
	raw, err := doc.Marshal()
	require.NoError(t, err)
	literal := &plan.Literal{Value: &plan.Literal_Sval{Sval: string(raw)}}
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used

	source, err := newJSONLiteralExpressionExecutor(proc, literal, nil)
	require.NoError(t, err)
	require.Greater(t, budget.Snapshot().Used, before)
	first := source.reader
	source.ResetForNextQuery()
	require.Equal(t, before, budget.Snapshot().Used)
	require.Nil(t, source.release)
	second, err := source.constantJSONReader()
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Greater(t, budget.Snapshot().Used, before)
	source.Free()
	source.Free()
	require.Equal(t, before, budget.Snapshot().Used)

	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	capacity := vector.JSONReaderMetadataCapacity(unsafe.Sizeof(jsonLiteralExpressionExecutor{}))
	account, err := registry.Open(capacity - 1)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	source, err = newJSONLiteralExpressionExecutor(proc, literal, selection)
	require.Error(t, err)
	require.Nil(t, source)
	require.Zero(t, account.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Equal(t, before, budget.Snapshot().Used)

	// Reacquisition may succeed for the executor but fail for its new reader.
	// The failed version must not retain the partial executor reservation.
	source, err = newJSONLiteralExpressionExecutor(proc, literal, nil)
	require.NoError(t, err)
	source.ResetForNextQuery()
	account, err = registry.Open(capacity)
	require.NoError(t, err)
	source.allocation, err = vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	_, err = source.constantJSONReader()
	require.Error(t, err)
	require.Nil(t, source.release)
	require.Zero(t, account.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	source.Free()
	require.Equal(t, before, budget.Snapshot().Used)
}

func TestJSONConstructorExecutorMetadataLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := budget.Snapshot().Used
	expr := NewFunctionExpressionExecutor()
	source, err := newJSONConstructorExpressionExecutor(proc, expr, nil)
	require.NoError(t, err)
	require.Greater(t, budget.Snapshot().Used, before)
	source.ResetForNextQuery()
	require.Equal(t, before, budget.Snapshot().Used)
	require.NoError(t, source.ensureMetadata())
	require.Greater(t, budget.Snapshot().Used, before)
	source.Free()
	source.Free()
	require.Equal(t, before, budget.Snapshot().Used)

	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	capacity := vector.JSONReaderMetadataCapacity(unsafe.Sizeof(jsonConstructorExpressionExecutor{}))
	account, err := registry.Open(capacity - 1)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	expr = NewFunctionExpressionExecutor()
	source, err = newJSONConstructorExpressionExecutor(proc, expr, selection)
	require.Error(t, err)
	require.Nil(t, source)
	expr.Free()
	require.Zero(t, account.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.Equal(t, before, budget.Snapshot().Used)
}

func TestBoundJSONTextEvaluationBorrowsExistingReader(t *testing.T) {
	proc := testutil.NewProc(t)
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	var live uint64
	calls := 0
	admission := func(capacity uint64) (func(), error) {
		calls++
		live += capacity
		return func() { live -= capacity }, nil
	}
	builder, err := vector.NewJSONReaderBuilder(proc.Mp(), nil, admission)
	require.NoError(t, err)
	defer builder.Close()
	doc, err := types.ParseStringToByteJson(`{"keep":"owned text"}`)
	require.NoError(t, err)
	require.NoError(t, builder.AppendByteJson(doc, false))
	reader, err := builder.Finish()
	require.NoError(t, err)
	source, err := NewJSONReaderExpressionExecutor(proc, reader)
	require.NoError(t, err)
	reader.Close()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
	defer bat.Clean(proc.Mp())
	bat.SetRowCount(1)
	column := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	path := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "$.keep"}}}}
	executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "json_value", column, path))
	require.NoError(t, err)
	defer executor.Free()
	fn := executor.(*FunctionExpressionExecutor)
	fn.parameterExecutor[0].Free()
	fn.SetParameter(0, source)
	before := calls
	out, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.Equal(t, "owned text", string(out.GetBytesAt(0)))
	require.Equal(t, before, calls, "a live binding needs no temporary window or cursor")
	source.ResetForNextQuery()
	require.Zero(t, live)
	require.Equal(t, "owned text", string(out.GetBytesAt(0)))
}
