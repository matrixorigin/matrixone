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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestExpressionExecutorRetainedBytesExcludesBorrowedPlanVector(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	source := testutil.MakeInt32Vector([]int32{1, 2, 3}, nil, proc.Mp())
	data, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())

	executor, err := NewExpressionExecutor(proc, &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{
			Len:  3,
			Data: data,
		}},
	})
	require.NoError(t, err)
	fixed, ok := executor.(*FixedVectorExpressionExecutor)
	require.True(t, ok)
	require.True(t, fixed.resultVector.NeedDup())
	require.Positive(t, fixed.resultVector.Allocated())

	retained, known := ExpressionExecutorRetainedBytes(executor)
	require.True(t, known)
	require.Zero(t, retained)
	executor.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestExpressionExecutorRetainedBytesCoversOwnedExecutorTree(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	newVec := func(values ...int32) *vector.Vector {
		return testutil.MakeInt32Vector(values, nil, proc.Mp())
	}

	fixedVec := newVec(1)
	listResult := newVec(1, 2)
	paramNull := newVec(1)
	paramMaskedNull := newVec(2)
	paramVec := newVec(3)
	varNull := newVec(4)
	varMaskedNull := newVec(5)
	varVec := newVec(6)
	selectedParameter := newVec(7, 8)
	selectedNull := vector.NewConstNull(types.T_int32.ToType(), 2, proc.Mp())
	iffNull0 := vector.NewConstNull(types.T_int32.ToType(), 2, proc.Mp())
	iffNull1 := vector.NewConstNull(types.T_int32.ToType(), 2, proc.Mp())
	ownedVectors := []*vector.Vector{
		fixedVec,
		listResult,
		paramNull,
		paramMaskedNull,
		paramVec,
		varNull,
		varMaskedNull,
		varVec,
		selectedParameter,
		selectedNull,
		iffNull0,
		iffNull1,
	}

	result := vector.NewFunctionResultWrapper(types.T_int32.ToType(), proc.Mp())
	require.NoError(t, result.PreExtendAndReset(4))
	selectedResult := vector.NewFunctionResultWrapper(types.T_int32.ToType(), proc.Mp())
	require.NoError(t, selectedResult.PreExtendAndReset(2))

	fixed := &FixedVectorExpressionExecutor{resultVector: fixedVec}
	list := &ListExpressionExecutor{
		resultVector:      listResult,
		parameterExecutor: []ExpressionExecutor{fixed},
	}
	param := &ParamExpressionExecutor{
		null:       paramNull,
		maskedNull: paramMaskedNull,
		vec:        paramVec,
	}
	variable := &VarExpressionExecutor{
		null:       varNull,
		maskedNull: varMaskedNull,
		vec:        varVec,
	}
	function := &FunctionExpressionExecutor{
		resultVector:             result,
		selectedResult:           selectedResult,
		selectedNullResult:       selectedNull,
		selectedParameterVectors: []*vector.Vector{selectedParameter},
		parameterExecutor:        []ExpressionExecutor{param},
		iffNullResults:           [2]*vector.Vector{iffNull0, iffNull1},
	}
	column := &ColumnExpressionExecutor{
		nullVecCache: vector.NewConstNull(types.T_int32.ToType(), 2, proc.Mp()),
	}

	executors := []ExpressionExecutor{list, variable, function, column, nil}
	retained, known := ExpressionExecutorsRetainedBytes(executors)
	require.True(t, known)
	require.Positive(t, retained)
	for _, executor := range executors {
		_, known = ExpressionExecutorRetainedBytes(executor)
		require.True(t, known)
	}
	_, known = ExpressionExecutorRetainedBytes(retainedBytesUnknownExecutor{})
	require.False(t, known)

	result.Free()
	selectedResult.Free()
	column.nullVecCache.Free(proc.Mp())
	for _, vec := range ownedVectors {
		vec.Free(proc.Mp())
	}
	require.Zero(t, proc.Mp().CurrNB())
}

type retainedBytesUnknownExecutor struct{}

func (retainedBytesUnknownExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}
func (retainedBytesUnknownExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return nil, nil
}
func (retainedBytesUnknownExecutor) ResetForNextQuery() {}
func (retainedBytesUnknownExecutor) Free()              {}
func (retainedBytesUnknownExecutor) IsColumnExpr() bool { return false }
func (retainedBytesUnknownExecutor) TypeName() string   { return "unknown" }
