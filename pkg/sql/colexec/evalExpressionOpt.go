// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var fixedOnlyOneRowBatch = []*batch.Batch{
	batch.EmptyForConstFoldBatch,
}
var doNothingFunc = func() {}

// GetReadonlyResultFromNoColumnExpression returns a readonly vector and its free method from planExpression.
// This function requires that
// expression cannot take any column expression,
// for example, 'a+1' is an invalid expression because a is a column expression.
func GetReadonlyResultFromNoColumnExpression(
	proc *process.Process,
	planExpr *plan.Expr) (vec *vector.Vector, freeMethod func(), err error) {
	return getReadonlyResultFromExpression(proc, planExpr, fixedOnlyOneRowBatch)
}

// GetWritableResultFromNoColumnExpression has the same requirement for input expression.
// this function returns a writable / editable vector whose memory will belong to caller.
func GetWritableResultFromNoColumnExpression(
	proc *process.Process,
	planExpr *plan.Expr) (vec *vector.Vector, err error) {
	return GetWritableResultFromExpression(proc, planExpr, fixedOnlyOneRowBatch)
}

// GetReadonlyResultFromExpression return a readonly result and its free method from expression and input data.
func GetReadonlyResultFromExpression(
	proc *process.Process,
	planExpr *plan.Expr, data []*batch.Batch) (vec *vector.Vector, freeMethod func(), err error) {

	// specific situation that no need to generate executor and do evaluation.
	if col, isSimpleCol := planExpr.Expr.(*plan.Expr_Col); isSimpleCol {
		return data[col.Col.RelPos].Vecs[col.Col.ColPos], doNothingFunc, nil
	}

	return getReadonlyResultFromExpression(proc, planExpr, data)
}

func getReadonlyResultFromExpression(
	proc *process.Process,
	planExpr *plan.Expr, data []*batch.Batch) (vec *vector.Vector, freeMethod func(), err error) {
	var executor ExpressionExecutor
	if executor, err = NewExpressionExecutor(proc, planExpr); err != nil {
		return nil, nil, err
	}
	if vec, err = executor.Eval(proc, data, nil); err != nil {
		executor.Free()
		return nil, nil, err
	}

	return vec, executor.Free, nil
}

// GetWritableResultFromExpression return a writable result and its free method from expression and input data.
func GetWritableResultFromExpression(
	proc *process.Process,
	planExpr *plan.Expr, data []*batch.Batch) (vec *vector.Vector, err error) {

	var executor ExpressionExecutor
	if executor, err = NewExpressionExecutor(proc, planExpr); err == nil {
		if vec, err = executor.Eval(proc, data, nil); err == nil {
			if !modifyResultOwnerToOuter(executor) {
				vec, err = vec.Dup(proc.Mp())
			}
		}
		executor.Free()
	}
	return vec, err
}

// modifyResultOwnerToOuter change the owner of expression result outer to make sure that
// the free method of executor cannot clean the result memory.
func modifyResultOwnerToOuter(executor ExpressionExecutor) (succeed bool) {
	// constant expression.
	if c, ok := executor.(*FixedVectorExpressionExecutor); ok {
		c.resultVector = nil
		return true
	}
	// function expression.
	if f, ok := executor.(*FunctionExpressionExecutor); ok {
		f.resultVector = nil
		f.folded.foldVector = nil
		return true
	}

	return false
}
