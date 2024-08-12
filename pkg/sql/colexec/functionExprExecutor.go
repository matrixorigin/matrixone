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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FunctionExpressionExecutor
//
// it's responsible for calculating a function expression, such as IF, CASE, ADD, CAST etc.
//
// It's important to note that,
// to better reuse the FunctionExpressionExecutor, even if a function can be folded,
// we will not transform it into a FixedVectorExpressionExecutor.
// Instead, we record this information through an extra attribute.
type FunctionExpressionExecutor struct {
	m *mpool.MPool
	functionInformationForEval
	folded      functionFolding
	selectList1 []bool
	selectList2 []bool
	selectList  function.FunctionSelectList

	resultVector vector.FunctionResultWrapper
	// parameters related
	parameterResults  []*vector.Vector
	parameterExecutor []ExpressionExecutor
}

// function information for eval.
type functionInformationForEval struct {
	// basic information for the function.
	fid        int32
	overloadID int64

	// whether the function is volatile or time-dependent.
	// they were used to determine whether the function can be folded.
	volatile, timeDependent bool

	// the function's evalFn and freeFn.
	evalFn func(
		parameters []*vector.Vector,
		result vector.FunctionResultWrapper,
		proc *process.Process,
		rowCount int,
		selectList *function.FunctionSelectList) error
	freeFn func() error
}

// function folding information.
type functionFolding struct {
	needFoldingCheck bool
	canFold          bool
	foldVector       *vector.Vector
}

func (expr *FunctionExpressionExecutor) getFoldedVector(requiredLength int) *vector.Vector {
	if expr.folded.foldVector.IsConst() {
		expr.folded.foldVector.SetLength(requiredLength)
	}
	return expr.folded.foldVector
}

func (expr *FunctionExpressionExecutor) doFold(proc *process.Process, atRuntime bool) (err error) {
	if !expr.folded.needFoldingCheck {
		return nil
	}

	expr.folded.needFoldingCheck = false
	expr.folded.canFold = false

	// fold parameters.
	allParametersFolded := true
	for i, param := range expr.parameterExecutor {
		// constant expression.
		if constant, ok := param.(*FixedVectorExpressionExecutor); ok {
			expr.parameterResults[i] = constant.resultVector
			if !constant.noNeedToSetLength {
				expr.parameterResults[i].SetLength(1)
			}
			continue
		}
		// function expression.
		if fExpr, ok := param.(*FunctionExpressionExecutor); ok {
			paramFoldError := fExpr.doFold(proc, atRuntime)
			if paramFoldError != nil {
				return err
			}
			if fExpr.folded.canFold {
				expr.parameterResults[i] = fExpr.getFoldedVector(1)
				continue
			}
		}

		allParametersFolded = false
	}
	if !allParametersFolded || expr.volatile || (!atRuntime && expr.timeDependent) {
		return nil
	}

	// todo: I cannot understand these following codes, but I keep them here.
	//  it seems to deal with some IN filter and very dangerous.
	//  See the pull request: https://github.com/matrixorigin/matrixone/pull/13403.
	execLen := 1
	if len(expr.parameterResults) > 0 { // todo: == 1 ?
		if !expr.parameterResults[0].IsConst() {
			execLen = expr.parameterResults[0].Length()
		}
	}

	// fold the function.
	if execLen > 1 {
		if err = expr.resultVector.PreExtendAndReset(execLen); err != nil {
			return err
		}
		if err = expr.evalFn(expr.parameterResults, expr.resultVector, proc, execLen, nil); err != nil {
			return err
		}
		expr.folded.foldVector = expr.resultVector.GetResultVector()
		expr.resultVector.SetResultVector(nil)

	} else {
		if err = expr.resultVector.PreExtendAndReset(1); err != nil {
			return err
		}
		if err = expr.evalFn(expr.parameterResults, expr.resultVector, proc, 1, nil); err != nil {
			return err
		}
		if expr.folded.foldVector, err = expr.resultVector.GetResultVector().ToConst(0, 1, proc.Mp()).Dup(proc.Mp()); err != nil {
			return err
		}
		expr.resultVector.Free()
	}
	expr.folded.canFold = true
	return nil
}

func (expr *FunctionExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	if expr.folded.needFoldingCheck {
		if err := expr.doFold(proc, proc.GetBaseProcessRunningStatus()); err != nil {
			return nil, err
		}
	}
	if expr.folded.canFold {
		if len(batches) > 0 {
			return expr.getFoldedVector(batches[0].RowCount()), nil
		}
		return expr.getFoldedVector(1), nil
	}

	var err error
	if expr.fid == function.IFF {
		err = expr.EvalIff(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else if expr.fid == function.CASE {
		err = expr.EvalCase(proc, batches, selectList)
		if err != nil {
			return nil, err
		}
	} else {
		for i := range expr.parameterExecutor {
			expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, selectList)
			if err != nil {
				return nil, err
			}
		}
	}

	if err = expr.resultVector.PreExtendAndReset(batches[0].RowCount()); err != nil {
		return nil, err
	}

	if len(expr.selectList.SelectList) < batches[0].RowCount() {
		expr.selectList.SelectList = make([]bool, batches[0].RowCount())
	}
	if selectList == nil {
		expr.selectList.AnyNull = false
		expr.selectList.AllNull = false
		for i := range expr.selectList.SelectList {
			expr.selectList.SelectList[i] = true
		}
	} else {
		expr.selectList.AllNull = true
		expr.selectList.AnyNull = false
		for i := range selectList {
			expr.selectList.SelectList[i] = selectList[i]
			if selectList[i] {
				expr.selectList.AllNull = false
			} else {
				expr.selectList.AnyNull = true
			}
		}
	}

	if err = expr.evalFn(
		expr.parameterResults, expr.resultVector, proc, batches[0].RowCount(), &expr.selectList); err != nil {
		return nil, err
	}
	return expr.resultVector.GetResultVector(), nil
}

func (expr *FunctionExpressionExecutor) EvalIff(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	expr.parameterResults[0], err = expr.parameterExecutor[0].Eval(proc, batches, selectList)
	if err != nil {
		return err
	}
	rowCount := batches[0].RowCount()
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}

	bs := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[0])
	for i, j := uint64(0), uint64(rowCount); i < j; i++ {
		b, null := bs.GetValue(i)
		if selectList != nil {
			expr.selectList1[i] = selectList[i]
			expr.selectList2[i] = selectList[i]
		} else {
			expr.selectList1[i] = true
			expr.selectList2[i] = true
		}
		if !null && b {
			expr.selectList2[i] = false
		} else {
			expr.selectList1[i] = false
		}
	}
	expr.parameterResults[1], err = expr.parameterExecutor[1].Eval(proc, batches, expr.selectList1)
	if err != nil {
		return err
	}
	expr.parameterResults[2], err = expr.parameterExecutor[2].Eval(proc, batches, expr.selectList2)
	return err
}

func (expr *FunctionExpressionExecutor) EvalCase(proc *process.Process, batches []*batch.Batch, selectList []bool) (err error) {
	rowCount := batches[0].RowCount()
	if len(expr.selectList1) < rowCount {
		expr.selectList1 = make([]bool, rowCount)
		expr.selectList2 = make([]bool, rowCount)
	}
	if selectList != nil {
		copy(expr.selectList1, selectList)
	} else {
		for i := range expr.selectList1 {
			expr.selectList1[i] = true
		}
	}
	for i := 0; i < len(expr.parameterExecutor); i += 2 {
		expr.parameterResults[i], err = expr.parameterExecutor[i].Eval(proc, batches, expr.selectList1)
		if err != nil {
			return err
		}
		if i != len(expr.parameterExecutor)-1 {

			bs := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[i])
			for j, k := uint64(0), uint64(rowCount); j < k; j++ {
				b, null := bs.GetValue(j)
				if !null && b {
					expr.selectList1[j] = false
					expr.selectList2[j] = true
				} else {
					expr.selectList2[j] = false
				}
			}
			expr.parameterResults[i+1], err = expr.parameterExecutor[i+1].Eval(proc, batches, expr.selectList2)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (expr *FunctionExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, nil)
	if err != nil {
		return nil, err
	}
	expr.resultVector.SetResultVector(nil)
	return vec, nil
}

func (fI *functionInformationForEval) reset() {
	// we need to regenerate the evalFn to avoid a wrong result since the function may take an own runtime contest.
	// todo: in fact, we can jump this step if the function is a pure function. but we don't have this information now.

	if fI.freeFn != nil {
		_ = fI.freeFn()
		fI.freeFn = nil
	}

	// get evalFn and freeFn from the function registry here.
	if fI.evalFn != nil {
		// we can set the context nil here since this function will never return an error.
		overload, _ := function.GetFunctionById(context.TODO(), fI.overloadID)
		fI.evalFn, fI.freeFn = overload.GetExecuteMethod()
	}
}

func (fF *functionFolding) reset(m *mpool.MPool) {
	if fF.foldVector != nil {
		fF.foldVector.Free(m)
	}
	fF.needFoldingCheck = true
	fF.canFold = false
	fF.foldVector = nil
}

func (expr *FunctionExpressionExecutor) ResetForNextQuery() {
	// reset the constant folding state.
	expr.folded.reset(expr.m)
	// reset the function information.
	expr.functionInformationForEval.reset()

	// reset its parameters.
	for i, param := range expr.parameterExecutor {
		if param == nil {
			continue
		}

		expr.parameterResults[i] = nil
		param.ResetForNextQuery()
	}
}

func (expr *FunctionExpressionExecutor) Init(
	proc *process.Process,
	parameterNum int,
	retType types.Type) (err error) {
	m := proc.Mp()

	expr.m = m
	expr.parameterResults = make([]*vector.Vector, parameterNum)
	expr.parameterExecutor = make([]ExpressionExecutor, parameterNum)

	expr.resultVector = vector.NewFunctionResultWrapper(proc.GetVector, proc.PutVector, retType, m)
	return err
}

func (expr *FunctionExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.resultVector != nil {
		expr.resultVector.Free()
		expr.resultVector = nil
	}
	expr.folded.reset(expr.m)

	for _, p := range expr.parameterExecutor {
		if p != nil {
			p.Free()
		}
	}
	if expr.freeFn != nil {
		_ = expr.freeFn()
		expr.freeFn = nil
	}
}

func (expr *FunctionExpressionExecutor) SetParameter(index int, executor ExpressionExecutor) {
	expr.parameterExecutor[index] = executor
}

func (expr *FunctionExpressionExecutor) IsColumnExpr() bool {
	return false
}
