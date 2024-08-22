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

package colexec

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (expr *ColumnExpressionExecutor) ResetForNextQuery() {
	// do nothing.
}

func (expr *FixedVectorExpressionExecutor) ResetForNextQuery() {
	// do nothing.
}

type functionFolding struct {
	needFoldingCheck bool
	canFold          bool
	foldVector       *vector.Vector
}

func (fF *functionFolding) reset(m *mpool.MPool) {
	if fF.foldVector != nil {
		fF.foldVector.Free(m)
	}
	fF.needFoldingCheck = true
	fF.canFold = false
	fF.foldVector = nil
}

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

func (expr *ParamExpressionExecutor) ResetForNextQuery() {
	// do nothing.
}

func (expr *VarExpressionExecutor) ResetForNextQuery() {
	// do nothing.
}
