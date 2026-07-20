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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
}

func (fF *functionFolding) reset(_ *mpool.MPool) {
	fF.needFoldingCheck = true
	fF.canFold = false
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
	resetFn func() error
	freeFn  func() error
}

func (fI *functionInformationForEval) reset() {
	// we need to regenerate the evalFn to avoid a wrong result since the function may take an own runtime contest.
	// todo: in fact, we can jump this step if the function is a pure function. but we don't have this information now.

	if fI.resetFn != nil {
		_ = fI.resetFn()
		return
	}

	if fI.freeFn != nil {
		_ = fI.freeFn()
		fI.freeFn = nil
	}

	// get evalFn and freeFn from the function registry here.
	if fI.evalFn != nil {
		// we can set the context nil here since this function will never return an error.
		overload, _ := function.GetFunctionById(context.TODO(), fI.overloadID)
		fI.evalFn, fI.resetFn, fI.freeFn = overload.GetExecuteMethod()
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
	rv := expr.resultVector.GetResultVector()
	if rv.IsConst() {
		rv.SetLength(requiredLength)
	}
	return rv
}

func (expr *FunctionExpressionExecutor) tryFoldParameter(
	proc *process.Process,
	atRuntime bool,
	index int,
) (bool, error) {
	parameter := expr.parameterExecutor[index]
	if constant, ok := parameter.(*FixedVectorExpressionExecutor); ok {
		expr.parameterResults[index] = constant.resultVector
		if !constant.noNeedToSetLength {
			expr.parameterResults[index].SetLength(1)
		}
		return true, nil
	}
	if functionParameter, ok := parameter.(*FunctionExpressionExecutor); ok {
		if err := functionParameter.doFold(proc, atRuntime); err != nil {
			return false, err
		}
		if functionParameter.folded.canFold {
			expr.parameterResults[index] = functionParameter.getFoldedVector(1)
			return true, nil
		}
		return false, nil
	}
	if atRuntime {
		if parameter, ok := parameter.(*ParamExpressionExecutor); ok {
			result, err := parameter.Eval(proc, nil, nil)
			if err != nil {
				return false, err
			}
			expr.parameterResults[index] = result
			return true, nil
		}
	}
	return false, nil
}

func (expr *FunctionExpressionExecutor) tryFoldFlowControl(
	proc *process.Process,
	atRuntime bool,
) (bool, error) {
	switch expr.fid {
	case function.IFF:
		folded, err := expr.tryFoldParameter(proc, atRuntime, 0)
		if err != nil || !folded {
			return folded, err
		}
		condition := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[0])
		value, isNull := condition.GetValue(0)
		selected := 2
		if !isNull && value {
			selected = 1
		}
		return expr.tryFoldParameter(proc, atRuntime, selected)

	case function.CASE:
		parameterCount := len(expr.parameterExecutor)
		for conditionIndex := 0; conditionIndex+1 < parameterCount; conditionIndex += 2 {
			folded, err := expr.tryFoldParameter(proc, atRuntime, conditionIndex)
			if err != nil || !folded {
				return folded, err
			}
			condition := vector.GenerateFunctionFixedTypeParameter[bool](expr.parameterResults[conditionIndex])
			value, isNull := condition.GetValue(0)
			if !isNull && value {
				return expr.tryFoldParameter(proc, atRuntime, conditionIndex+1)
			}
		}
		if parameterCount%2 == 1 {
			return expr.tryFoldParameter(proc, atRuntime, parameterCount-1)
		}
		return true, nil

	case function.COALESCE:
		for i := range expr.parameterExecutor {
			folded, err := expr.tryFoldParameter(proc, atRuntime, i)
			if err != nil || !folded {
				return folded, err
			}
			if !expr.parameterResults[i].IsNull(0) {
				return true, nil
			}
		}
		return true, nil
	}
	return false, nil
}

func (expr *FunctionExpressionExecutor) fillSkippedFlowControlParameters() func() {
	// The registered kernels still receive their complete argument list. Supply
	// typed NULLs for branches that lazy folding deliberately did not evaluate;
	// the selected conditions make those placeholders unobservable.
	var boolNull *vector.Vector
	var resultNull *vector.Vector
	temporaryIndexes := make([]int, 0, len(expr.parameterResults))
	parameterCount := len(expr.parameterResults)
	for i := range expr.parameterResults {
		if expr.parameterResults[i] != nil {
			continue
		}
		isCondition := expr.fid == function.IFF && i == 0
		if expr.fid == function.CASE && i%2 == 0 && (parameterCount%2 == 0 || i < parameterCount-1) {
			isCondition = true
		}
		if isCondition {
			if boolNull == nil {
				boolNull = vector.NewConstNull(types.T_bool.ToType(), 1, expr.m)
			}
			expr.parameterResults[i] = boolNull
		} else {
			if resultNull == nil {
				resultNull = vector.NewConstNull(expr.resultType, 1, expr.m)
			}
			expr.parameterResults[i] = resultNull
		}
		temporaryIndexes = append(temporaryIndexes, i)
	}
	return func() {
		for _, i := range temporaryIndexes {
			expr.parameterResults[i] = nil
		}
		if boolNull != nil {
			boolNull.Free(expr.m)
		}
		if resultNull != nil {
			resultNull.Free(expr.m)
		}
	}
}

func (expr *FunctionExpressionExecutor) finishFolding(proc *process.Process, execLen int) error {
	expr.resetResultType(expr.resultVector)
	if err := expr.resultVector.PreExtendAndReset(execLen); err != nil {
		return err
	}
	if err := expr.evalFn(expr.parameterResults, expr.resultVector, proc, execLen, nil); err != nil {
		return err
	}
	if execLen == 1 {
		expr.resultVector.GetResultVector().ToConst()
	}
	expr.folded.canFold = true
	return nil
}

func (expr *FunctionExpressionExecutor) doFold(proc *process.Process, atRuntime bool) (err error) {
	if !expr.folded.needFoldingCheck {
		return nil
	}

	expr.folded.needFoldingCheck = false
	expr.folded.canFold = false
	if expr.fid == function.IFF || expr.fid == function.CASE || expr.fid == function.COALESCE {
		if expr.volatile || (!atRuntime && expr.timeDependent) {
			return nil
		}
		folded, err := expr.tryFoldFlowControl(proc, atRuntime)
		if err != nil || !folded {
			return err
		}
		cleanup := expr.fillSkippedFlowControlParameters()
		defer cleanup()
		return expr.finishFolding(proc, 1)
	}

	// fold parameters.
	allParametersFolded := true
	for i := range expr.parameterExecutor {
		folded, foldErr := expr.tryFoldParameter(proc, atRuntime, i)
		if foldErr != nil {
			return foldErr
		}
		if !folded {
			allParametersFolded = false
		}
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

	return expr.finishFolding(proc, execLen)
}

func (expr *ParamExpressionExecutor) ResetForNextQuery() {
	if expr.null != nil {
		expr.null.CleanOnlyData()
	}
	if expr.vec != nil {
		expr.vec.CleanOnlyData()
	}
	expr.folded = false
}

func (expr *VarExpressionExecutor) ResetForNextQuery() {
	// do nothing.
}
