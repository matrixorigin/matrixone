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
	"bytes"
	"fmt"
	"reflect"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// DebugShowExecutor prints the tree of ExpressionExecutor starting from root, similar as func DebugShowScopes
func DebugShowExecutor(executor ExpressionExecutor) (string, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 800))
	if err := printExpressionExecutor(buffer, executor, "", true, true); err != nil {
		return "", err
	}

	return buffer.String(), nil
}

// printExpressionExecutor prints the tree of ExpressionExecutor starting from the given executor.
func printExpressionExecutor(buffer *bytes.Buffer, executor ExpressionExecutor, prefix string, isTail bool, isRoot bool) error {
	var newPrefix string
	var err error
	if isRoot {
		buffer.WriteString("\n")
		newPrefix = prefix
	} else {
		if isTail {
			buffer.WriteString(prefix + "  └── ")
			newPrefix = prefix + "      "
		} else {
			buffer.WriteString(prefix + "  ├── ")
			newPrefix = prefix + "  │   "
		}
	}

	switch e := executor.(type) {
	case *FixedVectorExpressionExecutor:
		if err = printFixedVectorExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	case *FunctionExpressionExecutor:
		if err = printFunctionExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	case *ColumnExpressionExecutor:
		if err = printColumnExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	case *ParamExpressionExecutor:
		if err = printParamExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	case *VarExpressionExecutor:
		if err = printVarExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	case *ListExpressionExecutor:
		if err = printListExpressionExecutor(buffer, e, newPrefix); err != nil {
			return err
		}
	default:
		err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("unknown type %T", executor))
	}

	return err
}

// printFixedVectorExpressionExecutor prints the details of a FixedVectorExpressionExecutor.
func printFixedVectorExpressionExecutor(buffer *bytes.Buffer, expr *FixedVectorExpressionExecutor, prefix string) error {
	buffer.WriteString("FixedVectorExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf("%s  noNeedToSetLength: %v\n", prefix, expr.noNeedToSetLength))
	if expr.resultVector.GetType() != nil {
		buffer.WriteString(fmt.Sprintf("%s  resultVector.typ: %s\n", prefix, expr.resultVector.GetType().String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  resultVector.typ is nil\n", prefix))
	}
	buffer.WriteString(fmt.Sprintf("%s  resultVector.value: %s\n", prefix, expr.resultVector.String()))

	return nil
}

// printFunctionExpressionExecutor prints the details of a FunctionExpressionExecutor.
func printFunctionExpressionExecutor(buffer *bytes.Buffer, expr *FunctionExpressionExecutor, prefix string) error {
	buffer.WriteString("FunctionExpressionExecutor\n")

	// functionInformationForEval
	buffer.WriteString(fmt.Sprintf("%s  functionInformationForEval\n", prefix))
	buffer.WriteString(fmt.Sprintf("%s    fid: %v\n", prefix, expr.fid))
	evalFn, freeFn := expr.evalFn, expr.freeFn

	if evalFn != nil {
		if fn := runtime.FuncForPC(uintptr(reflect.ValueOf(evalFn).Pointer())); fn != nil {
			buffer.WriteString(fmt.Sprintf("%s    evalFn: %s\n", prefix, fn.Name()))
		} else {
			buffer.WriteString(fmt.Sprintf("%s    evalFn: Unkown\n", prefix))
		}
	} else {
		return moerr.NewInternalErrorNoCtx("Function pointer for evalFn is nil, which should not happen")
	}

	if freeFn != nil {
		if fn := runtime.FuncForPC(uintptr(reflect.ValueOf(freeFn).Pointer())); fn != nil {
			buffer.WriteString(fmt.Sprintf("%s    freeFn: %s\n", prefix, fn.Name()))
		} else {
			buffer.WriteString(fmt.Sprintf("%s    freeFn: Unkown\n", prefix))
		}
	} else {
		buffer.WriteString(fmt.Sprintf("%s    freeFn: is nil\n", prefix))
	}

	volatile, timeDependent := expr.volatile, expr.timeDependent
	buffer.WriteString(fmt.Sprintf("%s  volatile: %v\n", prefix, volatile))
	buffer.WriteString(fmt.Sprintf("%s  timeDependent: %v\n", prefix, timeDependent))

	// foloded
	buffer.WriteString(fmt.Sprintf("%s  folded\n", prefix))

	buffer.WriteString(fmt.Sprintf("%s    needFoldingCheck: %v\n", prefix, expr.folded.needFoldingCheck))
	buffer.WriteString(fmt.Sprintf("%s    canFold: %v\n", prefix, expr.folded.canFold))
	if expr.folded.foldVector != nil {
		if expr.folded.foldVector.GetType() != nil {
			buffer.WriteString(fmt.Sprintf("%s    foldVector.typ: %s\n", prefix, expr.folded.foldVector.GetType().String()))
		}
		buffer.WriteString(fmt.Sprintf("%s    foldVector.value: %s\n", prefix, expr.folded.foldVector.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s    foldVector is nil\n", prefix))
	}

	// selectList1, selectList2
	buffer.WriteString(fmt.Sprintf("%s  selectList1: %v\n", prefix, expr.selectList1))
	buffer.WriteString(fmt.Sprintf("%s  selectList2: %v\n", prefix, expr.selectList2))

	// selectList
	buffer.WriteString(fmt.Sprintf("%s  selectList\n", prefix))
	buffer.WriteString(fmt.Sprintf("%s    anynull: %v\n", prefix, expr.selectList.AnyNull))
	buffer.WriteString(fmt.Sprintf("%s    allnull: %v\n", prefix, expr.selectList.AllNull))
	buffer.WriteString(fmt.Sprintf("%s    selectList: %v\n", prefix, expr.selectList.SelectList))

	// resultVector
	if expr.resultVector != nil {
		vec := expr.resultVector.GetResultVector()
		if vec != nil {
			if vec.GetType() != nil {
				buffer.WriteString(fmt.Sprintf("%s  resultVector.typ: %s\n", prefix, vec.GetType().String()))
			} else {
				buffer.WriteString(fmt.Sprintf("%s  resultVector.typ is nil\n", prefix))
			}
			buffer.WriteString(fmt.Sprintf("%s  resultVector.value: %s\n", prefix, vec.String()))
		} else {
			buffer.WriteString(fmt.Sprintf("%s  resultVector is nil\n", prefix))
		}
	} else {
		buffer.WriteString(fmt.Sprintf("%s  resultVector is nil\n", prefix))
	}

	buffer.WriteString("%s  parameterResults:\n")
	for i, p := range expr.parameterResults {
		if p != nil {
			if p.GetType() != nil {
				buffer.WriteString(fmt.Sprintf("%s    parameter[%d].typ: %s\n", prefix, i, p.GetType().String()))
			} else {
				buffer.WriteString(fmt.Sprintf("%s    parameter[%d].typ is nil\n", prefix, i))
			}
			buffer.WriteString(fmt.Sprintf("%s    parameter[%d].value: %s\n", prefix, i, p.String()))
		}
	}

	buffer.WriteString("%s  parameterExecutor:\n")
	for i, param := range expr.parameterExecutor {
		printExpressionExecutor(buffer, param, prefix, i == len(expr.parameterExecutor)-1, false)
	}

	return nil
}

// printColumnExpressionExecutor prints the details of a ColumnExpressionExecutor.
func printColumnExpressionExecutor(buffer *bytes.Buffer, expr *ColumnExpressionExecutor, prefix string) error {
	buffer.WriteString("ColumnExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf("%s  relIndex: %d\n", prefix, expr.relIndex))
	buffer.WriteString(fmt.Sprintf("%s  colIndex: %d\n", prefix, expr.colIndex))
	buffer.WriteString(fmt.Sprintf("%s  typ: %s\n", prefix, expr.typ.String()))

	if expr.nullVecCache != nil {
		buffer.WriteString(fmt.Sprintf("%s  nullVecCache: %s\n", prefix, expr.nullVecCache.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  nullVecCache is nil\n", prefix))
	}

	return nil
}

// printParamExpressionExecutor prints the details of a ParamExpressionExecutor.
func printParamExpressionExecutor(buffer *bytes.Buffer, expr *ParamExpressionExecutor, prefix string) error {
	buffer.WriteString("ParamExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf("%s  pos: %d\n", prefix, expr.pos))
	buffer.WriteString(fmt.Sprintf("%s  typ: %s\n", prefix, expr.typ.String()))

	if expr.null != nil {
		buffer.WriteString(fmt.Sprintf("%s  null: %s\n", prefix, expr.null.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  null is nil\n", prefix))
	}

	if expr.vec != nil {
		buffer.WriteString(fmt.Sprintf("%s  vec: %s\n", prefix, expr.vec.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  vec is nil\n", prefix))
	}

	return nil
}

// printVarExpressionExecutor prints the details of a VarExpressionExecutor.
func printVarExpressionExecutor(buffer *bytes.Buffer, expr *VarExpressionExecutor, prefix string) error {
	buffer.WriteString("VarExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf("%s  name: %s\n", prefix, expr.name))
	buffer.WriteString(fmt.Sprintf("%s  system: %v\n", prefix, expr.system))
	buffer.WriteString(fmt.Sprintf("%s  global: %v\n", prefix, expr.global))
	buffer.WriteString(fmt.Sprintf("%s  typ: %s\n", prefix, expr.typ.String()))

	if expr.null != nil {
		buffer.WriteString(fmt.Sprintf("%s  null: %s\n", prefix, expr.null.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  null is nil\n", prefix))
	}

	if expr.vec != nil {
		buffer.WriteString(fmt.Sprintf("%s  vec: %s\n", prefix, expr.vec.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  vec is nil\n", prefix))
	}

	return nil
}

// printListExpressionExecutor prints the details of a ListExpressionExecutor.
func printListExpressionExecutor(buffer *bytes.Buffer, expr *ListExpressionExecutor, prefix string) error {
	buffer.WriteString("ListExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf("%s  typ: %s\n", prefix, expr.typ.String()))

	if expr.resultVector != nil {
		if expr.resultVector.GetType() != nil {
			buffer.WriteString(fmt.Sprintf("%s  resultVector.typ: %s\n", prefix, expr.resultVector.GetType().String()))
		} else {
			buffer.WriteString(fmt.Sprintf("%s  resultVector.typ is nil\n", prefix))
		}
		buffer.WriteString(fmt.Sprintf("%s  resultVector.value: %s\n", prefix, expr.resultVector.String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  resultVector is nil\n", prefix))
	}

	for i, exec := range expr.parameterExecutor {
		printExpressionExecutor(buffer, exec, prefix, i == len(expr.parameterExecutor)-1, false)
	}

	return nil
}
