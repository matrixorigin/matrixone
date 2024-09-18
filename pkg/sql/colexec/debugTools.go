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

	buffer.WriteString(fmt.Sprintf(prefix+"  noNeedToSetLength: %v\n", expr.noNeedToSetLength))
	if expr.resultVector.GetType() != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.typ: %s\n", expr.resultVector.GetType().String()))
	} else {
		buffer.WriteString(fmt.Sprintf("%s  resultVector.typ is nil\n", prefix))
	}
	buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.value: %s\n", expr.resultVector.String()))

	return nil
}

// printFunctionExpressionExecutor prints the details of a FunctionExpressionExecutor.
func printFunctionExpressionExecutor(buffer *bytes.Buffer, expr *FunctionExpressionExecutor, prefix string) error {
	buffer.WriteString("FunctionExpressionExecutor\n")

	// functionInformationForEval
	buffer.WriteString(fmt.Sprintf("%s  functionInformationForEval\n", prefix))
	buffer.WriteString(fmt.Sprintf(prefix+"    fid: %v\n", expr.fid))
	evalFn, freeFn := expr.evalFn, expr.freeFn

	if evalFn != nil {
		if fn := runtime.FuncForPC(uintptr(reflect.ValueOf(evalFn).Pointer())); fn != nil {
			buffer.WriteString(fmt.Sprintf(prefix+"    evalFn: %s\n", fn.Name()))
		} else {
			buffer.WriteString(fmt.Sprintf("%s    evalFn: Unkown\n", prefix))
		}
	} else {
		return moerr.NewInternalErrorNoCtx("Function pointer for evalFn is nil, which should not happen")
	}

	if freeFn != nil {
		if fn := runtime.FuncForPC(uintptr(reflect.ValueOf(freeFn).Pointer())); fn != nil {
			buffer.WriteString(fmt.Sprintf(prefix+"    freeFn: %s\n", fn.Name()))
		} else {
			buffer.WriteString(fmt.Sprintf(prefix + "    freeFn: Unkown\n"))
		}
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "    freeFn: is nil\n"))
	}

	volatile, timeDependent := expr.volatile, expr.timeDependent
	buffer.WriteString(fmt.Sprintf(prefix+"  volatile: %v\n", volatile))
	buffer.WriteString(fmt.Sprintf(prefix+"  timeDependent: %v\n", timeDependent))

	// foloded
	buffer.WriteString(fmt.Sprintf(prefix + "  folded\n"))

	buffer.WriteString(fmt.Sprintf(prefix+"    needFoldingCheck: %v\n", expr.folded.needFoldingCheck))
	buffer.WriteString(fmt.Sprintf(prefix+"    canFold: %v\n", expr.folded.canFold))
	if expr.folded.foldVector != nil {
		if expr.folded.foldVector.GetType() != nil {
			buffer.WriteString(fmt.Sprintf(prefix+"    foldVector.typ: %s\n", expr.folded.foldVector.GetType().String()))
		}
		buffer.WriteString(fmt.Sprintf(prefix+"    foldVector.value: %s\n", expr.folded.foldVector.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "    foldVector is nil\n"))
	}

	// selectList1, selectList2
	buffer.WriteString(fmt.Sprintf(prefix+"  selectList1: %v\n", expr.selectList1))
	buffer.WriteString(fmt.Sprintf(prefix+"  selectList2: %v\n", expr.selectList2))

	// selectList
	buffer.WriteString(fmt.Sprintf(prefix + "  selectList\n"))
	buffer.WriteString(fmt.Sprintf(prefix+"    anynull: %v\n", expr.selectList.AnyNull))
	buffer.WriteString(fmt.Sprintf(prefix+"    allnull: %v\n", expr.selectList.AllNull))
	buffer.WriteString(fmt.Sprintf(prefix+"    selectList: %v\n", expr.selectList.SelectList))

	// resultVector
	if expr.resultVector != nil {
		vec := expr.resultVector.GetResultVector()
		if vec != nil {
			if vec.GetType() != nil {
				buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.typ: %s\n", vec.GetType().String()))
			} else {
				buffer.WriteString(fmt.Sprintf(prefix + "  resultVector.typ is nil\n"))
			}
			buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.value: %s\n", vec.String()))
		} else {
			buffer.WriteString(fmt.Sprintf(prefix + "  resultVector is nil\n"))
		}
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  resultVector is nil\n"))
	}

	buffer.WriteString(prefix + "  parameterResults:\n")
	for i, p := range expr.parameterResults {
		if p != nil {
			if p.GetType() != nil {
				buffer.WriteString(fmt.Sprintf(prefix+"    parameter[%d].typ: %s\n", i, p.GetType().String()))
			} else {
				buffer.WriteString(fmt.Sprintf(prefix+"    parameter[%d].typ is nil\n", i))
			}
			buffer.WriteString(fmt.Sprintf(prefix+"    parameter[%d].value: %s\n", i, p.String()))
		}
	}

	buffer.WriteString(prefix + "  parameterExecutor:\n")
	for i, param := range expr.parameterExecutor {
		printExpressionExecutor(buffer, param, prefix, i == len(expr.parameterExecutor)-1, false)
	}

	return nil
}

// printColumnExpressionExecutor prints the details of a ColumnExpressionExecutor.
func printColumnExpressionExecutor(buffer *bytes.Buffer, expr *ColumnExpressionExecutor, prefix string) error {
	buffer.WriteString("ColumnExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf(prefix+"  relIndex: %d\n", expr.relIndex))
	buffer.WriteString(fmt.Sprintf(prefix+"  colIndex: %d\n", expr.colIndex))
	buffer.WriteString(fmt.Sprintf(prefix+"  typ: %s\n", expr.typ.String()))

	if expr.nullVecCache != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  nullVecCache: %s\n", expr.nullVecCache.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  nullVecCache is nil\n"))
	}

	return nil
}

// printParamExpressionExecutor prints the details of a ParamExpressionExecutor.
func printParamExpressionExecutor(buffer *bytes.Buffer, expr *ParamExpressionExecutor, prefix string) error {
	buffer.WriteString("ParamExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf(prefix+"  pos: %d\n", expr.pos))
	buffer.WriteString(fmt.Sprintf(prefix+"  typ: %s\n", expr.typ.String()))

	if expr.null != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  null: %s\n", expr.null.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  null is nil\n"))
	}

	if expr.vec != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  vec: %s\n", expr.vec.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  vec is nil\n"))
	}

	return nil
}

// printVarExpressionExecutor prints the details of a VarExpressionExecutor.
func printVarExpressionExecutor(buffer *bytes.Buffer, expr *VarExpressionExecutor, prefix string) error {
	buffer.WriteString("VarExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf(prefix+"  name: %s\n", expr.name))
	buffer.WriteString(fmt.Sprintf(prefix+"  system: %s\n", expr.system))
	buffer.WriteString(fmt.Sprintf(prefix+"  global: %s\n", expr.global))
	buffer.WriteString(fmt.Sprintf(prefix+"  typ: %s\n", expr.typ.String()))

	if expr.null != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  null: %s\n", expr.null.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  null is nil\n"))
	}

	if expr.vec != nil {
		buffer.WriteString(fmt.Sprintf(prefix+"  vec: %s\n", expr.vec.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  vec is nil\n"))
	}

	return nil
}

// printListExpressionExecutor prints the details of a ListExpressionExecutor.
func printListExpressionExecutor(buffer *bytes.Buffer, expr *ListExpressionExecutor, prefix string) error {
	buffer.WriteString("ListExpressionExecutor\n")

	buffer.WriteString(fmt.Sprintf(prefix+"  typ: %s\n", expr.typ.String()))

	if expr.resultVector != nil {
		if expr.resultVector.GetType() != nil {
			buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.typ: %s\n", expr.resultVector.GetType().String()))
		} else {
			buffer.WriteString(fmt.Sprintf(prefix + "  resultVector.typ is nil\n"))
		}
		buffer.WriteString(fmt.Sprintf(prefix+"  resultVector.value: %s\n", expr.resultVector.String()))
	} else {
		buffer.WriteString(fmt.Sprintf(prefix + "  resultVector is nil\n"))
	}

	for i, exec := range expr.parameterExecutor {
		printExpressionExecutor(buffer, exec, prefix, i == len(expr.parameterExecutor)-1, false)
	}

	return nil
}
