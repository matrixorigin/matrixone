// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package disttae

import (
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type MinOrMax = uint8

const (
	MinOrMax_Max MinOrMax = 0
	MinOrMax_Min MinOrMax = 0
)

func checkExprIsMonotonical(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			isMonotonical := checkExprIsMonotonical(arg)
			if !isMonotonical {
				return false
			}
		}

		isMonotonical, _ := function.GetFunctionIsMonotonicalById(exprImpl.F.Func.GetObj())
		if !isMonotonical {
			return false
		}

		return true
	default:
		return true
	}
}

func replaceColumnWithZonemap(expr *plan.Expr, blkInfo BlockMeta, minOrMax MinOrMax) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColumnWithZonemap(arg, blkInfo, minOrMax)
		}
		return expr
	case *plan.Expr_Col:
		idx := exprImpl.Col.ColPos

		var val []byte
		if minOrMax == MinOrMax_Max {
			val = blkInfo.columns[idx].zoneMap.max
		} else {
			val = blkInfo.columns[idx].zoneMap.min
		}

		// TODO need decoder to decode the []byte to target type
		typ := types.T(blkInfo.columns[idx].typ)
		if typ.ToType().IsIntOrUint() {
			intVal := int64(binary.LittleEndian.Uint64(val))
			return plan2.MakePlan2Int64ConstExprWithType(intVal)

		} else if typ.ToType().IsFloat() {
			bits := binary.LittleEndian.Uint64(val)
			floatVal := math.Float64frombits(bits)
			return plan2.MakePlan2Float64ConstExprWithType(floatVal)

		} else {
			// TODO other type decode as what??
			strVal := string(val)
			return plan2.MakePlan2StringConstExprWithType(strVal)
		}

	default:
		return expr
	}
}

func evalFilterExpr(expr *plan.Expr, bat *batch.Batch) (bool, error) {
	expr, err := plan2.ConstantFold(bat, expr)
	if err != nil {
		return false, err
	}

	if cExpr, ok := expr.Expr.(*plan.Expr_C); ok {
		if bVal, bOk := cExpr.C.Value.(*plan.Const_Bval); bOk {
			return bVal.Bval, nil
		}
	}

	return false, moerr.NewInternalError("cannot eval filter expr")
}
