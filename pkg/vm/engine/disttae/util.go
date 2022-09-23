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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func _getColumnMapByExpr(expr *plan.Expr, columnMap map[int32]struct{}) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			_getColumnMapByExpr(arg, columnMap)
		}
	case *plan.Expr_Col:
		idx := exprImpl.Col.ColPos
		columnMap[idx] = struct{}{}
	}
}

func getColumnsByExpr(expr *plan.Expr) []int32 {
	columnMap := make(map[int32]struct{})
	_getColumnMapByExpr(expr, columnMap)

	columns := make([]int32, len(columnMap))
	i := 0
	for k := range columnMap {
		columns[i] = k
		i++
	}
	return columns
}

func decodeMinMax(typ types.T, value []byte) any {
	// TODO need zonemap decoder to decode the []byte to target type
	if typ.ToType().IsIntOrUint() {
		return int64(binary.LittleEndian.Uint64(value))

	} else if typ.ToType().IsFloat() {
		bits := binary.LittleEndian.Uint64(value)
		return math.Float64frombits(bits)

	} else {
		// TODO other type decode as what??
		return string(value)
	}
}

// func decodeMinMaxToExpr(typ types.T, value []byte) *plan.Expr {
// 	// TODO need decoder to decode the []byte to target type
// 	if typ.ToType().IsIntOrUint() {
// 		intVal := int64(binary.LittleEndian.Uint64(value))
// 		return plan2.MakePlan2Int64ConstExprWithType(intVal)

// 	} else if typ.ToType().IsFloat() {
// 		bits := binary.LittleEndian.Uint64(value)
// 		floatVal := math.Float64frombits(bits)
// 		return plan2.MakePlan2Float64ConstExprWithType(floatVal)

// 	} else {
// 		// TODO other type decode as what??
// 		strVal := string(value)
// 		return plan2.MakePlan2StringConstExprWithType(strVal)
// 	}
// }

func getZonemapDataFromMeta(columns []int32, meta BlockMeta, tableDef *plan.TableDef) ([][2]any, []uint8) {
	var columnMeta *ColumnMeta

	getIdx := func(idx int) int {
		return int(tableDef.Name2ColIndex[tableDef.Cols[columns[idx]].Name])
	}

	dataLength := len(columns)

	datas := make([][2]any, dataLength)
	dataTypes := make([]uint8, dataLength)

	for i := 0; i < dataLength; i++ {
		columnMeta = meta.columns[getIdx(i)]
		dataTypes[i] = columnMeta.typ
		typ := types.T(columnMeta.typ)

		datas[i] = [2]any{
			decodeMinMax(typ, columnMeta.zoneMap.min),
			decodeMinMax(typ, columnMeta.zoneMap.max),
		}
	}

	return datas, dataTypes
}

// func replaceColumnWithZonemap(expr *plan.Expr, columnIdx int32, columnExpr *plan.Expr) *plan.Expr {
// 	switch exprImpl := expr.Expr.(type) {
// 	case *plan.Expr_F:
// 		for i, arg := range exprImpl.F.Args {
// 			exprImpl.F.Args[i] = replaceColumnWithZonemap(arg, columnIdx, columnExpr)
// 		}
// 		return expr
// 	case *plan.Expr_Col:
// 		idx := exprImpl.Col.ColPos
// 		if idx == columnIdx {
// 			return columnExpr
// 		}
// 		return expr

// 	default:
// 		return expr
// 	}
// }

func evalFilterExpr(expr *plan.Expr, bat *batch.Batch, proc *process.Process, isConstant bool) (bool, error) {
	if isConstant {
		e, err := plan2.ConstantFold(bat, expr)
		if err != nil {
			return false, err
		}

		if cExpr, ok := e.Expr.(*plan.Expr_C); ok {
			if bVal, bOk := cExpr.C.Value.(*plan.Const_Bval); bOk {
				return bVal.Bval, nil
			}
		}
		return false, moerr.NewInternalError("cannot eval filter expr")
	} else {
		vec, err := colexec.EvalExpr(bat, proc, expr)
		if err != nil {
			return false, err
		}
		if vec.Typ.Oid != types.T_bool {
			return false, moerr.NewInternalError("cannot eval filter expr")
		}
		cols := vector.MustTCols[bool](vec)
		for _, isNeed := range cols {
			if isNeed {
				return true, nil
			}
		}
		return false, nil
	}
}

func getMoDatabaseTableDef(columns []string) *plan.TableDef {
	return _getTableDefBySchemaAndType(catalog.MO_DATABASE, columns, catalog.MoDatabaseSchema, catalog.MoDatabaseTypes)
}

func getMoTableTableDef(columns []string) *plan.TableDef {
	return _getTableDefBySchemaAndType(catalog.MO_TABLES, columns, catalog.MoTablesSchema, catalog.MoTablesTypes)
}

func getMoColumnTableDef(columns []string) *plan.TableDef {
	return _getTableDefBySchemaAndType(catalog.MO_COLUMNS, columns, catalog.MoColumnsSchema, catalog.MoColumnsTypes)
}

func _getTableDefBySchemaAndType(name string, columns []string, schema []string, types []types.Type) *plan.TableDef {
	columnsMap := make(map[string]struct{})
	for _, col := range columns {
		columnsMap[col] = struct{}{}
	}

	var cols []*plan.ColDef
	nameToIndex := make(map[string]int32)

	for i, col := range schema {
		if _, ok := columnsMap[col]; ok {
			cols = append(cols, &plan.ColDef{
				Name: col,
				Typ:  plan2.MakePlan2Type(&types[i]),
			})
		}
		nameToIndex[col] = int32(i)
	}

	return &plan.TableDef{
		Name:          name,
		Cols:          cols,
		Name2ColIndex: nameToIndex,
	}
}

func exchangeVectors(datas [][2]any, depth int, tmpResult []any, result *[]*vector.Vector, mp *mheap.Mheap) {
	for i := 0; i < len(datas[depth]); i++ {
		tmpResult[depth] = datas[depth][i]
		if depth != len(datas)-1 {
			exchangeVectors(datas, depth+1, tmpResult, result, mp)
		} else {
			for j, val := range tmpResult {
				(*result)[j].Append(val, false, mp)
			}
		}
	}
}

func buildVectorsByData(datas [][2]any, dataTypes []uint8, mp *mheap.Mheap) []*vector.Vector {
	vectors := make([]*vector.Vector, len(dataTypes))
	for i, typ := range dataTypes {
		vectors[i] = vector.New(types.T(typ).ToType())
	}

	tmpResult := make([]any, len(datas))
	exchangeVectors(datas, 0, tmpResult, &vectors, mp)

	return vectors
}
