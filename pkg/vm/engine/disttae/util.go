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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
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

func _getColumnMapByExpr(expr *plan.Expr, columnMap map[int]struct{}) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			_getColumnMapByExpr(arg, columnMap)
		}
	case *plan.Expr_Col:
		idx := exprImpl.Col.ColPos
		columnMap[int(idx)] = struct{}{}
	}
}

func getColumnsByExpr(expr *plan.Expr) []int {
	columnMap := make(map[int]struct{})
	_getColumnMapByExpr(expr, columnMap)

	columns := make([]int, len(columnMap))
	i := 0
	for k := range columnMap {
		columns[i] = k
		i++
	}
	sort.Ints(columns)
	return columns
}

func getIndexDataFromVec(idx uint16, vec *vector.Vector) (objectio.IndexData, objectio.IndexData, error) {
	var bloomFilter, zoneMap objectio.IndexData
	var err error

	// get min/max from  vector
	if vec.Length() > 0 {
		// create zone map
		zm := index.NewZoneMap(vec.Typ)
		for _, v := range vector.GetColumn[any](vec) {
			zm.Update(v)
		}
		min := types.EncodeValue(zm.GetMin(), vec.Typ)
		max := types.EncodeValue(zm.GetMax(), vec.Typ)
		zoneMap, err = objectio.NewZoneMap(idx, min, max)
		if err != nil {
			return nil, nil, err
		}

		// create bloomfilter
		sf, err := index.NewBinaryFuseFilter(moengine.MOToVector(vec, true))
		if err != nil {
			return nil, nil, err
		}
		bf, err := sf.Marshal()
		if err != nil {
			return nil, nil, err
		}
		alg := uint8(0)
		bloomFilter = objectio.NewBloomFilter(idx, alg, bf)
	}
	return bloomFilter, zoneMap, nil
}

func getZonemapDataFromMeta(columns []int, meta BlockMeta, tableDef *plan.TableDef) ([][2]any, []uint8) {
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
		typ := types.T(columnMeta.typ).ToType()

		// TODO : use types Encoder/Decoder?
		datas[i] = [2]any{
			types.DecodeValue(columnMeta.zoneMap.GetMin(), typ),
			types.DecodeValue(columnMeta.zoneMap.GetMax(), typ),
		}
	}

	return datas, dataTypes
}

// func evalFilterExpr(expr *plan.Expr, bat *batch.Batch, proc *process.Process) (bool, error) {
// 	if len(bat.Vecs) == 0 { //that's constant expr
// 		e, err := plan2.ConstantFold(bat, expr)
// 		if err != nil {
// 			return false, err
// 		}
// 		if cExpr, ok := e.Expr.(*plan.Expr_C); ok {
// 			if bVal, bOk := cExpr.C.Value.(*plan.Const_Bval); bOk {
// 				return bVal.Bval, nil
// 			}
// 		}
// 		return false, moerr.NewInternalError("cannot eval filter expr")
// 	} else {
// 		switch exprImpl := expr.Expr.(type) {
// 		case *plan.Expr_F:
// 			switch exprImpl.F.Func.ObjName {
// 			case "and":
// 				if checkExprMaybeTrue(exprImpl.F.Args[0], bat, proc) {
// 					return true, nil
// 				}
// 				return checkExprMaybeTrue(exprImpl.F.Args[1], bat, proc), nil
// 			case "or":
// 				if checkExprMaybeTrue(exprImpl.F.Args[0], bat, proc) {
// 					return true, nil
// 				}
// 				return checkExprMaybeTrue(exprImpl.F.Args[1], bat, proc), nil
// 			case ">", ">=", "<", "<=", "=":
// 				vec1 := evalBinaryExpr(exprImpl.F.Args[0], bat, proc)
// 				vec2 := evalBinaryExpr(exprImpl.F.Args[0], bat, proc)
// 				return checkExprIsIntersect(vec1, vec2), nil
// 			default:
// 				//unsupport expr, just return true
// 				return true, nil
// 			}
// 		default:
// 			//unsupport expr, just return true
// 			return true, nil
// 		}
// 	}
// }

func evalFilterExpr(expr *plan.Expr, bat *batch.Batch, proc *process.Process) (bool, error) {
	if len(bat.Vecs) == 0 { //that's constant expr
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

// getNameFromMeta  TODO change later
func getNameFromMeta(blkInfo BlockMeta) string {
	return fmt.Sprintf("%s:%d_%d_%d", "blk", blkInfo.header.blockId, blkInfo.header.segmentId, blkInfo.header.tableId)
}

// getExtentFromMeta  TODO change later
func getExtentFromMeta(blkInfo BlockMeta) objectio.Extent {
	return objectio.Extent{}
}
