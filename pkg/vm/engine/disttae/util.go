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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
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

	// get min/max from  vector
	if vec.Length() > 0 {
		cvec := containers.NewVectorWithSharedMemory(vec, true)

		// create zone map
		zm := index.NewZoneMap(vec.Typ)
		ctx := new(index.KeysCtx)
		ctx.Keys = cvec
		ctx.Count = vec.Length()
		defer ctx.Keys.Close()
		err := zm.BatchUpdate(ctx)
		if err != nil {
			return nil, nil, err
		}
		buf, err := zm.Marshal()
		if err != nil {
			return nil, nil, err
		}
		zoneMap, err = objectio.NewZoneMap(idx, buf)
		if err != nil {
			return nil, nil, err
		}

		// create bloomfilter
		sf, err := index.NewBinaryFuseFilter(cvec)
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

func getZonemapDataFromMeta(columns []int, meta BlockMeta, tableDef *plan.TableDef) ([][2]any, []uint8, error) {
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

		czm, _ := columnMeta.zoneMap.(*objectio.ZoneMap)
		buf := czm.GetData()
		zm := index.NewZoneMap(typ)
		err := zm.Unmarshal(buf)
		if err != nil {
			return nil, nil, err
		}

		datas[i] = [2]any{
			zm.GetMin(),
			zm.GetMax(),
		}
	}

	return datas, dataTypes, nil
}

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
		vec, err := colexec.EvalExprByZonemapBat(bat, proc, expr)
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
	return fmt.Sprintf("%s:%d_%d_%d.blk", "local", blkInfo.header.blockId, blkInfo.header.segmentId, blkInfo.header.tableId)
}

// getExtentFromMeta  TODO change later
func getExtentFromMeta(blkInfo BlockMeta) objectio.Extent {
	return objectio.NewExtent(blkInfo.localExtent.offset, blkInfo.localExtent.length, blkInfo.localExtent.originSize)
}
