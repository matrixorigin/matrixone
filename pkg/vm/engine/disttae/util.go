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
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	HASH_VALUE_FUN string = "hash_value"
	MAX_RANGE_SIZE int64  = 200
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

func getColumnMapByExpr(expr *plan.Expr, tableDef *plan.TableDef, columnMap map[int]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			getColumnMapByExpr(arg, tableDef, columnMap)
		}
	case *plan.Expr_Col:
		idx := exprImpl.Col.ColPos
		colName := exprImpl.Col.Name
		dotIdx := strings.Index(colName, ".")
		colName = colName[dotIdx+1:]
		columnMap[int(idx)] = int(tableDef.Name2ColIndex[colName])
	}
}

func getColumnsByExpr(expr *plan.Expr, tableDef *plan.TableDef) map[int]int {
	columnMap := make(map[int]int)
	getColumnMapByExpr(expr, tableDef, columnMap)
	return columnMap
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

func fetchZonemapAndRowsFromBlockInfo(idxs []uint16, blockInfo catalog.BlockInfo, fs fileservice.FileService, m *mpool.MPool) ([][64]byte, uint32, error) {
	name, extent, rows := blockio.DecodeMetaLoc(blockInfo.MetaLoc)
	zonemapList := make([][64]byte, len(idxs))

	// raed s3
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, 0, err
	}

	obs, err := reader.ReadMeta([]objectio.Extent{extent}, m)
	if err != nil {
		return nil, 0, err
	}

	for i, idx := range idxs {
		column, err := obs[0].GetColumn(idx)
		if err != nil {
			return nil, 0, err
		}
		data, err := column.GetIndex(objectio.ZoneMapType, m)
		if err != nil {
			return nil, 0, err
		}
		bytes := data.(*objectio.ZoneMap).GetData()
		copy(zonemapList[i][:], bytes[:])
	}

	return zonemapList, rows, nil
}

func getZonemapDataFromMeta(columns []int, meta BlockMeta, tableDef *plan.TableDef) ([][2]any, []uint8, error) {
	dataLength := len(columns)
	datas := make([][2]any, dataLength)
	dataTypes := make([]uint8, dataLength)

	for i := 0; i < dataLength; i++ {
		idx := columns[i]
		dataTypes[i] = uint8(tableDef.Cols[idx].Typ.Id)
		typ := types.T(dataTypes[i]).ToType()

		zm := index.NewZoneMap(typ)
		err := zm.Unmarshal(meta.Zonemap[idx][:])
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

func exchangeVectors(datas [][2]any, depth int, tmpResult []any, result *[]*vector.Vector, mp *mpool.MPool) {
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

func buildVectorsByData(datas [][2]any, dataTypes []uint8, mp *mpool.MPool) []*vector.Vector {
	vectors := make([]*vector.Vector, len(dataTypes))
	for i, typ := range dataTypes {
		vectors[i] = vector.New(types.T(typ).ToType())
	}

	tmpResult := make([]any, len(datas))
	exchangeVectors(datas, 0, tmpResult, &vectors, mp)

	return vectors
}

// getNewBlockName Each time a unique name is generated in one CN
func getNewBlockName(accountId uint32) (string, error) {
	uuid, err := types.BuildUuid()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d_%s.blk", accountId, uuid.ToString()), nil
}

func getConstantExprHashValue(constExpr *plan.Expr) (bool, uint64) {
	args := []*plan.Expr{constExpr}
	argTypes := []types.Type{types.T(constExpr.Typ.Id).ToType()}
	funId, returnType, _, _ := function.GetFunctionByName(HASH_VALUE_FUN, argTypes)
	funExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&returnType),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     funId,
					ObjName: HASH_VALUE_FUN,
				},
				Args: args,
			},
		},
	}

	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	ret, err := colexec.EvalExpr(bat, nil, funExpr)
	if err != nil {
		return false, 0
	}
	list := vector.MustTCols[int64](ret)
	return true, uint64(list[0])
}

func getNonIntPkExprValue(expr *plan.Expr, pkIdx int32) (bool, *plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funName := exprImpl.F.Func.ObjName
		switch funName {
		case "and":
			canCompute, pkBytes := getNonIntPkExprValue(exprImpl.F.Args[0], pkIdx)
			if canCompute {
				return canCompute, pkBytes
			}
			return getNonIntPkExprValue(exprImpl.F.Args[1], pkIdx)

		case "=":
			var pkVal *plan.Expr
			leftIsConstant := false
			switch subExpr := exprImpl.F.Args[0].Expr.(type) {
			case *plan.Expr_C:
				pkVal = exprImpl.F.Args[0]
				leftIsConstant = true
			case *plan.Expr_Col:
				if subExpr.Col.ColPos != pkIdx {
					return false, nil
				}
			default:
				return false, nil
			}

			switch subExpr := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_C:
				if leftIsConstant {
					return false, nil
				}
				return true, exprImpl.F.Args[1]
			case *plan.Expr_Col:
				if !leftIsConstant {
					return false, nil
				}
				if subExpr.Col.ColPos != pkIdx {
					return false, nil
				}
				return true, pkVal
			default:
				return false, nil
			}
		}
	}

	return false, nil
}

func getNonIntPkValueByExpr(expr *plan.Expr, pkIdx int32) (bool, any) {
	canCompute, valExpr := getNonIntPkExprValue(expr, pkIdx)
	if !canCompute {
		return canCompute, nil
	}
	switch val := valExpr.Expr.(*plan.Expr_C).C.Value.(type) {
	case *plan.Const_Ival:
		return true, val.Ival
	case *plan.Const_Dval:
		return true, val.Dval
	case *plan.Const_Sval:
		return true, val.Sval
	case *plan.Const_Bval:
		return true, val.Bval
	case *plan.Const_Uval:
		return true, val.Uval
	case *plan.Const_Fval:
		return true, val.Fval
	case *plan.Const_Dateval:
		return true, val.Dateval
	case *plan.Const_Timeval:
		return true, val.Timeval
	case *plan.Const_Datetimeval:
		return true, val.Datetimeval
	case *plan.Const_Decimal64Val:
		return true, val.Decimal64Val
	case *plan.Const_Decimal128Val:
		return true, val.Decimal128Val
	case *plan.Const_Timestampval:
		return true, val.Timestampval
	case *plan.Const_Jsonval:
		return true, val.Jsonval
	case *plan.Const_Defaultval:
		return true, val.Defaultval
	case *plan.Const_UpdateVal:
		return true, val.UpdateVal
	}
	return false, nil
}

// computeRangeByNonIntPk compute NonIntPk range Expr
// only support function :["and", "="]
// support eg: pk="a",  pk="a" and noPk > 200
// unsupport eg: pk>"a", pk=otherFun("a"),  pk="a" or noPk > 200,
func computeRangeByNonIntPk(expr *plan.Expr, pkIdx int32) (bool, uint64) {
	canCompute, valExpr := getNonIntPkExprValue(expr, pkIdx)
	if !canCompute {
		return canCompute, 0
	}
	ok, pkHashValue := getConstantExprHashValue(valExpr)
	if !ok {
		return false, 0
	}
	return true, pkHashValue
}

// computeRangeByIntPk compute primaryKey range by Expr
// only under the following conditions：
// 1、function named ["and", "or", ">", "<", ">=", "<=", "="]
// 2、if function name is not "and", "or".  then one arg is column, the other is constant
func computeRangeByIntPk(expr *plan.Expr, pkIdx int32, parentFun string) (bool, [][2]int64) {
	type argType int
	var typeConstant argType = 0
	var typeColumn argType = 1
	var leftArg argType
	var leftConstant, rightConstat int64
	var ok bool

	getConstant := func(e *plan.Expr_C) (bool, int64) {
		switch val := e.C.Value.(type) {
		case *plan.Const_Ival:
			return true, val.Ival
		case *plan.Const_Uval:
			if val.Uval > uint64(math.MaxInt64) {
				return false, 0
			}
			return true, int64(val.Uval)
		}
		return false, 0
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funName := exprImpl.F.Func.ObjName
		switch funName {
		case "and", "or":
			canCompute, leftRange := computeRangeByIntPk(exprImpl.F.Args[0], pkIdx, funName)
			if !canCompute {
				return canCompute, nil
			}

			canCompute, rightRange := computeRangeByIntPk(exprImpl.F.Args[1], pkIdx, funName)
			if !canCompute {
				return canCompute, nil
			}

			if funName == "and" {
				return true, _computeAnd(leftRange, rightRange)
			} else {
				return true, _computeOr(leftRange, rightRange)
			}

		case ">", "<", ">=", "<=", "=":
			switch subExpr := exprImpl.F.Args[0].Expr.(type) {
			case *plan.Expr_C:
				ok, leftConstant = getConstant(subExpr)
				if !ok {
					return false, nil
				}
				leftArg = typeConstant

			case *plan.Expr_Col:
				if subExpr.Col.ColPos != pkIdx {
					// if  pk > 10 and noPk < 10.  we just use pk > 10
					if parentFun == "and" {
						return true, [][2]int64{}
					}
					// if pk > 10 or noPk < 10,   we use all list
					return false, nil
				}
				leftArg = typeColumn

			default:
				return false, nil
			}

			switch subExpr := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_C:
				if leftArg == typeColumn {
					ok, rightConstat = getConstant(subExpr)
					if !ok {
						return false, nil
					}
					switch funName {
					case ">":
						return true, [][2]int64{{rightConstat + 1, math.MaxInt64}}
					case ">=":
						return true, [][2]int64{{rightConstat, math.MaxInt64}}
					case "<":
						return true, [][2]int64{{math.MinInt64, rightConstat - 1}}
					case "<=":
						return true, [][2]int64{{math.MinInt64, rightConstat}}
					case "=":
						return true, [][2]int64{{rightConstat, rightConstat}}
					}
					return false, nil
				}
			case *plan.Expr_Col:
				if subExpr.Col.ColPos != pkIdx {
					// if  pk > 10 and noPk < 10.  we just use pk > 10
					if parentFun == "and" {
						return true, [][2]int64{}
					}
					// if pk > 10 or noPk < 10,   we use all list
					return false, nil
				}

				if leftArg == typeConstant {
					switch funName {
					case ">":
						return true, [][2]int64{{math.MinInt64, leftConstant - 1}}
					case ">=":
						return true, [][2]int64{{math.MinInt64, leftConstant}}
					case "<":
						return true, [][2]int64{{leftConstant + 1, math.MaxInt64}}
					case "<=":
						return true, [][2]int64{{leftConstant, math.MaxInt64}}
					case "=":
						return true, [][2]int64{{leftConstant, leftConstant}}
					}
					return false, nil
				}
			}
		}
	}

	return false, nil
}

func _computeAnd(leftRange [][2]int64, rightRange [][2]int64) [][2]int64 {
	if len(leftRange) == 0 {
		return rightRange
	} else if len(rightRange) == 0 {
		return leftRange
	}

	compute := func(left [2]int64, right [2]int64) (bool, [2]int64) {
		min := left[0]
		max := left[1]

		if min > right[1] {
			// eg: a > 10 and a < 2
			return false, left
		} else if max < right[0] {
			// eg: a < 2 and a > 10
			return false, left
		} else {
			// eg: a > 2 and a < 10
			// a > 2 and a > 10
			// a > 2 and a = -2
			if right[0] > min {
				min = right[0]
			}
			if right[1] < max {
				max = right[1]
			}
			return true, [2]int64{min, max}
		}

	}

	// eg: (a >3 or a=1) and (a < 10 or a =11)
	var newRange [][2]int64
	for _, left := range leftRange {
		for _, right := range rightRange {
			ok, tmp := compute(left, right)
			if ok {
				newRange = append(newRange, tmp)
			}
		}
	}

	return newRange
}

func _computeOr(leftRange [][2]int64, rightRange [][2]int64) [][2]int64 {
	if len(leftRange) == 0 {
		return rightRange
	} else if len(rightRange) == 0 {
		return leftRange
	}

	compute := func(left [2]int64, right [2]int64) [][2]int64 {
		min := left[0]
		max := left[1]
		if min > right[1] {
			// eg: a > 10 or a < 2
			return [][2]int64{left, right}
		} else if max < right[0] {
			// eg: a < 2 or a > 10
			return [][2]int64{left, right}
		} else {
			// eg: a > 2 or a < 10
			// a > 2 or a > 10
			// a > 2 or a = -2
			if right[0] < min {
				min = right[0]
			}
			if right[1] > max {
				max = right[1]
			}
			return [][2]int64{{min, max}}
		}
	}

	// eg: (a>10 or a=1) or (a<5 or a=6)
	var newRange [][2]int64
	for _, left := range leftRange {
		for _, right := range rightRange {
			tmp := compute(left, right)
			newRange = append(newRange, tmp...)
		}
	}

	return newRange
}

func getHashValue(buf []byte) uint64 {
	buf = append([]byte{0}, buf...)
	var states [3]uint64
	if l := len(buf); l < 16 {
		buf = append(buf, hashtable.StrKeyPadding[l:]...)
	}
	hashtable.BytesBatchGenHashStates(&buf, &states, 1)
	return states[0]
}

func getListByRange[T DNStore](list []T, pkRange [][2]int64) []int {
	fullList := func() []int {
		dnList := make([]int, len(list))
		for i := range list {
			dnList[i] = i
		}
		return dnList
	}
	listLen := uint64(len(list))
	if listLen == 1 || len(pkRange) == 0 {
		return []int{0}
	}

	listMap := make(map[uint64]struct{})
	for _, r := range pkRange {
		if r[1]-r[0] > MAX_RANGE_SIZE {
			return fullList()
		}
		for i := r[0]; i <= r[1]; i++ {
			keys := make([]byte, 8)
			binary.LittleEndian.PutUint64(keys, uint64(i))
			val := getHashValue(keys)
			modVal := val % listLen
			listMap[modVal] = struct{}{}
			if len(listMap) == int(listLen) {
				return fullList()
			}
		}
	}
	dnList := make([]int, len(listMap))
	i := 0
	for idx := range listMap {
		dnList[i] = int(idx)
		i++
	}
	return dnList
}

func checkIfDataInBlock(data any, meta BlockMeta, colIdx int, typ types.Type) (bool, error) {
	zm := index.NewZoneMap(typ)
	err := zm.Unmarshal(meta.Zonemap[colIdx][:])
	if err != nil {
		return false, err
	}
	return zm.Contains(data), nil
}
