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
	"bytes"
	"context"
	"math"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	HASH_VALUE_FUN string = "hash_value"
	MAX_RANGE_SIZE int64  = 200
)

func fetchZonemapAndRowsFromBlockInfo(
	ctx context.Context,
	idxs []uint16,
	blockInfo catalog.BlockInfo,
	fs fileservice.FileService,
	m *mpool.MPool) ([][64]byte, uint32, error) {
	_, _, extent, rows, err := blockio.DecodeLocation(blockInfo.MetaLoc)
	if err != nil {
		return nil, 0, err
	}
	zonemapList := make([][64]byte, len(idxs))

	// raed s3
	reader, err := blockio.NewObjectReader(fs, blockInfo.MetaLoc)
	if err != nil {
		return nil, 0, err
	}

	obs, err := reader.LoadZoneMaps(ctx, idxs, []uint32{extent.Id()}, m)
	if err != nil {
		return nil, 0, err
	}

	for i := range idxs {
		bytes := obs[0][i].GetBuf()
		if err != nil {
			return nil, 0, err
		}
		copy(zonemapList[i][:], bytes[:])
	}

	return zonemapList, rows, nil
}

func getZonemapDataFromMeta(ctx context.Context, columns []int, meta BlockMeta, tableDef *plan.TableDef) ([][2]any, []uint8, error) {
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

		min := zm.GetMin()
		max := zm.GetMax()
		if min == nil || max == nil {
			return nil, nil, nil
		}
		datas[i] = [2]any{min, max}
	}

	return datas, dataTypes, nil
}

func getConstantExprHashValue(ctx context.Context, constExpr *plan.Expr, proc *process.Process) (bool, uint64) {
	args := []*plan.Expr{constExpr}
	argTypes := []types.Type{types.T(constExpr.Typ.Id).ToType()}
	funId, returnType, _, _ := function.GetFunctionByName(ctx, HASH_VALUE_FUN, argTypes)
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
	ret, err := colexec.EvalExpr(bat, proc, funExpr)
	if err != nil {
		return false, 0
	}
	list := vector.MustFixedCol[int64](ret)
	return true, uint64(list[0])
}

func compPkCol(colName string, pkName string) bool {
	dotIdx := strings.Index(colName, ".")
	colName = colName[dotIdx+1:]
	return colName == pkName
}

func getPkExpr(expr *plan.Expr, pkName string) (bool, *plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funName := exprImpl.F.Func.ObjName
		switch funName {
		case "and":
			canCompute, pkBytes := getPkExpr(exprImpl.F.Args[0], pkName)
			if canCompute {
				return canCompute, pkBytes
			}
			return getPkExpr(exprImpl.F.Args[1], pkName)

		case "=":
			switch leftExpr := exprImpl.F.Args[0].Expr.(type) {
			case *plan.Expr_C:
				if rightExpr, ok := exprImpl.F.Args[1].Expr.(*plan.Expr_Col); ok {
					if compPkCol(rightExpr.Col.Name, pkName) {
						return true, exprImpl.F.Args[0]
					}
				}

			case *plan.Expr_Col:
				if compPkCol(leftExpr.Col.Name, pkName) {
					if _, ok := exprImpl.F.Args[1].Expr.(*plan.Expr_C); ok {
						return true, exprImpl.F.Args[1]
					}
				}
			}

			return false, nil

		default:
			return false, nil
		}
	}

	return false, nil
}

func getPkValueByExpr(expr *plan.Expr, pkName string, oid types.T) (bool, any) {
	canCompute, valExpr := getPkExpr(expr, pkName)
	if !canCompute {
		return canCompute, nil
	}
	switch val := valExpr.Expr.(*plan.Expr_C).C.Value.(type) {
	case *plan.Const_I8Val:
		return transferIval(val.I8Val, oid)
	case *plan.Const_I16Val:
		return transferIval(val.I16Val, oid)
	case *plan.Const_I32Val:
		return transferIval(val.I32Val, oid)
	case *plan.Const_I64Val:
		return transferIval(val.I64Val, oid)
	case *plan.Const_Dval:
		return transferDval(val.Dval, oid)
	case *plan.Const_Sval:
		return transferSval(val.Sval, oid)
	case *plan.Const_Bval:
		return transferBval(val.Bval, oid)
	case *plan.Const_U8Val:
		return transferUval(val.U8Val, oid)
	case *plan.Const_U16Val:
		return transferUval(val.U16Val, oid)
	case *plan.Const_U32Val:
		return transferUval(val.U32Val, oid)
	case *plan.Const_U64Val:
		return transferUval(val.U64Val, oid)
	case *plan.Const_Fval:
		return transferFval(val.Fval, oid)
	case *plan.Const_Dateval:
		return transferDateval(val.Dateval, oid)
	case *plan.Const_Timeval:
		return transferTimeval(val.Timeval, oid)
	case *plan.Const_Datetimeval:
		return transferDatetimeval(val.Datetimeval, oid)
	case *plan.Const_Decimal64Val:
		return transferDecimal64val(val.Decimal64Val.A, oid)
	case *plan.Const_Decimal128Val:
		return transferDecimal128val(val.Decimal128Val.A, val.Decimal128Val.B, oid)
	case *plan.Const_Timestampval:
		return transferTimestampval(val.Timestampval, oid)
	case *plan.Const_Jsonval:
		return transferSval(val.Jsonval, oid)
	}
	return false, nil
}

// computeRangeByNonIntPk compute NonIntPk range Expr
// only support function :["and", "="]
// support eg: pk="a",  pk="a" and noPk > 200
// unsupport eg: pk>"a", pk=otherFun("a"),  pk="a" or noPk > 200,
func computeRangeByNonIntPk(ctx context.Context, expr *plan.Expr, pkName string, proc *process.Process) (bool, uint64) {
	canCompute, valExpr := getPkExpr(expr, pkName)
	if !canCompute {
		return canCompute, 0
	}
	ok, pkHashValue := getConstantExprHashValue(ctx, valExpr, proc)
	if !ok {
		return false, 0
	}
	return true, pkHashValue
}

// computeRangeByIntPk compute primaryKey range by Expr
// only under the following conditions：
// 1、function named ["and", "or", ">", "<", ">=", "<=", "="]
// 2、if function name is not "and", "or".  then one arg is column, the other is constant
func computeRangeByIntPk(expr *plan.Expr, pkName string, parentFun string) (bool, *pkRange) {
	type argType int
	var typeConstant argType = 0
	var typeColumn argType = 1
	var leftArg argType
	var leftConstant, rightConstat int64
	var ok bool

	getConstant := func(e *plan.Expr_C) (bool, int64) {
		switch val := e.C.Value.(type) {
		case *plan.Const_I8Val:
			return true, int64(val.I8Val)
		case *plan.Const_I16Val:
			return true, int64(val.I16Val)
		case *plan.Const_I32Val:
			return true, int64(val.I32Val)
		case *plan.Const_I64Val:
			return true, val.I64Val
		case *plan.Const_U8Val:
			return true, int64(val.U8Val)
		case *plan.Const_U16Val:
			return true, int64(val.U16Val)
		case *plan.Const_U32Val:
			return true, int64(val.U32Val)
		case *plan.Const_U64Val:
			if val.U64Val > uint64(math.MaxInt64) {
				return false, 0
			}
			return true, int64(val.U64Val)
		}
		return false, 0
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funName := exprImpl.F.Func.ObjName
		switch funName {
		case "and", "or":
			canCompute, leftRange := computeRangeByIntPk(exprImpl.F.Args[0], pkName, funName)
			if !canCompute {
				return canCompute, nil
			}

			canCompute, rightRange := computeRangeByIntPk(exprImpl.F.Args[1], pkName, funName)
			if !canCompute {
				return canCompute, nil
			}

			if funName == "and" {
				return _computeAnd(leftRange, rightRange)
			} else {
				return _computeOr(leftRange, rightRange)
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
				if !compPkCol(subExpr.Col.Name, pkName) {
					// if  pk > 10 and noPk < 10.  we just use pk > 10
					if parentFun == "and" {
						return true, &pkRange{
							isRange: false,
						}
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
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{rightConstat + 1, math.MaxInt64},
						}
					case ">=":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{rightConstat, math.MaxInt64},
						}
					case "<":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{math.MinInt64, rightConstat - 1},
						}
					case "<=":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{math.MinInt64, rightConstat},
						}
					case "=":
						return true, &pkRange{
							isRange: false,
							items:   []int64{rightConstat},
						}
					}
					return false, nil
				}
			case *plan.Expr_Col:
				if !compPkCol(subExpr.Col.Name, pkName) {
					// if  pk > 10 and noPk < 10.  we just use pk > 10
					if parentFun == "and" {
						return true, &pkRange{
							isRange: false,
						}
					}
					// if pk > 10 or noPk < 10,   we use all list
					return false, nil
				}

				if leftArg == typeConstant {
					switch funName {
					case ">":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{math.MinInt64, leftConstant - 1},
						}
					case ">=":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{math.MinInt64, leftConstant},
						}
					case "<":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{leftConstant + 1, math.MaxInt64},
						}
					case "<=":
						return true, &pkRange{
							isRange: true,
							ranges:  []int64{leftConstant, math.MaxInt64},
						}
					case "=":
						return true, &pkRange{
							isRange: false,
							items:   []int64{leftConstant},
						}
					}
					return false, nil
				}
			}
		}
	}

	return false, nil
}

func _computeOr(left *pkRange, right *pkRange) (bool, *pkRange) {
	result := &pkRange{
		isRange: false,
		items:   []int64{},
	}

	compute := func(left []int64, right []int64) [][]int64 {
		min := left[0]
		max := left[1]
		if min > right[1] {
			// eg: a > 10 or a < 2
			return [][]int64{left, right}
		} else if max < right[0] {
			// eg: a < 2 or a > 10
			return [][]int64{left, right}
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
			return [][]int64{{min, max}}
		}
	}

	if !left.isRange {
		if !right.isRange {
			result.items = append(left.items, right.items...)
			return len(result.items) < int(MAX_RANGE_SIZE), result
		} else {
			r := right.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return false, nil
			}
			result.items = append(result.items, left.items...)
			for i := right.ranges[0]; i <= right.ranges[1]; i++ {
				result.items = append(result.items, i)
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		}
	} else {
		if !right.isRange {
			r := left.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return false, nil
			}
			result.items = append(result.items, right.items...)
			for i := left.ranges[0]; i <= left.ranges[1]; i++ {
				result.items = append(result.items, i)
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		} else {
			newRange := compute(left.ranges, right.ranges)
			for _, r := range newRange {
				if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
					return false, nil
				}
				for i := r[0]; i <= r[1]; i++ {
					result.items = append(result.items, i)
				}
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		}
	}
}

func _computeAnd(left *pkRange, right *pkRange) (bool, *pkRange) {
	result := &pkRange{
		isRange: false,
		items:   []int64{},
	}

	compute := func(left []int64, right []int64) (bool, []int64) {
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
			return true, []int64{min, max}
		}
	}

	if !left.isRange {
		if !right.isRange {
			result.items = append(left.items, right.items...)
			return len(result.items) < int(MAX_RANGE_SIZE), result
		} else {
			r := right.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return false, nil
			}
			result.items = append(result.items, left.items...)
			for i := right.ranges[0]; i <= right.ranges[1]; i++ {
				result.items = append(result.items, i)
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		}
	} else {
		if !right.isRange {
			r := left.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return false, nil
			}
			result.items = append(result.items, right.items...)
			for i := left.ranges[0]; i <= left.ranges[1]; i++ {
				result.items = append(result.items, i)
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		} else {
			ok, r := compute(left.ranges, right.ranges)
			if !ok {
				return false, nil
			}
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return false, nil
			}
			for i := r[0]; i <= r[1]; i++ {
				result.items = append(result.items, i)
			}
			return len(result.items) < int(MAX_RANGE_SIZE), result
		}
	}
}

/*
func getHashValue(buf []byte) uint64 {
	buf = append([]byte{0}, buf...)
	var states [3]uint64
	if l := len(buf); l < 16 {
		buf = append(buf, hashtable.StrKeyPadding[l:]...)
	}
	hashtable.BytesBatchGenHashStates(&buf, &states, 1)
	return states[0]
}

func getListByItems[T DNStore](list []T, items []int64) []int {
	fullList := func() []int {
		dnList := make([]int, len(list))
		for i := range list {
			dnList[i] = i
		}
		return dnList
	}

	listLen := uint64(len(list))
	if listLen == 1 {
		return []int{0}
	}

	if len(items) == 0 || int64(len(items)) > MAX_RANGE_SIZE {
		return fullList()
	}

	listMap := make(map[uint64]struct{})
	for _, item := range items {
		keys := make([]byte, 8)
		binary.LittleEndian.PutUint64(keys, uint64(item))
		val := getHashValue(keys)
		modVal := val % listLen
		listMap[modVal] = struct{}{}
		if len(listMap) == int(listLen) {
			return fullList()
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
*/

// func getListByRange[T DNStore](list []T, pkRange [][2]int64) []int {
// 	fullList := func() []int {
// 		dnList := make([]int, len(list))
// 		for i := range list {
// 			dnList[i] = i
// 		}
// 		return dnList
// 	}
// 	listLen := uint64(len(list))
// 	if listLen == 1 || len(pkRange) == 0 {
// 		return []int{0}
// 	}

// 	listMap := make(map[uint64]struct{})
// 	for _, r := range pkRange {
// 		if r[1]-r[0] > MAX_RANGE_SIZE {
// 			return fullList()
// 		}
// 		for i := r[0]; i <= r[1]; i++ {
// 			keys := make([]byte, 8)
// 			binary.LittleEndian.PutUint64(keys, uint64(i))
// 			val := getHashValue(keys)
// 			modVal := val % listLen
// 			listMap[modVal] = struct{}{}
// 			if len(listMap) == int(listLen) {
// 				return fullList()
// 			}
// 		}
// 	}
// 	dnList := make([]int, len(listMap))
// 	i := 0
// 	for idx := range listMap {
// 		dnList[i] = int(idx)
// 		i++
// 	}
// 	return dnList
// }

func findRowByPkValue(vec *vector.Vector, v any) int {
	switch vec.GetType().Oid {
	case types.T_int8:
		rows := vector.MustFixedCol[int8](vec)
		val := v.(int8)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_int16:
		rows := vector.MustFixedCol[int16](vec)
		val := v.(int16)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_int32:
		rows := vector.MustFixedCol[int32](vec)
		val := v.(int32)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_int64:
		rows := vector.MustFixedCol[int64](vec)
		val := v.(int64)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_uint8:
		rows := vector.MustFixedCol[uint8](vec)
		val := v.(uint8)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_uint16:
		rows := vector.MustFixedCol[uint16](vec)
		val := v.(uint16)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_uint32:
		rows := vector.MustFixedCol[uint32](vec)
		val := v.(uint32)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_uint64:
		rows := vector.MustFixedCol[uint64](vec)
		val := v.(uint64)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_float32:
		rows := vector.MustFixedCol[float32](vec)
		val := v.(float32)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_float64:
		rows := vector.MustFixedCol[float64](vec)
		val := v.(float64)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_date:
		rows := vector.MustFixedCol[types.Date](vec)
		val := v.(types.Date)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_time:
		rows := vector.MustFixedCol[types.Time](vec)
		val := v.(types.Time)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_datetime:
		rows := vector.MustFixedCol[types.Datetime](vec)
		val := v.(types.Datetime)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_timestamp:
		rows := vector.MustFixedCol[types.Timestamp](vec)
		val := v.(types.Timestamp)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx] >= val
		})
	case types.T_uuid:
		rows := vector.MustFixedCol[types.Uuid](vec)
		val := v.(types.Uuid)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx].Ge(val)
		})
	case types.T_decimal64:
		rows := vector.MustFixedCol[types.Decimal64](vec)
		val := v.(types.Decimal64)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx].Compare(val) >= 0
		})
	case types.T_decimal128:
		rows := vector.MustFixedCol[types.Decimal128](vec)
		val := v.(types.Decimal128)
		return sort.Search(vec.Length(), func(idx int) bool {
			return rows[idx].Compare(val) >= 0
		})
	case types.T_char, types.T_text,
		types.T_binary, types.T_varbinary, types.T_varchar, types.T_json, types.T_blob:
		// rows := vector.MustStrCols(vec)
		// val := string(v.([]byte))
		// return sort.SearchStrings(rows, val)
		val := v.([]byte)
		area := vec.GetArea()
		varlenas := vector.MustFixedCol[types.Varlena](vec)
		return sort.Search(vec.Length(), func(idx int) bool {
			colVal := varlenas[idx].GetByteSlice(area)
			return bytes.Compare(colVal, val) >= 0
		})
	}

	return -1
}

func mustVectorFromProto(v *api.Vector) *vector.Vector {
	ret, err := vector.ProtoVectorToVector(v)
	if err != nil {
		panic(err)
	}
	return ret
}

func mustVectorToProto(v *vector.Vector) *api.Vector {
	ret, err := vector.VectorToProtoVector(v)
	if err != nil {
		panic(err)
	}
	return ret
}

func logDebugf(txnMeta txn.TxnMeta, msg string, infos ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		infos = append(infos, txnMeta.DebugString())
		logutil.Debugf(msg+" %s", infos...)
	}
}
