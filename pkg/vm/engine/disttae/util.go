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
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	HASH_VALUE_FUN string = "hash_value"
	MAX_RANGE_SIZE int64  = 200
)

func getConstantExprHashValue(ctx context.Context, constExpr *plan.Expr, proc *process.Process) (bool, uint64) {
	args := []*plan.Expr{constExpr}
	argTypes := []types.Type{types.T(constExpr.Typ.Id).ToType()}
	fGet, err := function.GetFunctionByName(ctx, HASH_VALUE_FUN, argTypes)
	if err != nil {
		panic(err)
	}
	funId, returnType := fGet.GetEncodedOverloadID(), fGet.GetReturnType()
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
	bat.SetRowCount(1)

	ret, err := colexec.EvalExpressionOnce(proc, funExpr, []*batch.Batch{bat})
	if err != nil {
		return false, 0
	}
	value := vector.MustFixedCol[int64](ret)[0]
	ret.Free(proc.Mp())

	return true, uint64(value)
}

func compPkCol(colName string, pkName string) bool {
	dotIdx := strings.Index(colName, ".")
	colName = colName[dotIdx+1:]
	return colName == pkName
}

func getPosInCompositPK(name string, pks []string) int {
	for i, pk := range pks {
		if compPkCol(name, pk) {
			return i
		}
	}
	return -1
}

func getValidCompositePKCnt(vals []*plan.Literal) int {
	if len(vals) == 0 {
		return 0
	}
	cnt := 0
	for _, val := range vals {
		if val == nil {
			break
		}
		cnt++
	}

	return cnt
}

func getCompositPKVals(
	expr *plan.Expr,
	pks []string,
	vals []*plan.Literal,
	proc *process.Process,
) (ok bool, hasNull bool) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		fname := exprImpl.F.Func.ObjName
		switch fname {
		case "and":
			_, hasNull = getCompositPKVals(exprImpl.F.Args[0], pks, vals, proc)
			if hasNull {
				return false, true
			}
			return getCompositPKVals(exprImpl.F.Args[1], pks, vals, proc)

		case "=":
			if leftExpr, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col); ok {
				if pos := getPosInCompositPK(leftExpr.Col.Name, pks); pos != -1 {
					ret := getConstValueByExpr(exprImpl.F.Args[1], proc)
					if ret == nil {
						return false, false
					} else if ret.Isnull {
						return false, true
					}
					vals[pos] = ret
					return true, false
				}
				return false, false
			}
			if rightExpr, ok := exprImpl.F.Args[1].Expr.(*plan.Expr_Col); ok {
				if pos := getPosInCompositPK(rightExpr.Col.Name, pks); pos != -1 {
					ret := getConstValueByExpr(exprImpl.F.Args[0], proc)
					if ret == nil {
						return false, false
					} else if ret.Isnull {
						return false, true
					}
					vals[pos] = ret
					return true, false
				}
				return false, false
			}
			return false, false

		case "in":
		}
	}
	return false, false
}

func getPkExpr(expr *plan.Expr, pkName string, proc *process.Process) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "and":
			pkBytes := getPkExpr(exprImpl.F.Args[0], pkName, proc)
			if pkBytes != nil {
				return pkBytes
			}
			return getPkExpr(exprImpl.F.Args[1], pkName, proc)

		case "=":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				constVal := getConstValueByExpr(exprImpl.F.Args[1], proc)
				if constVal == nil {
					return nil
				}
				return &plan.Expr{
					Typ: plan2.DeepCopyType(exprImpl.F.Args[1].Typ),
					Expr: &plan.Expr_Lit{
						Lit: constVal,
					},
				}
			}
			if col := exprImpl.F.Args[1].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				constVal := getConstValueByExpr(exprImpl.F.Args[0], proc)
				if constVal == nil {
					return nil
				}
				return &plan.Expr{
					Typ: plan2.DeepCopyType(exprImpl.F.Args[0].Typ),
					Expr: &plan.Expr_Lit{
						Lit: constVal,
					},
				}
			}
			return nil

		case "in":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				return exprImpl.F.Args[1]
			}

		case "prefix_eq", "prefix_between", "prefix_in":
			if col := exprImpl.F.Args[0].GetCol(); col != nil {
				if !compPkCol(col.Name, pkName) {
					return nil
				}
				return expr
			}
		}
	}

	return nil
}

func getNonCompositePKSearchFuncByExpr(
	expr *plan.Expr, pkName string, oid types.T, proc *process.Process,
) (bool, bool, blockio.ReadFilter) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return false, false, nil
	}

	var searchPKFunc func(*vector.Vector) []int32

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			return false, true, nil
		}

		switch val := exprImpl.Lit.Value.(type) {
		case *plan.Literal_I8Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]int8{int8(val.I8Val)})
		case *plan.Literal_I16Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]int16{int16(val.I16Val)})
		case *plan.Literal_I32Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]int32{int32(val.I32Val)})
		case *plan.Literal_I64Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]int64{val.I64Val})
		case *plan.Literal_Fval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]float32{val.Fval})
		case *plan.Literal_Dval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]float64{val.Dval})
		case *plan.Literal_U8Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint8{uint8(val.U8Val)})
		case *plan.Literal_U16Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint16{uint16(val.U16Val)})
		case *plan.Literal_U32Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint32{uint32(val.U32Val)})
		case *plan.Literal_U64Val:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]uint64{val.U64Val})
		case *plan.Literal_Dateval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Date{types.Date(val.Dateval)})
		case *plan.Literal_Timeval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Time{types.Time(val.Timeval)})
		case *plan.Literal_Datetimeval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Datetime{types.Datetime(val.Datetimeval)})
		case *plan.Literal_Timestampval:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Timestamp{types.Timestamp(val.Timestampval)})
		case *plan.Literal_Decimal64Val:
			searchPKFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal64{types.Decimal64(val.Decimal64Val.A)}, types.CompareDecimal64)
		case *plan.Literal_Decimal128Val:
			v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
			searchPKFunc = vector.FixedSizedBinarySearchOffsetByValFactory([]types.Decimal128{v}, types.CompareDecimal128)
		case *plan.Literal_Sval:
			searchPKFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{util.UnsafeStringToBytes(val.Sval)})
		case *plan.Literal_Jsonval:
			searchPKFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{util.UnsafeStringToBytes(val.Jsonval)})
		case *plan.Literal_EnumVal:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.Enum(val.EnumVal)})
		}

	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "prefix_eq":
			val := util.UnsafeStringToBytes(exprImpl.F.Args[1].GetLit().GetSval())
			searchPKFunc = vector.CollectOffsetsByPrefixEqFactory(val)

		case "prefix_between":
			lval := util.UnsafeStringToBytes(exprImpl.F.Args[1].GetLit().GetSval())
			rval := util.UnsafeStringToBytes(exprImpl.F.Args[2].GetLit().GetSval())
			searchPKFunc = vector.CollectOffsetsByPrefixBetweenFactory(lval, rval)

		case "prefix_in":
			vec := vector.NewVec(types.T_any.ToType())
			vec.UnmarshalBinary(exprImpl.F.Args[1].GetVec().Data)
			searchPKFunc = vector.CollectOffsetsByPrefixInFactory(vec)
		}

	case *plan.Expr_Vec:
		vec := vector.NewVec(types.T_any.ToType())
		vec.UnmarshalBinary(exprImpl.Vec.Data)

		switch vec.GetType().Oid {
		case types.T_int8:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int8](vec))
		case types.T_int16:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int16](vec))
		case types.T_int32:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int32](vec))
		case types.T_int64:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[int64](vec))
		case types.T_uint8:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint8](vec))
		case types.T_uint16:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint16](vec))
		case types.T_uint32:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint32](vec))
		case types.T_uint64:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint64](vec))
		case types.T_float32:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[float32](vec))
		case types.T_float64:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[float64](vec))
		case types.T_date:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Date](vec))
		case types.T_time:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Time](vec))
		case types.T_datetime:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Datetime](vec))
		case types.T_timestamp:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Timestamp](vec))
		case types.T_decimal64:
			searchPKFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Decimal64](vec), types.CompareDecimal64)
		case types.T_decimal128:
			searchPKFunc = vector.FixedSizedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Decimal128](vec), types.CompareDecimal128)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64:
			searchPKFunc = vector.VarlenBinarySearchOffsetByValFactory(vector.MustBytesCol(vec))
		case types.T_enum:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[types.Enum](vec))
		}
	}

	if searchPKFunc != nil {
		return true, false, func(vecs []*vector.Vector) []int32 {
			return searchPKFunc(vecs[0])
		}
	}

	return false, false, nil
}

func getPkValueByExpr(
	expr *plan.Expr,
	pkName string,
	oid types.T,
	proc *process.Process,
) (bool, bool, any) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return false, false, nil
	}

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			return false, true, nil
		}
		switch val := exprImpl.Lit.Value.(type) {
		case *plan.Literal_I8Val:
			return transferIval(val.I8Val, oid)
		case *plan.Literal_I16Val:
			return transferIval(val.I16Val, oid)
		case *plan.Literal_I32Val:
			return transferIval(val.I32Val, oid)
		case *plan.Literal_I64Val:
			return transferIval(val.I64Val, oid)
		case *plan.Literal_Dval:
			return transferDval(val.Dval, oid)
		case *plan.Literal_Sval:
			return transferSval(val.Sval, oid)
		case *plan.Literal_Bval:
			return transferBval(val.Bval, oid)
		case *plan.Literal_U8Val:
			return transferUval(val.U8Val, oid)
		case *plan.Literal_U16Val:
			return transferUval(val.U16Val, oid)
		case *plan.Literal_U32Val:
			return transferUval(val.U32Val, oid)
		case *plan.Literal_U64Val:
			return transferUval(val.U64Val, oid)
		case *plan.Literal_Fval:
			return transferFval(val.Fval, oid)
		case *plan.Literal_Dateval:
			return transferDateval(val.Dateval, oid)
		case *plan.Literal_Timeval:
			return transferTimeval(val.Timeval, oid)
		case *plan.Literal_Datetimeval:
			return transferDatetimeval(val.Datetimeval, oid)
		case *plan.Literal_Decimal64Val:
			return transferDecimal64val(val.Decimal64Val.A, oid)
		case *plan.Literal_Decimal128Val:
			return transferDecimal128val(val.Decimal128Val.A, val.Decimal128Val.B, oid)
		case *plan.Literal_Timestampval:
			return transferTimestampval(val.Timestampval, oid)
		case *plan.Literal_Jsonval:
			return transferSval(val.Jsonval, oid)
		case *plan.Literal_EnumVal:
			return transferUval(val.EnumVal, oid)
		}

	case *plan.Expr_Vec:
	}

	return false, false, nil
}

// computeRangeByNonIntPk compute NonIntPk range Expr
// only support function :["and", "="]
// support eg: pk="a",  pk="a" and noPk > 200
// unsupport eg: pk>"a", pk=otherFun("a"),  pk="a" or noPk > 200,
func computeRangeByNonIntPk(ctx context.Context, expr *plan.Expr, pkName string, proc *process.Process) (bool, uint64) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return false, 0
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

	getConstant := func(e *plan.Expr_Lit) (bool, int64) {
		switch val := e.Lit.Value.(type) {
		case *plan.Literal_I8Val:
			return true, int64(val.I8Val)
		case *plan.Literal_I16Val:
			return true, int64(val.I16Val)
		case *plan.Literal_I32Val:
			return true, int64(val.I32Val)
		case *plan.Literal_I64Val:
			return true, val.I64Val
		case *plan.Literal_U8Val:
			return true, int64(val.U8Val)
		case *plan.Literal_U16Val:
			return true, int64(val.U16Val)
		case *plan.Literal_U32Val:
			return true, int64(val.U32Val)
		case *plan.Literal_U64Val:
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
			case *plan.Expr_Lit:
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
			case *plan.Expr_Lit:
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

func logDebugf(txnMeta txn.TxnMeta, msg string, infos ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		infos = append(infos, txnMeta.DebugString())
		logutil.Debugf(msg+" %s", infos...)
	}
}

// Eval selected on column factories
// 1. Sorted column
// 1.1 ordered type column
// 1.2 Fixed len type column
// 1.3 Varlen type column
//
// 2. Unsorted column
// 2.1 Fixed len type column
// 2.2 Varlen type column

// 1.1 ordered column type + sorted column
func EvalSelectedOnOrderedSortedColumnFactory[T types.OrderedT](
	v T,
) func(*vector.Vector, []int32, *[]int32) {
	return func(col *vector.Vector, sels []int32, newSels *[]int32) {
		vals := vector.MustFixedCol[T](col)
		idx := vector.OrderedFindFirstIndexInSortedSlice(v, vals)
		if idx < 0 {
			return
		}
		if len(sels) == 0 {
			for idx < len(vals) {
				if vals[idx] != v {
					break
				}
				*newSels = append(*newSels, int32(idx))
				idx++
			}
		} else {
			// sels is not empty
			for valIdx, selIdx := idx, 0; valIdx < len(vals) && selIdx < len(sels); {
				if vals[valIdx] != v {
					break
				}
				sel := sels[selIdx]
				if sel < int32(valIdx) {
					selIdx++
				} else if sel == int32(valIdx) {
					*newSels = append(*newSels, sels[selIdx])
					selIdx++
					valIdx++
				} else {
					valIdx++
				}
			}
		}
	}
}

// 1.2 fixed size column type + sorted column
func EvalSelectedOnFixedSizeSortedColumnFactory[T types.FixedSizeTExceptStrType](
	v T, comp func(T, T) int,
) func(*vector.Vector, []int32, *[]int32) {
	return func(col *vector.Vector, sels []int32, newSels *[]int32) {
		vals := vector.MustFixedCol[T](col)
		idx := vector.FixedSizeFindFirstIndexInSortedSliceWithCompare(v, vals, comp)
		if idx < 0 {
			return
		}
		if len(sels) == 0 {
			for idx < len(vals) {
				if comp(vals[idx], v) != 0 {
					break
				}
				*newSels = append(*newSels, int32(idx))
				idx++
			}
		} else {
			// sels is not empty
			for valIdx, selIdx := idx, 0; valIdx < len(vals) && selIdx < len(sels); {
				if comp(vals[valIdx], v) != 0 {
					break
				}
				sel := sels[selIdx]
				if sel < int32(valIdx) {
					selIdx++
				} else if sel == int32(valIdx) {
					*newSels = append(*newSels, sel)
					selIdx++
					valIdx++
				} else {
					valIdx++
				}
			}
		}
	}
}

// 1.3 varlen type column + sorted
func EvalSelectedOnVarlenSortedColumnFactory(
	v []byte,
) func(*vector.Vector, []int32, *[]int32) {
	return func(col *vector.Vector, sels []int32, newSels *[]int32) {
		idx := vector.FindFirstIndexInSortedVarlenVector(col, v)
		if idx < 0 {
			return
		}
		length := col.Length()
		if len(sels) == 0 {
			for idx < length {
				if !bytes.Equal(col.GetBytesAt(idx), v) {
					break
				}
				*newSels = append(*newSels, int32(idx))
				idx++
			}
		} else {
			// sels is not empty
			for valIdx, selIdx := idx, 0; valIdx < length && selIdx < len(sels); {
				if !bytes.Equal(col.GetBytesAt(valIdx), v) {
					break
				}
				sel := sels[selIdx]
				if sel < int32(valIdx) {
					selIdx++
				} else if sel == int32(valIdx) {
					*newSels = append(*newSels, sels[selIdx])
					selIdx++
					valIdx++
				} else {
					valIdx++
				}
			}
		}
	}
}

// 2.1 fixedSize type column + non-sorted
func EvalSelectedOnFixedSizeColumnFactory[T types.FixedSizeTExceptStrType](
	v T,
) func(*vector.Vector, []int32, *[]int32) {
	return func(col *vector.Vector, sels []int32, newSels *[]int32) {
		vals := vector.MustFixedCol[T](col)
		if len(sels) == 0 {
			for idx, val := range vals {
				if val == v {
					*newSels = append(*newSels, int32(idx))
				}
			}
		} else {
			for _, idx := range sels {
				if vals[idx] == v {
					*newSels = append(*newSels, idx)
				}
			}
		}
	}
}

// 2.2 varlen type column + non-sorted
func EvalSelectedOnVarlenColumnFactory(
	v []byte,
) func(*vector.Vector, []int32, *[]int32) {
	return func(col *vector.Vector, sels []int32, newSels *[]int32) {
		if len(sels) == 0 {
			for idx := 0; idx < col.Length(); idx++ {
				if bytes.Equal(col.GetBytesAt(idx), v) {
					*newSels = append(*newSels, int32(idx))
				}
			}
		} else {
			for _, idx := range sels {
				if bytes.Equal(col.GetBytesAt(int(idx)), v) {
					*newSels = append(*newSels, idx)
				}
			}
		}
	}
}

// for composite primary keys:
// 1. all related columns may have duplicated values
// 2. only the first column is sorted
// the filter function receives a vector as the column values and selected rows
// it evaluates the filter expression only on the selected rows and returns the selected rows
// which are evaluated to true
func getCompositeFilterFuncByExpr(
	expr *plan.Literal, isSorted bool,
) func(*vector.Vector, []int32, *[]int32) {
	switch val := expr.Value.(type) {
	case *plan.Literal_Bval:
		return EvalSelectedOnFixedSizeColumnFactory(val.Bval)
	case *plan.Literal_I8Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int8(val.I8Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int8(val.I8Val))
	case *plan.Literal_I16Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int16(val.I16Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int16(val.I16Val))
	case *plan.Literal_I32Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int32(val.I32Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int32(val.I32Val))
	case *plan.Literal_I64Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int64(val.I64Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int64(val.I64Val))
	case *plan.Literal_U8Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint8(val.U8Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint8(val.U8Val))
	case *plan.Literal_U16Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint16(val.U16Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint16(val.U16Val))
	case *plan.Literal_U32Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint32(val.U32Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint32(val.U32Val))
	case *plan.Literal_U64Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint64(val.U64Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint64(val.U64Val))
	case *plan.Literal_Fval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(float32(val.Fval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(float32(val.Fval))
	case *plan.Literal_Dval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Dval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Dval)
	case *plan.Literal_Timeval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(types.Time(val.Timeval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(types.Time(val.Timeval))
	case *plan.Literal_Timestampval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(types.Timestamp(val.Timestampval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(types.Timestamp(val.Timestampval))
	case *plan.Literal_Dateval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(types.Date(val.Dateval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(types.Date(val.Dateval))
	case *plan.Literal_Datetimeval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(types.Datetime(val.Datetimeval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(types.Datetime(val.Datetimeval))
	case *plan.Literal_Decimal64Val:
		v := types.Decimal64(val.Decimal64Val.A)
		if isSorted {
			return EvalSelectedOnFixedSizeSortedColumnFactory(v, types.CompareDecimal64)
		}
		return EvalSelectedOnFixedSizeColumnFactory(v)
	case *plan.Literal_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		if isSorted {
			return EvalSelectedOnFixedSizeSortedColumnFactory(v, types.CompareDecimal128)
		}
		return EvalSelectedOnFixedSizeColumnFactory(v)
	case *plan.Literal_Sval:
		if isSorted {
			return EvalSelectedOnVarlenSortedColumnFactory(util.UnsafeStringToBytes(val.Sval))
		}
		return EvalSelectedOnVarlenColumnFactory(util.UnsafeStringToBytes(val.Sval))
	case *plan.Literal_Jsonval:
		if isSorted {
			return EvalSelectedOnVarlenSortedColumnFactory(util.UnsafeStringToBytes(val.Jsonval))
		}
		return EvalSelectedOnVarlenColumnFactory(util.UnsafeStringToBytes(val.Jsonval))
	case *plan.Literal_EnumVal:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.EnumVal)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.EnumVal)
	default:
		panic(fmt.Sprintf("unexpected const expr %v", expr))
	}
}

func serialTupleByConstExpr(expr *plan.Literal, packer *types.Packer) {
	switch val := expr.Value.(type) {
	case *plan.Literal_Bval:
		packer.EncodeBool(val.Bval)
	case *plan.Literal_I8Val:
		packer.EncodeInt8(int8(val.I8Val))
	case *plan.Literal_I16Val:
		packer.EncodeInt16(int16(val.I16Val))
	case *plan.Literal_I32Val:
		packer.EncodeInt32(val.I32Val)
	case *plan.Literal_I64Val:
		packer.EncodeInt64(val.I64Val)
	case *plan.Literal_U8Val:
		packer.EncodeUint8(uint8(val.U8Val))
	case *plan.Literal_U16Val:
		packer.EncodeUint16(uint16(val.U16Val))
	case *plan.Literal_U32Val:
		packer.EncodeUint32(val.U32Val)
	case *plan.Literal_U64Val:
		packer.EncodeUint64(val.U64Val)
	case *plan.Literal_Fval:
		packer.EncodeFloat32(val.Fval)
	case *plan.Literal_Dval:
		packer.EncodeFloat64(val.Dval)
	case *plan.Literal_Timeval:
		packer.EncodeTime(types.Time(val.Timeval))
	case *plan.Literal_Timestampval:
		packer.EncodeTimestamp(types.Timestamp(val.Timestampval))
	case *plan.Literal_Dateval:
		packer.EncodeDate(types.Date(val.Dateval))
	case *plan.Literal_Datetimeval:
		packer.EncodeDatetime(types.Datetime(val.Datetimeval))
	case *plan.Literal_Decimal64Val:
		packer.EncodeDecimal64(types.Decimal64(val.Decimal64Val.A))
	case *plan.Literal_Decimal128Val:
		packer.EncodeDecimal128(types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)})
	case *plan.Literal_Sval:
		packer.EncodeStringType(util.UnsafeStringToBytes(val.Sval))
	case *plan.Literal_Jsonval:
		packer.EncodeStringType(util.UnsafeStringToBytes(val.Jsonval))
	case *plan.Literal_EnumVal:
		packer.EncodeEnum(types.Enum(val.EnumVal))
	default:
		panic(fmt.Sprintf("unexpected const expr %v", expr))
	}
}

func getConstValueByExpr(expr *plan.Expr,
	proc *process.Process) *plan.Literal {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil
	}
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return nil
	}
	defer exec.Free()
	return rule.GetConstantValue(vec, true, 0)
}

func getConstExpr(oid int32, c *plan.Literal) *plan.Expr {
	return &plan.Expr{
		Typ:  &plan.Type{Id: oid},
		Expr: &plan.Expr_Lit{Lit: c},
	}
}

func extractCompositePKValueFromEqualExprs(
	exprs []*plan.Expr,
	pkDef *plan.PrimaryKeyDef,
	proc *process.Process,
	pool *fileservice.Pool[*types.Packer],
) (val []byte) {
	var packer *types.Packer
	put := pool.Get(&packer)
	defer put.Put()

	vals := make([]*plan.Literal, len(pkDef.Names))
	for _, expr := range exprs {
		tmpVals := make([]*plan.Literal, len(pkDef.Names))
		if _, hasNull := getCompositPKVals(
			expr, pkDef.Names, tmpVals, proc,
		); hasNull {
			return
		}
		for i := range tmpVals {
			if tmpVals[i] == nil {
				continue
			}
			vals[i] = tmpVals[i]
		}
	}

	// check all composite pk values are exist
	// if not, check next expr
	cnt := getValidCompositePKCnt(vals)
	if cnt != len(vals) {
		return
	}

	// serialize composite pk values into bytes as the pk value
	// and break the loop
	for i := 0; i < cnt; i++ {
		serialTupleByConstExpr(vals[i], packer)
	}
	val = packer.Bytes()
	return
}

func extractPKValueFromEqualExprs(
	def *plan.TableDef,
	exprs []*plan.Expr,
	pkIdx int,
	proc *process.Process,
	pool *fileservice.Pool[*types.Packer],
) (val []byte) {
	pk := def.Pkey
	if pk.CompPkeyCol != nil {
		return extractCompositePKValueFromEqualExprs(
			exprs, pk, proc, pool,
		)
	}
	column := def.Cols[pkIdx]
	name := column.Name
	colType := types.T(column.Typ.Id)
	for _, expr := range exprs {
		if ok, _, v := getPkValueByExpr(expr, name, colType, proc); ok {
			val = types.EncodeValue(v, colType)
			break
		}
	}
	return
}

// ListTnService gets all tn service in the cluster
func ListTnService(appendFn func(service *metadata.TNService)) {
	mc := clusterservice.GetMOCluster()
	mc.GetTNService(clusterservice.NewSelector(), func(tn metadata.TNService) bool {
		if appendFn != nil {
			appendFn(&tn)
		}
		return true
	})
}

// util function for object stats

// UnfoldBlkInfoFromObjStats constructs a block info list from the given object stats.
// this unfolds all block info at one operation, if an object contains a great many of blocks,
// this operation is memory sensitive, we recommend another way, StatsBlkIter or ForEach.
func UnfoldBlkInfoFromObjStats(stats *objectio.ObjectStats) (blks []catalog.BlockInfo) {
	if stats.IsZero() {
		return blks
	}

	name := stats.ObjectName()
	blkCnt := uint16(stats.BlkCnt())
	rowTotalCnt := stats.Rows()
	accumulate := uint32(0)

	for idx := uint16(0); idx < blkCnt; idx++ {
		blkRows := uint32(options.DefaultBlockMaxRows)
		if idx == blkCnt-1 {
			blkRows = rowTotalCnt - accumulate
		}
		accumulate += blkRows
		loc := objectio.BuildLocation(name, stats.Extent(), blkRows, idx)
		blks = append(blks, catalog.BlockInfo{
			BlockID:   *objectio.BuildObjectBlockid(name, idx),
			MetaLoc:   catalog.ObjectLocation(loc),
			SegmentID: name.SegmentId(),
		})
	}

	return blks
}

// ForeachBlkInObjStatsList receives an object stats list,
// and visits each blk of these object stats by OnBlock,
// until the onBlock returns false or all blks have been enumerated.
// when onBlock returns a false,
// the nextStats argument decides whether continue onBlock on the next stats or exit foreach completely.
func ForeachBlkInObjStatsList(
	nextStats bool,
	onBlock func(blk *catalog.BlockInfo) bool, statsList ...objectio.ObjectStats) {
	stop := false
	statsCnt := len(statsList)

	for idx := 0; idx < statsCnt && !stop; idx++ {
		iter := NewStatsBlkIter(&statsList[idx])
		for iter.Next() {
			blk := iter.Entry()
			if !onBlock(blk) {
				stop = true
				break
			}
		}

		if stop && nextStats {
			stop = false
		}
	}
}

type StatsBlkIter struct {
	name       objectio.ObjectName
	extent     objectio.Extent
	blkCnt     uint16
	totalRows  uint32
	cur        int
	accRows    uint32
	curBlkRows uint32
}

func NewStatsBlkIter(stats *objectio.ObjectStats) *StatsBlkIter {
	return &StatsBlkIter{
		name:       stats.ObjectName(),
		blkCnt:     uint16(stats.BlkCnt()),
		extent:     stats.Extent(),
		cur:        -1,
		accRows:    0,
		totalRows:  stats.Rows(),
		curBlkRows: options.DefaultBlockMaxRows,
	}
}

func (i *StatsBlkIter) Next() bool {
	if i.cur >= 0 {
		i.accRows += i.curBlkRows
	}
	i.cur++
	return i.cur < int(i.blkCnt)
}

func (i *StatsBlkIter) Entry() *catalog.BlockInfo {
	if i.cur == -1 {
		i.cur = 0
	}

	// assume that all blks have DefaultBlockMaxRows, except the last one
	if i.cur == int(i.blkCnt-1) {
		i.curBlkRows = i.totalRows - i.accRows
	}

	loc := objectio.BuildLocation(i.name, i.extent, i.curBlkRows, uint16(i.cur))
	blk := &catalog.BlockInfo{
		BlockID:   *objectio.BuildObjectBlockid(i.name, uint16(i.cur)),
		SegmentID: i.name.SegmentId(),
		MetaLoc:   catalog.ObjectLocation(loc),
	}
	return blk
}

func ConstructObjStatsByLoadObjMeta(
	ctx context.Context, metaLoc objectio.Location,
	fs fileservice.FileService) (stats objectio.ObjectStats, dataMeta objectio.ObjectDataMeta, err error) {

	// 1. load object meta
	var meta objectio.ObjectMeta
	if meta, err = objectio.FastLoadObjectMeta(ctx, &metaLoc, false, fs); err != nil {
		logutil.Error("fast load object meta failed when split object stats. ", zap.Error(err))
		return
	}
	dataMeta = meta.MustDataMeta()

	// 2. construct an object stats
	objectio.SetObjectStatsObjectName(&stats, metaLoc.Name())
	objectio.SetObjectStatsExtent(&stats, metaLoc.Extent())
	objectio.SetObjectStatsBlkCnt(&stats, dataMeta.BlockCount())

	sortKeyIdx := dataMeta.BlockHeader().SortKey()
	objectio.SetObjectStatsSortKeyZoneMap(&stats, dataMeta.MustGetColumn(sortKeyIdx).ZoneMap())

	totalRows := uint32(0)
	for idx := uint32(0); idx < dataMeta.BlockCount(); idx++ {
		totalRows += dataMeta.GetBlockMeta(idx).GetRows()
	}

	objectio.SetObjectStatsRowCnt(&stats, totalRows)

	return
}
