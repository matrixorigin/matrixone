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
	"context"
	"math"
	"strings"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	bat.Zs = []int64{1}

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

func getBinarySearchFuncByExpr(expr *plan.Expr, pkName string, oid types.T) (bool, func(*vector.Vector) int) {
	canCompute, valExpr := getPkExpr(expr, pkName)
	if !canCompute {
		return canCompute, nil
	}

	c := valExpr.Expr.(*plan.Expr_C)
	switch val := c.C.Value.(type) {
	case *plan.Const_I8Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(int8(val.I8Val))
	case *plan.Const_I16Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(int16(val.I16Val))
	case *plan.Const_I32Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(int32(val.I32Val))
	case *plan.Const_I64Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(int64(val.I64Val))
	case *plan.Const_Fval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Fval)
	case *plan.Const_Dval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Dval)
	case *plan.Const_U8Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(uint8(val.U8Val))
	case *plan.Const_U16Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(uint16(val.U16Val))
	case *plan.Const_U32Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(uint32(val.U32Val))
	case *plan.Const_U64Val:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.U64Val)
	case *plan.Const_Dateval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Dateval)
	case *plan.Const_Timeval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Timeval)
	case *plan.Const_Datetimeval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Datetimeval)
	case *plan.Const_Timestampval:
		return true, vector.OrderedBinarySearchOffsetByValFactory(val.Timestampval)
	case *plan.Const_Decimal64Val:
		return true, vector.FixSizedBinarySearchOffsetByValFactory(types.Decimal64(val.Decimal64Val.A), types.CompareDecimal64)
	case *plan.Const_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		return true, vector.FixSizedBinarySearchOffsetByValFactory(v, types.CompareDecimal128)
	case *plan.Const_Sval:
		return true, vector.VarlenBinarySearchOffsetByValFactory(util.UnsafeStringToBytes(val.Sval))
	case *plan.Const_Jsonval:
		return true, vector.VarlenBinarySearchOffsetByValFactory(util.UnsafeStringToBytes(val.Jsonval))
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

func logDebugf(txnMeta txn.TxnMeta, msg string, infos ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		infos = append(infos, txnMeta.DebugString())
		logutil.Debugf(msg+" %s", infos...)
	}
}

// SelectForSuperTenant is used to select CN servers for sys tenant.
// For sys tenant, there are some special strategies to select CN servers.
// The following strategies are listed in order of priority:
//  1. The CN servers which are configured as sys account.
//  2. The CN servers which are configured as some labels whose key is not account.
//  3. The CN servers which are configured as no labels.
//  4. At last, if no CN servers are selected,
//     4.1 If the username is dump or root, we just select one randomly.
//     4.2 Else, no servers are selected.
func SelectForSuperTenant(
	selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) {
	mc := clusterservice.GetMOCluster()

	// found is true indicates that we have find some available CN services.
	var found bool
	var emptyCNs, allCNs []*metadata.CNService

	// S1: Select servers that configured as sys account.
	mc.GetCNService(selector, func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		// At this phase, only append non-empty servers.
		if len(s.Labels) == 0 {
			emptyCNs = append(emptyCNs, &s)
		} else {
			found = true
			appendFn(&s)
		}
		return true
	})
	if found {
		return
	}

	// S2: If there are no servers that are configured as sys account.
	// There may be some performance issues, but we need to do this still.
	// Select all CN servers and select ones which are not configured as
	// label with key "account".
	mc.GetCNService(clusterservice.NewSelector(), func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		// Append CN servers that are not configured as label with key "account".
		if _, ok := s.Labels["account"]; len(s.Labels) > 0 && !ok {
			found = true
			appendFn(&s)
		}
		allCNs = append(allCNs, &s)
		return true
	})
	if found {
		return
	}

	// S3: Select CN servers which has no labels.
	if len(emptyCNs) > 0 {
		for _, cn := range emptyCNs {
			appendFn(cn)
		}
		return
	}

	// S4.1: If the root is super, return all servers.
	username = strings.ToLower(username)
	if username == "dump" || username == "root" {
		for _, cn := range allCNs {
			appendFn(cn)
		}
		return
	}

	// S4.2: No servers are returned.
}

// SelectForCommonTenant selects CN services for common tenant.
// If there are CN services for the selector, just select them,
// else, return CN services with empty labels if there are any.
func SelectForCommonTenant(
	selector clusterservice.Selector, filter func(string) bool, appendFn func(service *metadata.CNService),
) {
	mc := clusterservice.GetMOCluster()

	// found is true indicates that there are CN services for the selector.
	var found bool

	// preEmptyCNs keeps the CN services that has empty labels before we
	// find any CN service with non-empty label.
	var preEmptyCNs []*metadata.CNService

	mc.GetCNService(selector, func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		if len(s.Labels) > 0 {
			// Find available CN, append it.
			found = true
			appendFn(&s)
		} else {
			if found {
				// If there are already CN services with non-empty labels,
				// then ignore those with empty labels.
				return true
			} else {
				// If there are no CN services with non-empty labels yet,
				// save the CNs to preEmptyCNs first.
				preEmptyCNs = append(preEmptyCNs, &s)
				return true
			}
		}
		return true
	})

	// If there are no CN services with non-empty labels,
	// return those with empty labels.
	if !found && len(preEmptyCNs) > 0 {
		for _, cn := range preEmptyCNs {
			appendFn(cn)
		}
	}
}
