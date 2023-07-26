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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
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

func getValidCompositePKCnt(vals []*plan.Const) int {
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
	vals []*plan.Const,
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
					_, ret := getConstValueByExpr(exprImpl.F.Args[1], proc)
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
					_, ret := getConstValueByExpr(exprImpl.F.Args[0], proc)
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
		}
	}
	return false, false
}

func getPkExpr(expr *plan.Expr, pkName string, proc *process.Process) (bool, *plan.Const) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funName := exprImpl.F.Func.ObjName
		switch funName {
		case "and":
			canCompute, pkBytes := getPkExpr(exprImpl.F.Args[0], pkName, proc)
			if canCompute {
				return canCompute, pkBytes
			}
			return getPkExpr(exprImpl.F.Args[1], pkName, proc)

		case "=":
			if leftExpr, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col); ok {
				if !compPkCol(leftExpr.Col.Name, pkName) {
					return false, nil
				}
				return getConstValueByExpr(exprImpl.F.Args[1], proc)
			}
			if rightExpr, ok := exprImpl.F.Args[1].Expr.(*plan.Expr_Col); ok {
				if !compPkCol(rightExpr.Col.Name, pkName) {
					return false, nil
				}
				return getConstValueByExpr(exprImpl.F.Args[0], proc)
			}
			return false, nil

		default:
			return false, nil
		}
	}

	return false, nil
}

func getNonCompositePKSearchFuncByExpr(
	expr *plan.Expr, pkName string, oid types.T, proc *process.Process,
) (bool, bool, func(*vector.Vector) int) {
	canCompute, valExpr := getPkExpr(expr, pkName, proc)
	if !canCompute {
		return false, false, nil
	}

	if valExpr.Isnull {
		return false, true, nil
	}

	switch val := valExpr.Value.(type) {
	case *plan.Const_I8Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(int8(val.I8Val))
	case *plan.Const_I16Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(int16(val.I16Val))
	case *plan.Const_I32Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(int32(val.I32Val))
	case *plan.Const_I64Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(int64(val.I64Val))
	case *plan.Const_Fval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Fval)
	case *plan.Const_Dval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Dval)
	case *plan.Const_U8Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(uint8(val.U8Val))
	case *plan.Const_U16Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(uint16(val.U16Val))
	case *plan.Const_U32Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(uint32(val.U32Val))
	case *plan.Const_U64Val:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.U64Val)
	case *plan.Const_Dateval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Dateval)
	case *plan.Const_Timeval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Timeval)
	case *plan.Const_Datetimeval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Datetimeval)
	case *plan.Const_Timestampval:
		return true, false, vector.OrderedBinarySearchOffsetByValFactory(val.Timestampval)
	case *plan.Const_Decimal64Val:
		return true, false, vector.FixSizedBinarySearchOffsetByValFactory(types.Decimal64(val.Decimal64Val.A), types.CompareDecimal64)
	case *plan.Const_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		return true, false, vector.FixSizedBinarySearchOffsetByValFactory(v, types.CompareDecimal128)
	case *plan.Const_Sval:
		return true, false, vector.VarlenBinarySearchOffsetByValFactory(util.UnsafeStringToBytes(val.Sval))
	case *plan.Const_Jsonval:
		return true, false, vector.VarlenBinarySearchOffsetByValFactory(util.UnsafeStringToBytes(val.Jsonval))
	}
	return false, false, nil
}

func getPkValueByExpr(
	expr *plan.Expr,
	pkName string,
	oid types.T,
	proc *process.Process,
) (bool, bool, any) {
	canCompute, valExpr := getPkExpr(expr, pkName, proc)
	if !canCompute {
		return false, false, nil
	}
	if valExpr.Isnull {
		return false, true, nil
	}
	switch val := valExpr.Value.(type) {
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
	return false, false, nil
}

// computeRangeByNonIntPk compute NonIntPk range Expr
// only support function :["and", "="]
// support eg: pk="a",  pk="a" and noPk > 200
// unsupport eg: pk>"a", pk=otherFun("a"),  pk="a" or noPk > 200,
func computeRangeByNonIntPk(ctx context.Context, expr *plan.Expr, pkName string, proc *process.Process) (bool, uint64) {
	canCompute, valExpr := getPkExpr(expr, pkName, proc)
	if !canCompute {
		return canCompute, 0
	}
	ok, pkHashValue := getConstantExprHashValue(ctx, &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_C{
			C: valExpr,
		},
	}, proc)
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
	v T, comp func(T, T) int64,
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
	expr *plan.Const, isSorted bool,
) func(*vector.Vector, []int32, *[]int32) {
	switch val := expr.Value.(type) {
	case *plan.Const_Bval:
		return EvalSelectedOnFixedSizeColumnFactory(val.Bval)
	case *plan.Const_I8Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int8(val.I8Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int8(val.I8Val))
	case *plan.Const_I16Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int16(val.I16Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int16(val.I16Val))
	case *plan.Const_I32Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int32(val.I32Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int32(val.I32Val))
	case *plan.Const_I64Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(int64(val.I64Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(int64(val.I64Val))
	case *plan.Const_U8Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint8(val.U8Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint8(val.U8Val))
	case *plan.Const_U16Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint16(val.U16Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint16(val.U16Val))
	case *plan.Const_U32Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint32(val.U32Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint32(val.U32Val))
	case *plan.Const_U64Val:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(uint64(val.U64Val))
		}
		return EvalSelectedOnFixedSizeColumnFactory(uint64(val.U64Val))
	case *plan.Const_Fval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(float32(val.Fval))
		}
		return EvalSelectedOnFixedSizeColumnFactory(float32(val.Fval))
	case *plan.Const_Dval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Dval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Dval)
	case *plan.Const_Timeval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Timeval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Timeval)
	case *plan.Const_Timestampval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Timestampval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Timestampval)
	case *plan.Const_Dateval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Dateval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Dateval)
	case *plan.Const_Datetimeval:
		if isSorted {
			return EvalSelectedOnOrderedSortedColumnFactory(val.Datetimeval)
		}
		return EvalSelectedOnFixedSizeColumnFactory(val.Datetimeval)
	case *plan.Const_Decimal64Val:
		v := types.Decimal64(val.Decimal64Val.A)
		if isSorted {
			return EvalSelectedOnFixedSizeSortedColumnFactory(v, types.CompareDecimal64)
		}
		return EvalSelectedOnFixedSizeColumnFactory(v)
	case *plan.Const_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		if isSorted {
			return EvalSelectedOnFixedSizeSortedColumnFactory(v, types.CompareDecimal128)
		}
		return EvalSelectedOnFixedSizeColumnFactory(v)
	case *plan.Const_Sval:
		if isSorted {
			return EvalSelectedOnVarlenSortedColumnFactory(util.UnsafeStringToBytes(val.Sval))
		}
		return EvalSelectedOnVarlenColumnFactory(util.UnsafeStringToBytes(val.Sval))
	case *plan.Const_Jsonval:
		if isSorted {
			return EvalSelectedOnVarlenSortedColumnFactory(util.UnsafeStringToBytes(val.Jsonval))
		}
		return EvalSelectedOnVarlenColumnFactory(util.UnsafeStringToBytes(val.Jsonval))

	default:
		panic(fmt.Sprintf("unexpected const expr %v", expr))
	}
}

func serialTupleByConstExpr(expr *plan.Const, packer *types.Packer) {
	switch val := expr.Value.(type) {
	case *plan.Const_Bval:
		packer.EncodeBool(val.Bval)
	case *plan.Const_I8Val:
		packer.EncodeInt8(int8(val.I8Val))
	case *plan.Const_I16Val:
		packer.EncodeInt16(int16(val.I16Val))
	case *plan.Const_I32Val:
		packer.EncodeInt32(val.I32Val)
	case *plan.Const_I64Val:
		packer.EncodeInt64(val.I64Val)
	case *plan.Const_U8Val:
		packer.EncodeUint8(uint8(val.U8Val))
	case *plan.Const_U16Val:
		packer.EncodeUint16(uint16(val.U16Val))
	case *plan.Const_U32Val:
		packer.EncodeUint32(val.U32Val)
	case *plan.Const_U64Val:
		packer.EncodeUint64(val.U64Val)
	case *plan.Const_Fval:
		packer.EncodeFloat32(val.Fval)
	case *plan.Const_Dval:
		packer.EncodeFloat64(val.Dval)
	case *plan.Const_Timeval:
		packer.EncodeTime(types.Time(val.Timeval))
	case *plan.Const_Timestampval:
		packer.EncodeTimestamp(types.Timestamp(val.Timestampval))
	case *plan.Const_Dateval:
		packer.EncodeDate(types.Date(val.Dateval))
	case *plan.Const_Datetimeval:
		packer.EncodeDatetime(types.Datetime(val.Datetimeval))
	case *plan.Const_Decimal64Val:
		packer.EncodeDecimal64(types.Decimal64(val.Decimal64Val.A))
	case *plan.Const_Decimal128Val:
		packer.EncodeDecimal128(types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)})
	case *plan.Const_Sval:
		packer.EncodeStringType(util.UnsafeStringToBytes(val.Sval))
	case *plan.Const_Jsonval:
		packer.EncodeStringType(util.UnsafeStringToBytes(val.Jsonval))
	default:
		panic(fmt.Sprintf("unexpected const expr %v", expr))
	}
}

func getConstValueByExpr(expr *plan.Expr,
	proc *process.Process) (bool, *plan.Const) {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return false, nil
	}
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return false, nil
	}
	defer exec.Free()
	if ret := rule.GetConstantValue(vec, true, 0); ret != nil {
		return true, ret
	}
	return false, nil
}

func getConstExpr(oid int32, c *plan.Const) *plan.Expr {
	return &plan.Expr{
		Typ:  &plan.Type{Id: oid},
		Expr: &plan.Expr_C{C: c},
	}
}
