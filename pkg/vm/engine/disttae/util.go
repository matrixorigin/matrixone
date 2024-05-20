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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"

	"go.uber.org/zap"

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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

func getColDefByName(name string, tableDef *plan.TableDef) *plan.ColDef {
	idx := strings.Index(name, ".")
	var pos int32
	if idx >= 0 {
		subName := name[idx+1:]
		pos = tableDef.Name2ColIndex[subName]
	} else {
		pos = tableDef.Name2ColIndex[name]
	}
	return tableDef.Cols[pos]
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

func MustGetFullCompositePKValue(
	expr *plan.Expr,
	pkName string,
	keys []string,
	packer *types.Packer,
	proc *process.Process,
) (canEval, isVec bool, val []byte) {
	ok, rExpr := MustGetFullCompositePK(expr, pkName, keys, packer, proc)
	if !ok || rExpr == nil {
		return false, false, nil
	}

	switch rExprImpl := rExpr.Expr.(type) {
	case *plan.Expr_Lit:
		return true, false, []byte(rExprImpl.Lit.Value.(*plan.Literal_Sval).Sval)
	case *plan.Expr_Vec:
		return true, true, rExprImpl.Vec.Data
	case *plan.Expr_List:
		ok, vec, put := evalExprListToVec(types.T_char, rExprImpl, proc)
		if !ok || vec == nil || vec.Length() == 0 {
			return false, false, nil
		}
		data, _ := vec.MarshalBinary()
		put()
		return true, true, data
	}
	return false, false, nil
}

func MustGetFullCompositePK(
	expr *plan.Expr,
	pkName string,
	keys []string,
	packer *types.Packer,
	proc *process.Process,
) (bool, *plan.Expr) {
	tmpExprs := make([]*plan.Literal, len(keys))
	ok, expr := mustGetFullCompositePK(expr, pkName, keys, tmpExprs, packer, proc)
	if !ok {
		return false, nil
	}
	if expr != nil {
		return true, expr
	}
	packer.Reset()
	for i := 0; i < len(tmpExprs); i++ {
		if tmpExprs[i] == nil {
			packer.Reset()
			return false, nil
		}
	}
	val := packer.Bytes()
	packer.Reset()
	return true, &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Sval{
					Sval: util.UnsafeBytesToString(val),
				},
			},
		},
	}
}

func mustGetFullCompositePK(
	expr *plan.Expr,
	pkName string,
	keys []string,
	tmpExprs []*plan.Literal,
	packer *types.Packer,
	proc *process.Process,
) (bool, *plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "=":
			if leftExpr, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col); ok {
				// if it is a composite pk
				if compPkCol(leftExpr.Col.Name, pkName) {
					rExpr := getConstValueByExpr(exprImpl.F.Args[1], proc)
					if rExpr == nil || rExpr.Isnull {
						return false, nil
					}
					return true, &plan.Expr{
						Expr: &plan.Expr_Lit{Lit: rExpr},
					}
				}
				// if it is one of the composite pks
				if pos := getPosInCompositPK(leftExpr.Col.Name, keys); pos != -1 {
					rExpr := getConstValueByExpr(exprImpl.F.Args[1], proc)
					if rExpr != nil && !rExpr.Isnull {
						tmpExprs[pos] = rExpr
					}
					return true, nil
				}

				return false, nil
			}
			if rightExpr, ok := exprImpl.F.Args[1].Expr.(*plan.Expr_Col); ok {
				// if it is a composite pk
				if compPkCol(rightExpr.Col.Name, pkName) {
					rExpr := getConstValueByExpr(exprImpl.F.Args[0], proc)
					if rExpr == nil || rExpr.Isnull {
						return false, nil
					}
					return true, &plan.Expr{
						Expr: &plan.Expr_Lit{Lit: rExpr},
					}
				}
				// if it is one of the composite pks
				if pos := getPosInCompositPK(rightExpr.Col.Name, keys); pos != -1 {
					rExpr := getConstValueByExpr(exprImpl.F.Args[0], proc)
					if rExpr != nil && !rExpr.Isnull {
						tmpExprs[pos] = rExpr
					}
					return true, nil
				}
				return false, nil
			}
			return false, nil
		case "and":
			ok, leftPkExpr := mustGetFullCompositePK(
				exprImpl.F.Args[0], pkName, keys, tmpExprs, packer, proc,
			)
			if !ok || leftPkExpr != nil {
				return ok, leftPkExpr
			}
			all := true
			for _, expr := range tmpExprs {
				if expr == nil {
					all = false
				}
			}
			if all {
				packer.Reset()
				for i, expr := range tmpExprs {
					serialTupleByConstExpr(expr, packer)
					tmpExprs[i] = nil
				}
				val := packer.Bytes()
				packer.Reset()
				return true, &plan.Expr{
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Sval{
								Sval: util.UnsafeBytesToString(val),
							},
						},
					},
				}
			}
			ok, rightPkExpr := mustGetFullCompositePK(
				exprImpl.F.Args[1], pkName, keys, tmpExprs, packer, proc,
			)
			if !ok || rightPkExpr != nil {
				return ok, rightPkExpr
			}
			all = true
			for _, expr := range tmpExprs {
				if expr == nil {
					all = false
				}
			}
			if all {
				packer.Reset()
				for i, expr := range tmpExprs {
					serialTupleByConstExpr(expr, packer)
					tmpExprs[i] = nil
				}
				val := packer.Bytes()
				packer.Reset()

				return true, &plan.Expr{
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_Sval{
								Sval: util.UnsafeBytesToString(val),
							},
						},
					},
				}
			}
			return true, nil
		case "or":
			for i := 0; i < len(tmpExprs); i++ {
				tmpExprs[i] = nil
			}
			ok, leftPkExpr := mustGetFullCompositePK(
				exprImpl.F.Args[0], pkName, keys, tmpExprs, packer, proc,
			)
			for i := 0; i < len(tmpExprs); i++ {
				tmpExprs[i] = nil
			}
			if !ok || leftPkExpr == nil {
				return false, nil
			}
			ok, rightPkExpr := mustGetFullCompositePK(
				exprImpl.F.Args[1], pkName, keys, tmpExprs, packer, proc,
			)
			for i := 0; i < len(tmpExprs); i++ {
				tmpExprs[i] = nil
			}
			if !ok || rightPkExpr == nil {
				return false, nil
			}
			return true, &plan.Expr{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{leftPkExpr, rightPkExpr},
					},
				},
				Typ: plan.Type{
					Id: int32(types.T_tuple),
				},
			}

		case "in":
			if leftExpr, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col); ok {
				if !compPkCol(leftExpr.Col.Name, pkName) {
					return false, nil
				}
				return true, exprImpl.F.Args[1]
			}
		}
	}
	return false, nil
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

func getPkExpr(
	expr *plan.Expr, pkName string, proc *process.Process,
) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			leftPK := getPkExpr(exprImpl.F.Args[0], pkName, proc)
			if leftPK == nil {
				return nil
			}
			rightPK := getPkExpr(exprImpl.F.Args[1], pkName, proc)
			if rightPK == nil {
				return nil
			}
			if litExpr, ok := leftPK.Expr.(*plan.Expr_Lit); ok {
				if litExpr.Lit.Isnull {
					return rightPK
				}
			}
			if litExpr, ok := rightPK.Expr.(*plan.Expr_Lit); ok {
				if litExpr.Lit.Isnull {
					return leftPK
				}
			}
			return &plan.Expr{
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: []*plan.Expr{leftPK, rightPK},
					},
				},
				Typ: plan.Type{
					Id: int32(types.T_tuple),
				},
			}

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
					Typ: exprImpl.F.Args[1].Typ,
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
					Typ: exprImpl.F.Args[0].Typ,
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

func LinearSearchOffsetByValFactory(pk *vector.Vector) func(*vector.Vector) []int32 {
	mp := make(map[any]bool)
	switch pk.GetType().Oid {
	case types.T_bool:
		vs := vector.MustFixedCol[bool](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_bit:
		vs := vector.MustFixedCol[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int8:
		vs := vector.MustFixedCol[int8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int16:
		vs := vector.MustFixedCol[int16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int32:
		vs := vector.MustFixedCol[int32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_int64:
		vs := vector.MustFixedCol[int64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint8:
		vs := vector.MustFixedCol[uint8](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint16:
		vs := vector.MustFixedCol[uint16](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint32:
		vs := vector.MustFixedCol[uint32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uint64:
		vs := vector.MustFixedCol[uint64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal64:
		vs := vector.MustFixedCol[types.Decimal64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_decimal128:
		vs := vector.MustFixedCol[types.Decimal128](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_uuid:
		vs := vector.MustFixedCol[types.Uuid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float32:
		vs := vector.MustFixedCol[float32](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_float64:
		vs := vector.MustFixedCol[float64](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_date:
		vs := vector.MustFixedCol[types.Date](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_timestamp:
		vs := vector.MustFixedCol[types.Timestamp](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_time:
		vs := vector.MustFixedCol[types.Time](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_datetime:
		vs := vector.MustFixedCol[types.Datetime](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_enum:
		vs := vector.MustFixedCol[types.Enum](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_TS:
		vs := vector.MustFixedCol[types.TS](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Rowid:
		vs := vector.MustFixedCol[types.Rowid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_Blockid:
		vs := vector.MustFixedCol[types.Blockid](pk)
		for _, v := range vs {
			mp[v] = true
		}
	case types.T_char, types.T_varchar, types.T_json,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		for i := 0; i < pk.Length(); i++ {
			v := pk.GetStringAt(i)
			mp[v] = true
		}
	case types.T_array_float32:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float32](vector.GetArrayAt[float32](pk, i))
			mp[v] = true
		}
	case types.T_array_float64:
		for i := 0; i < pk.Length(); i++ {
			v := types.ArrayToString[float64](vector.GetArrayAt[float64](pk, i))
			mp[v] = true
		}
	default:
		panic(moerr.NewInternalErrorNoCtx("%s not supported", pk.GetType().String()))
	}

	return func(vec *vector.Vector) []int32 {
		var sels []int32
		switch vec.GetType().Oid {
		case types.T_bool:
			vs := vector.MustFixedCol[bool](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_bit:
			vs := vector.MustFixedCol[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int8:
			vs := vector.MustFixedCol[int8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int16:
			vs := vector.MustFixedCol[int16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int32:
			vs := vector.MustFixedCol[int32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_int64:
			vs := vector.MustFixedCol[int64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint8:
			vs := vector.MustFixedCol[uint8](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint16:
			vs := vector.MustFixedCol[uint16](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint32:
			vs := vector.MustFixedCol[uint32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uint64:
			vs := vector.MustFixedCol[uint64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_decimal64:
			vs := vector.MustFixedCol[types.Decimal64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_decimal128:
			vs := vector.MustFixedCol[types.Decimal128](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_uuid:
			vs := vector.MustFixedCol[types.Uuid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_float32:
			vs := vector.MustFixedCol[float32](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_float64:
			vs := vector.MustFixedCol[float64](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_date:
			vs := vector.MustFixedCol[types.Date](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_timestamp:
			vs := vector.MustFixedCol[types.Timestamp](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_time:
			vs := vector.MustFixedCol[types.Time](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_datetime:
			vs := vector.MustFixedCol[types.Datetime](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_enum:
			vs := vector.MustFixedCol[types.Enum](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_TS:
			vs := vector.MustFixedCol[types.TS](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_Rowid:
			vs := vector.MustFixedCol[types.Rowid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_Blockid:
			vs := vector.MustFixedCol[types.Blockid](vec)
			for i, v := range vs {
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_char, types.T_varchar, types.T_json,
			types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
			for i := 0; i < vec.Length(); i++ {
				v := vec.GetStringAt(i)
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_array_float32:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float32](vector.GetArrayAt[float32](vec, i))
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		case types.T_array_float64:
			for i := 0; i < vec.Length(); i++ {
				v := types.ArrayToString[float64](vector.GetArrayAt[float64](vec, i))
				if mp[v] {
					sels = append(sels, int32(i))
				}
			}
		default:
			panic(moerr.NewInternalErrorNoCtx("%s not supported", vec.GetType().String()))
		}
		return sels
	}
}

func getNonSortedPKSearchFuncByPKVec(
	vec *vector.Vector,
) blockio.ReadFilter {

	searchPKFunc := LinearSearchOffsetByValFactory(vec)

	if searchPKFunc != nil {
		return func(vecs []*vector.Vector) []int32 {
			return searchPKFunc(vecs[0])
		}
	}
	return nil
}

func getNonCompositePKSearchFuncByExpr(
	expr *plan.Expr, pkName string, proc *process.Process,
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
			searchPKFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{[]byte(val.Sval)})
		case *plan.Literal_Jsonval:
			searchPKFunc = vector.VarlenBinarySearchOffsetByValFactory([][]byte{[]byte(val.Jsonval)})
		case *plan.Literal_EnumVal:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory([]types.Enum{types.Enum(val.EnumVal)})
		}

	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "prefix_eq":
			val := []byte(exprImpl.F.Args[1].GetLit().GetSval())
			searchPKFunc = vector.CollectOffsetsByPrefixEqFactory(val)

		case "prefix_between":
			lval := []byte(exprImpl.F.Args[1].GetLit().GetSval())
			rval := []byte(exprImpl.F.Args[2].GetLit().GetSval())
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
		case types.T_bit:
			searchPKFunc = vector.OrderedBinarySearchOffsetByValFactory(vector.MustFixedCol[uint64](vec))
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

func evalLiteralExpr2(expr *plan.Literal, oid types.T) (ret []byte, can bool) {
	can = true
	switch val := expr.Value.(type) {
	case *plan.Literal_I8Val:
		i8 := int8(val.I8Val)
		ret = types.EncodeInt8(&i8)
	case *plan.Literal_I16Val:
		i16 := int16(val.I16Val)
		ret = types.EncodeInt16(&i16)
	case *plan.Literal_I32Val:
		i32 := int32(val.I32Val)
		ret = types.EncodeInt32(&i32)
	case *plan.Literal_I64Val:
		i64 := int64(val.I64Val)
		ret = types.EncodeInt64(&i64)
	case *plan.Literal_Dval:
		if oid == types.T_float32 {
			fval := float32(val.Dval)
			ret = types.EncodeFloat32(&fval)
		} else {
			dval := val.Dval
			ret = types.EncodeFloat64(&dval)
		}
	case *plan.Literal_Sval:
		ret = []byte(val.Sval)
	case *plan.Literal_Bval:
		ret = types.EncodeBool(&val.Bval)
	case *plan.Literal_U8Val:
		u8 := uint8(val.U8Val)
		ret = types.EncodeUint8(&u8)
	case *plan.Literal_U16Val:
		u16 := uint16(val.U16Val)
		ret = types.EncodeUint16(&u16)
	case *plan.Literal_U32Val:
		u32 := uint32(val.U32Val)
		ret = types.EncodeUint32(&u32)
	case *plan.Literal_U64Val:
		u64 := uint64(val.U64Val)
		ret = types.EncodeUint64(&u64)
	case *plan.Literal_Fval:
		if oid == types.T_float32 {
			fval := float32(val.Fval)
			ret = types.EncodeFloat32(&fval)
		} else {
			fval := float64(val.Fval)
			ret = types.EncodeFloat64(&fval)
		}
	case *plan.Literal_Dateval:
		v := types.Date(val.Dateval)
		ret = types.EncodeDate(&v)
	case *plan.Literal_Timeval:
		v := types.Time(val.Timeval)
		ret = types.EncodeTime(&v)
	case *plan.Literal_Datetimeval:
		v := types.Datetime(val.Datetimeval)
		ret = types.EncodeDatetime(&v)
	case *plan.Literal_Timestampval:
		v := types.Timestamp(val.Timestampval)
		ret = types.EncodeTimestamp(&v)
	case *plan.Literal_Decimal64Val:
		v := types.Decimal64(val.Decimal64Val.A)
		ret = types.EncodeDecimal64(&v)
	case *plan.Literal_Decimal128Val:
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		ret = types.EncodeDecimal128(&v)
	case *plan.Literal_EnumVal:
		v := types.Enum(val.EnumVal)
		ret = types.EncodeEnum(&v)
	case *plan.Literal_Jsonval:
		ret = []byte(val.Jsonval)
	default:
		can = false
	}

	return
}

func evalLiteralExpr(expr *plan.Literal, oid types.T) (canEval bool, val any) {
	switch val := expr.Value.(type) {
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
	return
}

type PKFilter struct {
	op      uint8
	val     any
	data    []byte
	isVec   bool
	isValid bool
	isNull  bool
}

func (f *PKFilter) String() string {
	var buf bytes.Buffer
	buf.WriteString(
		fmt.Sprintf("PKFilter{op: %d, isVec: %v, isValid: %v, isNull: %v, val: %v, data(len=%d)",
			f.op, f.isVec, f.isValid, f.isNull, f.val, len(f.data),
		))
	return buf.String()
}

func (f *PKFilter) SetNull() {
	f.isNull = true
	f.isValid = false
}

func (f *PKFilter) SetFullData(op uint8, isVec bool, val []byte) {
	f.data = val
	f.op = op
	f.isVec = isVec
	f.isValid = true
	f.isNull = false
}

func (f *PKFilter) SetVal(op uint8, isVec bool, val any) {
	f.op = op
	f.val = val
	f.isValid = true
	f.isVec = false
	f.isNull = false
}

// func (f *PKFilter) MustGetVector() *vector.Vector {
// 	if !f.isVec || !f.isValid || f.isNull {
// 		panic(moerr.NewInternalErrorNoCtx("MustGetVector failed"))
// 	}
// 	vec := vector.NewVec(types.T_any.ToType())
// 	err := vec.UnmarshalBinary(f.data)
// 	if err != nil {
// 		panic(moerr.NewInternalErrorNoCtx(fmt.Sprintf("MustGetVector failed: %v", err)))
// 	}

// 	return vec
// }

func getPKFilterByExpr(
	expr *plan.Expr,
	pkName string,
	oid types.T,
	proc *process.Process,
) (retFilter PKFilter) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return
	}
	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			retFilter.SetNull()
			return
		}

		canEval, val := evalLiteralExpr(exprImpl.Lit, oid)
		if !canEval {
			return
		}
		retFilter.SetVal(function.EQUAL, false, val)
		return
	case *plan.Expr_Vec:
		retFilter.SetFullData(function.IN, true, exprImpl.Vec.Data)
		return
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "prefix_eq":
			val := []byte(exprImpl.F.Args[1].GetLit().GetSval())
			retFilter.SetVal(function.PREFIX_EQ, false, val)
			return
			// case "prefix_between":
			// case "prefix_in":
		}
	}
	return
}

// return canEval, isNull, isVec, evaledVal
func getPkValueByExpr(
	expr *plan.Expr,
	pkName string,
	oid types.T,
	mustOne bool,
	proc *process.Process,
) (bool, bool, bool, any) {
	valExpr := getPkExpr(expr, pkName, proc)
	if valExpr == nil {
		return false, false, false, nil
	}

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Lit:
		if exprImpl.Lit.Isnull {
			return false, true, false, nil
		}
		canEval, val := evalLiteralExpr(exprImpl.Lit, oid)
		if canEval {
			return true, false, false, val
		} else {
			return false, false, false, nil
		}

	case *plan.Expr_Vec:
		if mustOne {
			vec := vector.NewVec(types.T_any.ToType())
			vec.UnmarshalBinary(exprImpl.Vec.Data)
			if vec.Length() != 1 {
				return false, false, false, nil
			}
			exprLit := rule.GetConstantValue(vec, true, 0)
			if exprLit == nil {
				return false, false, false, nil
			}
			if exprLit.Isnull {
				return false, true, false, nil
			}
			canEval, val := evalLiteralExpr(exprLit, oid)
			if canEval {
				return true, false, false, val
			}
			return false, false, false, nil
		}
		return true, false, true, exprImpl.Vec.Data

	case *plan.Expr_List:
		if mustOne {
			return false, false, false, nil
		}
		canEval, vec, put := evalExprListToVec(oid, exprImpl, proc)
		if !canEval || vec == nil || vec.Length() == 0 {
			return false, false, false, nil
		}
		data, _ := vec.MarshalBinary()
		put()
		return true, false, true, data
	}

	return false, false, false, nil
}

func evalExprListToVec(
	oid types.T, expr *plan.Expr_List, proc *process.Process,
) (canEval bool, vec *vector.Vector, put func()) {
	if expr == nil {
		return false, nil, nil
	}
	canEval, vec = recurEvalExprList(oid, expr, nil, proc)
	if !canEval {
		if vec != nil {
			proc.PutVector(vec)
		}
		return false, nil, nil
	}
	put = func() {
		proc.PutVector(vec)
	}
	vec.InplaceSort()
	return
}

func recurEvalExprList(
	oid types.T, inputExpr *plan.Expr_List, inputVec *vector.Vector, proc *process.Process,
) (canEval bool, outputVec *vector.Vector) {
	outputVec = inputVec
	for _, expr := range inputExpr.List.List {
		switch expr2 := expr.Expr.(type) {
		case *plan.Expr_Lit:
			canEval, val := evalLiteralExpr(expr2.Lit, oid)
			if !canEval {
				return false, outputVec
			}
			if outputVec == nil {
				outputVec = proc.GetVector(oid.ToType())
			}
			// TODO: not use appendAny
			if err := vector.AppendAny(outputVec, val, false, proc.Mp()); err != nil {
				return false, outputVec
			}
		case *plan.Expr_Vec:
			vec := vector.NewVec(oid.ToType())
			if err := vec.UnmarshalBinary(expr2.Vec.Data); err != nil {
				return false, outputVec
			}
			if outputVec == nil {
				outputVec = proc.GetVector(oid.ToType())
			}
			sels := make([]int32, vec.Length())
			for i := 0; i < vec.Length(); i++ {
				sels[i] = int32(i)
			}
			union := vector.GetUnionAllFunction(*outputVec.GetType(), proc.Mp())
			if err := union(outputVec, vec); err != nil {
				return false, outputVec
			}
		case *plan.Expr_List:
			if canEval, outputVec = recurEvalExprList(oid, expr2, outputVec, proc); !canEval {
				return false, outputVec
			}
		default:
			return false, outputVec
		}
	}
	return true, outputVec
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
			return EvalSelectedOnVarlenSortedColumnFactory([]byte(val.Sval))
		}
		return EvalSelectedOnVarlenColumnFactory([]byte(val.Sval))
	case *plan.Literal_Jsonval:
		if isSorted {
			return EvalSelectedOnVarlenSortedColumnFactory([]byte(val.Jsonval))
		}
		return EvalSelectedOnVarlenColumnFactory([]byte(val.Jsonval))
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

func getConstValueByExpr(
	expr *plan.Expr, proc *process.Process,
) *plan.Literal {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil
	}
	defer exec.Free()
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch})
	if err != nil {
		return nil
	}
	return rule.GetConstantValue(vec, true, 0)
}

func getConstExpr(oid int32, c *plan.Literal) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: oid},
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
) (val []byte, isVec bool) {
	var canEval bool
	pk := def.Pkey
	column := def.Cols[pkIdx]
	name := column.Name
	if pk.CompPkeyCol != nil {
		if len(exprs) == 1 {
			expr := exprs[0]
			var packer *types.Packer
			put := pool.Get(&packer)
			defer put.Put()
			if canEval, isVec, v := MustGetFullCompositePKValue(
				expr, name, pk.Names, packer, proc,
			); canEval {
				return v, isVec
			}
			return nil, false
		} else {
			// PXU TODO:
			// we need to change the pushdown fiter exprs in
			// the composite pk scenario.
			val = extractCompositePKValueFromEqualExprs(
				exprs, pk, proc, pool,
			)
			return
		}
	}

	colType := types.T(column.Typ.Id)
	for _, expr := range exprs {
		var v any
		if canEval, _, isVec, v = getPkValueByExpr(expr, name, colType, false, proc); canEval {
			if isVec {
				val = v.([]byte)
			} else {
				val = types.EncodeValue(v, colType)
			}
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
func UnfoldBlkInfoFromObjStats(stats *objectio.ObjectStats) (blks []objectio.BlockInfo) {
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
		blks = append(blks, objectio.BlockInfo{
			BlockID:   *objectio.BuildObjectBlockid(name, idx),
			MetaLoc:   objectio.ObjectLocation(loc),
			SegmentID: name.SegmentId(),
		})
	}

	return blks
}

// ForeachBlkInObjStatsList receives an object info list,
// and visits each blk of these object info by OnBlock,
// until the onBlock returns false or all blks have been enumerated.
// when onBlock returns a false,
// the next argument decides whether continue onBlock on the next stats or exit foreach completely.
func ForeachBlkInObjStatsList(
	next bool,
	dataMeta objectio.ObjectDataMeta,
	onBlock func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool,
	objects ...objectio.ObjectStats,
) {
	stop := false
	objCnt := len(objects)

	for idx := 0; idx < objCnt && !stop; idx++ {
		iter := NewStatsBlkIter(&objects[idx], dataMeta)
		pos := uint32(0)
		for iter.Next() {
			blk := iter.Entry()
			var meta objectio.BlockObject
			if !dataMeta.IsEmpty() {
				meta = dataMeta.GetBlockMeta(pos)
			}
			pos++
			if !onBlock(blk, meta) {
				stop = true
				break
			}
		}

		if stop && next {
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
	meta       objectio.ObjectDataMeta
}

func NewStatsBlkIter(stats *objectio.ObjectStats, meta objectio.ObjectDataMeta) *StatsBlkIter {
	return &StatsBlkIter{
		name:       stats.ObjectName(),
		blkCnt:     uint16(stats.BlkCnt()),
		extent:     stats.Extent(),
		cur:        -1,
		accRows:    0,
		totalRows:  stats.Rows(),
		curBlkRows: options.DefaultBlockMaxRows,
		meta:       meta,
	}
}

func (i *StatsBlkIter) Next() bool {
	if i.cur >= 0 {
		i.accRows += i.curBlkRows
	}
	i.cur++
	return i.cur < int(i.blkCnt)
}

func (i *StatsBlkIter) Entry() objectio.BlockInfo {
	if i.cur == -1 {
		i.cur = 0
	}

	// assume that all blks have DefaultBlockMaxRows, except the last one
	if i.meta.IsEmpty() {
		if i.cur == int(i.blkCnt-1) {
			i.curBlkRows = i.totalRows - i.accRows
		}
	} else {
		i.curBlkRows = i.meta.GetBlockMeta(uint32(i.cur)).GetRows()
	}

	loc := objectio.BuildLocation(i.name, i.extent, i.curBlkRows, uint16(i.cur))
	blk := objectio.BlockInfo{
		BlockID:   *objectio.BuildObjectBlockid(i.name, uint16(i.cur)),
		SegmentID: i.name.SegmentId(),
		MetaLoc:   objectio.ObjectLocation(loc),
	}
	return blk
}

func ForeachCommittedObjects(
	createObjs map[objectio.ObjectNameShort]struct{},
	delObjs map[objectio.ObjectNameShort]struct{},
	p *logtailreplay.PartitionState,
	onObj func(info logtailreplay.ObjectInfo) error) (err error) {
	for obj := range createObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	for obj := range delObjs {
		if objInfo, ok := p.GetObject(obj); ok {
			if err = onObj(objInfo); err != nil {
				return
			}
		}
	}
	return nil

}

func ForeachSnapshotObjects(
	ts timestamp.Timestamp,
	onObject func(obj logtailreplay.ObjectInfo, isCommitted bool) error,
	tableSnapshot *logtailreplay.PartitionState,
	uncommitted ...objectio.ObjectStats,
) (err error) {
	// process all uncommitted objects first
	for _, obj := range uncommitted {
		info := logtailreplay.ObjectInfo{
			ObjectStats: obj,
		}
		if err = onObject(info, false); err != nil {
			return
		}
	}

	// process all committed objects
	if tableSnapshot == nil {
		return
	}

	iter, err := tableSnapshot.NewObjectsIter(types.TimestampToTS(ts))
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.Next() {
		obj := iter.Entry()
		if err = onObject(obj.ObjectInfo, true); err != nil {
			return
		}
	}
	return
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

// getDatabasesExceptDeleted remove databases delete in the txn from the CatalogCache
func getDatabasesExceptDeleted(accountId uint32, cache *cache.CatalogCache, txn *Transaction) []string {
	//first get all delete tables
	deleteDatabases := make(map[string]any)
	txn.deletedDatabaseMap.Range(func(k, _ any) bool {
		key := k.(databaseKey)
		if key.accountId == accountId {
			deleteDatabases[key.name] = nil
		}
		return true
	})

	dbs := cache.Databases(accountId, txn.op.SnapshotTS())
	dbs = removeIf[string](dbs, func(t string) bool {
		return find[string](deleteDatabases, t)
	})
	return dbs
}

// removeIf removes the elements that pred is true.
func removeIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func find[T ~string | ~int, S any](data map[T]S, val T) bool {
	if len(data) == 0 {
		return false
	}
	if _, exists := data[val]; exists {
		return true
	}
	return false
}
