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

package engine_util

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

func compPkCol(colName string, pkName string) bool {
	dotIdx := strings.Index(colName, ".")
	colName = colName[dotIdx+1:]
	return colName == pkName
}

func evalValue(
	exprImpl *plan.Expr_F,
	tblDef *plan.TableDef,
	isVec bool,
	pkName string,
	proc *process.Process,
) (
	ok bool, oid types.T, vals [][]byte,
) {
	var val []byte
	var col *plan.Expr_Col

	if !isVec {
		col, vals, ok = mustColConstValueFromBinaryFuncExpr(exprImpl, tblDef, proc)
	} else {
		col, val, ok = mustColVecValueFromBinaryFuncExpr(proc, exprImpl)
	}

	if !ok {
		return false, 0, nil
	}
	if !compPkCol(col.Col.Name, pkName) {
		return false, 0, nil
	}

	var colPos int32
	if col.Col.Name == "" {
		colPos = col.Col.ColPos
		logutil.Warnf("colExpr.Col.Name is empty")
	} else {
		idx := strings.Index(col.Col.Name, ".")
		if idx == -1 {
			colPos = tblDef.Name2ColIndex[col.Col.Name]
		} else {
			colPos = tblDef.Name2ColIndex[col.Col.Name[idx+1:]]
		}
	}

	if isVec {
		return true, types.T(tblDef.Cols[colPos].Typ.Id), [][]byte{val}
	}
	return true, types.T(tblDef.Cols[colPos].Typ.Id), vals
}

func mustColConstValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, [][]byte, bool) {
	var (
		colExpr  *plan.Expr_Col
		tmpExpr  *plan.Expr_Col
		valExprs []*plan.Expr
		ok       bool
	)

	for idx := range expr.F.Args {
		if tmpExpr, ok = expr.F.Args[idx].Expr.(*plan.Expr_Col); !ok {
			valExprs = append(valExprs, expr.F.Args[idx])
		} else {
			colExpr = tmpExpr
		}
	}

	if len(valExprs) == 0 || colExpr == nil {
		return nil, nil, false
	}

	vals, ok := getConstBytesFromExpr(
		valExprs,
		tableDef.Cols[colExpr.Col.ColPos],
		proc,
	)
	if !ok {
		return nil, nil, false
	}
	return colExpr, vals, true
}

func getConstBytesFromExpr(exprs []*plan.Expr, colDef *plan.ColDef, proc *process.Process) ([][]byte, bool) {
	vals := make([][]byte, len(exprs))
	_ = colDef
	_ = proc
	for idx := range exprs {
		if fExpr, ok := exprs[idx].Expr.(*plan.Expr_Fold); ok {
			if len(fExpr.Fold.Data) == 0 {
				return nil, false
			}
			if !fExpr.Fold.IsConst {
				return nil, false
			}

			vals[idx] = nil
			vals[idx] = append(vals[idx], fExpr.Fold.Data...)
		} else {
			logutil.Warnf("const folded val expr is not a fold expr: %s\n", plan2.FormatExpr(exprs[idx]))
			return nil, false
		}
	}

	return vals, true
}

func getConstValueByExpr(
	expr *plan.Expr, proc *process.Process,
) *plan.Literal {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil
	}
	defer exec.Free()
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil
	}
	return rule.GetConstantValue(vec, true, 0)
}

func mustColVecValueFromBinaryFuncExpr(proc *process.Process, expr *plan.Expr_F) (*plan.Expr_Col, []byte, bool) {
	var (
		colExpr  *plan.Expr_Col
		valExpr  *plan.Expr
		ok       bool
		exprImpl *plan.Expr_Vec
	)
	if colExpr, ok = expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[1]
	} else if colExpr, ok = expr.F.Args[1].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[0]
	} else {
		return nil, nil, false
	}

	if exprImpl, ok = valExpr.Expr.(*plan.Expr_Vec); !ok {
		if fExpr, ok := valExpr.Expr.(*plan.Expr_Fold); ok {
			if len(fExpr.Fold.Data) == 0 {
				return nil, nil, false
			}
			if fExpr.Fold.IsConst {
				return nil, nil, false
			}
			return colExpr, fExpr.Fold.Data, ok
		}

		logutil.Warnf("const folded val expr is not a vec expr: %s\n", plan2.FormatExpr(valExpr))
		return nil, nil, false
		// foldedExprs, err := plan2.ConstandFoldList([]*plan.Expr{valExpr}, proc, true)
		// if err != nil {
		// 	logutil.Errorf("try const fold val expr failed: %s", plan2.FormatExpr(valExpr))
		// 	return nil, nil, false
		// }

		// if exprImpl, ok = foldedExprs[0].Expr.(*plan.Expr_Vec); !ok {
		// 	logutil.Errorf("const folded val expr is not a vec expr: %s\n", plan2.FormatExpr(valExpr))
		// 	return nil, nil, false
		// }
	}

	return colExpr, exprImpl.Vec.Data, ok
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
				Typ: leftPK.Typ,
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

		case "prefix_eq", "prefix_between", "prefix_in", "between":
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
			vec.Free(proc.GetMPool())
		}
		return false, nil, nil
	}
	put = func() {
		vec.Free(proc.GetMPool())
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
				outputVec = vector.NewVec(oid.ToType())
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
				outputVec = vector.NewVec(oid.ToType())
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

func transferIval[T int32 | int64](v T, oid types.T) (bool, any) {
	switch oid {
	case types.T_bit:
		return true, uint64(v)
	case types.T_int8:
		return true, int8(v)
	case types.T_int16:
		return true, int16(v)
	case types.T_int32:
		return true, int32(v)
	case types.T_int64:
		return true, int64(v)
	case types.T_uint8:
		return true, uint8(v)
	case types.T_uint16:
		return true, uint16(v)
	case types.T_uint32:
		return true, uint32(v)
	case types.T_uint64:
		return true, uint64(v)
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferUval[T uint32 | uint64](v T, oid types.T) (bool, any) {
	switch oid {
	case types.T_bit:
		return true, uint64(v)
	case types.T_int8:
		return true, int8(v)
	case types.T_int16:
		return true, int16(v)
	case types.T_int32:
		return true, int32(v)
	case types.T_int64:
		return true, int64(v)
	case types.T_uint8:
		return true, uint8(v)
	case types.T_uint16:
		return true, uint16(v)
	case types.T_uint32:
		return true, uint32(v)
	case types.T_uint64:
		return true, uint64(v)
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferFval(v float32, oid types.T) (bool, any) {
	switch oid {
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferDval(v float64, oid types.T) (bool, any) {
	switch oid {
	case types.T_float32:
		return true, float32(v)
	case types.T_float64:
		return true, float64(v)
	default:
		return false, nil
	}
}

func transferSval(v string, oid types.T) (bool, any) {
	switch oid {
	case types.T_json:
		return true, []byte(v)
	case types.T_char, types.T_varchar:
		return true, []byte(v)
	case types.T_text, types.T_blob, types.T_datalink:
		return true, []byte(v)
	case types.T_binary, types.T_varbinary:
		return true, []byte(v)
	case types.T_uuid:
		var uv types.Uuid
		copy(uv[:], []byte(v)[:])
		return true, uv
		//TODO: should we add T_array for this code?
	default:
		return false, nil
	}
}

func transferBval(v bool, oid types.T) (bool, any) {
	switch oid {
	case types.T_bool:
		return true, v
	default:
		return false, nil
	}
}

func transferDateval(v int32, oid types.T) (bool, any) {
	switch oid {
	case types.T_date:
		return true, types.Date(v)
	default:
		return false, nil
	}
}

func transferTimeval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_time:
		return true, types.Time(v)
	default:
		return false, nil
	}
}

func transferDatetimeval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_datetime:
		return true, types.Datetime(v)
	default:
		return false, nil
	}
}

func transferTimestampval(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_timestamp:
		return true, types.Timestamp(v)
	default:
		return false, nil
	}
}

func transferDecimal64val(v int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_decimal64:
		return true, types.Decimal64(v)
	default:
		return false, nil
	}
}

func transferDecimal128val(a, b int64, oid types.T) (bool, any) {
	switch oid {
	case types.T_decimal128:
		return true, types.Decimal128{B0_63: uint64(a), B64_127: uint64(b)}
	default:
		return false, nil
	}
}

// for test
func MakeColExprForTest(idx int32, typ types.T, colName ...string) *plan.Expr {
	schema := []string{"a", "b", "c", "d"}
	var name = schema[idx]
	if len(colName) > 0 {
		name = colName[0]
	}

	containerType := typ.ToType()
	exprType := plan2.MakePlan2Type(&containerType)

	return &plan.Expr{
		Typ: exprType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: idx,
				Name:   name,
			},
		},
	}
}

func MakeFunctionExprForTest(name string, args []*plan.Expr) *plan.Expr {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = plan2.MakeTypeByPlan2Expr(arg)
	}

	finfo, err := function.GetFunctionByName(context.TODO(), name, argTypes)
	if err != nil {
		panic(err)
	}

	retTyp := finfo.GetReturnType()

	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&retTyp),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     finfo.GetEncodedOverloadID(),
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func MakeInExprForTest[T any](
	arg0 *plan.Expr, vals []T, oid types.T, mp *mpool.MPool,
) *plan.Expr {
	vec := vector.NewVec(oid.ToType())
	for _, val := range vals {
		_ = vector.AppendAny(vec, val, false, mp)
	}
	data, _ := vec.MarshalBinary()
	vec.Free(mp)
	return &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     function.InFunctionEncodedID,
					ObjName: function.InFunctionName,
				},
				Args: []*plan.Expr{
					arg0,
					{
						Typ: plan2.MakePlan2Type(vec.GetType()),
						Expr: &plan.Expr_Vec{
							Vec: &plan.LiteralVec{
								Len:  int32(len(vals)),
								Data: data,
							},
						},
					},
				},
			},
		},
	}
}
