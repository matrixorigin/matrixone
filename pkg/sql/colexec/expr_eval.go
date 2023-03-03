// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.Type{Oid: types.T_bool}
	constI8Type         = types.Type{Oid: types.T_int8}
	constI16Type        = types.Type{Oid: types.T_int16}
	constI32Type        = types.Type{Oid: types.T_int32}
	constI64Type        = types.Type{Oid: types.T_int64}
	constU8Type         = types.Type{Oid: types.T_uint8}
	constU16Type        = types.Type{Oid: types.T_uint16}
	constU32Type        = types.Type{Oid: types.T_uint32}
	constU64Type        = types.Type{Oid: types.T_uint64}
	constFType          = types.Type{Oid: types.T_float32}
	constDType          = types.Type{Oid: types.T_float64}
	constSType          = types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen}
	constBinType        = types.Type{Oid: types.T_blob}
	constDateType       = types.Type{Oid: types.T_date}
	constTimeType       = types.Type{Oid: types.T_time}
	constDatetimeType   = types.Type{Oid: types.T_datetime}
	constDecimal64Type  = types.Type{Oid: types.T_decimal64}
	constDecimal128Type = types.Type{Oid: types.T_decimal128}
	constTimestampTypes = []types.Type{
		{Oid: types.T_timestamp},
		{Oid: types.T_timestamp, Precision: 1},
		{Oid: types.T_timestamp, Precision: 2},
		{Oid: types.T_timestamp, Precision: 3},
		{Oid: types.T_timestamp, Precision: 4},
		{Oid: types.T_timestamp, Precision: 5},
		{Oid: types.T_timestamp, Precision: 6},
	}
)

func getConstVecInList(ctx context.Context, proc *process.Process, exprs []*plan.Expr) (*vector.Vector, error) {
	lenList := len(exprs)
	vec, err := proc.AllocVectorOfRows(types.T(exprs[0].Typ.Id).ToType(), int64(lenList), nil)
	if err != nil {
		panic(moerr.NewOOM(proc.Ctx))
	}
	for i := 0; i < lenList; i++ {
		expr := exprs[i]
		t, ok := expr.Expr.(*plan.Expr_C)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "args in list must be constant")
		}
		if t.C.GetIsnull() {
			vec.Nsp.Set(uint64(i))
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				veccol := vec.Col.([]bool)
				veccol[i] = t.C.GetBval()
			case *plan.Const_I8Val:
				veccol := vec.Col.([]int8)
				veccol[i] = int8(t.C.GetI8Val())
			case *plan.Const_I16Val:
				veccol := vec.Col.([]int16)
				veccol[i] = int16(t.C.GetI16Val())
			case *plan.Const_I32Val:
				veccol := vec.Col.([]int32)
				veccol[i] = t.C.GetI32Val()
			case *plan.Const_I64Val:
				veccol := vec.Col.([]int64)
				veccol[i] = t.C.GetI64Val()
			case *plan.Const_U8Val:
				veccol := vec.Col.([]uint8)
				veccol[i] = uint8(t.C.GetU8Val())
			case *plan.Const_U16Val:
				veccol := vec.Col.([]uint16)
				veccol[i] = uint16(t.C.GetU16Val())
			case *plan.Const_U32Val:
				veccol := vec.Col.([]uint32)
				veccol[i] = t.C.GetU32Val()
			case *plan.Const_U64Val:
				veccol := vec.Col.([]uint64)
				veccol[i] = t.C.GetU64Val()
			case *plan.Const_Fval:
				veccol := vec.Col.([]float32)
				veccol[i] = t.C.GetFval()
			case *plan.Const_Dval:
				veccol := vec.Col.([]float64)
				veccol[i] = t.C.GetDval()
			case *plan.Const_Dateval:
				veccol := vec.Col.([]types.Date)
				veccol[i] = types.Date(t.C.GetDateval())
			case *plan.Const_Timeval:
				veccol := vec.Col.([]types.Time)
				veccol[i] = types.Time(t.C.GetTimeval())
			case *plan.Const_Datetimeval:
				veccol := vec.Col.([]types.Datetime)
				veccol[i] = types.Datetime(t.C.GetDatetimeval())
			case *plan.Const_Decimal64Val:
				cd64 := t.C.GetDecimal64Val()
				d64 := types.Decimal64FromInt64Raw(cd64.A)
				veccol := vec.Col.([]types.Decimal64)
				veccol[i] = d64
			case *plan.Const_Decimal128Val:
				cd128 := t.C.GetDecimal128Val()
				d128 := types.Decimal128FromInt64Raw(cd128.A, cd128.B)
				veccol := vec.Col.([]types.Decimal128)
				veccol[i] = d128
			case *plan.Const_Timestampval:
				pre := expr.Typ.Precision
				if pre < 0 || pre > 6 {
					return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp precision")
				}
				veccol := vec.Col.([]types.Timestamp)
				veccol[i] = types.Timestamp(t.C.GetTimestampval())
			case *plan.Const_Sval:
				sval := t.C.GetSval()
				vector.SetStringAt(vec, i, sval, proc.Mp())
			case *plan.Const_Defaultval:
				defaultVal := t.C.GetDefaultval()
				veccol := vec.Col.([]bool)
				veccol[i] = defaultVal
			default:
				return nil, moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", t.C.GetValue()))
			}
			vec.SetIsBin(t.C.IsBin)
		}
	}
	return vec, nil
}

func getConstVec(ctx context.Context, proc *process.Process, expr *plan.Expr, length int) (*vector.Vector, error) {
	var vec *vector.Vector
	t := expr.Expr.(*plan.Expr_C)
	if t.C.GetIsnull() {
		if types.T(expr.Typ.GetId()) == types.T_any {
			vec = vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, length)
		} else {
			vec = vector.NewConstNullWithData(types.Type{Oid: types.T(expr.Typ.GetId())}, length, proc.Mp())
		}
	} else {
		switch t.C.GetValue().(type) {
		case *plan.Const_Bval:
			vec = vector.NewConstFixed(constBType, length, t.C.GetBval(), proc.Mp())
		case *plan.Const_I8Val:
			vec = vector.NewConstFixed(constI8Type, length, int8(t.C.GetI8Val()), proc.Mp())
		case *plan.Const_I16Val:
			vec = vector.NewConstFixed(constI16Type, length, int16(t.C.GetI16Val()), proc.Mp())
		case *plan.Const_I32Val:
			vec = vector.NewConstFixed(constI32Type, length, t.C.GetI32Val(), proc.Mp())
		case *plan.Const_I64Val:
			vec = vector.NewConstFixed(constI64Type, length, t.C.GetI64Val(), proc.Mp())
		case *plan.Const_U8Val:
			vec = vector.NewConstFixed(constU8Type, length, uint8(t.C.GetU8Val()), proc.Mp())
		case *plan.Const_U16Val:
			vec = vector.NewConstFixed(constU16Type, length, uint16(t.C.GetU16Val()), proc.Mp())
		case *plan.Const_U32Val:
			vec = vector.NewConstFixed(constU32Type, length, t.C.GetU32Val(), proc.Mp())
		case *plan.Const_U64Val:
			vec = vector.NewConstFixed(constU64Type, length, t.C.GetU64Val(), proc.Mp())
		case *plan.Const_Fval:
			vec = vector.NewConstFixed(constFType, length, t.C.GetFval(), proc.Mp())
		case *plan.Const_Dval:
			vec = vector.NewConstFixed(constDType, length, t.C.GetDval(), proc.Mp())
		case *plan.Const_Dateval:
			vec = vector.NewConstFixed(constDateType, length, types.Date(t.C.GetDateval()), proc.Mp())
		case *plan.Const_Timeval:
			vec = vector.NewConstFixed(constTimeType, length, types.Time(t.C.GetTimeval()), proc.Mp())
		case *plan.Const_Datetimeval:
			vec = vector.NewConstFixed(constDatetimeType, length, types.Datetime(t.C.GetDatetimeval()), proc.Mp())
		case *plan.Const_Decimal64Val:
			cd64 := t.C.GetDecimal64Val()
			d64 := types.Decimal64FromInt64Raw(cd64.A)
			vec = vector.NewConstFixed(constDecimal64Type, length, d64, proc.Mp())
		case *plan.Const_Decimal128Val:
			cd128 := t.C.GetDecimal128Val()
			d128 := types.Decimal128FromInt64Raw(cd128.A, cd128.B)
			vec = vector.NewConstFixed(constDecimal128Type, length, d128, proc.Mp())
		case *plan.Const_Timestampval:
			pre := expr.Typ.Precision
			if pre < 0 || pre > 6 {
				return nil, moerr.NewInternalError(proc.Ctx, "invalid timestamp precision")
			}
			vec = vector.NewConstFixed(constTimestampTypes[pre], length, types.Timestamp(t.C.GetTimestampval()), proc.Mp())
		case *plan.Const_Sval:
			sval := t.C.GetSval()
			// Distingush binary with non-binary string.
			if expr.Typ != nil {
				if expr.Typ.Id == int32(types.T_binary) || expr.Typ.Id == int32(types.T_varbinary) || expr.Typ.Id == int32(types.T_blob) {
					vec = vector.NewConstString(constBinType, length, sval, proc.Mp())
				} else {
					vec = vector.NewConstString(constSType, length, sval, proc.Mp())
				}
			} else {
				vec = vector.NewConstString(constSType, length, sval, proc.Mp())
			}
		case *plan.Const_Defaultval:
			defaultVal := t.C.GetDefaultval()
			vec = vector.NewConstFixed(constBType, length, defaultVal, proc.Mp())
		default:
			return nil, moerr.NewNYI(ctx, fmt.Sprintf("const expression %v", t.C.GetValue()))
		}
	}
	vec.SetIsBin(t.C.IsBin)
	return vec, nil
}

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	if len(bat.Zs) == 0 {
		return vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, 1), nil
	}

	var length = len(bat.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConst(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}, len(bat.Zs)), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsScalarNull() {
			vec.Typ = types.T(expr.Typ.GetId()).ToType()
		}
		return vec, nil
	case *plan.Expr_List:
		return getConstVecInList(proc.Ctx, proc, t.List.List)
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = EvalExpr(bat, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, bat.Vecs)
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, len(bat.Zs))
		cleanVectorsExceptList(proc, functionParameters, append(bat.Vecs, result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExpr(r, s *batch.Batch, rRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, 1)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConst(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}, len(s.Zs)), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos], nil
	case *plan.Expr_List:
		return getConstVecInList(proc.Ctx, proc, t.List.List)
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = JoinFilterEvalExpr(r, s, rRow, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, append(r.Vecs, s.Vecs...))
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, len(s.Zs))
		cleanVectorsExceptList(proc, functionParameters, append(append(r.Vecs, s.Vecs...), result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("eval expr '%v'", t))
	}
}

func EvalExprByZonemapBat(ctx context.Context, bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	if len(bat.Zs) == 0 {
		return vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, 1), nil
	}

	var length = len(bat.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(ctx, proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConst(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}, len(bat.Zs)), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsScalarNull() {
			vec.Typ = types.T(expr.Typ.GetId()).ToType()
		}
		return vec, nil
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = EvalExprByZonemapBat(ctx, bat, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, bat.Vecs)
			return nil, err
		}

		compareAndReturn := func(isTrue bool, err error) (*vector.Vector, error) {
			if err != nil {
				// if it can't compare, just return true.
				// that means we don't know this filter expr's return, so you must readBlock
				return vector.NewConstFixed(types.T_bool.ToType(), 1, true, proc.Mp()), nil
			}
			return vector.NewConstFixed(types.T_bool.ToType(), 1, isTrue, proc.Mp()), nil
		}

		switch t.F.Func.ObjName {
		case ">":
			// if someone in left > someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], ">"))
		case "<":
			// if someone in left < someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], "<"))
		case "=":
			// if left intersect right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckIntersect(ctx, functionParameters[1]))
		case ">=":
			// if someone in left >= someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], ">="))
		case "<=":
			// if someone in left <= someone in right, that will be true
			return compareAndReturn(functionParameters[0].CompareAndCheckAnyResultIsTrue(ctx, functionParameters[1], "<="))
		case "and":
			// if left has one true and right has one true, that will be true
			cols1 := vector.MustTCols[bool](functionParameters[0])
			cols2 := vector.MustTCols[bool](functionParameters[1])

			for _, leftHasTrue := range cols1 {
				if leftHasTrue {
					for _, rightHasTrue := range cols2 {
						if rightHasTrue {
							return vector.NewConstFixed(types.T_bool.ToType(), 1, true, proc.Mp()), nil
						}
					}
					break
				}
			}
			return vector.NewConstFixed(types.T_bool.ToType(), 1, false, proc.Mp()), nil
		case "or":
			// if someone is true in left/right, that will be true
			cols1 := vector.MustTCols[bool](functionParameters[0])
			cols2 := vector.MustTCols[bool](functionParameters[1])
			for _, flag := range cols1 {
				if flag {
					return vector.NewConstFixed(types.T_bool.ToType(), 1, true, proc.Mp()), nil
				}
			}
			for _, flag := range cols2 {
				if flag {
					return vector.NewConstFixed(types.T_bool.ToType(), 1, true, proc.Mp()), nil
				}
			}
			return vector.NewConstFixed(types.T_bool.ToType(), 1, false, proc.Mp()), nil
		}

		result, err = evalFunction(proc, f, functionParameters, len(bat.Zs))
		cleanVectorsExceptList(proc, functionParameters, append(bat.Vecs, result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr,  *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(ctx, fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExprInBucket(r, s *batch.Batch, rRow, sRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc.Ctx, proc, expr, 1)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.NewConst(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}, 1), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos].ToConst(sRow, proc.Mp()), nil
	case *plan.Expr_F:
		var result *vector.Vector

		fid := t.F.GetFunc().GetObj()
		f, err := function.GetFunctionByID(proc.Ctx, fid)
		if err != nil {
			return nil, err
		}

		functionParameters := make([]*vector.Vector, len(t.F.Args))
		for i := range functionParameters {
			functionParameters[i], err = JoinFilterEvalExprInBucket(r, s, rRow, sRow, proc, t.F.Args[i])
			if err != nil {
				break
			}
		}
		if err != nil {
			cleanVectorsExceptList(proc, functionParameters, append(r.Vecs, s.Vecs...))
			return nil, err
		}

		result, err = evalFunction(proc, f, functionParameters, 1)
		cleanVectorsExceptList(proc, functionParameters, append(append(r.Vecs, s.Vecs...), result))
		if err != nil {
			return nil, err
		}
		return result, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(proc.Ctx, fmt.Sprintf("eval expr '%v'", t))
	}
}

func evalFunction(proc *process.Process, f *function.Function, args []*vector.Vector, length int) (*vector.Vector, error) {
	if !f.UseNewFramework {
		v, err := f.VecFn(args, proc)
		if err != nil {
			return nil, err
		}
		vector.SetLength(v, length)
		v.FillDefaultValue()
		return v, nil
	}
	var resultWrapper vector.FunctionResultWrapper
	var err error

	var parameterTypes []types.Type
	if f.FlexibleReturnType != nil {
		parameterTypes = make([]types.Type, len(args))
		for i := range args {
			parameterTypes[i] = args[i].GetType()
		}
	}
	rTyp, _ := f.ReturnType(parameterTypes)
	numScalar := 0
	// If any argument is `NULL`, return NULL.
	// If all arguments are scalar, return scalar.
	for i := range args {
		if args[i].IsScalar() {
			numScalar++
		} else {
			if len(f.ParameterMustScalar) > i && f.ParameterMustScalar[i] {
				return nil, moerr.NewInternalError(proc.Ctx,
					fmt.Sprintf("the %dth parameter of function can only be constant", i+1))
			}
		}
	}

	if !f.Volatile && numScalar == len(args) {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp(), true, length)
		// XXX only evaluate the first row.
		err = f.NewFn(args, resultWrapper, proc, 1)
	} else {
		resultWrapper = vector.NewFunctionResultWrapper(rTyp, proc.Mp(), false, length)
		err = f.NewFn(args, resultWrapper, proc, length)
	}
	if err != nil {
		resultWrapper.Free()
	}
	return resultWrapper.GetResultVector(), err
}

func cleanVectorsExceptList(proc *process.Process, vs []*vector.Vector, excepts []*vector.Vector) {
	mp := proc.Mp()
	for i := range vs {
		if vs[i] == nil {
			continue
		}
		needClean := true
		for j := range excepts {
			if excepts[j] == vs[i] {
				needClean = false
				break
			}
		}
		if needClean {
			vs[i].Free(mp)
		}
	}
}

// RewriteFilterExprList will convert an expression list to be an AndExpr
func RewriteFilterExprList(list []*plan.Expr) *plan.Expr {
	l := len(list)
	if l == 0 {
		return nil
	} else if l == 1 {
		return list[0]
	} else {
		left := list[0]
		right := RewriteFilterExprList(list[1:])
		return &plan.Expr{
			Typ:  left.Typ,
			Expr: makeAndExpr(left, right),
		}
	}
}

func SplitAndExprs(list []*plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, len(list))
	for i := range list {
		exprs = append(exprs, splitAndExpr(list[i])...)
	}
	return exprs
}

func splitAndExpr(expr *plan.Expr) []*plan.Expr {
	exprs := make([]*plan.Expr, 0, 1)
	if e, ok := expr.Expr.(*plan.Expr_F); ok {
		fid, _ := function.DecodeOverloadID(e.F.Func.GetObj())
		if fid == function.AND {
			exprs = append(exprs, splitAndExpr(e.F.Args[0])...)
			exprs = append(exprs, splitAndExpr(e.F.Args[1])...)
			return exprs
		}
	}
	exprs = append(exprs, expr)
	return exprs
}

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.AndFunctionEncodedID, ObjName: function.AndFunctionName},
			Args: []*plan.Expr{left, right},
		},
	}
}
