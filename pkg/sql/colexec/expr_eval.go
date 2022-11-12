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
	constSType          = types.Type{Oid: types.T_varchar}
	constDateType       = types.Type{Oid: types.T_date}
	constTimeType       = types.Type{Oid: types.T_time}
	constDatetimeType   = types.Type{Oid: types.T_datetime}
	constDecimal64Type  = types.Type{Oid: types.T_decimal64}
	constDecimal128Type = types.Type{Oid: types.T_decimal128}
	constTimestampType  = types.Type{Oid: types.T_timestamp}
)

func getConstVec(proc *process.Process, expr *plan.Expr, length int) (*vector.Vector, error) {
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
			vec = vector.NewConstFixed(constTimestampType, length, types.Timestamp(t.C.GetTimestampval()), proc.Mp())
		case *plan.Const_Sval:
			sval := t.C.GetSval()
			vec = vector.NewConstString(constSType, length, sval, proc.Mp())
		default:
			return nil, moerr.NewNYI(fmt.Sprintf("const expression %v", t.C.GetValue()))
		}
	}
	vec.SetIsBin(t.C.IsBin)
	return vec, nil
}

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector

	if len(bat.Zs) == 0 {
		return vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, 1), nil
	}

	var length = len(bat.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.New(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsScalarNull() {
			vec.Typ = types.T(expr.Typ.GetId()).ToType()
		}
		return vec, nil
	case *plan.Expr_F:
		overloadId := t.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadId)
		if err != nil {
			return nil, err
		}
		vs := make([]*vector.Vector, len(t.F.Args))
		for i := range vs {
			v, err := EvalExpr(bat, proc, t.F.Args[i])
			if err != nil {
				if proc != nil {
					mp := make(map[*vector.Vector]uint8)
					for i := range bat.Vecs {
						mp[bat.Vecs[i]] = 0
					}
					for j := 0; j < i; j++ {
						if _, ok := mp[vs[j]]; !ok {
							vector.Clean(vs[j], proc.Mp())
						}
					}
				}
				return nil, err
			}
			vs[i] = v
		}
		defer func() {
			if proc != nil {
				mp := make(map[*vector.Vector]uint8)
				for i := range bat.Vecs {
					mp[bat.Vecs[i]] = 0
				}
				for i := range vs {
					if _, ok := mp[vs[i]]; !ok {
						vector.Clean(vs[i], proc.Mp())
					}
				}
			}
		}()
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}
		vector.SetLength(vec, len(bat.Zs))
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExpr(r, s *batch.Batch, rRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc, expr, 1)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.New(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos], nil
	case *plan.Expr_F:
		overloadId := t.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadId)
		if err != nil {
			return nil, err
		}
		vs := make([]*vector.Vector, len(t.F.Args))
		for i := range vs {
			v, err := JoinFilterEvalExpr(r, s, rRow, proc, t.F.Args[i])
			if err != nil {
				mp := make(map[*vector.Vector]uint8)
				for i := range s.Vecs {
					mp[s.Vecs[i]] = 0
				}
				for j := 0; j < i; j++ {
					if _, ok := mp[vs[j]]; !ok {
						vector.Clean(vs[j], proc.Mp())
					}
				}
				return nil, err
			}
			vs[i] = v
		}
		defer func() {
			mp := make(map[*vector.Vector]uint8)
			for i := range s.Vecs {
				mp[s.Vecs[i]] = 0
			}
			for i := range vs {
				if _, ok := mp[vs[i]]; !ok {
					vector.Clean(vs[i], proc.Mp())
				}
			}
		}()
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}
		vector.SetLength(vec, len(s.Zs))
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(fmt.Sprintf("eval expr '%v'", t))
	}
}

func EvalExprByZonemapBat(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector

	if len(bat.Zs) == 0 {
		return vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, 1), nil
	}

	var length = len(bat.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc, expr, length)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.New(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}), nil
	case *plan.Expr_Col:
		vec := bat.Vecs[t.Col.ColPos]
		if vec.IsScalarNull() {
			vec.Typ = types.T(expr.Typ.GetId()).ToType()
		}
		return vec, nil
	case *plan.Expr_F:
		overloadId := t.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadId)
		if err != nil {
			return nil, err
		}
		vs := make([]*vector.Vector, len(t.F.Args))
		for i := range vs {
			v, err := EvalExprByZonemapBat(bat, proc, t.F.Args[i])
			if err != nil {
				if proc != nil {
					mp := make(map[*vector.Vector]uint8)
					for i := range bat.Vecs {
						mp[bat.Vecs[i]] = 0
					}
					for j := 0; j < i; j++ {
						if _, ok := mp[vs[j]]; !ok {
							vector.Clean(vs[j], proc.Mp())
						}
					}
				}
				return nil, err
			}
			vs[i] = v
		}
		defer func() {
			if proc != nil {
				mp := make(map[*vector.Vector]uint8)
				for i := range bat.Vecs {
					mp[bat.Vecs[i]] = 0
				}
				for i := range vs {
					if _, ok := mp[vs[i]]; !ok {
						vector.Clean(vs[i], proc.Mp())
					}
				}
			}
		}()

		compareAndReturn := func(isTrue bool, err error) (*vector.Vector, error) {
			if err != nil {
				// if cann't compare, just return true.
				// that means we don't known this filter expr's return, so you must readBlock
				return vector.NewConstFixed(types.T_bool.ToType(), 1, true, proc.Mp()), nil
			}
			return vector.NewConstFixed(types.T_bool.ToType(), 1, isTrue, proc.Mp()), nil
		}

		switch t.F.Func.ObjName {
		case ">":
			// if some one in left > some one in right, that will be true
			return compareAndReturn(vs[0].CompareAndCheckAnyResultIsTrue(vs[1], ">"))
		case "<":
			// if some one in left < some one in right, that will be true
			return compareAndReturn(vs[0].CompareAndCheckAnyResultIsTrue(vs[1], "<"))
		case "=":
			// if left intersect right, that will be true
			return compareAndReturn(vs[0].CompareAndCheckIntersect(vs[1]))
		case ">=":
			// if some one in left >= some one in right, that will be true
			return compareAndReturn(vs[0].CompareAndCheckAnyResultIsTrue(vs[1], ">="))
		case "<=":
			// if some one in left <= some one in right, that will be true
			return compareAndReturn(vs[0].CompareAndCheckAnyResultIsTrue(vs[1], "<="))
		case "and":
			// if left has one true and right has one true, that will be true
			cols1 := vector.MustTCols[bool](vs[0])
			cols2 := vector.MustTCols[bool](vs[1])

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
			// if some one is true in left/right, that will be true
			cols1 := vector.MustTCols[bool](vs[0])
			cols2 := vector.MustTCols[bool](vs[1])
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

		vec, err = f.VecFn(vs, proc)

		if err != nil {
			return nil, err
		}
		vector.SetLength(vec, len(bat.Zs))
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExprInBucket(r, s *batch.Batch, rRow, sRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		return getConstVec(proc, expr, 1)
	case *plan.Expr_T:
		// return a vector recorded type information but without real data
		return vector.New(types.Type{
			Oid:       types.T(t.T.Typ.GetId()),
			Width:     t.T.Typ.GetWidth(),
			Scale:     t.T.Typ.GetScale(),
			Precision: t.T.Typ.GetPrecision(),
		}), nil
	case *plan.Expr_Col:
		if t.Col.RelPos == 0 {
			return r.Vecs[t.Col.ColPos].ToConst(rRow, proc.Mp()), nil
		}
		return s.Vecs[t.Col.ColPos].ToConst(sRow, proc.Mp()), nil
	case *plan.Expr_F:
		overloadId := t.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadId)
		if err != nil {
			return nil, err
		}
		vs := make([]*vector.Vector, len(t.F.Args))
		for i := range vs {
			v, err := JoinFilterEvalExprInBucket(r, s, rRow, sRow, proc, t.F.Args[i])
			if err != nil {
				mp := make(map[*vector.Vector]uint8)
				for i := range s.Vecs {
					mp[s.Vecs[i]] = 0
				}
				for j := 0; j < i; j++ {
					if _, ok := mp[vs[j]]; !ok {
						vector.Clean(vs[j], proc.Mp())
					}
				}
				return nil, err
			}
			vs[i] = v
		}
		defer func() {
			mp := make(map[*vector.Vector]uint8)
			for i := range s.Vecs {
				mp[s.Vecs[i]] = 0
			}
			for i := range vs {
				if _, ok := mp[vs[i]]; !ok {
					vector.Clean(vs[i], proc.Mp())
				}
			}
		}()
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}

		vector.SetLength(vec, 1)
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, moerr.NewNYI(fmt.Sprintf("eval expr '%v'", t))
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
