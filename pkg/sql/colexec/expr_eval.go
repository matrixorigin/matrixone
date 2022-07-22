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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType          = types.Type{Oid: types.T_bool}
	constIType          = types.Type{Oid: types.T_int64}
	constUType          = types.Type{Oid: types.T_uint64}
	constFType          = types.Type{Oid: types.T_float32}
	constDType          = types.Type{Oid: types.T_float64}
	constSType          = types.Type{Oid: types.T_varchar}
	constDateType       = types.Type{Oid: types.T_date}
	constDatetimeType   = types.Type{Oid: types.T_datetime}
	constDecimal64Type  = types.Type{Oid: types.T_decimal64}
	constDecimal128Type = types.Type{Oid: types.T_decimal128}
	constTimestampType  = types.Type{Oid: types.T_timestamp}
)

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector

	if len(bat.Zs) == 0 {
		return vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, 1), nil
	}

	var length = len(bat.Zs)
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		if t.C.GetIsnull() {
			vec = vector.NewConstNull(types.Type{Oid: types.T(expr.Typ.GetId())}, length)
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				vec = vector.NewConst(constBType, length)
				vec.Col = []bool{t.C.GetBval()}
			case *plan.Const_Ival:
				vec = vector.NewConst(constIType, length)
				vec.Col = []int64{t.C.GetIval()}
			case *plan.Const_Fval:
				vec = vector.NewConst(constFType, length)
				vec.Col = []float32{t.C.GetFval()}
			case *plan.Const_Uval:
				vec = vector.NewConst(constUType, length)
				vec.Col = []uint64{t.C.GetUval()}
			case *plan.Const_Dval:
				vec = vector.NewConst(constDType, length)
				vec.Col = []float64{t.C.GetDval()}
			case *plan.Const_Dateval:
				vec = vector.NewConst(constDateType, length)
				vec.Col = []types.Date{types.Date(t.C.GetDateval())}
			case *plan.Const_Datetimeval:
				vec = vector.NewConst(constDatetimeType, length)
				vec.Col = []types.Datetime{types.Datetime(t.C.GetDatetimeval())}
			case *plan.Const_Decimal64Val:
				vec = vector.NewConst(constDecimal64Type, length)
				d64 := t.C.GetDecimal64Val()
				vec.Col = []types.Decimal64{types.Decimal64FromInt64Raw(d64.A)}
			case *plan.Const_Decimal128Val:
				vec = vector.NewConst(constDecimal128Type, length)
				d128 := t.C.GetDecimal128Val()
				vec.Col = []types.Decimal128{types.Decimal128FromInt64Raw(d128.A, d128.B)}
			case *plan.Const_Timestampval:
				vec = vector.NewConst(constTimestampType, length)
				vec.Col = []types.Timestamp{types.Timestamp(t.C.GetTimestampval())}
			case *plan.Const_Sval:
				vec = vector.NewConst(constSType, length)
				sval := t.C.GetSval()
				vec.Col = &types.Bytes{
					Data:    []byte(sval),
					Offsets: []uint32{0},
					Lengths: []uint32{uint32(len(sval))},
				}
			default:
				return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unimplemented const expression %v", t.C.GetValue()))
			}
		}
		return vec, nil
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
							vector.Clean(vs[j], proc.Mp)
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
						vector.Clean(vs[i], proc.Mp)
					}
				}
			}
		}()
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}
		vec.Length = len(bat.Zs)
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupported eval expr '%v'", t))
	}
}

func JoinFilterEvalExpr(r, s *batch.Batch, rRow int, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		if t.C.GetIsnull() {
			vec = vector.NewConst(types.Type{Oid: types.T(expr.Typ.GetId())}, 1)
			nulls.Add(vec.Nsp, 0)
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				vec = vector.NewConst(constBType, 1)
				vec.Col = []bool{t.C.GetBval()}
			case *plan.Const_Ival:
				vec = vector.NewConst(constIType, 1)
				vec.Col = []int64{t.C.GetIval()}
			case *plan.Const_Dval:
				vec = vector.NewConst(constDType, 1)
				vec.Col = []float64{t.C.GetDval()}
			case *plan.Const_Sval:
				vec = vector.NewConst(constSType, 1)
				sval := t.C.GetSval()
				vec.Col = &types.Bytes{
					Data:    []byte(sval),
					Offsets: []uint32{0},
					Lengths: []uint32{uint32(len(sval))},
				}
			default:
				return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unimplemented const expression %v", t.C.GetValue()))
			}
		}
		return vec, nil
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
			return r.Vecs[t.Col.ColPos].ToConst(rRow), nil
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
						vector.Clean(vs[j], proc.Mp)
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
					vector.Clean(vs[i], proc.Mp)
				}
			}
		}()
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}
		vec.Length = len(s.Zs)
		vec.FillDefaultValue()
		return vec, nil
	default:
		// *plan.Expr_Corr, *plan.Expr_List, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Sub
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupported eval expr '%v'", t))
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

func makeAndExpr(left, right *plan.Expr) *plan.Expr_F {
	return &plan.Expr_F{
		F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.AndFunctionEncodedID},
			Args: []*plan.Expr{left, right},
		},
	}
}
