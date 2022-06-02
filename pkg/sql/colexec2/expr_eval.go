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

package colexec2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	constBType = types.Type{Oid: types.T_bool}
	constIType = types.Type{Oid: types.T_int64}
	constDType = types.Type{Oid: types.T_float64}
	constSType = types.Type{Oid: types.T_varchar}
)

func EvalExpr(bat *batch.Batch, proc *process.Process, expr *plan.Expr) (*vector.Vector, error) {
	var vec *vector.Vector
	e := expr.Expr
	switch t := e.(type) {
	case *plan.Expr_C:
		if t.C.GetIsnull() {
			vec = vector.NewConst(types.Type{Oid: types.T(expr.Typ.GetId())})
			nulls.Add(vec.Nsp, 0)
		} else {
			switch t.C.GetValue().(type) {
			case *plan.Const_Bval:
				vec = vector.NewConst(constBType)
				vec.Col = []bool{t.C.GetBval()}
			case *plan.Const_Ival:
				vec = vector.NewConst(constIType)
				vec.Col = []int64{t.C.GetIval()}
			case *plan.Const_Dval:
				vec = vector.NewConst(constDType)
				vec.Col = []float64{t.C.GetDval()}
			case *plan.Const_Sval:
				vec = vector.NewConst(constSType)
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
		vec.Length = len(bat.Zs)
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
		return bat.Vecs[t.Col.ColPos], nil
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
				return nil, err
			}
			vs[i] = v
		}
		vec, err = f.VecFn(vs, proc)
		if err != nil {
			return nil, err
		}
		vec.Length = len(bat.Zs)
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
