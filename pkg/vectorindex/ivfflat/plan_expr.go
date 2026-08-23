// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func ivfPlanType(t types.Type) plan.Type {
	return plan.Type{Id: int32(t.Oid), Width: t.Width, Scale: t.Scale}
}

func ivfColExpr(pos int32, typ plan.Type) *plan.Expr {
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
}

func ivfStringExpr(value string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}},
	}
}

func ivfInt64Expr(value int64) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}},
	}
}

func ivfUint64Expr(value uint64) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_uint64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: value}}},
	}
}

func ivfFuncExpr(ctx context.Context, name string, args ...*plan.Expr) (*plan.Expr, error) {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = types.New(types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale)
	}
	fn, err := function.GetFunctionByName(ctx, name, argTypes)
	if err != nil {
		return nil, err
	}
	return &plan.Expr{
		Typ: ivfPlanType(fn.GetReturnType()),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: name},
			Args: args,
		}},
	}, nil
}

func ivfAndExpr(ctx context.Context, exprs ...*plan.Expr) (*plan.Expr, error) {
	var out *plan.Expr
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		if out == nil {
			out = expr
			continue
		}
		var err error
		out, err = ivfFuncExpr(ctx, "and", out, expr)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func ivfInInt64Expr(ctx context.Context, mp *mpool.MPool, left *plan.Expr, values []int64) (*plan.Expr, error) {
	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	if err := vector.AppendFixedList(vec, values, nil, mp); err != nil {
		return nil, err
	}
	data, err := vec.MarshalBinary()
	if err != nil {
		return nil, err
	}
	right := &plan.Expr{
		Typ:  left.Typ,
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: int32(len(values)), Data: data}},
	}
	return ivfFuncExpr(ctx, function.InFunctionName, left, right)
}
