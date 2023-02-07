// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func makePlan2DecimalExprWithType(ctx context.Context, v string, isBin ...bool) (*plan.Expr, error) {
	_, scale, err := types.ParseStringToDecimal128WithoutTable(v, isBin...)
	if err != nil {
		return nil, err
	}
	typ := &plan.Type{
		Id:          int32(types.T_decimal128),
		Width:       34,
		Scale:       scale,
		Precision:   34,
		NotNullable: true,
	}
	return appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(v, isBin...), typ)
}

func makePlan2DateConstNullExpr(t types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(t),
			NotNullable: false,
		},
	}
}

func makePlan2Decimal128ConstNullExpr() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       34,
			Scale:       0,
			Precision:   34,
			NotNullable: false,
		},
	}
}

func makePlan2NullConstExprWithType() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	}
}

func makePlan2BoolConstExpr(v bool) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Bval{
			Bval: v,
		},
	}}
}

func makePlan2BoolConstExprWithType(v bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2BoolConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
			Size:        1,
		},
	}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_I64Val{
			I64Val: v,
		},
	}}
}

var MakePlan2Int64ConstExprWithType = makePlan2Int64ConstExprWithType

func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
			Size:        8,
		},
	}
}

func makePlan2Uint64ConstExpr(v uint64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_U64Val{
			U64Val: v,
		},
	}}
}

func makePlan2Uint64ConstExprWithType(v uint64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_uint64),
			NotNullable: true,
			Size:        8,
		},
	}
}

func makePlan2Float64ConstExpr(v float64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Dval{
			Dval: v,
		},
	}}
}

var MakePlan2Float64ConstExprWithType = makePlan2Float64ConstExprWithType

func makePlan2Float64ConstExprWithType(v float64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Float64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
			Size:        8,
		},
	}
}

func makePlan2StringConstExpr(v string, isBin ...bool) *plan.Expr_C {
	c := &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Sval{
			Sval: v,
		},
	}}
	if len(isBin) > 0 {
		c.C.IsBin = isBin[0]
	}
	return c
}

var MakePlan2StringConstExprWithType = makePlan2StringConstExprWithType

func makePlan2StringConstExprWithType(v string, isBin ...bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, isBin...),
		Typ: &plan.Type{
			Id:          int32(types.T_varchar),
			NotNullable: true,
			Size:        4,
			Width:       int32(utf8.RuneCountInString(v)),
		},
	}
}

func makePlan2NullTextConstExpr(v string) *plan.Expr_C {
	c := &plan.Expr_C{C: &plan.Const{
		Isnull: true,
	}}
	return c
}

func MakePlan2NullTextConstExprWithType(v string) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2NullTextConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_text),
			NotNullable: false,
			Size:        4,
			Width:       int32(utf8.RuneCountInString(v)),
		},
	}
}

func makePlan2CastExpr(ctx context.Context, expr *Expr, targetType *Type) (*Expr, error) {
	if isSameColumnType(expr.Typ, targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	t1, t2 := makeTypeByPlan2Expr(expr), makeTypeByPlan2Type(targetType)
	if types.T(expr.Typ.Id) == types.T_any {
		expr.Typ = targetType
		return expr, nil
	}
	id, _, _, err := function.GetFunctionByName(ctx, "cast", []types.Type{t1, t2})
	if err != nil {
		return nil, err
	}
	t := &plan.Expr{
		Typ: targetType,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{
				Typ: targetType,
			},
		},
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{Obj: id, ObjName: "cast"},
				Args: []*Expr{expr, t},
			},
		},
		Typ: targetType,
	}, nil
}

// if typ is decimal128 and decimal64 without scalar and precision
// set a default value for it.
func rewriteDecimalTypeIfNecessary(typ *plan.Type) *plan.Type {
	if typ.Id == int32(types.T_decimal128) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 10
		typ.Width = 38 // precision
		typ.Size = int32(types.T_decimal128.TypeLen())
	}
	if typ.Id == int32(types.T_decimal64) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 2
		typ.Width = 6 // precision
		typ.Size = int32(types.T_decimal64.TypeLen())
	}
	return typ
}

var MakePlan2Type = makePlan2Type

func makePlan2Type(typ *types.Type) *plan.Type {
	return &plan.Type{
		Id:        int32(typ.Oid),
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}

var MakeTypeByPlan2Type = makeTypeByPlan2Type

func makeTypeByPlan2Type(typ *plan.Type) types.Type {
	var size int32 = 0
	oid := types.T(typ.Id)
	if oid != types.T_any && oid != types.T_interval {
		size = int32(oid.TypeLen())
	}
	return types.Type{
		Oid:       types.T(typ.Id),
		Size:      size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

var MakeTypeByPlan2Expr = makeTypeByPlan2Expr

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	var size int32 = 0
	oid := types.T(expr.Typ.Id)
	if oid != types.T_any && oid != types.T_interval {
		size = int32(oid.TypeLen())
	}
	return types.Type{
		Oid:       oid,
		Size:      size,
		Width:     expr.Typ.Width,
		Scale:     expr.Typ.Scale,
		Precision: expr.Typ.Precision,
	}
}
