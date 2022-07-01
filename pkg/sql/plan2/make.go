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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func makePlan2BoolConstExpr(v bool) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Bval{
			Bval: v,
		},
	}}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Ival{
			Ival: v,
		},
	}}
}

func makePlan2Uint64ConstExpr(v uint64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Uval{
			Uval: v,
		},
	}}
}

func makePlan2Float32ConstExpr(v float32) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Fval{
			Fval: v,
		},
	}}
}

func makePlan2Float64ConstExpr(v float64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Dval{
			Dval: v,
		},
	}}
}

func makePlan2StringConstExpr(v string) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Sval{
			Sval: v,
		},
	}}
}

func makePlan2DateConstExpr(v types.Date) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Dateval{
			Dateval: int32(v),
		},
	}}
}

func makePlan2DatetimeConstExpr(v types.Datetime) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Datetimeval{
			Datetimeval: int64(v),
		},
	}}
}

func makePlan2Decimal64ConstExpr(v types.Decimal64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Decimal64Val{
			Decimal64Val: int64(v),
		},
	}}
}

func makePlan2Decimal128ConstExpr(v types.Decimal128) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Decimal128Val{
			Decimal128Val: &plan.Decimal128{Lo: v.Lo, Hi: v.Hi},
		},
	}}
}

func makePlan2TimestampConstExpr(v types.Timestamp) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Timestampval{
			Timestampval: int64(v),
		},
	}}
}

func makePlan2CastExpr(expr *Expr, targetType *Type) (*Expr, error) {
	t1, t2 := types.T(expr.Typ.Id), types.T(targetType.Id)
	if isSameColumnType(expr.Typ, targetType) {
		return expr, nil
	}
	if types.T(expr.Typ.Id) == types.T_any {
		expr.Typ = copyType(targetType)
		return expr, nil
	}
	_, id, _, err := function.GetFunctionByName("cast", []types.T{t1, t2})
	if err != nil {
		return nil, err
	}
	t := &plan.Expr{Expr: &plan.Expr_T{T: &plan.TargetType{
		Typ: rewriteDecimalTypeIfNecessary(copyType(targetType)),
	}}}
	return &plan.Expr{Expr: &plan.Expr_F{
		F: &plan.Function{
			Func: &ObjectRef{Obj: id},
			Args: []*Expr{expr, t},
		},
	}}, nil
}

func copyType(t *Type) *Type {
	return &Type{
		Id:        t.Id,
		Nullable:  t.Nullable,
		Width:     t.Width,
		Precision: t.Precision,
		Size:      t.Size,
		Scale:     t.Scale,
	}
}

func MakePlan2DefaultExpr(expr engine.DefaultExpr) *plan.DefaultExpr {
	ret := &plan.DefaultExpr{}
	ret.Exist = expr.Exist
	if !ret.Exist {
		return ret
	}
	ret.IsNull = expr.IsNull
	if ret.IsNull {
		return ret
	}
	ret.Value = &plan.ConstantValue{}
	switch t := expr.Value.(type) {
	case bool:
		ret.Value.ConstantValue = &plan.ConstantValue_BoolV{BoolV: t}
	case float32:
		ret.Value.ConstantValue = &plan.ConstantValue_Float32V{Float32V: t}
	case float64:
		ret.Value.ConstantValue = &plan.ConstantValue_Float64V{Float64V: t}
	case string:
		ret.Value.ConstantValue = &plan.ConstantValue_StringV{StringV: t}
	case int8:
		ret.Value.ConstantValue = &plan.ConstantValue_Int64V{Int64V: int64(t)}
	case int16:
		ret.Value.ConstantValue = &plan.ConstantValue_Int64V{Int64V: int64(t)}
	case int32:
		ret.Value.ConstantValue = &plan.ConstantValue_Int64V{Int64V: int64(t)}
	case int64:
		ret.Value.ConstantValue = &plan.ConstantValue_Int64V{Int64V: t}
	case uint8:
		ret.Value.ConstantValue = &plan.ConstantValue_Uint64V{Uint64V: uint64(t)}
	case uint16:
		ret.Value.ConstantValue = &plan.ConstantValue_Uint64V{Uint64V: uint64(t)}
	case uint32:
		ret.Value.ConstantValue = &plan.ConstantValue_Uint64V{Uint64V: uint64(t)}
	case uint64:
		ret.Value.ConstantValue = &plan.ConstantValue_Uint64V{Uint64V: t}
	case types.Date:
		ret.Value.ConstantValue = &plan.ConstantValue_DateV{DateV: int32(t)}
	case types.Datetime:
		ret.Value.ConstantValue = &plan.ConstantValue_DateTimeV{DateTimeV: int64(t)}
	case types.Decimal64:
		ret.Value.ConstantValue = &plan.ConstantValue_Decimal64V{Decimal64V: int64(t)}
	case types.Decimal128:
		ret.Value.ConstantValue = &plan.ConstantValue_Decimal128V{Decimal128V: &plan.Decimal128{
			Lo: t.Lo,
			Hi: t.Hi,
		}}
	case types.Timestamp:
		ret.Value.ConstantValue = &plan.ConstantValue_TimeStampV{TimeStampV: int64(t)}
	}
	return ret
}

// if typ is decimal128 and decimal64 without scalar and precision
// set a default value for it.
func rewriteDecimalTypeIfNecessary(typ *plan.Type) *plan.Type {
	if typ.Id == plan.Type_DECIMAL128 && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 10
		typ.Width = 38 // precision
		typ.Size = int32(types.T_decimal128.TypeLen())
	}
	if typ.Id == plan.Type_DECIMAL64 && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 2
		typ.Width = 6 // precision
		typ.Size = int32(types.T_decimal64.TypeLen())
	}
	return typ
}
