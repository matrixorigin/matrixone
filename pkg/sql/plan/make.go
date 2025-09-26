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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

func MakePlan2Decimal64ExprWithType(v types.Decimal64, typ *Type) *plan.Expr {
	rawA := int64(v)
	return &plan.Expr{
		Typ: *typ,
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: false,
				Value: &plan.Literal_Decimal64Val{
					Decimal64Val: &plan.Decimal64{
						A: rawA,
					},
				},
			},
		},
	}
}

func MakePlan2Decimal128ExprWithType(v types.Decimal128, typ *Type) *plan.Expr {
	rawA := v.B0_63
	rawB := v.B64_127
	return &plan.Expr{
		Typ: *typ,
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: false,
				Value: &plan.Literal_Decimal128Val{
					Decimal128Val: &plan.Decimal128{
						A: int64(rawA),
						B: int64(rawB),
					},
				},
			},
		},
	}
}

func makePlan2DecimalExprWithType(ctx context.Context, v string, isBin ...bool) (*plan.Expr, error) {
	_, scale, err := types.Parse128(v)
	if err != nil {
		return nil, err
	}
	var typ plan.Type
	if scale < 18 && len(v) < 18 {
		typ = plan.Type{
			Id:          int32(types.T_decimal64),
			Width:       18,
			Scale:       scale,
			NotNullable: true,
		}
	} else {
		typ = plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       38,
			Scale:       scale,
			NotNullable: true,
		}
	}
	return appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(v, isBin...), typ)
}

func makePlan2DateConstNullExpr(t types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: true,
			},
		},
		Typ: plan.Type{
			Id:          int32(t),
			NotNullable: false,
		},
	}
}

func makePlan2Decimal128ConstNullExpr() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: true,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       38,
			Scale:       0,
			NotNullable: false,
		},
	}
}

func makePlan2NullConstExprWithType() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: true,
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	}
}

func makePlan2BoolConstExpr(v bool) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Bval{
			Bval: v,
		},
	}}
}

func makePlan2BoolConstExprWithType(v bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2BoolConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}
}

func makePlan2Int8ConstExpr(v int8) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I8Val{
			I8Val: int32(v),
		},
	}}
}

func makePlan2Int16ConstExpr(v int16) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I16Val{
			I16Val: int32(v),
		},
	}}
}

func makePlan2Int32ConstExpr(v int32) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I32Val{
			I32Val: v,
		},
	}}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I64Val{
			I64Val: v,
		},
	}}
}

func makePlan2TimeConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Timeval{
			Timeval: v,
		},
	}}
}

func makePlan2DateConstExpr(v int32) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Dateval{
			Dateval: v,
		},
	}}
}

func makePlan2DateTimeConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Datetimeval{
			Datetimeval: v,
		},
	}}
}

func makePlan2TimestampConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Timestampval{
			Timestampval: v,
		},
	}}
}

var MakePlan2BoolConstExprWithType = makePlan2BoolConstExprWithType
var MakePlan2Int8ConstExprWithType = makePlan2Int8ConstExprWithType
var MakePlan2Int16ConstExprWithType = makePlan2Int16ConstExprWithType
var MakePlan2Int32ConstExprWithType = makePlan2Int32ConstExprWithType
var MakePlan2Int64ConstExprWithType = makePlan2Int64ConstExprWithType
var MakePlan2Uint8ConstExprWithType = makePlan2Uint8ConstExprWithType
var MakePlan2Uint16ConstExprWithType = makePlan2Uint16ConstExprWithType
var MakePlan2Uint32ConstExprWithType = makePlan2Uint32ConstExprWithType
var MakePlan2Uint64ConstExprWithType = makePlan2Uint64ConstExprWithType

var MakePlan2TimeConstExprWithType = makePlan2TimeConstExprWithType
var MakePlan2DateConstExprWithType = makePlan2DateConstExprWithType
var MakePlan2DateTimeConstExprWithType = makePlan2DateTimeConstExprWithType
var MakePlan2TimestampConstExprWithType = makePlan2TimestampConstExprWithType

func makePlan2Int8ConstExprWithType(v int8) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int8ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int8),
			NotNullable: true,
		},
	}
}

func makePlan2Int16ConstExprWithType(v int16) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int16ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int16),
			NotNullable: true,
		},
	}
}

func makePlan2Int32ConstExprWithType(v int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int32),
			NotNullable: true,
		},
	}
}

func makePlan2TimeConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2TimeConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_time),
			NotNullable: true,
		},
	}
}

func makePlan2DateConstExprWithType(v int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2DateConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_date),
			NotNullable: true,
		},
	}
}

func makePlan2DateTimeConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2DateTimeConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_datetime),
			NotNullable: true,
		},
	}
}

func makePlan2TimestampConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2TimestampConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_timestamp),
			NotNullable: true,
		},
	}
}

func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
}

var MakePlan2Vecf32ConstExprWithType = makePlan2Vecf32ConstExprWithType

// makePlan2Vecf32ConstExprWithType makes a vecf32 const expr.
// usage: makePlan2Vecf32ConstExprWithType("[1,2,3]", 3)
func makePlan2Vecf32ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_float32),
			Width:       l,
			NotNullable: true,
		},
	}
}

func makePlan2Vecf32ConstExpr(v string) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Sval{
			Sval: v,
		},
	}}
}

var MakePlan2Vecf64ConstExprWithType = makePlan2Vecf64ConstExprWithType

// makePlan2Vecf64ConstExprWithType makes a vecf64 const expr.
// usage: makePlan2Vecf64ConstExprWithType("[1,2,3]", 3)
func makePlan2Vecf64ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_float64),
			Width:       l,
			NotNullable: true,
		},
	}
}

var MakePlan2StringVecExprWithType = makePlan2StringVecExprWithType

func makePlan2StringVecExprWithType(mp *mpool.MPool, vals ...string) *plan.Expr {
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, val := range vals {
		vector.AppendBytes(vec, []byte(val), false, mp)
	}
	data, _ := vec.MarshalBinary()
	vec.Free(mp)
	return &plan.Expr{
		Typ: makePlan2Type(vec.GetType()),
		Expr: &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  int32(len(vals)),
				Data: data,
			},
		},
	}
}

var MakePlan2Int64VecExprWithType = makePlan2Int64VecExprWithType

func makePlan2Int64VecExprWithType(mp *mpool.MPool, vals ...int64) *plan.Expr {
	vec := vector.NewVec(types.T_int64.ToType())
	for _, val := range vals {
		vector.AppendFixed(vec, val, false, mp)
	}
	data, _ := vec.MarshalBinary()
	vec.Free(mp)
	return &plan.Expr{
		Typ: makePlan2Type(vec.GetType()),
		Expr: &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  int32(len(vals)),
				Data: data,
			},
		},
	}
}

func makePlan2Uint8ConstExpr(v uint8) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_U8Val{
			U8Val: uint32(v),
		},
	}}
}

func makePlan2Uint16ConstExpr(v uint16) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_U16Val{
			U16Val: uint32(v),
		},
	}}
}

func makePlan2Uint32ConstExpr(v uint32) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_U32Val{
			U32Val: v,
		},
	}}
}

func makePlan2Uint64ConstExpr(v uint64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_U64Val{
			U64Val: v,
		},
	}}
}

func makePlan2Uint64ConstExprWithType(v uint64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint64ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_uint64),
			NotNullable: true,
		},
	}
}

func makePlan2Uint8ConstExprWithType(v uint8) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint8ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_uint8),
			NotNullable: true,
		},
	}
}

func makePlan2Uint16ConstExprWithType(v uint16) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint16ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_uint16),
			NotNullable: true,
		},
	}
}

func makePlan2Uint32ConstExprWithType(v uint32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Uint32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_uint32),
			NotNullable: true,
		},
	}
}

func makePlan2Float32ConstExpr(v float32) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Fval{
			Fval: v,
		},
	}}
}

func makePlan2Float64ConstExpr(v float64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Dval{
			Dval: v,
		},
	}}
}

var MakePlan2Float32ConstExprWithType = makePlan2Float32ConstExprWithType
var MakePlan2Float64ConstExprWithType = makePlan2Float64ConstExprWithType

func makePlan2Float64ConstExprWithType(v float64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Float64ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
		},
	}
}

func makePlan2Float32ConstExprWithType(v float32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Float32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_float32),
			NotNullable: true,
		},
	}
}

func makePlan2StringConstExpr(v string, isBin ...bool) *plan.Expr_Lit {
	c := &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Sval{
			Sval: v,
		},
	}}
	if len(isBin) > 0 {
		c.Lit.IsBin = isBin[0]
	}
	return c
}

var MakePlan2StringConstExprWithType = makePlan2StringConstExprWithType

func makePlan2StringConstExprWithType(v string, isBin ...bool) *plan.Expr {
	width := int32(utf8.RuneCountInString(v))
	id := int32(types.T_varchar)
	if width == 0 {
		id = int32(types.T_char)
	}
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, isBin...),
		Typ: plan.Type{
			Id:          id,
			NotNullable: true,
			Width:       width,
		},
	}
}

func makePlan2NullTextConstExpr(v string) *plan.Expr_Lit {
	c := &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: true,
	}}
	return c
}

func MakePlan2NullTextConstExprWithType(v string) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2NullTextConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_text),
			NotNullable: false,
			Width:       int32(utf8.RuneCountInString(v)),
		},
	}
}

func makePlan2CastExpr(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	var err error
	if isSameColumnType(expr.Typ, targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	if types.T(expr.Typ.Id) == types.T_any {
		expr.Typ = targetType
		return expr, nil
	}

	if targetType.Id == int32(types.T_enum) {
		expr, err = funcCastForEnumType(ctx, expr, targetType)
		if err != nil {
			return nil, err
		}
	}

	t1, t2 := makeTypeByPlan2Expr(expr), makeTypeByPlan2Type(targetType)
	fGet, err := function.GetFunctionByName(ctx, "cast", []types.Type{t1, t2})
	if err != nil {
		return nil, err
	}
	t := &plan.Expr{
		Typ: targetType,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{},
		},
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &ObjectRef{Obj: fGet.GetEncodedOverloadID(), ObjName: "cast"},
				Args: []*Expr{expr, t},
			},
		},
		Typ: targetType,
	}, nil
}

func funcCastForEnumType(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	var err error
	if targetType.Id != int32(types.T_enum) {
		return expr, nil
	}

	astArgs := []tree.Expr{
		tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char),
	}

	// bind ast function's args
	args := make([]*Expr, len(astArgs)+1)
	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	for idx, arg := range astArgs {
		if idx == len(args)-1 {
			continue
		}
		expr, err := binder.BindExpr(arg, 0, false)
		if err != nil {
			return nil, err
		}
		args[idx] = expr
	}
	args[len(args)-1] = expr
	if 20 <= expr.Typ.Id && expr.Typ.Id <= 29 {
		expr, err = BindFuncExprImplByPlanExpr(ctx, moEnumCastIndexValueToIndexFun, args)
		if err != nil {
			return nil, err
		}
	} else {
		expr, err = BindFuncExprImplByPlanExpr(ctx, moEnumCastValueToIndexFun, args)
		if err != nil {
			return nil, err
		}
	}
	return expr, nil
}

// if typ is decimal128 and decimal64 without scalar and width
// set a default value for it.
func rewriteDecimalTypeIfNecessary(typ *plan.Type) *plan.Type {
	if typ.Id == int32(types.T_decimal128) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 10
		typ.Width = 38 // width
	}
	if typ.Id == int32(types.T_decimal64) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 2
		typ.Width = 6 // width
	}
	return typ
}

var MakePlan2Type = makePlan2Type

func makeSimplePlan2Type(typT types.T) plan.Type {
	return plan.Type{
		Id:    int32(typT),
		Width: 0,
		Scale: 0,
	}
}

func makePlan2Type(typ *types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}
func makePlan2TypeValue(typ *types.Type) plan.Type {
	return plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

var MakeTypeByPlan2Type = makeTypeByPlan2Type
var MakePlan2TypeValue = makePlan2TypeValue

func makeTypeByPlan2Type(typ plan.Type) types.Type {
	oid := types.T(typ.Id)
	return types.New(oid, typ.Width, typ.Scale)
}

var MakeTypeByPlan2Expr = makeTypeByPlan2Expr

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	oid := types.T(expr.Typ.Id)
	return types.New(oid, expr.Typ.Width, expr.Typ.Scale)
}

func makeHiddenColTyp() Type {
	return Type{
		Id:    int32(types.T_varchar),
		Width: types.MaxVarcharLen,
	}
}

// used for Compound primary key column name && clusterby column name
func MakeHiddenColDefByName(name string) *ColDef {
	return &ColDef{
		Name:   name,
		Hidden: true,
		Typ:    makeHiddenColTyp(),
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
}

func MakeRowIdColDef() *ColDef {
	return &ColDef{
		Name:   catalog.Row_ID,
		Hidden: true,
		Typ: Type{
			Id: int32(types.T_Rowid),
		},
		Default: &plan.Default{
			NullAbility:  false,
			Expr:         nil,
			OriginString: "",
		},
	}
}

func isSameColumnType(t1 Type, t2 Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Scale == t2.Scale {
		return true
	}
	return true
}

// GetColDefFromTable Find the target column definition from the predefined
// table columns and return its deep copy
func GetColDefFromTable(Cols []*ColDef, hidenColName string) *ColDef {
	for _, coldef := range Cols {
		if coldef.Name == hidenColName {
			return DeepCopyColDef(coldef)
		}
	}
	panic("Unable to find target column from predefined table columns")
}
