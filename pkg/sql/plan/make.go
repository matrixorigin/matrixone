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
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	var typ plan.Type
	width := decimalLiteralPrecision(v)
	_, scale, err := types.Parse128(v)
	if err == nil && scale < 18 && len(v) < 18 {
		typ = plan.Type{
			Id:          int32(types.T_decimal64),
			Width:       width,
			Scale:       scale,
			NotNullable: true,
		}
	} else if err == nil {
		typ = plan.Type{
			Id:          int32(types.T_decimal128),
			Width:       width,
			Scale:       scale,
			NotNullable: true,
		}
	} else {
		_, scale, err = types.Parse256(v)
		if err != nil {
			return nil, err
		}
		typ = plan.Type{
			Id:          int32(types.T_decimal256),
			Width:       width,
			Scale:       scale,
			NotNullable: true,
		}
	}
	return appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(v, isBin...), typ)
}

func makePlan2LegacyDecimalExprWithType(ctx context.Context, v string, isBin ...bool) (*plan.Expr, error) {
	var typ plan.Type
	_, scale, err := types.Parse128(v)
	switch {
	case err == nil && scale < 18 && len(v) < 18:
		typ = plan.Type{Id: int32(types.T_decimal64), Width: 18, Scale: scale, NotNullable: true}
	case err == nil:
		typ = plan.Type{Id: int32(types.T_decimal128), Width: 38, Scale: scale, NotNullable: true}
	default:
		_, scale, err = types.Parse256(v)
		if err != nil {
			return nil, err
		}
		typ = plan.Type{Id: int32(types.T_decimal256), Width: 65, Scale: scale, NotNullable: true}
	}
	return appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(v, isBin...), typ)
}

func decimalLiteralPrecision(v string) int32 {
	var width int32
	for i := 0; i < len(v); i++ {
		if v[i] >= '0' && v[i] <= '9' {
			width++
		}
	}
	if width == 0 {
		return 1
	}
	return width
}

// makePlan2ExactDecimalStringExprWithType constructs the narrowest DECIMAL
// that exactly represents a character string's numeric prefix. It is kept
// separate from makePlan2DecimalExprWithType because ordinary unquoted numeric
// literals have longstanding arithmetic type and overflow semantics.
func makePlan2ExactDecimalStringExprWithType(ctx context.Context, v string) (*plan.Expr, bool, error) {
	canonical, width, scale, ok := canonicalExactDecimalString(v)
	if !ok {
		return nil, false, nil
	}

	var oid types.T
	var err error
	switch {
	case width <= types.T_decimal64.ToType().Width:
		oid = types.T_decimal64
		_, parsedScale, parseErr := types.Parse128(canonical)
		err = parseErr
		if err == nil && parsedScale != scale {
			oid = types.T_decimal256
			_, _, err = types.Parse256(canonical)
		}
	case width <= types.T_decimal128.ToType().Width:
		oid = types.T_decimal128
		_, parsedScale, parseErr := types.Parse128(canonical)
		err = parseErr
		if err == nil && parsedScale != scale {
			oid = types.T_decimal256
			_, _, err = types.Parse256(canonical)
		}
	default:
		oid = types.T_decimal256
		_, _, err = types.Parse256(canonical)
	}
	if err != nil {
		return nil, false, nil
	}

	typ := plan.Type{
		Id:          int32(oid),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	}
	expr, err := appendCastBeforeExpr(ctx, makePlan2StringConstExprWithType(canonical), typ)
	return expr, err == nil, err
}

func canonicalExactDecimalString(v string) (string, int32, int32, bool) {
	if v == "" {
		return "", 0, 0, false
	}
	i := 0
	negative := false
	if v[i] == '+' || v[i] == '-' {
		negative = v[i] == '-'
		i++
	}
	if i == len(v) {
		return "", 0, 0, false
	}

	var digits strings.Builder
	integerDigits := int64(0)
	seenDot := false
	seenDigit := false
	nonZero := false
	for i < len(v) && v[i] != 'e' && v[i] != 'E' {
		switch {
		case v[i] >= '0' && v[i] <= '9':
			digits.WriteByte(v[i])
			seenDigit = true
			nonZero = nonZero || v[i] != '0'
			if !seenDot {
				integerDigits++
			}
		case v[i] == '.' && !seenDot:
			seenDot = true
		default:
			return "", 0, 0, false
		}
		i++
	}
	if !seenDigit {
		return "", 0, 0, false
	}
	if !nonZero {
		return "0", 1, 0, true
	}

	var exponent int64
	if i < len(v) {
		i++
		parsed, err := strconv.ParseInt(v[i:], 10, 64)
		if err != nil {
			return "", 0, 0, false
		}
		exponent = parsed
	}

	coefficient := digits.String()
	leading := 0
	for leading < len(coefficient) && coefficient[leading] == '0' {
		leading++
	}
	coefficient = coefficient[leading:]
	point, overflow := safeAddInt64(integerDigits-int64(leading), exponent)
	if overflow {
		return "", 0, 0, false
	}
	for len(coefficient) > 0 && int64(len(coefficient)) > point && coefficient[len(coefficient)-1] == '0' {
		coefficient = coefficient[:len(coefficient)-1]
	}

	maxWidth := int64(types.T_decimal256.ToType().Width)
	if point < -maxWidth || point > maxWidth || int64(len(coefficient)) > maxWidth {
		return "", 0, 0, false
	}
	var width, scale int64
	switch {
	case point <= 0:
		scale, overflow = safeAddInt64(-point, int64(len(coefficient)))
		width = scale
	case point >= int64(len(coefficient)):
		width = point
	default:
		width = int64(len(coefficient))
		scale = int64(len(coefficient)) - point
	}
	if overflow || width <= 0 || width > maxWidth || scale > maxWidth {
		return "", 0, 0, false
	}

	var result strings.Builder
	if negative {
		result.WriteByte('-')
	}
	switch {
	case point <= 0:
		result.WriteString("0.")
		result.WriteString(strings.Repeat("0", int(-point)))
		result.WriteString(coefficient)
	case point >= int64(len(coefficient)):
		result.WriteString(coefficient)
		result.WriteString(strings.Repeat("0", int(point)-len(coefficient)))
	default:
		result.WriteString(coefficient[:point])
		result.WriteByte('.')
		result.WriteString(coefficient[point:])
	}
	return result.String(), int32(width), int32(scale), true
}

func safeAddInt64(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right || right < 0 && left < math.MinInt64-right {
		return 0, true
	}
	return left + right, false
}

func makePlan2DateConstNullExpr(t types.T) *plan.Expr {
	return makePlan2DateConstNullExprWithScale(t, 0)
}

func makePlan2DateConstNullExprWithScale(t types.T, scale int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Isnull: true,
			},
		},
		Typ: plan.Type{
			Id:          int32(t),
			Scale:       scale,
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

var MakePlan2VecBf16ConstExprWithType = makePlan2VecBf16ConstExprWithType

// makePlan2VecBf16ConstExprWithType makes a vecbf16 const expr.
func makePlan2VecBf16ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_bf16),
			Width:       l,
			NotNullable: true,
		},
	}
}

var MakePlan2VecF16ConstExprWithType = makePlan2VecF16ConstExprWithType

// makePlan2VecF16ConstExprWithType makes a vecf16 const expr.
func makePlan2VecF16ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_float16),
			Width:       l,
			NotNullable: true,
		},
	}
}

var MakePlan2VecInt8ConstExprWithType = makePlan2VecInt8ConstExprWithType

// makePlan2VecInt8ConstExprWithType makes a vecint8 const expr.
func makePlan2VecInt8ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_int8),
			Width:       l,
			NotNullable: true,
		},
	}
}

var MakePlan2VecUint8ConstExprWithType = makePlan2VecUint8ConstExprWithType

// makePlan2VecUint8ConstExprWithType makes a vecuint8 const expr.
func makePlan2VecUint8ConstExprWithType(v string, l int32) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Vecf32ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_array_uint8),
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
	charset := uint32(types.CharsetUTF8)
	if len(isBin) > 0 && isBin[0] {
		// Hex and bit literals use a VARCHAR-shaped container for their raw
		// payload, but remain binary strings for comparison and protocol metadata.
		charset = uint32(types.CharsetBinary)
	}
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, isBin...),
		Typ: plan.Type{
			Id:          id,
			Charset:     charset,
			NotNullable: true,
			Width:       width,
		},
	}
}

func makePlan2VarBinaryConstExprWithType(v string) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, false),
		Typ: plan.Type{
			Id:          int32(types.T_varbinary),
			NotNullable: true,
			Width:       int32(len(v)),
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
	return makePlan2CastExprWithName(ctx, expr, targetType, "cast")
}

// MakePlan2CastExpr builds the ordinary SQL CAST used at a prepared result
// boundary. Keeping this separate from parameter binding lets callers preserve
// result metadata without changing how runtime parameters are parsed.
func MakePlan2CastExpr(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	return makePlan2CastExpr(ctx, expr, targetType)
}

// makePlan2AssignmentCastExpr builds a cast used when validating/storing a value
// against a real column type at the DDL layer (e.g. column DEFAULT / ON UPDATE).
// It uses cast_strict for width-constrained strings and temporal zero-date
// preservation. DDL-specific error mapping is applied by the DDL validation
// layer rather than changing cast_strict's execution contract.
func makePlan2AssignmentCastExpr(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	funcName := "cast"
	if useAssignmentStrictCast(targetType) {
		funcName = "cast_strict"
	}
	return makePlan2CastExprWithName(ctx, expr, targetType, funcName)
}

// MakePlan2AssignmentCastExpr coerces an expression using assignment
// semantics. Stored procedure declarations and assignments use the same
// conversion contract as values written to SQL columns.
func MakePlan2AssignmentCastExpr(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	return makePlan2AssignmentCastExpr(ctx, expr, targetType)
}

func makePlan2CastExprWithName(ctx context.Context, expr *Expr, targetType Type, funcName string) (*Expr, error) {
	var err error
	if expr == nil {
		return nil, moerr.NewInvalidInput(ctx, "nil expression in cast")
	}
	var rewritten bool
	expr, rewritten, err = rewriteMySQLSpecialTypeDisplayCast(ctx, expr, targetType)
	if err != nil {
		return nil, err
	}
	if rewritten {
		return expr, nil
	}
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
		if isSameColumnType(expr.Typ, targetType) {
			return expr, nil
		}
	}
	if isSetPlanType(&targetType) {
		expr, err = funcCastForSetType(ctx, expr, targetType)
		if err != nil {
			return nil, err
		}
		if isSameColumnType(expr.Typ, targetType) {
			return expr, nil
		}
	}
	if isGeometryPlanType(&targetType) {
		expr, err = funcCastForGeometryType(ctx, expr, targetType)
		if err != nil {
			return nil, err
		}
		if isSameColumnType(expr.Typ, targetType) {
			return expr, nil
		}
	}
	if isTypedArrayPlanType(&targetType) {
		expr, err = funcCastForTypedArrayType(ctx, expr, targetType)
		if err != nil {
			return nil, err
		}
		if isSameColumnType(expr.Typ, targetType) {
			return expr, nil
		}
	}

	t1, t2 := makeTypeByPlan2Expr(expr), makeTypeByPlan2Type(targetType)
	fGet, err := function.GetFunctionByName(ctx, funcName, []types.Type{t1, t2})
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
				Func: &ObjectRef{Obj: fGet.GetEncodedOverloadID(), ObjName: funcName},
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
	if isEnumPlanType(&expr.Typ) && expr.Typ.Enumvalues == targetType.Enumvalues {
		expr.Typ = targetType
		return expr, nil
	}
	sourceExpr := expr

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
		argExpr, err := binder.BindExpr(arg, 0, false)
		if err != nil {
			return nil, err
		}
		args[idx] = argExpr
	}
	args[len(args)-1] = sourceExpr
	if 20 <= sourceExpr.Typ.Id && sourceExpr.Typ.Id <= 29 {
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
	expr.Typ = targetType
	return expr, nil
}

func funcCastForSetType(ctx context.Context, expr *Expr, targetType Type) (*Expr, error) {
	var err error
	if !isSetPlanType(&targetType) {
		return expr, nil
	}
	if isSetPlanType(&expr.Typ) && expr.Typ.Enumvalues == targetType.Enumvalues {
		expr.Typ = targetType
		return expr, nil
	}
	sourceExpr := expr

	astArgs := []tree.Expr{
		tree.NewNumVal(targetType.Enumvalues, targetType.Enumvalues, false, tree.P_char),
	}

	args := make([]*Expr, len(astArgs)+1)
	binder := NewDefaultBinder(ctx, nil, nil, targetType, nil)
	for idx, arg := range astArgs {
		if idx == len(args)-1 {
			continue
		}
		argExpr, err := binder.BindExpr(arg, 0, false)
		if err != nil {
			return nil, err
		}
		args[idx] = argExpr
	}
	args[len(args)-1] = sourceExpr
	if types.T(sourceExpr.Typ.Id).IsInteger() {
		expr, err = BindFuncExprImplByPlanExpr(ctx, moSetCastIndexValueToIndexFun, args)
		if err != nil {
			return nil, err
		}
	} else {
		expr, err = BindFuncExprImplByPlanExpr(ctx, moSetCastValueToIndexFun, args)
		if err != nil {
			return nil, err
		}
	}
	expr.Typ = targetType
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
	if typ.Id == int32(types.T_decimal256) && typ.Scale == 0 && typ.Width == 0 {
		typ.Scale = 10
		typ.Width = 65 // width
	}
	return typ
}

var MakePlan2Type = makePlan2Type

func makeSimplePlan2Type(typT types.T) plan.Type {
	return plan.Type{
		Id:      int32(typT),
		Width:   0,
		Scale:   0,
		Charset: uint32(types.CharsetType(typT)),
	}
}

// makeGeneratedPlan2Type constructs types for schemas authored by the current
// planner. In particular, new CHAR/VARCHAR/TEXT values must carry an explicit
// charset: zero is reserved for plans and catalog metadata written before
// charset became meaningful. Text-shaped opaque bytes should instead be built
// with makePlan2Type and types.NewWithCharset(..., types.CharsetBinary).
func makeGeneratedPlan2Type(oid types.T, width, scale int32, notNullable bool) plan.Type {
	typ := types.New(oid, width, scale)
	result := makePlan2Type(&typ)
	result.NotNullable = notNullable
	return result
}

func makePlan2Type(typ *types.Type) plan.Type {
	return plan.Type{
		Id:      int32(typ.Oid),
		Width:   typ.Width,
		Scale:   typ.Scale,
		Charset: uint32(typ.Charset),
	}
}
func makePlan2TypeValue(typ *types.Type) plan.Type {
	return plan.Type{
		Id:      int32(typ.Oid),
		Width:   typ.Width,
		Scale:   typ.Scale,
		Charset: uint32(typ.Charset),
	}
}

var MakeTypeByPlan2Type = makeTypeByPlan2Type
var MakePlan2TypeValue = makePlan2TypeValue

func makeTypeByPlan2Type(typ plan.Type) types.Type {
	oid := types.T(typ.Id)
	return types.NewWithCharset(oid, typ.Width, typ.Scale, uint8(typ.Charset))
}

var MakeTypeByPlan2Expr = makeTypeByPlan2Expr

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	oid := types.T(expr.Typ.Id)
	return types.NewWithCharset(oid, expr.Typ.Width, expr.Typ.Scale, uint8(expr.Typ.Charset))
}

func makeHiddenColTyp() Type {
	return Type{
		Id:      int32(types.T_varchar),
		Width:   types.MaxVarcharLen,
		Charset: uint32(types.CharsetBinary),
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
	if t1.Id != t2.Id || t1.Charset != t2.Charset {
		return false
	}
	if t1.Enumvalues != t2.Enumvalues {
		return false
	}
	if t1.Width == t2.Width && t1.Scale == t2.Scale {
		return true
	}
	return false
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
