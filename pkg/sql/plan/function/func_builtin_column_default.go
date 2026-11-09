// Copyright 2026 Matrix Origin
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

package function

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Issue #31161: typed COLUMN_DEFAULT rendering and the DEFAULT_GENERATED
// attribute for expression defaults, implemented per
// docs/design/20261105-column-default-metadata-publication.md (revision 6).
//
// mo_show_visible_bin(att_default, 7) classifies a persisted plan.Default:
// expression defaults (functions outside the transparent wrapper set,
// parenthesized or call-syntax origins, column references, variable
// references) publish "DEFAULT_GENERATED"; literal defaults publish "".
// Modes 1 and 4 run the same classification first: expression defaults keep
// their persisted origin text, while literal-classified DECIMAL/BIT defaults
// render the resolved value at the declared type through the executor's own
// assignment-conversion helpers, so the metadata names the inserted value.
//
// The planner-owned recognition shapes (the unsigned-DECIMAL admission
// composition, the planner's isExpressionDefault reference walk) are mirrored
// here by typed shape because this package cannot import pkg/sql/plan.

const defaultGeneratedExtra = "DEFAULT_GENERATED"

// isTransparentCastName mirrors the binder-generated value-cast family. A
// user-written CAST always carries its "cast(" spelling in OriginString, so
// classification rule (c) marks it before any peel runs. The enum/set
// value-to-index wrappers are the same planner-owned conversion machinery
// (funcCastForEnumType/funcCastForSetType): the string-input and
// integer-input spellings wrap only literal defaults, so ENUM/SET literal
// defaults stay literal-classified like MySQL.
func isTransparentCastName(name string) bool {
	switch name {
	case "cast", "cast_assign", "cast_strict", "cast_ignore",
		"cast_value_to_index", "cast_index_value_to_index",
		"cast_set_value_to_index", "cast_set_index_value_to_index":
		return true
	}
	return false
}

// isUnsignedDecimalAdmissionName mirrors pkg/sql/plan's admission entries;
// plan function names are matched case-folded there.
func isUnsignedDecimalAdmissionName(name string) bool {
	switch strings.ToLower(name) {
	case "unsigned_decimal_admission",
		"unsigned_decimal_admission_strict",
		"unsigned_decimal_admission_ignore":
		return true
	}
	return false
}

func isAttributedDecimalDefaultType(t plan.Type) bool {
	return types.T(t.Id).IsDecimal() &&
		t.NumericAttributes&types.NumericAttributeUnsigned != 0
}

// matchAdmissionComposition mirrors pkg/sql/plan's
// matchUnsignedDecimalAssignmentComposition by typed shape: a cast-family
// outer node whose arg0 is an unsigned_decimal_admission[_strict|_ignore]
// node whose arg1 is the Expr_T carrier for the identical attributed type.
// Name-only transparency is deliberately insufficient: a stray or
// user-spelled admission-named node still counts as a function.
func matchAdmissionComposition(expr *plan.Expr) (src *plan.Expr, ok bool) {
	if expr == nil || !isAttributedDecimalDefaultType(expr.Typ) {
		return nil, false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || len(f.Args) != 2 {
		return nil, false
	}
	switch f.Func.ObjName {
	case "cast", "cast_assign", "cast_strict", "cast_ignore":
	default:
		return nil, false
	}
	inner := f.Args[0].GetF()
	if inner == nil || inner.Func == nil || !isUnsignedDecimalAdmissionName(inner.Func.ObjName) {
		return nil, false
	}
	if len(inner.Args) != 2 && len(inner.Args) != 3 {
		return nil, false
	}
	carrier := inner.Args[1]
	if carrier == nil || carrier.GetT() == nil || !isAttributedDecimalDefaultType(carrier.Typ) {
		return nil, false
	}
	if carrier.Typ.Id != expr.Typ.Id || carrier.Typ.Width != expr.Typ.Width ||
		carrier.Typ.Scale != expr.Typ.Scale ||
		carrier.Typ.NumericAttributes != expr.Typ.NumericAttributes {
		return nil, false
	}
	return inner.Args[0], true
}

// defaultExprMarksExpression walks the persisted tree for classification
// rules (a), (d) and (e): any function node outside the transparent wrapper
// set, any Expr_Col row reference or any Expr_V variable reference marks an
// expression default. Transparent wrappers (the admission composition, the
// cast family, the canonical-sign chain) are traversed through.
func defaultExprMarksExpression(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_V:
		return true
	}
	f := expr.GetF()
	if f == nil {
		return false
	}
	if src, ok := matchAdmissionComposition(expr); ok {
		return defaultExprMarksExpression(src)
	}
	if f.Func != nil {
		name := f.Func.ObjName
		transparent := (isTransparentCastName(name) && len(f.Args) >= 1) ||
			((name == "unary_plus" || name == "unary_minus") && len(f.Args) == 1)
		if !transparent {
			return true
		}
	}
	for _, arg := range f.Args {
		if defaultExprMarksExpression(arg) {
			return true
		}
	}
	return false
}

// isSignedNumericLiteralText recognizes a leading-sign numeric literal
// (`-12.34`, `+1.2e0`): the formatter wraps such literals in parentheses, so
// a parenthesized origin with this inner text is a literal, not an
// expression (rule (b)'s exclusion).
func isSignedNumericLiteralText(s string) bool {
	n := len(s)
	if n < 2 || (s[0] != '+' && s[0] != '-') {
		return false
	}
	i := 1
	digits := 0
	for i < n && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < n && s[i] == '.' {
		i++
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		exp := 0
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
			exp++
		}
		if exp == 0 {
			return false
		}
	}
	return i == n
}

// originHasCallSyntax implements classification rule (c): an identifier
// immediately followed by '(' outside quoted spans. Single-quoted spans
// handle doubled single quotes and backslash escapes (the origin formatter's
// WithSingleQuoteString form); double-quoted and backtick spans are skipped
// defensively, so string literals that merely spell a call stay literals.
func originHasCallSyntax(origin string) bool {
	isIdent := func(c byte) bool {
		return c == '_' || c == '$' ||
			(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	}
	i := 0
	for i < len(origin) {
		switch origin[i] {
		case '\'':
			i++
			for i < len(origin) {
				if origin[i] == '\\' && i+1 < len(origin) {
					i += 2
					continue
				}
				if origin[i] == '\'' {
					if i+1 < len(origin) && origin[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '"', '`':
			q := origin[i]
			i++
			for i < len(origin) && origin[i] != q {
				i++
			}
			if i < len(origin) {
				i++
			}
		default:
			if origin[i] == '(' && i > 0 && isIdent(origin[i-1]) {
				return true
			}
			i++
		}
	}
	return false
}

// isExpressionDefault implements classification rules (a)-(e): the tree walk
// covers (a) function nodes outside the transparent wrapper set, (d)
// Expr_Col row references and (e) Expr_V variable references anywhere; the
// origin rules cover folded shapes: (b) a parenthesized origin whose inner
// text is not a signed numeric literal, and (c) user-written call syntax
// scanned outside quoted spans.
func isExpressionDefault(expr *plan.Expr, origin string) bool {
	if defaultExprMarksExpression(expr) {
		return true
	}
	n := len(origin)
	if n >= 2 && origin[0] == '(' && origin[n-1] == ')' &&
		!isSignedNumericLiteralText(origin[1:n-1]) {
		return true
	}
	return originHasCallSyntax(origin)
}

// peelTransparentDefault removes the transparent wrapper set for rendering:
// the admission composition peels to its source, cast-family nodes to their
// input, canonical-sign nodes to their child while recording the sign.
func peelTransparentDefault(expr *plan.Expr) (inner *plan.Expr, sign int) {
	inner = expr
	sign = 1
	for inner != nil {
		if src, ok := matchAdmissionComposition(inner); ok {
			inner = src
			continue
		}
		f := inner.GetF()
		if f == nil || f.Func == nil {
			return inner, sign
		}
		switch {
		case isTransparentCastName(f.Func.ObjName) && len(f.Args) >= 1:
			inner = f.Args[0]
		case f.Func.ObjName == "unary_plus" && len(f.Args) == 1:
			inner = f.Args[0]
		case f.Func.ObjName == "unary_minus" && len(f.Args) == 1:
			sign = -sign
			inner = f.Args[0]
		default:
			return inner, sign
		}
	}
	return inner, sign
}

// renderTypedLiteralDefault renders a literal-classified default at the
// declared column type. ok=false keeps the existing origin rendering, so
// unrecognized future shapes degrade to base behavior.
func renderTypedLiteralDefault(def *plan.Default) (string, bool) {
	root := def.Expr
	if root == nil {
		return "", false
	}
	inner, sign := peelTransparentDefault(root)
	if inner == nil {
		return "", false
	}
	dest := root.Typ
	switch types.T(dest.Id) {
	case types.T_decimal64:
		v, ok := convertLiteralToDecimal64(inner, dest.Width, dest.Scale)
		if !ok {
			return "", false
		}
		if sign < 0 {
			v = v.Minus()
		}
		return v.Format(dest.Scale), true
	case types.T_decimal128:
		v, ok := convertLiteralToDecimal128(inner, dest.Width, dest.Scale)
		if !ok {
			return "", false
		}
		if sign < 0 {
			v = v.Minus()
		}
		return v.Format(dest.Scale), true
	case types.T_decimal256:
		v, ok := convertLiteralToDecimal256(inner, dest.Width, dest.Scale)
		if !ok {
			return "", false
		}
		if sign < 0 {
			v = v.Minus()
		}
		return v.Format(dest.Scale), true
	case types.T_bit:
		return renderBitDefaultLiteral(inner)
	}
	return "", false
}

// convertLiteralToDecimal64 converts the extracted literal the way the
// assignment cast does: an IsBin payload (bit/hex spellings persisted as the
// decoded byte string) decodes as a big-endian base-256 coefficient through
// the binary branch; any other string keeps the single-quantize text
// conversion. types.ParseDecimal64/128/256 is deliberately not used for text:
// it double-rounds tokens the assignment cast rounds once.
func convertLiteralToDecimal64(inner *plan.Expr, w, s int32) (types.Decimal64, bool) {
	if vec := inner.GetVec(); vec != nil {
		return 0, false
	}
	lit := inner.GetLit()
	if lit == nil || lit.Isnull {
		return 0, false
	}
	switch val := lit.Value.(type) {
	case *plan.Literal_Sval:
		if lit.IsBin {
			v, err := types.ParseDecimal64FromByte(val.Sval, w, s)
			return v, err == nil
		}
		v, err := parseDecimal64CastString(val.Sval, w, s)
		return v, err == nil
	case *plan.Literal_Decimal64Val:
		if inner.Typ.Id != int32(types.T_decimal64) {
			return 0, false
		}
		scaled, err := types.Decimal64(val.Decimal64Val.A).Scale(s - inner.Typ.Scale)
		if err != nil {
			return 0, false
		}
		if err := checkIntDecimal64Width(int64(0), scaled, w, s); err != nil {
			return 0, false
		}
		return scaled, true
	case *plan.Literal_Dval:
		v, err := types.Decimal64FromFloat64(val.Dval, w, s)
		return v, err == nil
	case *plan.Literal_Fval:
		v, err := types.Decimal64FromFloat64(float64(val.Fval), w, s)
		return v, err == nil
	case *plan.Literal_Bval:
		if !val.Bval {
			return 0, true
		}
		one, err := types.Decimal64(1).Scale(s)
		if err != nil {
			return 0, false
		}
		if err := checkIntDecimal64Width(int64(1), one, w, s); err != nil {
			return 0, false
		}
		return one, true
	case *plan.Literal_I8Val:
		return signedIntToDecimal64(int64(val.I8Val), w, s)
	case *plan.Literal_I16Val:
		return signedIntToDecimal64(int64(val.I16Val), w, s)
	case *plan.Literal_I32Val:
		return signedIntToDecimal64(int64(val.I32Val), w, s)
	case *plan.Literal_I64Val:
		return signedIntToDecimal64(val.I64Val, w, s)
	case *plan.Literal_U8Val:
		return unsignedIntToDecimal64(uint64(val.U8Val), w, s)
	case *plan.Literal_U16Val:
		return unsignedIntToDecimal64(uint64(val.U16Val), w, s)
	case *plan.Literal_U32Val:
		return unsignedIntToDecimal64(uint64(val.U32Val), w, s)
	case *plan.Literal_U64Val:
		return unsignedIntToDecimal64(val.U64Val, w, s)
	}
	return 0, false
}

// signedIntToDecimal64 mirrors signedToDecimal64: the uint64 round trip
// keeps the two's-complement bit pattern, so negatives convert exactly.
func signedIntToDecimal64(v int64, w, s int32) (types.Decimal64, bool) {
	scaled, err := types.Decimal64(uint64(v)).Scale(s)
	if err != nil {
		return 0, false
	}
	if err := checkIntDecimal64Width(v, scaled, w, s); err != nil {
		return 0, false
	}
	return scaled, true
}

// unsignedIntToDecimal64 mirrors unsignedToDecimal64: a uint64 magnitude
// beyond math.MaxInt64 would wrap negative inside Decimal64 (#30367).
func unsignedIntToDecimal64(v uint64, w, s int32) (types.Decimal64, bool) {
	if v > uint64(math.MaxInt64) {
		return 0, false
	}
	scaled, err := types.Decimal64(v).Scale(s)
	if err != nil {
		return 0, false
	}
	if err := checkIntDecimal64Width(v, scaled, w, s); err != nil {
		return 0, false
	}
	return scaled, true
}

func convertLiteralToDecimal128(inner *plan.Expr, w, s int32) (types.Decimal128, bool) {
	if vec := inner.GetVec(); vec != nil {
		return types.Decimal128{}, false
	}
	lit := inner.GetLit()
	if lit == nil || lit.Isnull {
		return types.Decimal128{}, false
	}
	limit := intDecimal128WidthLimit(w)
	switch val := lit.Value.(type) {
	case *plan.Literal_Sval:
		if lit.IsBin {
			v, err := types.ParseDecimal128FromByte(val.Sval, w, s)
			return v, err == nil
		}
		v, err := parseDecimal128CastString(val.Sval, w, s)
		return v, err == nil
	case *plan.Literal_Decimal128Val:
		if inner.Typ.Id != int32(types.T_decimal128) {
			return types.Decimal128{}, false
		}
		v := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
		scaled, err := v.Scale(s - inner.Typ.Scale)
		if err != nil {
			return types.Decimal128{}, false
		}
		if err := checkIntDecimal128Width(int64(0), scaled, limit, w, s); err != nil {
			return types.Decimal128{}, false
		}
		return scaled, true
	case *plan.Literal_Decimal64Val:
		if inner.Typ.Id != int32(types.T_decimal64) {
			return types.Decimal128{}, false
		}
		// Widen the raw coefficient exactly like the executor's
		// decimal64ToDecimal128 assignment cast, then rescale once.
		v := types.Decimal128FromDecimal64(types.Decimal64(val.Decimal64Val.A), inner.Typ.Scale)
		scaled, err := v.Scale(s - inner.Typ.Scale)
		if err != nil {
			return types.Decimal128{}, false
		}
		if err := checkIntDecimal128Width(int64(0), scaled, limit, w, s); err != nil {
			return types.Decimal128{}, false
		}
		return scaled, true
	case *plan.Literal_Dval:
		v, err := types.Decimal128FromFloat64(val.Dval, w, s)
		return v, err == nil
	case *plan.Literal_Fval:
		v, err := types.Decimal128FromFloat64(float64(val.Fval), w, s)
		return v, err == nil
	case *plan.Literal_Bval:
		if !val.Bval {
			return types.Decimal128{}, true
		}
		one, err := types.Decimal128{B0_63: 1}.Scale(s)
		if err != nil {
			return types.Decimal128{}, false
		}
		if err := checkIntDecimal128Width(int64(1), one, limit, w, s); err != nil {
			return types.Decimal128{}, false
		}
		return one, true
	case *plan.Literal_I8Val:
		return signedIntToDecimal128(int64(val.I8Val), limit, w, s)
	case *plan.Literal_I16Val:
		return signedIntToDecimal128(int64(val.I16Val), limit, w, s)
	case *plan.Literal_I32Val:
		return signedIntToDecimal128(int64(val.I32Val), limit, w, s)
	case *plan.Literal_I64Val:
		return signedIntToDecimal128(val.I64Val, limit, w, s)
	case *plan.Literal_U8Val:
		return unsignedIntToDecimal128(uint64(val.U8Val), limit, w, s)
	case *plan.Literal_U16Val:
		return unsignedIntToDecimal128(uint64(val.U16Val), limit, w, s)
	case *plan.Literal_U32Val:
		return unsignedIntToDecimal128(uint64(val.U32Val), limit, w, s)
	case *plan.Literal_U64Val:
		return unsignedIntToDecimal128(val.U64Val, limit, w, s)
	}
	return types.Decimal128{}, false
}

// signedIntToDecimal128 mirrors signedToDecimal128's sign extension.
func signedIntToDecimal128(v int64, limit types.Decimal128, w, s int32) (types.Decimal128, bool) {
	result := types.Decimal128{B0_63: uint64(v), B64_127: 0}
	if v < 0 {
		result.B64_127 = ^result.B64_127
	}
	scaled, err := result.Scale(s)
	if err != nil {
		return types.Decimal128{}, false
	}
	if err := checkIntDecimal128Width(v, scaled, limit, w, s); err != nil {
		return types.Decimal128{}, false
	}
	return scaled, true
}

func unsignedIntToDecimal128(v uint64, limit types.Decimal128, w, s int32) (types.Decimal128, bool) {
	scaled, err := (types.Decimal128{B0_63: v}).Scale(s)
	if err != nil {
		return types.Decimal128{}, false
	}
	if err := checkIntDecimal128Width(v, scaled, limit, w, s); err != nil {
		return types.Decimal128{}, false
	}
	return scaled, true
}

func convertLiteralToDecimal256(inner *plan.Expr, w, s int32) (types.Decimal256, bool) {
	if vec := inner.GetVec(); vec != nil {
		return decodeDecimal256Vec(vec, w, s)
	}
	lit := inner.GetLit()
	if lit == nil || lit.Isnull {
		return types.Decimal256{}, false
	}
	limit := intDecimal256WidthLimit(w)
	switch val := lit.Value.(type) {
	case *plan.Literal_Sval:
		if lit.IsBin {
			v, err := types.ParseDecimal256FromByte(val.Sval, w, s)
			return v, err == nil
		}
		v, err := parseDecimal256CastString(val.Sval, w, s)
		return v, err == nil
	case *plan.Literal_Decimal64Val:
		if inner.Typ.Id != int32(types.T_decimal64) {
			return types.Decimal256{}, false
		}
		// Widen dec64 -> dec128 -> dec256 with sign extension, matching
		// the executor's assignment-cast chain, then rescale once.
		v := types.Decimal256FromDecimal128(
			types.Decimal128FromDecimal64(types.Decimal64(val.Decimal64Val.A), inner.Typ.Scale))
		scaled, err := v.Scale(s - inner.Typ.Scale)
		if err != nil {
			return types.Decimal256{}, false
		}
		if err := checkIntDecimal256Width(int64(0), scaled, limit, w, s); err != nil {
			return types.Decimal256{}, false
		}
		return scaled, true
	case *plan.Literal_Decimal128Val:
		if inner.Typ.Id != int32(types.T_decimal128) {
			return types.Decimal256{}, false
		}
		// Widen exactly like the executor's decimal128ToDecimal256
		// assignment cast, then rescale once.
		v := types.Decimal256FromDecimal128(
			types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)})
		scaled, err := v.Scale(s - inner.Typ.Scale)
		if err != nil {
			return types.Decimal256{}, false
		}
		if err := checkIntDecimal256Width(int64(0), scaled, limit, w, s); err != nil {
			return types.Decimal256{}, false
		}
		return scaled, true
	case *plan.Literal_Dval:
		v, err := types.Decimal256FromFloat64(val.Dval, w, s)
		return v, err == nil
	case *plan.Literal_Fval:
		v, err := types.Decimal256FromFloat64(float64(val.Fval), w, s)
		return v, err == nil
	case *plan.Literal_Bval:
		if !val.Bval {
			return types.Decimal256{}, true
		}
		one, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: 1}).Scale(s)
		if err != nil {
			return types.Decimal256{}, false
		}
		if err := checkIntDecimal256Width(int64(1), one, limit, w, s); err != nil {
			return types.Decimal256{}, false
		}
		return one, true
	case *plan.Literal_I8Val:
		return signedIntToDecimal256(int64(val.I8Val), limit, w, s)
	case *plan.Literal_I16Val:
		return signedIntToDecimal256(int64(val.I16Val), limit, w, s)
	case *plan.Literal_I32Val:
		return signedIntToDecimal256(int64(val.I32Val), limit, w, s)
	case *plan.Literal_I64Val:
		return signedIntToDecimal256(val.I64Val, limit, w, s)
	case *plan.Literal_U8Val:
		return unsignedIntToDecimal256(uint64(val.U8Val), limit, w, s)
	case *plan.Literal_U16Val:
		return unsignedIntToDecimal256(uint64(val.U16Val), limit, w, s)
	case *plan.Literal_U32Val:
		return unsignedIntToDecimal256(uint64(val.U32Val), limit, w, s)
	case *plan.Literal_U64Val:
		return unsignedIntToDecimal256(val.U64Val, limit, w, s)
	}
	return types.Decimal256{}, false
}

func signedIntToDecimal256(v int64, limit types.Decimal256, w, s int32) (types.Decimal256, bool) {
	d128 := types.Decimal128{B0_63: uint64(v), B64_127: 0}
	if v < 0 {
		d128.B64_127 = ^d128.B64_127
	}
	scaled, err := types.Decimal256FromDecimal128(d128).Scale(s)
	if err != nil {
		return types.Decimal256{}, false
	}
	if err := checkIntDecimal256Width(v, scaled, limit, w, s); err != nil {
		return types.Decimal256{}, false
	}
	return scaled, true
}

func unsignedIntToDecimal256(v uint64, limit types.Decimal256, w, s int32) (types.Decimal256, bool) {
	scaled, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: v}).Scale(s)
	if err != nil {
		return types.Decimal256{}, false
	}
	if err := checkIntDecimal256Width(v, scaled, limit, w, s); err != nil {
		return types.Decimal256{}, false
	}
	return scaled, true
}

// decodeDecimal256Vec pure-decodes a folded decimal256 const vector
// (LiteralVec{Len:1}); it never evaluates expressions.
func decodeDecimal256Vec(lv *plan.LiteralVec, w, s int32) (types.Decimal256, bool) {
	var vec vector.Vector
	if err := vec.UnmarshalBinary(lv.Data); err != nil {
		return types.Decimal256{}, false
	}
	if vec.GetType().Oid != types.T_decimal256 || vec.Length() != 1 ||
		vec.GetNulls().Contains(0) {
		return types.Decimal256{}, false
	}
	value := vector.MustFixedColNoTypeCheck[types.Decimal256](&vec)[0]
	scaled, err := value.Scale(s - vec.GetType().Scale)
	if err != nil {
		return types.Decimal256{}, false
	}
	if err := checkIntDecimal256Width(int64(0), scaled, intDecimal256WidthLimit(w), w, s); err != nil {
		return types.Decimal256{}, false
	}
	return scaled, true
}

// renderBitDefaultLiteral renders a BIT default as b'<base-2 digits>' with
// leading zeros stripped; zero renders b'0'.
func renderBitDefaultLiteral(inner *plan.Expr) (string, bool) {
	lit := inner.GetLit()
	if lit == nil || lit.Isnull {
		return "", false
	}
	var v uint64
	switch val := lit.Value.(type) {
	case *plan.Literal_U64Val:
		v = val.U64Val
	case *plan.Literal_U8Val:
		v = uint64(val.U8Val)
	case *plan.Literal_U16Val:
		v = uint64(val.U16Val)
	case *plan.Literal_U32Val:
		v = uint64(val.U32Val)
	case *plan.Literal_I64Val:
		if val.I64Val < 0 {
			return "", false
		}
		v = uint64(val.I64Val)
	case *plan.Literal_I32Val:
		if val.I32Val < 0 {
			return "", false
		}
		v = uint64(val.I32Val)
	default:
		return "", false
	}
	return "b'" + strconv.FormatUint(v, 2) + "'", true
}
