// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// regexpDeclaredStringType reconstructs MySQL's declared VARCHAR/STRING/BLOB
// distinction for compatibility only. It never changes execution types or
// evaluates a branch/value. In particular, a string user variable has an
// unbounded declaration even when its current value is short or NULL.
func regexpDeclaredStringType(expr *Expr) types.Type {
	if expr == nil {
		return types.T_any.ToType()
	}
	typ := makeTypeByPlan2Expr(expr)
	if isExplicitPreparedCast(expr) && typ.Oid == types.T_binary {
		if typ.Width >= 0 {
			return regexpBinaryTypeForBound(uint64(typ.Width), true)
		}
		// An unassigned variable is propagated as a zero-bound VARCHAR by
		// MySQL. Its binder TEXT envelope must not become a BLOB declaration.
		if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil &&
			source.GetLit() != nil && source.GetLit().Isnull && types.T(source.Typ.Id) == types.T_binary {
			return regexpBinaryTypeForBound(0, true)
		}
		args := expr.GetF().Args
		if len(args) == 0 {
			return regexpBinaryTypeForBound(0, true)
		}
		if _, marker := preparedParamPosition(args[0]); marker {
			return regexpBinaryTypeForBound(uint64(types.MaxVarBinaryLen), true)
		}
		bound, known := regexpExpressionByteBound(args[0])
		return regexpBinaryTypeForBound(bound, known)
	}
	if lit := expr.GetLit(); lit != nil && lit.Src != nil {
		return regexpDeclaredStringType(lit.Src)
	}
	if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil {
		return regexpDeclaredStringType(source)
	}
	userVariable := expr.GetV() != nil && !expr.GetV().System
	if lit := expr.GetLit(); lit != nil {
		userVariable = userVariable || types.StringSource(lit.StringSource) == types.StringSourceUserVariable
	}
	if userVariable && typ.Oid.IsMySQLString() {
		if types.StaticStringDomain(typ) == types.StringDomainBinary {
			return types.T_blob.ToType()
		}
		result := types.T_text.ToType()
		result.Width, result.Charset = 0, typ.Charset
		return result
	}
	if lit := expr.GetLit(); lit != nil {
		if typ.Oid == types.T_binary &&
			(types.StringSource(lit.StringSource) == types.StringSourceExpression ||
				types.StringSource(lit.StringSource) == types.StringSourceLiteral) {
			if lit.Isnull && typ.Width < 0 {
				return regexpBinaryTypeForBound(0, true)
			}
			bound, known := function.StringResultByteBound(typ)
			return regexpBinaryTypeForBound(bound, known)
		}
		return typ
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !typ.Oid.IsMySQLString() {
		return typ
	}
	name := strings.ToLower(fn.Func.ObjName)
	parameters := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		parameters[i] = regexpDeclaredStringType(arg)
	}
	if name == "cast" && len(fn.Args) > 0 && !isExplicitPreparedCast(expr) {
		// Overload alignment is not a user-declared maximum length.
		if source := parameters[0]; source.Oid.IsMySQLString() || regexpBareNull(fn.Args[0]) {
			return source
		}
	}
	if indexes := controlFlowValueIndexes(name, len(fn.Args)); len(indexes) > 0 {
		binary, varchar, unbounded := false, false, false
		var maxBytes uint64
		for _, index := range indexes {
			value := parameters[index]
			if value.Oid == types.T_any {
				continue // Only bare NULL is domainless, not a typed NULL branch.
			}
			binary = binary || types.StaticStringDomain(value) == types.StringDomainBinary
			varchar = varchar || value.Oid == types.T_varchar || value.Oid == types.T_varbinary
			bound, known := function.StringResultByteBound(value)
			unbounded = unbounded || !known || bound > uint64(types.MaxVarBinaryLen)
			maxBytes = max(maxBytes, bound)
		}
		if binary {
			if unbounded {
				return types.T_blob.ToType()
			}
			if varchar {
				return regexpBinaryTypeForBound(maxBytes, true)
			}
			return types.NewWithCharset(types.T_binary, int32(maxBytes), 0, types.CharsetBinary)
		}
	}
	if (name == "max" || name == "min") && len(parameters) == 1 {
		return parameters[0] // An aggregate retains its input declaration.
	}
	if result, ok := function.GetFunctionByNameWithoutError(name, parameters); ok {
		declared := result.GetReturnType()
		// The binder can refine static literal functions (for example REPEAT).
		// A generic overload must not erase that already proven finite bound.
		oldBound, oldKnown := function.StringResultByteBound(typ)
		newBound, newKnown := function.StringResultByteBound(declared)
		if !oldKnown || (newKnown && newBound < oldBound) || preparedExprStringDomainDependsOnRuntime(expr) {
			typ = declared
		}
	}
	// A constant substring/LEFT/RIGHT length limits an unbounded input too.
	lengthIndex := -1
	switch name {
	case "substring", "substr", "sub_str", "mid":
		if len(fn.Args) == 3 {
			lengthIndex = 2
		} else if len(fn.Args) == 2 {
			if position, signed, known := regexpConstantInteger(fn.Args[1]); known && signed && int64(position) < 0 {
				// A negative start can retain at most its distance from the end.
				// Unsigned subtraction also handles MinInt64 without overflow.
				bound := uint64(0) - position
				if bound <= uint64(types.MaxVarcharLen) {
					if types.StaticStringDomain(typ) == types.StringDomainBinary {
						return regexpBinaryTypeForBound(bound, true)
					}
					return types.NewWithCharset(types.T_varchar, int32(bound), 0, typ.Charset)
				}
			}
		}
	case "left", "right":
		if len(fn.Args) == 2 {
			lengthIndex = 1
		}
	case "lpad", "rpad":
		if len(fn.Args) == 3 {
			lengthIndex = 1
		}
	case "repeat":
		if len(fn.Args) == 2 {
			count, signed, known := regexpConstantInteger(fn.Args[1])
			if known && signed && int64(count) < 0 {
				count = 0
			}
			source := parameters[0]
			if known && count == 0 {
				if types.StaticStringDomain(source) == types.StringDomainBinary {
					return regexpBinaryTypeForBound(0, true)
				}
				return types.NewWithCharset(types.T_varchar, 0, 0, typ.Charset)
			}
			bound, bounded := function.StringResultByteBound(source)
			if known && bounded && (count == 0 || bound <= math.MaxUint64/count) {
				if types.StaticStringDomain(source) == types.StringDomainBinary {
					return regexpBinaryTypeForBound(bound*count, true)
				}
				// Text declaration widths are characters, not formatted bytes.
				if source.Oid == types.T_char || source.Oid == types.T_varchar || source.Oid == types.T_text {
					bound = uint64(source.Width)
				}
				if count == 0 || bound <= uint64(types.MaxVarcharLen)/count {
					return types.NewWithCharset(types.T_varchar, int32(bound*count), 0, typ.Charset)
				}
			}
		}
	}
	if lengthIndex >= 0 {
		if length, signed, known := regexpConstantInteger(fn.Args[lengthIndex]); known {
			if signed && int64(length) < 0 {
				length = 0
			}
			if length <= uint64(types.MaxVarcharLen) {
				if types.StaticStringDomain(typ) == types.StringDomainBinary {
					return regexpBinaryTypeForBound(length, true)
				}
				return types.NewWithCharset(types.T_varchar, int32(length), 0, typ.Charset)
			}
		}
	}
	return typ
}

// regexpBareNull ignores only alignment casts, never explicit typed NULLs.
func regexpBareNull(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Src != nil {
			return regexpBareNull(lit.Src)
		}
		return lit.Isnull && types.T(expr.Typ.Id) == types.T_any
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "cast") &&
		!isExplicitPreparedCast(expr) && len(fn.Args) > 0 {
		return regexpBareNull(fn.Args[0])
	}
	return false
}

// regexpConstantInteger recognizes values, not witness payloads or current
// parameter bindings. Recognized CASTs use SQL integer-prefix conversion and
// decimal rounding; unsupported conversions remain unknown.
func regexpConstantInteger(expr *Expr) (value uint64, signed, known bool) {
	if expr == nil || expr.GetP() != nil || expr.GetV() != nil ||
		expr.GetPreparedNumeric().GetStringDomainSource() != nil {
		return 0, false, false
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Isnull {
			return 0, false, false
		}
		if lit.Src != nil {
			return regexpConstantInteger(lit.Src)
		}
		if source := types.StringSource(lit.StringSource); source != types.StringSourceExpression &&
			source != types.StringSourceLiteral {
			return 0, false, false
		}
		switch number := lit.Value.(type) {
		case *planpb.Literal_I64Val:
			return uint64(number.I64Val), true, true
		case *planpb.Literal_U64Val:
			return number.U64Val, false, true
		case *planpb.Literal_Dval:
			return regexpFloatingInteger(number.Dval)
		case *planpb.Literal_Fval:
			return regexpFloatingInteger(float64(number.Fval))
		case *planpb.Literal_Sval:
			integer, err := function.ParsePreparedStringToInt64(number.Sval)
			return uint64(integer), true, err == nil
		case *planpb.Literal_Decimal64Val:
			decimal := types.Decimal64(number.Decimal64Val.A)
			rounded, err := decimal.Scale(-expr.Typ.Scale)
			return uint64(rounded), true, err == nil
		case *planpb.Literal_Decimal128Val:
			decimal := types.Decimal128{B0_63: uint64(number.Decimal128Val.A), B64_127: uint64(number.Decimal128Val.B)}
			rounded, err := decimal.Scale(-expr.Typ.Scale)
			if err != nil {
				return 0, false, false
			}
			integer, err := strconv.ParseInt(rounded.Format(0), 10, 64)
			return uint64(integer), true, err == nil
		}
		return 0, false, false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.ObjName, "abs") && len(fn.Args) == 1 {
		value, signed, known = regexpConstantInteger(fn.Args[0])
		if known && signed && int64(value) < 0 {
			if int64(value) == math.MinInt64 {
				return 0, false, false // ABS can fail; do not change error timing.
			}
			value = uint64(0) - value
		}
		return value, signed, known
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.ObjName, "cast") && len(fn.Args) > 0 {
		if types.T(expr.Typ.Id).IsDecimal() {
			if literal := fn.Args[0].GetLit(); literal != nil && !literal.Isnull && literal.Src == nil &&
				(types.StringSource(literal.StringSource) == types.StringSourceExpression ||
					types.StringSource(literal.StringSource) == types.StringSourceLiteral) {
				if text, ok := literal.Value.(*planpb.Literal_Sval); ok {
					decimal, err := types.ParseDecimal128(text.Sval, expr.Typ.Width, expr.Typ.Scale)
					if err != nil {
						return 0, false, false
					}
					rounded, err := decimal.Scale(-expr.Typ.Scale)
					if err != nil {
						return 0, false, false
					}
					integer, err := strconv.ParseInt(rounded.Format(0), 10, 64)
					return uint64(integer), true, err == nil
				}
			}
			return 0, false, false
		}
		value, signed, known = regexpConstantInteger(fn.Args[0])
		_, overload := function.DecodeOverloadID(fn.Func.GetObj())
		switch types.T(expr.Typ.Id) {
		case types.T_int64:
			if known && !signed && value > math.MaxInt64 && overload != 1 && !fn.GetSyntaxExplicitCast() {
				// An implicit narrowing cast can fail at execution. It cannot
				// prove a negative/zero declaration length at PREPARE time.
				return 0, false, false
			}
			return value, true, known
		case types.T_uint64:
			return value, false, known
		}
	}
	return 0, false, false
}

func regexpFloatingInteger(value float64) (uint64, bool, bool) {
	rounded := math.RoundToEven(value)
	if math.IsNaN(rounded) || math.IsInf(rounded, 0) || rounded < -math.Exp2(63) || rounded >= math.Exp2(63) {
		return 0, false, false
	}
	return uint64(int64(rounded)), true, true
}

func regexpExpressionByteBound(expr *Expr) (uint64, bool) {
	if expr == nil {
		return 0, true
	}
	if expr.GetV() == nil && expr.GetP() == nil && expr.GetPreparedNumeric().GetStringDomainSource() == nil {
		if lit := expr.GetLit(); lit != nil && lit.Src == nil {
			if types.T(expr.Typ.Id) == types.T_any {
				return 0, true
			}
			if value, ok := lit.Value.(*planpb.Literal_Sval); ok &&
				types.StringSource(lit.StringSource) != types.StringSourceUserVariable {
				return uint64(len(value.Sval)), true
			}
		}
	}
	typ := regexpDeclaredStringType(expr)
	if typ.Oid == types.T_any {
		return 0, true
	}
	return function.StringResultByteBound(typ)
}

func regexpBinaryTypeForBound(bound uint64, known bool) types.Type {
	if !known || bound > uint64(types.MaxVarBinaryLen) {
		return types.T_blob.ToType()
	}
	return types.NewWithCharset(types.T_varbinary, int32(bound), 0, types.CharsetBinary)
}

// regexpOwnsBinaryCast limits compatibility overrides to CAST provenance;
// runtime specialization of an ordinary marker/function remains unchanged.
func regexpOwnsBinaryCast(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if isExplicitPreparedCast(expr) {
		return types.T(expr.Typ.Id) == types.T_binary
	}
	if lit := expr.GetLit(); lit != nil {
		if lit.Src != nil {
			return regexpOwnsBinaryCast(lit.Src)
		}
		source := types.StringSource(lit.StringSource)
		return types.T(expr.Typ.Id) == types.T_binary &&
			(source == types.StringSourceExpression || source == types.StringSourceLiteral)
	}
	if source := expr.GetPreparedNumeric().GetStringDomainSource(); source != nil {
		return regexpOwnsBinaryCast(source)
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if regexpOwnsBinaryCast(arg) {
				return true
			}
		}
	}
	return false
}

func regexpBinaryCastOperand(expr *Expr) bool {
	return expr != nil && regexpOwnsBinaryCast(expr) &&
		regexpDeclaredStringType(expr).Oid == types.T_varbinary
}
