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
	if name == "cast" && len(fn.Args) > 0 && !isExplicitPreparedCast(expr) {
		// Overload alignment is not a user-declared maximum length.
		if source := regexpDeclaredStringType(fn.Args[0]); source.Oid.IsMySQLString() {
			return source
		}
	}
	if indexes := controlFlowValueIndexes(name, len(fn.Args)); len(indexes) > 0 {
		binary, varchar, unbounded := false, false, false
		var maxBytes uint64
		for _, index := range indexes {
			value := regexpDeclaredStringType(fn.Args[index])
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
	parameters := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		parameters[i] = regexpDeclaredStringType(arg)
	}
	if result, ok := function.GetFunctionByNameWithoutError(name, parameters); ok {
		typ = result.GetReturnType()
	}
	// A constant substring/LEFT/RIGHT length limits an unbounded input too.
	lengthIndex := -1
	switch name {
	case "substring", "substr", "sub_str":
		if len(fn.Args) == 3 {
			lengthIndex = 2
		}
	case "left", "right":
		if len(fn.Args) == 2 {
			lengthIndex = 1
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

// regexpConstantInteger recognizes values, not witness payloads or current
// parameter bindings. CAST preserves integer bits and signedness; unsupported
// conversions stay unknown rather than being evaluated speculatively.
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
		}
		return 0, false, false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil &&
		strings.EqualFold(fn.Func.ObjName, "cast") && len(fn.Args) > 0 {
		value, _, known = regexpConstantInteger(fn.Args[0])
		switch types.T(expr.Typ.Id) {
		case types.T_int64:
			return value, true, known
		case types.T_uint64:
			return value, false, known
		}
	}
	return 0, false, false
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
