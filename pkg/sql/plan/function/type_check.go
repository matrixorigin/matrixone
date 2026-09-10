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

package function

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// a fixed type cast rule for
// 1. Plus Sub Multi
// 2. Equal NotEqual
// 3. >= > < <=
// 4. Mod
func fixedTypeCastRule1(s1, s2 types.Type) (bool, types.Type, types.Type) {
	// MySQL evaluates ENUM by its 1-based index in numeric contexts.  Treat it
	// as uint16 while selecting binary arithmetic and numeric comparison rules;
	// the later cast still starts from T_enum and preserves NULL handling.
	if s1.Oid == types.T_enum {
		s1 = types.T_uint16.ToType()
	}
	if s2.Oid == types.T_enum {
		s2 = types.T_uint16.ToType()
	}
	check := fixedBinaryCastRule1[s1.Oid][s2.Oid]
	if check.cast {
		t1, t2 := check.left.ToType(), check.right.ToType()
		// special case: null + type, null + null, type + null
		if s1.Oid == types.T_any && s2.Oid == types.T_any {
			return true, t1, t2
		}
		if s1.Oid == types.T_any {
			return true, s2, s2
		}
		if s2.Oid == types.T_any {
			return true, s1, s1
		}

		// too bad.
		// but how to make sure we can let `time = varchar` always right if we want to convert varchar to be time.
		if t1.Oid.IsDateRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		if t1.Oid.IsArrayRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		SetTargetScaleFromSource(&s1, &t1)
		SetTargetScaleFromSource(&s2, &t2)
		if t1.Oid == t2.Oid && t1.Oid.IsDecimal() &&
			(s1.Oid.IsMySQLString() || s2.Oid.IsMySQLString()) {
			targets := []types.Type{t1, t2}
			setMaxScaleForAll(targets)
			t1, t2 = targets[0], targets[1]
		}

		if (t1.Oid.IsArrayRelate() && t2.IsNumeric()) || (t1.IsNumeric() && t2.Oid.IsArrayRelate()) {
			// Vector <Op> Scalar or
			// Scalar <Op> Vector
			if t1.Oid.IsArrayRelate() {
				switch t1.Oid {
				case types.T_array_float32:
					return true, s1, types.T_float32.ToType()
				case types.T_array_float64:
					return true, s1, types.T_float64.ToType()
				}

			} else {
				switch t1.Oid {
				case types.T_array_float32:
					return true, s2, types.T_float32.ToType()
				case types.T_array_float64:
					return true, s2, types.T_float64.ToType()
				}
			}
		}

		return true, t1, t2
	}
	return false, s1, s2
}

// a fixed type cast rule for
//  1. Div
//  2. IntegerDiv
func fixedTypeCastRule2(s1, s2 types.Type) (bool, types.Type, types.Type) {
	check := fixedBinaryCastRule2[s1.Oid][s2.Oid]
	if check.cast {
		t1, t2 := check.left.ToType(), check.right.ToType()

		// special case: null + type, null + null, type + null
		if s1.Oid == types.T_any && s2.Oid == types.T_any {
			return true, t1, t2
		}
		if s1.Oid == types.T_any {
			return true, s2, s2
		}
		if s2.Oid == types.T_any {
			return true, s1, s1
		}

		if t1.Oid.IsDateRelate() {
			if t1.Oid == t2.Oid {
				if s1.Oid == t1.Oid {
					return true, s1, s1
				} else if s2.Oid == t2.Oid {
					return true, s2, s2
				}
			}
		}

		SetTargetScaleFromSource(&s1, &t1)
		SetTargetScaleFromSource(&s2, &t2)
		if t1.Oid == t2.Oid && t1.Oid.IsDecimal() &&
			(s1.Oid.IsMySQLString() || s2.Oid.IsMySQLString()) {
			targets := []types.Type{t1, t2}
			setMaxScaleForAll(targets)
			t1, t2 = targets[0], targets[1]
		}

		if t1.Oid.IsArrayRelate() && t2.IsNumeric() {
			// Vector / Scalar => Vector
			switch t1.Oid {
			case types.T_array_float32:
				return true, s1, types.T_float32.ToType()
			case types.T_array_float64:
				return true, s1, types.T_float64.ToType()
			}

		}

		return true, t1, t2
	}
	return false, s1, s2
}

type matchCheckStatus int

const (
	matchDirectly matchCheckStatus = 0
	matchByCast   matchCheckStatus = 1
	matchFailed   matchCheckStatus = 2
)

// a fixed type match method.
func fixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return fixedTypeMatchExcept(overloads, inputs, -1)
}

// fixedTypeMatchExcept is the fixed matcher with one overload omitted. Keeping
// the original overload slice avoids planner-time allocations for matchers
// that need to reserve a dedicated string overload.
func fixedTypeMatchExcept(overloads []overload, inputs []types.Type, excluded int) checkResult {
	minIndex := -1
	minCost := math.MaxInt
	for i, ov := range overloads {
		if i == excluded {
			continue
		}
		if len(ov.args) != len(inputs) {
			continue
		}

		sta, cos := tryToMatch(inputs, ov.args)
		if sta == matchFailed {
			continue
		} else if sta == matchDirectly {
			return newCheckResultWithSuccess(i)
		} else {
			if cos < minCost {
				minIndex = i
				minCost = cos
			}
		}
	}
	if minIndex == -1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	castType := make([]types.Type, len(inputs))
	ov := overloads[minIndex]
	for i := range castType {
		if ov.args[i] == inputs[i].Oid {
			castType[i] = inputs[i]
		} else {
			castType[i] = ov.args[i].ToType()
			if ov.args[i] == types.T_varchar && !inputs[i].Oid.IsMySQLString() {
				castType[i] = formattedScalarStringType(inputs[i])
			}
			SetTargetScaleFromSource(&inputs[i], &castType[i])
			if isCollatedTextType(inputs[i].Oid) && isCollatedTextType(castType[i].Oid) {
				// CHAR/VARCHAR/TEXT conversions change the storage shape, not the
				// collation. Retaining it here prevents an implicit overload cast
				// from erasing metadata before a derived-string return callback runs.
				castType[i].Charset = inputs[i].Charset
			}
		}
	}
	return newCheckResultWithCast(minIndex, castType)
}

// binTypeMatch keeps numeric inputs on their existing typed overloads and
// routes MySQL string domains through the prefix-aware string executor. An
// unresolved parameter is cast to VARCHAR so each execution can retain the
// normal NULL and runtime string conversion behavior.
func binTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 1 || len(overloads) == 0 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	stringOverload := -1
	for i, ov := range overloads {
		if len(ov.args) == 1 && ov.args[0] == types.T_varchar {
			if stringOverload == -1 {
				stringOverload = i
			}
		}
	}
	if stringOverload == -1 {
		return fixedTypeMatch(overloads, inputs)
	}
	if inputs[0].Oid == types.T_any {
		return newCheckResultWithCast(stringOverload, []types.Type{types.T_varchar.ToType()})
	}
	if inputs[0].Oid.IsMySQLString() {
		return newCheckResultWithSuccess(stringOverload)
	}

	return fixedTypeMatchExcept(overloads, inputs, stringOverload)
}

// fixedTypeMatchWithBoolNumericCast applies MySQL's numeric-context rule for
// BOOL only to callers that explicitly opt in.  BOOL is intentionally not
// added to fixedCanImplicitCastRule globally: that table is shared by
// unrelated functions where changing overload resolution would be a silent
// compatibility regression (for example string/bit and binary functions).
//
// The matcher first resolves BOOL as INT64 so overload ordering remains the
// same as for an integer literal.  It then returns the selected overload's
// actual target type, forcing a real BOOL cast at execution time.  This keeps
// direct BOOL expressions and prepared parameters on the same path while
// preserving the existing string fallback for functions that do not opt in.
func fixedTypeMatchWithBoolNumericCast(overloads []overload, inputs []types.Type) checkResult {
	hasBool := false
	for _, input := range inputs {
		if input.Oid == types.T_bool {
			hasBool = true
			break
		}
	}
	if !hasBool {
		return fixedTypeMatch(overloads, inputs)
	}

	normalized := append([]types.Type(nil), inputs...)
	for i := range normalized {
		if normalized[i].Oid == types.T_bool {
			normalized[i] = types.T_int64.ToType()
		}
	}

	matched := fixedTypeMatch(overloads, normalized)
	if matched.status != succeedMatched && matched.status != succeedWithCast {
		// Preserve the ordinary checker's behavior when this overload set has a
		// non-numeric BOOL-compatible path.
		return fixedTypeMatch(overloads, inputs)
	}

	selected := overloads[matched.idx]
	for i, input := range inputs {
		if input.Oid != types.T_bool {
			continue
		}
		target := selected.args[i]
		if !target.ToType().IsNumeric() || !IfTypeCastSupported(types.T_bool, target) {
			return fixedTypeMatch(overloads, inputs)
		}
	}

	if matched.status == succeedWithCast {
		finalTypes := append([]types.Type(nil), matched.finalType...)
		for i, input := range inputs {
			if input.Oid == types.T_bool {
				finalTypes[i] = selected.args[i].ToType()
				SetTargetScaleFromSource(&normalized[i], &finalTypes[i])
			}
		}
		return newCheckResultWithCast(matched.idx, finalTypes)
	}

	finalTypes := make([]types.Type, len(inputs))
	for i, input := range inputs {
		finalTypes[i] = input
		if input.Oid == types.T_bool {
			finalTypes[i] = selected.args[i].ToType()
			SetTargetScaleFromSource(&normalized[i], &finalTypes[i])
		}
	}
	return newCheckResultWithCast(matched.idx, finalTypes)
}

// mathStringTypeMatch routes every MySQL character input through the existing
// DOUBLE overload.  The implicit cast is warning-aware and binary-aware, so
// HEX/BIT literals retain their numeric byte semantics and malformed text
// follows the same diagnostics as an ordinary string-to-DOUBLE cast.  Using
// an existing numeric overload also avoids serializing new overload IDs that
// older CNs cannot resolve during rolling upgrades.
func mathStringTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 1 && len(inputs) != 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	// Temporal values must retain the historical planner error instead of
	// becoming a numeric-prefix conversion of their formatted year text.
	switch inputs[0].Oid {
	case types.T_date, types.T_datetime, types.T_timestamp, types.T_time:
		return fixedTypeMatchWithBoolNumericCast(overloads, inputs)
	}
	if inputs[0].Oid.IsMySQLString() {
		normalized := append([]types.Type(nil), inputs...)
		normalized[0] = types.T_float64.ToType()
		matched := fixedTypeMatchWithBoolNumericCast(overloads, normalized)
		if matched.status == succeedMatched || matched.status == succeedWithCast {
			finalTypes := append([]types.Type(nil), matched.finalType...)
			if matched.status == succeedMatched {
				finalTypes = make([]types.Type, len(inputs))
				for i := range inputs {
					finalTypes[i] = normalized[i]
				}
			}
			finalTypes[0] = types.T_float64.ToType()
			return newCheckResultWithCast(matched.idx, finalTypes)
		}
	}

	return fixedTypeMatchWithBoolNumericCast(overloads, inputs)
}

// mathStringTypeMatchKeepBoolStringFallback preserves CEIL/FLOOR's existing
// BOOL-to-VARCHAR overload while routing character arguments through the
// stable DOUBLE cast path.
func mathStringTypeMatchKeepBoolStringFallback(overloads []overload, inputs []types.Type) checkResult {
	for _, input := range inputs {
		if input.Oid == types.T_bool {
			return fixedTypeMatch(overloads, inputs)
		}
	}
	return mathStringTypeMatch(overloads, inputs)
}

// modTypeMatch gives character operands the DOUBLE conversion domain used by
// MySQL math functions.  Keep the existing arithmetic matrix for native
// numeric inputs so its integer, floating-point, and DECIMAL return contracts
// remain unchanged.
func modTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) != 2 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if inputs[0].Oid.IsMySQLString() || inputs[1].Oid.IsMySQLString() ||
		inputs[0].Oid == types.T_any || inputs[1].Oid == types.T_any {
		return newCheckResultWithCast(0, []types.Type{types.T_float64.ToType(), types.T_float64.ToType()})
	}
	has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
	if has {
		if modOperatorSupports(t1, t2) {
			return newCheckResultWithCast(0, []types.Type{t1, t2})
		}
	} else if modOperatorSupports(inputs[0], inputs[1]) {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// stringDomainFixedTypeMatch keeps every MySQL string input in its original
// OID/width/charset while applying the ordinary fixed matcher to control
// arguments. Varlena string executors can consume every string family; casting
// them through VARCHAR would truncate BLOB and erase the binary domain before
// return-type derivation.
func stringDomainFixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return stringDomainFixedTypeMatchIf(overloads, inputs, func(oid types.T) bool { return oid.IsMySQLString() })
}

// sha2TypeMatch defers an unknown hash-length operand to SHA2's string
// overload. A parameter marker is represented as T_any during prepare, but a
// later execution may bind a character value such as "256tail". Resolving it
// to the BIGINT overload at prepare time would perform a strict cast before
// SHA2 can apply MySQL's prefix conversion.
func sha2TypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 2 && inputs[1].Oid == types.T_any {
		for i, ov := range overloads {
			if len(ov.args) == 2 && ov.args[0] == types.T_varchar && ov.args[1] == types.T_varchar {
				return stringDomainMatchSingleOverload(overloads, inputs, i)
			}
		}
	}
	return stringDomainFixedTypeMatch(overloads, inputs)
}

func stringDomainMatchSingleOverload(overloads []overload, inputs []types.Type, index int) checkResult {
	if index < 0 || index >= len(overloads) || len(overloads[index].args) != len(inputs) {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	ov := overloads[index]
	targets := make([]types.Type, len(inputs))
	needsCast := false
	for i, expected := range ov.args {
		if expected.IsMySQLString() && inputs[i].Oid.IsMySQLString() {
			targets[i] = inputs[i]
			continue
		}
		status, _ := tryToMatch([]types.Type{inputs[i]}, []types.T{expected})
		if status == matchFailed {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if status == matchByCast {
			needsCast = true
			targets[i] = expected.ToType()
			if expected == types.T_varchar && !inputs[i].Oid.IsMySQLString() {
				targets[i] = formattedScalarStringType(inputs[i])
			}
			SetTargetScaleFromSource(&inputs[i], &targets[i])
		} else {
			targets[i] = inputs[i]
		}
	}
	if needsCast {
		return newCheckResultWithCast(index, targets)
	}
	return newCheckResultWithSuccess(index)
}

// crc32TypeMatch retains CRC32's historical acceptance of every varlen type
// while extending the function to scalar values through the normal formatted
// string cast. The executor hashes the resulting bytes, so changing the
// matcher must not make JSON/vector inputs (which are also varlen internally)
// stop binding.
func crc32TypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 && (inputs[0].IsVarlen() || inputs[0].Oid == types.T_any) {
		return newCheckResultWithSuccess(0)
	}
	return stringDomainFixedTypeMatch(overloads, inputs)
}

const (
	// RegexpMatchStringOperandCount is the subject-pattern pair that owns
	// matching and any string result domain for every REGEXP function.
	RegexpMatchStringOperandCount = 2
	// RegexpReplaceCompatibilityStringOperandCount additionally includes the
	// replacement in REGEXP_REPLACE's charset compatibility check.
	RegexpReplaceCompatibilityStringOperandCount = 3
)

// regexpStringDomainFixedTypeMatch applies the MySQL REGEXP two-stage
// contract. Statically known binary and nonbinary strings cannot participate
// in one regexp call, while T_any parameter markers and ordinary NULL remain
// unresolved until execution. The normal string-domain matcher then preserves
// every accepted operand instead of erasing its domain through VARCHAR casts.
func regexpStringDomainFixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return regexpStringDomainFixedTypeMatchN(overloads, inputs, RegexpMatchStringOperandCount, nil)
}

func regexpStringDomainTypeMatchWithModes(
	overloads []overload, inputs []types.Type, modes []StringDomainCheckMode,
) checkResult {
	return regexpStringDomainFixedTypeMatchN(overloads, inputs, RegexpMatchStringOperandCount, modes)
}

// regexpReplaceStringDomainFixedTypeMatch includes the replacement string in
// the same compatibility domain as the subject and pattern.
func regexpReplaceStringDomainFixedTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	return regexpStringDomainFixedTypeMatchN(
		overloads, inputs, RegexpReplaceCompatibilityStringOperandCount, nil)
}

func regexpReplaceStringDomainTypeMatchWithModes(
	overloads []overload, inputs []types.Type, modes []StringDomainCheckMode,
) checkResult {
	return regexpStringDomainFixedTypeMatchN(
		overloads, inputs, RegexpReplaceCompatibilityStringOperandCount, modes)
}

func regexpStringDomainFixedTypeMatchN(
	overloads []overload, inputs []types.Type, stringOperands int, modes []StringDomainCheckMode,
) checkResult {
	matched := stringDomainFixedTypeMatch(overloads, inputs)
	if matched.status != succeedMatched && matched.status != succeedWithCast {
		return matched
	}

	var firstText, firstBinaryTrigger types.Type
	hasText, hasBinaryTrigger := false, false
	if stringOperands > len(inputs) {
		stringOperands = len(inputs)
	}
	for i := 0; i < stringOperands; i++ {
		mode := StringDomainCheckKnown
		if i < len(modes) {
			mode = modes[i]
		}
		if mode == StringDomainCheckDeferred || mode == StringDomainCheckDomainless {
			continue
		}
		domain := types.StaticStringDomain(inputs[i])
		if domain == types.StringDomainNone {
			// T_any is the binder-visible representation of both an ordinary
			// untyped NULL and a parameter marker. Non-string scalars are also
			// regexp-compatible through the ordinary string conversion path.
			continue
		}
		switch domain {
		case types.StringDomainText:
			if hasBinaryTrigger {
				return newCheckResultWithCharacterSetMismatch(
					regexpCharsetName(firstBinaryTrigger), regexpCharsetName(inputs[i]))
			}
			if !hasText {
				firstText, hasText = inputs[i], true
			}
		case types.StringDomainBinary:
			// MySQL's is_binary_string() is narrower than its binary-compatible
			// domain: only MYSQL_TYPE_VARCHAR with the binary charset is a 3995
			// trigger. BINARY (MYSQL_TYPE_STRING), BLOB, and direct PARAM_ITEM
			// values remain byte-domain operands without making text peers illegal.
			if mode == StringDomainCheckParamMarker || inputs[i].Oid != types.T_varbinary {
				continue
			}
			if hasText {
				return newCheckResultWithCharacterSetMismatch(
					regexpCharsetName(firstText), regexpCharsetName(inputs[i]))
			}
			if !hasBinaryTrigger {
				firstBinaryTrigger, hasBinaryTrigger = inputs[i], true
			}
		default:
			continue
		}
		if hasText && hasBinaryTrigger {
			return newCheckResultWithCharacterSetMismatch(
				regexpCharsetName(firstText), regexpCharsetName(firstBinaryTrigger))
		}
	}
	return matched
}

func regexpCharsetName(typ types.Type) string {
	if types.StaticStringDomain(typ) == types.StringDomainBinary {
		return "binary"
	}
	switch typ.Charset {
	case types.CharsetUTF8:
		return "utf8mb4_general_ci"
	case types.CharsetUTF8MB4Bin, types.CharsetLegacy:
		return "utf8mb4_bin"
	default:
		return "utf8mb4"
	}
}

func stringDomainFixedTypeMatchIf(overloads []overload, inputs []types.Type, preserve func(types.T) bool) checkResult {
	// Never let an earlier castable overload shadow an exact overload.
	for overloadIndex, ov := range overloads {
		if len(ov.args) != len(inputs) {
			continue
		}
		if status, _ := tryToMatch(inputs, ov.args); status == matchDirectly {
			return newCheckResultWithSuccess(overloadIndex)
		}
	}

	minIndex, minCost := -1, math.MaxInt
	var minTargets []types.Type
	minNeedsCast := false
	for overloadIndex, ov := range overloads {
		if len(ov.args) != len(inputs) {
			continue
		}
		targets := make([]types.Type, len(inputs))
		needsCast, matched, cost := false, true, 0
		for i, expected := range ov.args {
			if expected.IsMySQLString() && preserve(inputs[i].Oid) {
				targets[i] = inputs[i]
				continue
			}
			status, castCost := tryToMatch([]types.Type{inputs[i]}, []types.T{expected})
			if status == matchFailed {
				matched = false
				break
			}
			cost += castCost
			if status == matchByCast {
				needsCast = true
				targets[i] = expected.ToType()
				if expected == types.T_varchar && !inputs[i].Oid.IsMySQLString() {
					targets[i] = formattedScalarStringType(inputs[i])
				}
				SetTargetScaleFromSource(&inputs[i], &targets[i])
			} else {
				targets[i] = inputs[i]
			}
		}
		if matched && cost < minCost {
			minIndex, minCost = overloadIndex, cost
			minTargets, minNeedsCast = targets, needsCast
		}
	}
	if minIndex == -1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if minNeedsCast {
		return newCheckResultWithCast(minIndex, minTargets)
	}
	return newCheckResultWithSuccess(minIndex)
}

func isCollatedTextType(oid types.T) bool {
	return oid == types.T_char || oid == types.T_varchar || oid == types.T_text
}

// a fixed type match method without any type convert. (const null exception)
// if all parameters were `constant null`, match the first one whose number of parameters was same.
func fixedDirectlyTypeMatch(overload []overload, inputs []types.Type) checkResult {
	for i, o := range overload {
		if len(o.args) != len(inputs) {
			continue
		}
		allSame := true
		allNull := true
		for j := range o.args {
			if inputs[j].Oid == types.T_any {
				continue
			}
			allNull = false
			if o.args[j] != inputs[j].Oid {
				allSame = false
				break
			}
		}
		if allSame || allNull {
			return newCheckResultWithSuccess(i)
		}
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func unaryTildeTypeMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		switch inputs[0].Oid {
		case types.T_char, types.T_varchar, types.T_text:
			for i, overload := range overloads {
				if len(overload.args) == 1 && overload.args[0] == types.T_int64 {
					return newCheckResultWithCast(i, []types.Type{types.T_int64.ToType()})
				}
			}
		}
	}
	return fixedDirectlyTypeMatch(overloads, inputs)
}

// return whether `from` can match `to` implicitly, and match cost.
func tryToMatch(from []types.Type, to []types.T) (sta matchCheckStatus, cost int) {
	if len(from) != len(to) {
		return matchFailed, -1
	}
	l := len(from)
	for i, source := range from {
		if source.Oid == to[i] {
			cost += 0
		} else {
			can, c := fixedImplicitTypeCast(source, to[i])
			if !can {
				return matchFailed, -1
			}
			if c == 1 {
				cost += c
			} else {
				cost += c * l
			}
		}
	}
	if cost == 0 {
		return matchDirectly, cost
	}
	return matchByCast, cost
}

// a fixed type implicit cast rule
// return can cast or not and cast cost.
func fixedImplicitTypeCast(from types.Type, to types.T) (canCast bool, cost int) {
	if from.Oid == types.T_any {
		return true, 1
	}
	rule := fixedCanImplicitCastRule[from.Oid].toList[to]
	return rule.canCast, rule.preferLevel
}

// a fixed type check method for Agg(only one column).
// do not do any implicit type conversion.
func fixedUnaryAggTypeCheck(inputs []types.Type, supported []types.T) checkResult {
	if len(inputs) == 1 && len(supported) > 0 {
		t := inputs[0]
		// if select agg(null), just match the first one.
		if t.Oid == types.T_any {
			return newCheckResultWithCast(0, []types.Type{supported[0].ToType()})
		}
		for _, supportT := range supported {
			if t.Oid == supportT {
				return newCheckResultWithSuccess(0)
			}
		}
	}
	return newCheckResultWithFailure(failedAggParametersWrong)
}

var fixedBinaryCastRule1 [300][300]tarTypes
var fixedBinaryCastRule2 [300][300]tarTypes
var fixedCanImplicitCastRule [300]implicitTypeCastRule

func SetTargetScaleFromSource(source, target *types.Type) {
	if source.Oid == target.Oid {
		target.Width = source.Width
		target.Scale = source.Scale
		return
	}

	if target.IsFloat() {
		if source.IsInt() || source.IsUInt() {
			target.Scale = 0
		} else if source.IsFloat() || source.IsDecimal() {
			if target.Oid == types.T_float32 {
				// significant figures of float32 are 7
				target.Scale = int32(math.Min(float64(source.Scale), 7))
			} else if target.Oid == types.T_float64 {
				// significant figures of float64 are 16
				target.Scale = int32(math.Min(float64(source.Scale), 16))
			}
		} else {
			target.Scale = -1
		}
	}

	if target.Oid == types.T_datetime {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_time {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_timestamp {
		if source.Oid.IsMySQLString() {
			target.Scale = 6
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_decimal64 {
		if source.Oid == types.T_decimal64 {
			target.Scale = source.Scale
		} else if source.Oid.IsInteger() {
			target.Scale = 0
		} else if source.Oid.IsFloat() {
			// When converting float to decimal64, use a reasonable scale
			// float32 has ~7 significant digits, float64 has ~16 significant digits
			if source.Oid == types.T_float32 {
				target.Scale = 7
			} else if source.Oid == types.T_float64 {
				target.Scale = 16
			}
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_decimal128 {
		if source.Oid == types.T_decimal64 || source.Oid == types.T_decimal128 {
			target.Scale = source.Scale
		} else if source.Oid.IsInteger() {
			target.Scale = 0
		} else if source.Oid.IsFloat() {
			// When converting float to decimal128, use a reasonable scale
			// float32 has ~7 significant digits, float64 has ~16 significant digits
			if source.Oid == types.T_float32 {
				target.Scale = 7
			} else if source.Oid == types.T_float64 {
				target.Scale = 16
			}
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}

	if target.Oid == types.T_decimal256 {
		if source.Oid == types.T_decimal64 || source.Oid == types.T_decimal128 || source.Oid == types.T_decimal256 {
			target.Scale = source.Scale
		} else if source.Oid.IsInteger() {
			target.Scale = 0
		} else if source.Oid.IsFloat() {
			if source.Oid == types.T_float32 {
				target.Scale = 7
			} else if source.Oid == types.T_float64 {
				target.Scale = 16
			}
		} else if source.Oid.IsDateRelate() {
			target.Scale = source.Scale
		}
		return
	}
}

func setMaxScaleFromSource(t *types.Type, source []types.Type) {
	for i := range source {
		if source[i].Oid == t.Oid {
			if source[i].Scale > t.Scale {
				t.Scale = source[i].Scale
			}
		}
	}
}

func setSafeDecimalWidthAndScaleFromSource(t *types.Type, source []types.Type) bool {
	if !t.Oid.IsDecimal() {
		return true
	}
	hasDecimalOrInteger := false
	maxScale := int32(0)
	maxIntegralWidth := int32(0)

	for i := range source {
		if source[i].Oid.IsDecimal() {
			if !hasDecimalOrInteger {
				maxScale = source[i].Scale
				maxIntegralWidth = source[i].Width - source[i].Scale
				if maxIntegralWidth < 0 {
					maxIntegralWidth = 0
				}
				hasDecimalOrInteger = true
			}
			if source[i].Scale > maxScale {
				maxScale = source[i].Scale
			}
			integralWidth := source[i].Width - source[i].Scale
			if integralWidth > maxIntegralWidth {
				maxIntegralWidth = integralWidth
			}
			continue
		}

		if source[i].IsIntOrUint() {
			hasDecimalOrInteger = true
			if integralWidth := integerIntegralWidth(source[i].Oid); integralWidth > maxIntegralWidth {
				maxIntegralWidth = integralWidth
			}
		}
	}
	if !hasDecimalOrInteger {
		return true
	}

	requiredWidth := maxIntegralWidth + maxScale
	oid, ok := decimalTypeForRequiredWidth(t.Oid, requiredWidth)
	if !ok {
		return false
	}
	t.Oid = oid
	t.Size = int32(t.Oid.TypeLen())
	t.Scale = maxScale
	t.Width = requiredWidth
	return true
}

func decimalTypeForRequiredWidth(oid types.T, requiredWidth int32) (types.T, bool) {
	switch oid {
	case types.T_decimal64:
		if requiredWidth <= types.T_decimal64.ToType().Width {
			return types.T_decimal64, true
		}
		if requiredWidth <= types.T_decimal128.ToType().Width {
			return types.T_decimal128, true
		}
		if requiredWidth <= types.T_decimal256.ToType().Width {
			return types.T_decimal256, true
		}
		return oid, false
	case types.T_decimal128:
		if requiredWidth <= types.T_decimal128.ToType().Width {
			return types.T_decimal128, true
		}
		if requiredWidth <= types.T_decimal256.ToType().Width {
			return types.T_decimal256, true
		}
		return oid, false
	case types.T_decimal256:
		return types.T_decimal256, requiredWidth <= types.T_decimal256.ToType().Width
	default:
		return oid, true
	}
}

func integerIntegralWidth(oid types.T) int32 {
	switch oid {
	case types.T_int8:
		return 3
	case types.T_uint8:
		return 3
	case types.T_int16:
		return 5
	case types.T_uint16:
		return 5
	case types.T_int32:
		return 10
	case types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	case types.T_uint64:
		return 20
	default:
		return 0
	}
}

func setMaxWidthFromSource(t *types.Type, source []types.Type) {
	t.Width = -1
	for i := range source {
		if source[i].Oid == t.Oid || source[i].Oid.IsMySQLString() {
			if source[i].Width > t.Width {
				t.Width = source[i].Width
			}
		}
	}
	if t.Width == -1 {
		t.Width = types.MaxVarBinaryLen
	}
}

func setMaxScaleForAll(source []types.Type) {
	maxScale := int32(math.MinInt32)
	maxWidth := int32(math.MinInt32)
	for _, t := range source {
		if t.Scale > maxScale {
			maxScale = t.Scale
		}
		if t.Width > maxWidth {
			maxWidth = t.Width
		}
	}
	for k := range source {
		source[k].Scale = maxScale
		source[k].Width = maxWidth
	}
}

func initFixed1() {
	// cast [0] + [1] ==> [2] + [3]
	ru := [][4]types.T{
		{types.T_any, types.T_any, types.T_int64, types.T_int64},
		{types.T_any, types.T_bool, types.T_bool, types.T_bool},
		{types.T_any, types.T_int8, types.T_int8, types.T_int8},
		{types.T_any, types.T_int16, types.T_int16, types.T_int16},
		{types.T_any, types.T_int32, types.T_int32, types.T_int32},
		{types.T_any, types.T_int64, types.T_int64, types.T_int64},
		{types.T_any, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_any, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_any, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_any, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_any, types.T_float32, types.T_float32, types.T_float32},
		{types.T_any, types.T_float64, types.T_float64, types.T_float64},
		{types.T_any, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_any, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_any, types.T_date, types.T_date, types.T_date},
		{types.T_any, types.T_time, types.T_time, types.T_time},
		{types.T_any, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_any, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_any, types.T_year, types.T_year, types.T_year},
		{types.T_any, types.T_char, types.T_char, types.T_char},
		{types.T_any, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_any, types.T_binary, types.T_binary, types.T_binary},
		{types.T_any, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_any, types.T_blob, types.T_blob, types.T_blob},
		{types.T_any, types.T_text, types.T_text, types.T_text},
		{types.T_any, types.T_json, types.T_json, types.T_json},
		{types.T_bool, types.T_any, types.T_bool, types.T_bool},
		{types.T_bool, types.T_int8, types.T_int8, types.T_int8},
		{types.T_bool, types.T_int16, types.T_int16, types.T_int16},
		{types.T_bool, types.T_int32, types.T_int32, types.T_int32},
		{types.T_bool, types.T_int64, types.T_int64, types.T_int64},
		{types.T_bool, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_bool, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_bool, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_bool, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_bool, types.T_char, types.T_bool, types.T_bool},
		{types.T_bool, types.T_varchar, types.T_bool, types.T_bool},
		{types.T_bool, types.T_binary, types.T_bool, types.T_bool},
		{types.T_bool, types.T_varbinary, types.T_bool, types.T_bool},
		{types.T_bool, types.T_blob, types.T_bool, types.T_bool},
		{types.T_bool, types.T_text, types.T_bool, types.T_bool},
		{types.T_bool, types.T_json, types.T_bool, types.T_bool},
		{types.T_int8, types.T_any, types.T_int8, types.T_int8},
		{types.T_int8, types.T_bool, types.T_int8, types.T_int8},
		{types.T_int8, types.T_int16, types.T_int16, types.T_int16},
		{types.T_int8, types.T_int32, types.T_int32, types.T_int32},
		{types.T_int8, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int8, types.T_uint8, types.T_int16, types.T_int16},
		{types.T_int8, types.T_uint16, types.T_int32, types.T_int32},
		{types.T_int8, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int8, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_date, types.T_int64, types.T_int64},
		{types.T_int8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_char, types.T_int8, types.T_int8},
		{types.T_int8, types.T_varchar, types.T_int8, types.T_int8},
		{types.T_int8, types.T_binary, types.T_int8, types.T_int8},
		{types.T_int8, types.T_varbinary, types.T_int8, types.T_int8},
		{types.T_int8, types.T_blob, types.T_int8, types.T_int8},
		{types.T_int8, types.T_text, types.T_int8, types.T_int8},
		{types.T_int16, types.T_any, types.T_int16, types.T_int16},
		{types.T_int16, types.T_bool, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int8, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int32, types.T_int32, types.T_int32},
		{types.T_int16, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int16, types.T_uint8, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint16, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint32, types.T_int32, types.T_int32},
		{types.T_int16, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_date, types.T_int64, types.T_int64},
		{types.T_int16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_char, types.T_int16, types.T_int16},
		{types.T_int16, types.T_varchar, types.T_int16, types.T_int16},
		{types.T_int16, types.T_binary, types.T_int16, types.T_int16},
		{types.T_int16, types.T_varbinary, types.T_int16, types.T_int16},
		{types.T_int16, types.T_blob, types.T_int16, types.T_int16},
		{types.T_int16, types.T_text, types.T_int16, types.T_int16},
		{types.T_int32, types.T_any, types.T_int32, types.T_int32},
		{types.T_int32, types.T_bool, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int8, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int16, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int64, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int32, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_date, types.T_int64, types.T_int64},
		{types.T_int32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_char, types.T_int32, types.T_int32},
		{types.T_int32, types.T_varchar, types.T_int32, types.T_int32},
		{types.T_int32, types.T_binary, types.T_int32, types.T_int32},
		{types.T_int32, types.T_varbinary, types.T_int32, types.T_int32},
		{types.T_int32, types.T_blob, types.T_int32, types.T_int32},
		{types.T_int32, types.T_text, types.T_int32, types.T_int32},
		{types.T_int64, types.T_any, types.T_int64, types.T_int64},
		{types.T_int64, types.T_bool, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_date, types.T_int64, types.T_int64},
		{types.T_int64, types.T_year, types.T_int64, types.T_int64},
		{types.T_int64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_char, types.T_int64, types.T_int64},
		{types.T_int64, types.T_varchar, types.T_int64, types.T_int64},
		{types.T_int64, types.T_binary, types.T_int64, types.T_int64},
		{types.T_int64, types.T_varbinary, types.T_int64, types.T_int64},
		{types.T_int64, types.T_blob, types.T_int64, types.T_int64},
		{types.T_int64, types.T_text, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_any, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_bool, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint8, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint8, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_uint8, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_uint8, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_char, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_varchar, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_binary, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_varbinary, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_blob, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_text, types.T_uint8, types.T_uint8},
		{types.T_uint16, types.T_any, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_bool, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint16, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint16, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_uint8, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_uint16, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_char, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_varchar, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_binary, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_varbinary, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_blob, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_text, types.T_uint16, types.T_uint16},
		{types.T_uint32, types.T_any, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_bool, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_int8, types.T_int16, types.T_int16},
		{types.T_uint32, types.T_int16, types.T_int32, types.T_int32},
		{types.T_uint32, types.T_int32, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_int64, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_uint8, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_uint16, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_uint32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_char, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_varchar, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_binary, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_varbinary, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_blob, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_text, types.T_uint32, types.T_uint32},
		{types.T_uint64, types.T_any, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_bool, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int8, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int16, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int32, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_uint8, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_uint16, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_uint32, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_char, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_varchar, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_binary, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_varbinary, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_blob, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_text, types.T_uint64, types.T_uint64},
		{types.T_float32, types.T_any, types.T_float32, types.T_float32},
		{types.T_float32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_float64, types.T_float64, types.T_float64},
		// Balanced approach: float32 + decimal → float64 for all operations
		// Rationale: float32 has only 7 decimal digits precision, insufficient for large decimal values
		// float64 provides 15-16 digits precision, balancing performance and accuracy
		// This applies to comparison, arithmetic, and multiplication operations
		{types.T_float32, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float32, types.T_char, types.T_float32, types.T_float32},
		{types.T_float32, types.T_varchar, types.T_float32, types.T_float32},
		{types.T_float32, types.T_binary, types.T_float32, types.T_float32},
		{types.T_float32, types.T_varbinary, types.T_float32, types.T_float32},
		{types.T_float32, types.T_blob, types.T_float32, types.T_float32},
		{types.T_float32, types.T_text, types.T_float32, types.T_float32},
		{types.T_float64, types.T_any, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float64, types.T_char, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		// Symmetric rule: decimal64 vs float32 → convert to float64 (see comment above for rationale)
		{types.T_decimal64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal64, types.T_date, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_any, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint64, types.T_decimal128, types.T_decimal128},
		// Symmetric rule: decimal128 vs float32 → convert to float64 (see comment above for rationale)
		{types.T_decimal128, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal128, types.T_date, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_time, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_any, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int8, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int16, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int32, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint8, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint16, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint32, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_decimal64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_decimal128, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_date, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_time, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_datetime, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_timestamp, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_char, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_varchar, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_binary, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_varbinary, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_blob, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_text, types.T_decimal256, types.T_decimal256},
		{types.T_date, types.T_any, types.T_date, types.T_date},
		{types.T_date, types.T_int8, types.T_int64, types.T_int64},
		{types.T_date, types.T_int16, types.T_int64, types.T_int64},
		{types.T_date, types.T_int32, types.T_int64, types.T_int64},
		{types.T_date, types.T_int64, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_date, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_date, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_date, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_date, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_date, types.T_char, types.T_date, types.T_date},
		{types.T_date, types.T_varchar, types.T_date, types.T_date},
		{types.T_date, types.T_binary, types.T_date, types.T_date},
		{types.T_date, types.T_varbinary, types.T_date, types.T_date},
		{types.T_date, types.T_blob, types.T_date, types.T_date},
		{types.T_date, types.T_text, types.T_date, types.T_date},
		{types.T_time, types.T_any, types.T_time, types.T_time},
		{types.T_time, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_time, types.T_char, types.T_time, types.T_time},
		{types.T_time, types.T_varchar, types.T_time, types.T_time},
		{types.T_time, types.T_binary, types.T_time, types.T_time},
		{types.T_time, types.T_varbinary, types.T_time, types.T_time},
		{types.T_time, types.T_blob, types.T_time, types.T_time},
		{types.T_time, types.T_text, types.T_time, types.T_time},
		{types.T_datetime, types.T_any, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_date, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_datetime, types.T_char, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_varchar, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_binary, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_varbinary, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_blob, types.T_datetime, types.T_datetime},
		{types.T_datetime, types.T_text, types.T_datetime, types.T_datetime},
		{types.T_timestamp, types.T_any, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_date, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_datetime, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_char, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_varchar, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_binary, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_varbinary, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_blob, types.T_timestamp, types.T_timestamp},
		{types.T_timestamp, types.T_text, types.T_timestamp, types.T_timestamp},
		{types.T_year, types.T_any, types.T_year, types.T_year},
		{types.T_year, types.T_year, types.T_year, types.T_year},
		{types.T_year, types.T_int8, types.T_int64, types.T_int64},
		{types.T_year, types.T_int16, types.T_int64, types.T_int64},
		{types.T_year, types.T_int32, types.T_int64, types.T_int64},
		{types.T_year, types.T_int64, types.T_int64, types.T_int64},
		{types.T_year, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_year, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_year, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_year, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_year, types.T_float32, types.T_float64, types.T_float64},
		{types.T_year, types.T_float64, types.T_float64, types.T_float64},
		{types.T_year, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_year, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_year, types.T_char, types.T_year, types.T_year},
		{types.T_year, types.T_varchar, types.T_year, types.T_year},
		{types.T_year, types.T_binary, types.T_year, types.T_year},
		{types.T_year, types.T_varbinary, types.T_year, types.T_year},
		{types.T_year, types.T_blob, types.T_year, types.T_year},
		{types.T_year, types.T_text, types.T_year, types.T_year},
		{types.T_char, types.T_any, types.T_char, types.T_char},
		{types.T_char, types.T_bool, types.T_bool, types.T_bool},
		{types.T_char, types.T_int8, types.T_int8, types.T_int8},
		{types.T_char, types.T_int16, types.T_int16, types.T_int16},
		{types.T_char, types.T_int32, types.T_int32, types.T_int32},
		{types.T_char, types.T_int64, types.T_int64, types.T_int64},
		{types.T_char, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_char, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_char, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_char, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_char, types.T_float32, types.T_float32, types.T_float32},
		{types.T_char, types.T_float64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_char, types.T_date, types.T_date, types.T_date},
		{types.T_char, types.T_time, types.T_time, types.T_time},
		{types.T_char, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_char, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_char, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_char, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_char, types.T_binary, types.T_char, types.T_char},
		{types.T_char, types.T_varbinary, types.T_char, types.T_char},
		{types.T_char, types.T_blob, types.T_char, types.T_char},
		{types.T_char, types.T_text, types.T_char, types.T_char},
		{types.T_varchar, types.T_any, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_bool, types.T_bool, types.T_bool},
		{types.T_varchar, types.T_int8, types.T_int8, types.T_int8},
		{types.T_varchar, types.T_int16, types.T_int16, types.T_int16},
		{types.T_varchar, types.T_int32, types.T_int32, types.T_int32},
		{types.T_varchar, types.T_int64, types.T_int64, types.T_int64},
		{types.T_varchar, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_varchar, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_varchar, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_varchar, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_varchar, types.T_float32, types.T_float32, types.T_float32},
		{types.T_varchar, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_varchar, types.T_date, types.T_date, types.T_date},
		{types.T_varchar, types.T_time, types.T_time, types.T_time},
		{types.T_varchar, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_varchar, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_varchar, types.T_char, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_varchar, types.T_binary, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_varbinary, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_blob, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_text, types.T_varchar, types.T_varchar},
		{types.T_varchar, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_varchar, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_varchar, types.T_array_bf16, types.T_array_bf16, types.T_array_bf16},
		{types.T_varchar, types.T_array_float16, types.T_array_float16, types.T_array_float16},
		{types.T_varchar, types.T_array_int8, types.T_array_int8, types.T_array_int8},
		{types.T_varchar, types.T_array_uint8, types.T_array_uint8, types.T_array_uint8},
		{types.T_json, types.T_any, types.T_json, types.T_json},
		{types.T_json, types.T_bool, types.T_bool, types.T_bool},
		{types.T_json, types.T_int8, types.T_int8, types.T_int8},
		{types.T_json, types.T_int16, types.T_int16, types.T_int16},
		{types.T_json, types.T_int32, types.T_int32, types.T_int32},
		{types.T_json, types.T_int64, types.T_int64, types.T_int64},
		{types.T_json, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_json, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_json, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_json, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_json, types.T_float32, types.T_float32, types.T_float32},
		{types.T_json, types.T_float64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_json, types.T_date, types.T_date, types.T_date},
		{types.T_json, types.T_time, types.T_time, types.T_time},
		{types.T_json, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_json, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_json, types.T_char, types.T_varchar, types.T_varchar},
		{types.T_json, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_json, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_json, types.T_binary, types.T_json, types.T_json},
		{types.T_json, types.T_varbinary, types.T_json, types.T_json},
		{types.T_json, types.T_blob, types.T_varchar, types.T_varchar},
		{types.T_json, types.T_text, types.T_varchar, types.T_varchar},
		{types.T_uuid, types.T_char, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_varchar, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_binary, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_varbinary, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_blob, types.T_uuid, types.T_uuid},
		{types.T_uuid, types.T_text, types.T_uuid, types.T_uuid},
		{types.T_binary, types.T_any, types.T_binary, types.T_binary},
		{types.T_binary, types.T_bool, types.T_bool, types.T_bool},
		{types.T_binary, types.T_int8, types.T_int8, types.T_int8},
		{types.T_binary, types.T_int16, types.T_int16, types.T_int16},
		{types.T_binary, types.T_int32, types.T_int32, types.T_int32},
		{types.T_binary, types.T_int64, types.T_int64, types.T_int64},
		{types.T_binary, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_binary, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_binary, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_binary, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_binary, types.T_float32, types.T_float32, types.T_float32},
		{types.T_binary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_binary, types.T_date, types.T_date, types.T_date},
		{types.T_binary, types.T_time, types.T_time, types.T_time},
		{types.T_binary, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_binary, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_binary, types.T_char, types.T_char, types.T_char},
		{types.T_binary, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_binary, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_binary, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_binary, types.T_blob, types.T_blob, types.T_blob},
		{types.T_binary, types.T_text, types.T_blob, types.T_blob},
		{types.T_varbinary, types.T_any, types.T_varbinary, types.T_varbinary},
		{types.T_varbinary, types.T_bool, types.T_bool, types.T_bool},
		{types.T_varbinary, types.T_int8, types.T_int8, types.T_int8},
		{types.T_varbinary, types.T_int16, types.T_int16, types.T_int16},
		{types.T_varbinary, types.T_int32, types.T_int32, types.T_int32},
		{types.T_varbinary, types.T_int64, types.T_int64, types.T_int64},
		{types.T_varbinary, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_varbinary, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_varbinary, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_varbinary, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_varbinary, types.T_float32, types.T_float32, types.T_float32},
		{types.T_varbinary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_varbinary, types.T_date, types.T_date, types.T_date},
		{types.T_varbinary, types.T_time, types.T_time, types.T_time},
		{types.T_varbinary, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_varbinary, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_varbinary, types.T_char, types.T_char, types.T_char},
		{types.T_varbinary, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_varbinary, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_varbinary, types.T_binary, types.T_varbinary, types.T_varbinary},
		{types.T_varbinary, types.T_blob, types.T_varbinary, types.T_varbinary},
		{types.T_varbinary, types.T_text, types.T_varbinary, types.T_varbinary},
		{types.T_blob, types.T_any, types.T_blob, types.T_blob},
		{types.T_blob, types.T_bool, types.T_bool, types.T_bool},
		{types.T_blob, types.T_int8, types.T_int8, types.T_int8},
		{types.T_blob, types.T_int16, types.T_int16, types.T_int16},
		{types.T_blob, types.T_int32, types.T_int32, types.T_int32},
		{types.T_blob, types.T_int64, types.T_int64, types.T_int64},
		{types.T_blob, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_blob, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_blob, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_blob, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_blob, types.T_float32, types.T_float32, types.T_float32},
		{types.T_blob, types.T_float64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_blob, types.T_date, types.T_date, types.T_date},
		{types.T_blob, types.T_time, types.T_time, types.T_time},
		{types.T_blob, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_blob, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_blob, types.T_char, types.T_char, types.T_char},
		{types.T_blob, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_blob, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_blob, types.T_binary, types.T_blob, types.T_blob},
		{types.T_blob, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_blob, types.T_text, types.T_blob, types.T_blob},
		{types.T_text, types.T_any, types.T_text, types.T_text},
		{types.T_text, types.T_bool, types.T_bool, types.T_bool},
		{types.T_text, types.T_int8, types.T_int8, types.T_int8},
		{types.T_text, types.T_int16, types.T_int16, types.T_int16},
		{types.T_text, types.T_int32, types.T_int32, types.T_int32},
		{types.T_text, types.T_int64, types.T_int64, types.T_int64},
		{types.T_text, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_text, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_text, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_text, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_text, types.T_float32, types.T_float32, types.T_float32},
		{types.T_text, types.T_float64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_text, types.T_date, types.T_date, types.T_date},
		{types.T_text, types.T_time, types.T_time, types.T_time},
		{types.T_text, types.T_datetime, types.T_datetime, types.T_datetime},
		{types.T_text, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_text, types.T_char, types.T_char, types.T_char},
		{types.T_text, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_text, types.T_uuid, types.T_uuid, types.T_uuid},
		{types.T_text, types.T_binary, types.T_blob, types.T_blob},
		{types.T_text, types.T_varbinary, types.T_varbinary, types.T_varbinary},
		{types.T_text, types.T_blob, types.T_blob, types.T_blob},
		{types.T_array_float32, types.T_varchar, types.T_array_float32, types.T_array_float32},
		{types.T_array_float32, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_varchar, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_array_float32, types.T_array_float64, types.T_array_float64},
		{types.T_array_float32, types.T_text, types.T_array_float32, types.T_array_float32},
		{types.T_text, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_array_float64, types.T_text, types.T_array_float64, types.T_array_float64},
		{types.T_text, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		// narrow vector types: string<->narrow for comparison/equality only.
		// (No scalar-arithmetic rules below are added for these types, so + - * /
		// still fail to resolve — arithmetic requires an explicit CAST to vecf32.)
		{types.T_array_bf16, types.T_varchar, types.T_array_bf16, types.T_array_bf16},
		{types.T_array_bf16, types.T_text, types.T_array_bf16, types.T_array_bf16},
		{types.T_text, types.T_array_bf16, types.T_array_bf16, types.T_array_bf16},
		{types.T_array_float16, types.T_varchar, types.T_array_float16, types.T_array_float16},
		{types.T_array_float16, types.T_text, types.T_array_float16, types.T_array_float16},
		{types.T_text, types.T_array_float16, types.T_array_float16, types.T_array_float16},
		{types.T_array_int8, types.T_varchar, types.T_array_int8, types.T_array_int8},
		{types.T_array_int8, types.T_text, types.T_array_int8, types.T_array_int8},
		{types.T_text, types.T_array_int8, types.T_array_int8, types.T_array_int8},
		{types.T_array_uint8, types.T_varchar, types.T_array_uint8, types.T_array_uint8},
		{types.T_array_uint8, types.T_text, types.T_array_uint8, types.T_array_uint8},
		{types.T_text, types.T_array_uint8, types.T_array_uint8, types.T_array_uint8},

		/** VEC <Op> Scalar => VEC **/
		// VECF32 <Op> Scalar => VECF32
		{types.T_array_float32, types.T_uint8, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_uint16, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_uint32, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_uint64, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_int8, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_int16, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_int32, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_int64, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_float32, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_float64, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_decimal64, types.T_array_float32, types.T_float32},
		// VECF64 <Op> Scalar => VECF64
		{types.T_array_float64, types.T_uint8, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_uint16, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_uint32, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_uint64, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_int8, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_int16, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_int32, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_int64, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_float32, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_float64, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_decimal64, types.T_array_float64, types.T_float64},
		// Scalar <Op> VECF32 => VECF32
		{types.T_uint8, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_uint16, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_uint32, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_uint64, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_int8, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_int16, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_int32, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_int64, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_float32, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_float64, types.T_array_float32, types.T_float32, types.T_array_float32},
		{types.T_decimal64, types.T_array_float32, types.T_float32, types.T_array_float32},
		// Scalar <Op> VECF64 => VECF64
		{types.T_uint8, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_uint16, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_uint32, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_uint64, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_int8, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_int16, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_int32, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_int64, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_float32, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_float64, types.T_array_float64, types.T_float64, types.T_array_float64},
		{types.T_decimal64, types.T_array_float64, types.T_float64, types.T_array_float64},
		// bit -> xx
		{types.T_bit, types.T_any, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_bool, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_int8, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_int16, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_int32, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_int64, types.T_int64, types.T_int64},
		{types.T_bit, types.T_uint8, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_uint16, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_uint32, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_float32, types.T_float64, types.T_float64},
		{types.T_bit, types.T_float64, types.T_float64, types.T_float64},
		{types.T_bit, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_bit, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_bit, types.T_date, types.T_int64, types.T_int64},
		{types.T_bit, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_bit, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_bit, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_bit, types.T_char, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_varchar, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_binary, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_varbinary, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_blob, types.T_uint64, types.T_uint64},
		{types.T_bit, types.T_text, types.T_uint64, types.T_uint64},
		// xx -> bit
		{types.T_any, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_bool, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_int8, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_int16, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_int32, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_int64, types.T_bit, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_uint16, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_uint32, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_float32, types.T_bit, types.T_float64, types.T_float64},
		{types.T_float64, types.T_bit, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_bit, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_bit, types.T_decimal128, types.T_decimal128},
		{types.T_date, types.T_bit, types.T_int64, types.T_int64},
		{types.T_time, types.T_bit, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_bit, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_bit, types.T_decimal64, types.T_decimal64},
		{types.T_char, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_varchar, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_binary, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_varbinary, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_blob, types.T_bit, types.T_uint64, types.T_uint64},
		{types.T_text, types.T_bit, types.T_uint64, types.T_uint64},
		// T_TS -> xx
		{types.T_TS, types.T_varchar, types.T_varchar, types.T_varchar},
		{types.T_TS, types.T_timestamp, types.T_timestamp, types.T_timestamp},
		{types.T_TS, types.T_int64, types.T_int64, types.T_int64},
		// xx -> T_TS
		{types.T_varchar, types.T_TS, types.T_varchar, types.T_varchar},
		{types.T_timestamp, types.T_TS, types.T_timestamp, types.T_timestamp},
		{types.T_int64, types.T_TS, types.T_int64, types.T_int64},
	}

	for _, r := range ru {
		addFixedBinaryCastRule1(r[0], r[1], r[2], r[3])
	}
}

func initFixed2() {
	ru := [][4]types.T{
		{types.T_any, types.T_any, types.T_float64, types.T_float64},
		{types.T_any, types.T_bool, types.T_float64, types.T_float64},
		{types.T_any, types.T_int8, types.T_float64, types.T_float64},
		{types.T_any, types.T_int16, types.T_float64, types.T_float64},
		{types.T_any, types.T_int32, types.T_float64, types.T_float64},
		{types.T_any, types.T_int64, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_any, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_any, types.T_float32, types.T_float64, types.T_float64},
		{types.T_any, types.T_float64, types.T_float64, types.T_float64},
		{types.T_any, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_any, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_any, types.T_date, types.T_float64, types.T_float64},
		{types.T_any, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_any, types.T_char, types.T_float64, types.T_float64},
		{types.T_any, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_any, types.T_binary, types.T_float64, types.T_float64},
		{types.T_any, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_any, types.T_blob, types.T_float64, types.T_float64},
		{types.T_any, types.T_text, types.T_float64, types.T_float64},
		{types.T_any, types.T_json, types.T_float64, types.T_float64},
		{types.T_bool, types.T_any, types.T_float64, types.T_float64},
		{types.T_bool, types.T_int8, types.T_int8, types.T_int8},
		{types.T_bool, types.T_int16, types.T_int16, types.T_int16},
		{types.T_bool, types.T_int32, types.T_int32, types.T_int32},
		{types.T_bool, types.T_int64, types.T_int64, types.T_int64},
		{types.T_bool, types.T_uint8, types.T_uint8, types.T_uint8},
		{types.T_bool, types.T_uint16, types.T_uint16, types.T_uint16},
		{types.T_bool, types.T_uint32, types.T_uint32, types.T_uint32},
		{types.T_bool, types.T_uint64, types.T_uint64, types.T_uint64},
		{types.T_int8, types.T_any, types.T_float64, types.T_float64},
		{types.T_int8, types.T_bool, types.T_int8, types.T_int8},
		{types.T_int8, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_date, types.T_int64, types.T_int64},
		{types.T_int8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int8, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int8, types.T_char, types.T_float64, types.T_float64},
		{types.T_int8, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int8, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int8, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int8, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int8, types.T_text, types.T_float64, types.T_float64},
		{types.T_int16, types.T_any, types.T_float64, types.T_float64},
		{types.T_int16, types.T_bool, types.T_int16, types.T_int16},
		{types.T_int16, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_date, types.T_int64, types.T_int64},
		{types.T_int16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int16, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int16, types.T_char, types.T_float64, types.T_float64},
		{types.T_int16, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int16, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int16, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int16, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int16, types.T_text, types.T_float64, types.T_float64},
		{types.T_int32, types.T_any, types.T_float64, types.T_float64},
		{types.T_int32, types.T_bool, types.T_int32, types.T_int32},
		{types.T_int32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_date, types.T_int64, types.T_int64},
		{types.T_int32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int32, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int32, types.T_char, types.T_float64, types.T_float64},
		{types.T_int32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int32, types.T_text, types.T_float64, types.T_float64},
		{types.T_int64, types.T_any, types.T_float64, types.T_float64},
		{types.T_int64, types.T_bool, types.T_int64, types.T_int64},
		{types.T_int64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_int64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_int64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_int64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_date, types.T_int64, types.T_int64},
		{types.T_int64, types.T_year, types.T_int64, types.T_int64},
		{types.T_int64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_int64, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_int64, types.T_char, types.T_float64, types.T_float64},
		{types.T_int64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_int64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_int64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_int64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_int64, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_bool, types.T_uint8, types.T_uint8},
		{types.T_uint8, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint8, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint8, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint8, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint8, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_bool, types.T_uint16, types.T_uint16},
		{types.T_uint16, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint16, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint16, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint16, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint16, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_bool, types.T_uint32, types.T_uint32},
		{types.T_uint32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint32, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint32, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint32, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint32, types.T_text, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_any, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_bool, types.T_uint64, types.T_uint64},
		{types.T_uint64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_date, types.T_int64, types.T_int64},
		{types.T_uint64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_uint64, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_uint64, types.T_char, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_uint64, types.T_text, types.T_float64, types.T_float64},
		{types.T_float32, types.T_any, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float32, types.T_uint64, types.T_float64, types.T_float64},
		// Balanced approach: float32 + decimal → float64 for arithmetic operations
		// Rationale: float32 has only 7 decimal digits precision, insufficient for large decimal values
		// float64 provides 15-16 digits precision, balancing performance and accuracy
		// Note: Comparison operators still use float32 for performance (see comparison type rules)
		{types.T_float32, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float32, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float32, types.T_char, types.T_float64, types.T_float64},
		{types.T_float32, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float32, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float32, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float32, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float32, types.T_text, types.T_float64, types.T_float64},
		{types.T_float64, types.T_any, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_int64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_float64, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_float64, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_float64, types.T_char, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_float64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_float64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_float64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_uint64, types.T_decimal128, types.T_decimal128},
		// Symmetric rule: decimal64 vs float32 → convert to float64 (see comment above for rationale)
		{types.T_decimal64, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_decimal64, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal64, types.T_date, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_time, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_datetime, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_timestamp, types.T_decimal64, types.T_decimal64},
		{types.T_decimal64, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal64, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_any, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_uint64, types.T_decimal128, types.T_decimal128},
		// Symmetric rule: decimal128 vs float32 → convert to float64 (see comment above for rationale)
		{types.T_decimal128, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_decimal64, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal128, types.T_date, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_time, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_datetime, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_timestamp, types.T_decimal128, types.T_decimal128},
		{types.T_decimal128, types.T_char, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varchar, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_binary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_varbinary, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_blob, types.T_float64, types.T_float64},
		{types.T_decimal128, types.T_text, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_any, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int8, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int16, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int32, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_int64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint8, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint16, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint32, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_uint64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_float32, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_float64, types.T_float64, types.T_float64},
		{types.T_decimal256, types.T_decimal64, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_decimal128, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_date, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_time, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_datetime, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_timestamp, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_char, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_varchar, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_binary, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_varbinary, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_blob, types.T_decimal256, types.T_decimal256},
		{types.T_decimal256, types.T_text, types.T_decimal256, types.T_decimal256},
		{types.T_date, types.T_any, types.T_int64, types.T_int64},
		{types.T_date, types.T_int8, types.T_int64, types.T_int64},
		{types.T_date, types.T_int16, types.T_int64, types.T_int64},
		{types.T_date, types.T_int32, types.T_int64, types.T_int64},
		{types.T_date, types.T_int64, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint8, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint16, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint32, types.T_int64, types.T_int64},
		{types.T_date, types.T_uint64, types.T_int64, types.T_int64},
		{types.T_date, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_date, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_time, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_int64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint8, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint16, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint32, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_uint64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_time, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_datetime, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_datetime, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_any, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_int8, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int16, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int32, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_int64, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint8, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint16, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint32, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_uint64, types.T_decimal128, types.T_decimal128},
		{types.T_timestamp, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_timestamp, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_year, types.T_any, types.T_float64, types.T_float64},
		{types.T_year, types.T_int8, types.T_float64, types.T_float64},
		{types.T_year, types.T_int16, types.T_float64, types.T_float64},
		{types.T_year, types.T_int32, types.T_float64, types.T_float64},
		{types.T_year, types.T_int64, types.T_float64, types.T_float64},
		{types.T_year, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_year, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_year, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_year, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_year, types.T_float32, types.T_float64, types.T_float64},
		{types.T_year, types.T_float64, types.T_float64, types.T_float64},
		{types.T_year, types.T_decimal64, types.T_decimal64, types.T_decimal64},
		{types.T_year, types.T_decimal128, types.T_decimal128, types.T_decimal128},
		{types.T_char, types.T_any, types.T_float64, types.T_float64},
		{types.T_char, types.T_int8, types.T_float64, types.T_float64},
		{types.T_char, types.T_int16, types.T_float64, types.T_float64},
		{types.T_char, types.T_int32, types.T_float64, types.T_float64},
		{types.T_char, types.T_int64, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_char, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_char, types.T_float32, types.T_float64, types.T_float64},
		{types.T_char, types.T_float64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_char, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_varchar, types.T_any, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int8, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int16, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_int64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_float32, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varchar, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		//A
		{types.T_varchar, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_varchar, types.T_array_float64, types.T_array_float64, types.T_array_float64},
		{types.T_varchar, types.T_array_bf16, types.T_array_bf16, types.T_array_bf16},
		{types.T_varchar, types.T_array_float16, types.T_array_float16, types.T_array_float16},
		{types.T_varchar, types.T_array_int8, types.T_array_int8, types.T_array_int8},
		{types.T_varchar, types.T_array_uint8, types.T_array_uint8, types.T_array_uint8},
		{types.T_binary, types.T_any, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int8, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int16, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_int64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_float32, types.T_float64, types.T_float64},
		{types.T_binary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_binary, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_varbinary, types.T_any, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int8, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int16, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_int64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_float32, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_float64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_varbinary, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_blob, types.T_any, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int8, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int16, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_int64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_float32, types.T_float64, types.T_float64},
		{types.T_blob, types.T_float64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_blob, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		{types.T_json, types.T_any, types.T_float64, types.T_float64},
		{types.T_json, types.T_int8, types.T_float64, types.T_float64},
		{types.T_json, types.T_int16, types.T_float64, types.T_float64},
		{types.T_json, types.T_int32, types.T_float64, types.T_float64},
		{types.T_json, types.T_int64, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_json, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_json, types.T_float32, types.T_float64, types.T_float64},
		{types.T_json, types.T_float64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_json, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_text, types.T_any, types.T_float64, types.T_float64},
		{types.T_text, types.T_int8, types.T_float64, types.T_float64},
		{types.T_text, types.T_int16, types.T_float64, types.T_float64},
		{types.T_text, types.T_int32, types.T_float64, types.T_float64},
		{types.T_text, types.T_int64, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint8, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint16, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint32, types.T_float64, types.T_float64},
		{types.T_text, types.T_uint64, types.T_float64, types.T_float64},
		{types.T_text, types.T_float32, types.T_float64, types.T_float64},
		{types.T_text, types.T_float64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal64, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal128, types.T_float64, types.T_float64},
		{types.T_text, types.T_decimal256, types.T_decimal256, types.T_decimal256},
		//B
		{types.T_array_float32, types.T_varchar, types.T_array_float32, types.T_array_float32},
		{types.T_array_float32, types.T_array_float32, types.T_array_float32, types.T_array_float32},
		{types.T_array_float64, types.T_varchar, types.T_array_float64, types.T_array_float64},
		{types.T_array_float64, types.T_array_float32, types.T_array_float64, types.T_array_float64},
		// narrow vector types: narrow<->string for comparison/equality only.
		{types.T_array_bf16, types.T_varchar, types.T_array_bf16, types.T_array_bf16},
		{types.T_array_bf16, types.T_array_bf16, types.T_array_bf16, types.T_array_bf16},
		{types.T_array_float16, types.T_varchar, types.T_array_float16, types.T_array_float16},
		{types.T_array_float16, types.T_array_float16, types.T_array_float16, types.T_array_float16},
		{types.T_array_int8, types.T_varchar, types.T_array_int8, types.T_array_int8},
		{types.T_array_int8, types.T_array_int8, types.T_array_int8, types.T_array_int8},
		{types.T_array_uint8, types.T_varchar, types.T_array_uint8, types.T_array_uint8},
		{types.T_array_uint8, types.T_array_uint8, types.T_array_uint8, types.T_array_uint8},
		/** VEC <Op> Scalar => VEC **/
		// VECF32 <Op> Scalar => VECF32
		{types.T_array_float32, types.T_int32, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_int64, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_float32, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_float64, types.T_array_float32, types.T_float32},
		{types.T_array_float32, types.T_decimal64, types.T_array_float32, types.T_float32},
		// VECF64 <Op> Scalar => VECF64
		{types.T_array_float64, types.T_int32, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_int64, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_float32, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_float64, types.T_array_float64, types.T_float64},
		{types.T_array_float64, types.T_decimal64, types.T_array_float64, types.T_float64},
	}

	for _, r := range ru {
		addFixedBinaryCastRule2(r[0], r[1], r[2], r[3])
	}
}

func initFixed3() {
	type toRule struct {
		toType      types.T
		preferLevel int
	}

	type rule struct {
		from   types.T
		toList []toRule
	}

	implicitCastSupported := []rule{
		{
			from: types.T_bool,
			toList: []toRule{
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int8,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 1},
				{toType: types.T_int32, preferLevel: 1},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int16,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 1},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_int64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint8,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 1},
				{toType: types.T_uint32, preferLevel: 1},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint16,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 1},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 1},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 1},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_uint64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_float32,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_float64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_decimal64,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal128, preferLevel: 1},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_decimal128,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 2},
				{toType: types.T_decimal256, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_decimal256,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 1},
				{toType: types.T_decimal64, preferLevel: 2},
				{toType: types.T_decimal128, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_date,
			toList: []toRule{
				{toType: types.T_datetime, preferLevel: 1},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_time,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_datetime,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_timestamp,
			toList: []toRule{
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_year,
			toList: []toRule{
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
			},
		},

		{
			from: types.T_char,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 1},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_varchar,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 1},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
				//C
				{toType: types.T_array_float32, preferLevel: 2},
				{toType: types.T_array_float64, preferLevel: 2},
				{toType: types.T_array_bf16, preferLevel: 2},
				{toType: types.T_array_float16, preferLevel: 2},
				{toType: types.T_array_int8, preferLevel: 2},
				{toType: types.T_array_uint8, preferLevel: 2},
			},
		},

		{
			from: types.T_uuid,
			toList: []toRule{
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_binary,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 1},
				{toType: types.T_blob, preferLevel: 1},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_varbinary,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 1},
				{toType: types.T_blob, preferLevel: 1},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_blob,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},

		{
			from: types.T_text,
			toList: []toRule{
				{toType: types.T_bool, preferLevel: 2},
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_date, preferLevel: 2},
				{toType: types.T_time, preferLevel: 2},
				{toType: types.T_datetime, preferLevel: 2},
				{toType: types.T_timestamp, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_array_float32, preferLevel: 2},
				{toType: types.T_array_float64, preferLevel: 2},
				{toType: types.T_array_bf16, preferLevel: 2},
				{toType: types.T_array_float16, preferLevel: 2},
				{toType: types.T_array_int8, preferLevel: 2},
				{toType: types.T_array_uint8, preferLevel: 2},
			},
		},
		{
			from: types.T_enum,
			toList: []toRule{
				{toType: types.T_int8, preferLevel: 2},
				{toType: types.T_int16, preferLevel: 2},
				{toType: types.T_int32, preferLevel: 2},
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint16, preferLevel: 1},
				{toType: types.T_uint8, preferLevel: 2},
				{toType: types.T_uint32, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 2},
				{toType: types.T_uint128, preferLevel: 2},
				{toType: types.T_float32, preferLevel: 2},
				{toType: types.T_float64, preferLevel: 2},
				{toType: types.T_decimal64, preferLevel: 2},
				{toType: types.T_decimal128, preferLevel: 2},
				{toType: types.T_decimal256, preferLevel: 2},
				{toType: types.T_char, preferLevel: 2},
				{toType: types.T_varchar, preferLevel: 2},
				{toType: types.T_binary, preferLevel: 2},
				{toType: types.T_varbinary, preferLevel: 2},
				{toType: types.T_blob, preferLevel: 2},
				{toType: types.T_text, preferLevel: 2},
			},
		},
		{
			from: types.T_array_float32,
			toList: []toRule{
				{toType: types.T_array_float64, preferLevel: 2},
			},
		},
		{
			from: types.T_array_float64,
			toList: []toRule{
				{toType: types.T_array_float32, preferLevel: 1},
			},
		},
		{
			from: types.T_bit,
			toList: []toRule{
				{toType: types.T_int64, preferLevel: 2},
				{toType: types.T_uint64, preferLevel: 1},
			},
		},
	}

	for _, r := range implicitCastSupported {
		for _, to := range r.toList {
			addFixedImplicitTypeCastRule(r.from, to.toType, to.preferLevel)
		}
	}
}

type tarTypes struct {
	cast  bool
	left  types.T
	right types.T
}

type implicitTypeCastRule struct {
	from   types.T
	toList [300]struct {
		canCast     bool
		preferLevel int // 1 is the highest prefer level.
	}
}

func addFixedBinaryCastRule1(sourceLeft, sourceRight types.T, targetLeft, targetRight types.T) {
	fixedBinaryCastRule1[sourceLeft][sourceRight] = tarTypes{
		cast:  true,
		left:  targetLeft,
		right: targetRight,
	}
}

func addFixedBinaryCastRule2(sourceLeft, sourceRight types.T, targetLeft, targetRight types.T) {
	fixedBinaryCastRule2[sourceLeft][sourceRight] = tarTypes{
		cast:  true,
		left:  targetLeft,
		right: targetRight,
	}
}

func addFixedImplicitTypeCastRule(fromType types.T, toType types.T, preferLevel int) {
	fixedCanImplicitCastRule[fromType].from = fromType
	fixedCanImplicitCastRule[fromType].toList[toType].canCast = true
	fixedCanImplicitCastRule[fromType].toList[toType].preferLevel = preferLevel
}
