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
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

var (
	// operater `CASE` supported return type.
	retOperatorCaseSupports = []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_bool,
		types.T_bit,
		types.T_uuid,
		types.T_date, types.T_datetime, types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_json, types.T_datalink,
	}
)

func mixedStringNumericToVarchar(source []types.Type) (types.Type, bool) {
	hasString := false
	hasNumeric := false
	for _, t := range source {
		if t.Oid.IsMySQLString() {
			hasString = true
		}
		// MySQL treats TRUE/FALSE as the integers 1/0, so BOOL counts as
		// numeric here: a string value branch next to a BOOL branch resolves
		// to a string result instead of casting the string to BOOL
		// (issue #31294).
		if t.Oid.IsInteger() || t.Oid.IsFloat() || t.Oid.IsDecimal() || t.Oid == types.T_bool {
			hasNumeric = true
		}
		if hasString && hasNumeric {
			retType := types.T_varchar.ToType()
			retType.Width = types.MaxVarBinaryLen
			return commonConditionalStringType(retType, source), true
		}
	}
	return types.Type{}, false
}

// conditionalTemporalCommonType keeps the widest temporal precision when a
// CASE/IF branch is coerced. Reconstructing a type from only its OID loses the
// source FSP and causes the cast inserted by the binder to round away
// fractional seconds.
func conditionalTemporalCommonType(source []types.Type) (types.Type, bool) {
	var (
		hasTemporal bool
		result      types.Type
		hasClock    bool
		hasDate     bool
	)
	for _, typ := range source {
		switch typ.Oid {
		case types.T_any:
			// Untyped NULL does not constrain the temporal domain.
			continue
		case types.T_date, types.T_datetime, types.T_timestamp, types.T_time:
			hasTemporal = true
			if typ.Oid == types.T_time {
				hasClock = true
			} else {
				hasDate = true
			}
			if result.Oid == types.T_any {
				result = typ
				continue
			}
			// DATE/DATETIME/TIMESTAMP share a wall-clock domain for conditional
			// expressions. TIME mixed with a date-like branch is promoted to
			// DATETIME while retaining the widest fractional precision.
			if typ.Oid == types.T_datetime || result.Oid == types.T_datetime ||
				(typ.Oid == types.T_timestamp && result.Oid == types.T_date) ||
				(typ.Oid == types.T_date && result.Oid == types.T_timestamp) {
				result.Oid = types.T_datetime
			}
			if typ.Scale > result.Scale {
				result.Scale = typ.Scale
			}
		default:
			return types.Type{}, false
		}
	}
	if !hasTemporal {
		return types.Type{}, false
	}
	if hasClock && hasDate {
		result.Oid = types.T_datetime
	}
	if result.Oid == types.T_any {
		return types.Type{}, false
	}
	if result.Oid == types.T_time || result.Oid == types.T_datetime || result.Oid == types.T_timestamp {
		result.Width = result.Scale
	}
	return result, true
}

func signedUnsignedIntegerCommonType(source []types.Type) (types.Type, bool) {
	hasSigned := false
	hasUnsigned := false
	maxWidth := int32(0)
	for _, typ := range source {
		switch typ.Oid {
		case types.T_any:
			// An untyped NULL branch affects result nullability, but it has no
			// numeric domain and must not change the common integer type.
			continue
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			hasSigned = true
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			hasUnsigned = true
		default:
			return types.Type{}, false
		}
		if width := integerIntegralWidth(typ.Oid); width > maxWidth {
			maxWidth = width
		}
	}
	if !hasSigned || !hasUnsigned {
		return types.Type{}, false
	}

	width := maxWidth + 1
	oid, ok := decimalTypeForRequiredWidth(types.T_decimal64, width)
	if !ok {
		return types.Type{}, false
	}
	return types.New(oid, width, 0), true
}

func binaryStringCommonType(source []types.Type) (types.Type, bool) {
	hasBinary := false
	sameFixedBinary := true
	hasFixedBinary := false
	fixedBinaryWidth := int32(0)
	bound := stringResultBound{}
	for _, typ := range source {
		switch typ.Oid {
		case types.T_any:
			// NULL has no concrete binary representation. It affects result
			// nullability, but must not change the common type of the value
			// branches.
			continue
		case types.T_binary:
			hasBinary = true
			if !hasFixedBinary {
				fixedBinaryWidth = typ.Width
				hasFixedBinary = true
			} else if typ.Width != fixedBinaryWidth {
				sameFixedBinary = false
			}
		case types.T_varbinary:
			hasBinary = true
			sameFixedBinary = false
		case types.T_char, types.T_varchar:
			sameFixedBinary = false
		default:
			return types.Type{}, false
		}
		inputBound := declaredStringByteBound(typ)
		bound.unknown = bound.unknown || inputBound.unknown
		bound.bytes = max(bound.bytes, inputBound.bytes)
	}
	if !hasBinary {
		return types.Type{}, false
	}
	if sameFixedBinary {
		return types.New(types.T_binary, fixedBinaryWidth, 0), true
	}
	// This is an expression bound, not a storage assignment capacity. Capping
	// UTF-8 bytes at MaxVarBinaryLen would make the implicit branch CAST discard
	// selected bytes before the real assignment owner can check them. Promote
	// large/unknown bounds to BLOB; keep finite small results indexable.
	return binaryStringResultType(bound), true
}

// MixedTextBinaryCommonType returns a binary common type only when a set
// column contains both character and binary strings. Other combinations keep
// their existing common-type rules.
func MixedTextBinaryCommonType(source []types.Type) (types.Type, bool) {
	hasText, hasBinary := false, false
	needsBlob, unknownWidth := false, false
	var byteWidth int64
	for _, typ := range source {
		switch typ.Oid {
		case types.T_char, types.T_varchar:
			hasText = true
			// Character widths count code points; TEXT widths count bytes.
			if typ.Width > 0 {
				byteWidth = max(byteWidth, int64(typ.Width)*utf8.UTFMax)
			}
		case types.T_text:
			hasText, needsBlob = true, true
			byteWidth = max(byteWidth, int64(typ.Width))
		case types.T_binary, types.T_varbinary, types.T_blob:
			hasBinary = true
			needsBlob = needsBlob || typ.Oid == types.T_blob
			byteWidth = max(byteWidth, int64(typ.Width))
		case types.T_any:
			// An untyped NULL contributes no width.
		default:
			return types.Type{}, false
		}
		unknownWidth = unknownWidth || typ.Oid != types.T_any && typ.Width <= 0
	}
	if !hasText || !hasBinary {
		return types.Type{}, false
	}
	if unknownWidth || byteWidth > types.MaxVarBinaryLen || needsBlob {
		// A zero width is unknown, not empty. Keep the materialized BLOB/TEXT
		// capacity large enough for the largest possible input byte value.
		capacity := int32(types.MaxStringSize)
		switch {
		case unknownWidth || byteWidth > types.MaxMediumTextLen:
			capacity = types.MaxLongTextLen
		case byteWidth > types.MaxStringSize:
			capacity = types.MaxMediumTextLen
		}
		return types.New(types.T_blob, capacity, 0), true
	}
	return binaryStringCommonType(source)
}

func needDecimalMetadataCast(source []types.Type, target types.Type) bool {
	if !target.Oid.IsDecimal() {
		return false
	}
	for i := range source {
		if source[i].Oid != target.Oid {
			return true
		}
		if source[i].Scale != target.Scale || source[i].Width != target.Width {
			return true
		}
	}
	return false
}

func requireDecimalReturn(source []types.Type) bool {
	hasDecimal := false
	for i := range source {
		if source[i].Oid.IsDecimal() {
			hasDecimal = true
			continue
		}
		if source[i].IsIntOrUint() {
			continue
		}
		return false
	}
	return hasDecimal
}

func textStringCommonType(source []types.Type) (types.Type, bool, bool) {
	hasString := false
	hasText := false
	aligned := true
	for _, input := range source {
		switch input.Oid {
		case types.T_text:
			hasString = true
			hasText = true
		case types.T_uuid:
			// A real text branch selects text; UUID/NULL-only stays native.
			aligned = false
		case types.T_any:
			aligned = false
		case types.T_char, types.T_varchar:
			hasString = true
			aligned = false
		default:
			return types.Type{}, false, false
		}
	}
	if !hasString {
		return types.Type{}, false, false
	}
	result := types.T_varchar.ToType()
	if hasText {
		result = types.T_text.ToType()
	}
	return commonConditionalStringType(result, source), aligned, true
}

// coalesceBinaryStringResult handles the bounded binary family before the
// generic overload-cost search. The generic search prefers BLOB for a
// BINARY/VARBINARY mixture, which loses the common finite byte bound and makes
// a materialized result ineligible for ordinary indexes.
func coalesceBinaryStringResult(overloads []overload, inputs []types.Type) (checkResult, bool) {
	hasBinary := false
	for _, input := range inputs {
		switch input.Oid {
		case types.T_any:
			continue
		case types.T_binary, types.T_varbinary:
			hasBinary = true
		default:
			return checkResult{}, false
		}
	}
	if !hasBinary {
		return checkResult{}, false
	}
	target, ok := binaryStringCommonType(inputs)
	if !ok {
		return newCheckResultWithFailure(failedFunctionParametersWrong), true
	}
	for i, over := range overloads {
		if len(over.args) != 1 || over.args[0] != target.Oid {
			continue
		}
		aligned := true
		for _, input := range inputs {
			if !input.Eq(target) {
				aligned = false
				break
			}
		}
		if aligned {
			return newCheckResultWithSuccess(i), true
		}
		castTypes := make([]types.Type, len(inputs))
		for j := range castTypes {
			castTypes[j] = target
		}
		return newCheckResultWithCast(i, castTypes), true
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong), true
}

// caseCheck check `case X then Y case X1 then Y1 ... (else Z)`
// caseConditionFinalType returns the execution type for a searched CASE
// condition slot. MySQL evaluates a JSON condition with conditional numeric
// coercion per row (the same kernel IF uses), so it stays in-domain; every
// other non-BOOL condition casts to BOOL.
func caseConditionFinalType(inputs []types.Type, i int) types.Type {
	if inputs[i].Oid == types.T_json {
		return inputs[i]
	}
	return types.T_bool.ToType()
}

// isEnumToBoolCastFunctionId reports whether the function id resolves
// explicit cast applications over the supportedTypeCast table. The D11 gate
// in getFunctionByName consults it to keep the ENUM -> BOOL cast pair - added
// only for the conditional storage-truth contracts - unresolvable for a
// native-mode statement, exactly as the pre-contract table admitted no such
// pair at all.
func isEnumToBoolCastFunctionId(id int32) bool {
	switch id {
	case CAST, CAST_STRICT, CAST_ASSIGN, CAST_IGNORE:
		return true
	}
	return false
}

// legacyConditionalBooleanCheck reproduces the pre-contract IF/CASE condition
// admission for native-mode statements (D11): JSON conditions reject, and an
// ENUM/SET operand keeps the ordinary string/numeric typing base produced.
// The condition admission is byte-for-byte the pre-contract checkers'; vector
// value branches resolve through commonVectorType in both modes.
func legacyConditionalBooleanCheck(id int32, _ []overload, inputs []types.Type) (checkResult, bool) {
	switch id {
	case IFF:
		return legacyIffBooleanCheck(inputs), true
	case CASE:
		return legacyCaseBooleanCheck(inputs), true
	}
	return checkResult{}, false
}

func legacyIffBooleanCheck(inputs []types.Type) checkResult {
	// iff(x, y, z)
	if len(inputs) == 3 {
		if widened, ok := widenMixedLowPrecisionFloats(inputs, func(i int) bool { return i > 0 }); ok {
			return castFromWidened(legacyIffBooleanCheck(widened), widened)
		}
		conditionType := inputs[0]
		needCast := conditionType.Oid == types.T_any
		if needCast {
			conditionType = types.T_bool.ToType()
		} else if conditionType.Oid != types.T_bool &&
			!conditionType.IsNumeric() && !conditionType.Oid.IsMySQLString() {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}

		source := []types.Type{inputs[1], inputs[2]}
		if retType, ok := CommonMediumIntType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast ||
				!sameMediumIntCommonType(source[0], retType) ||
				!sameMediumIntCommonType(source[1], retType)
			if shouldCast {
				return newCheckResultWithCast(0, finalTypes)
			}
			return newCheckResultWithSuccess(0)
		}
		if retType, ok := conditionalTemporalCommonType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid || inputs[i].Scale != finalTypes[i].Scale {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		sameCollatedText := false
		switch source[0].Oid {
		case types.T_char, types.T_varchar, types.T_text:
			sameCollatedText = source[0].Eq(source[1])
		}
		if sameCollatedText {
			// Matching text branches already have a common collation identity.
			// Preserve it instead of rebuilding the same OID with ToType(), which
			// would turn legacy or utf8mb4_bin metadata into general_ci.
			if needCast {
				return newCheckResultWithCast(0, []types.Type{conditionType, source[0], source[1]})
			}
			return newCheckResultWithSuccess(0)
		}
		if lowPrecType, ok := sameLowPrecisionFloatType(source); ok {
			if needCast || source[0].Oid != lowPrecType.Oid || source[1].Oid != lowPrecType.Oid {
				return newCheckResultWithCast(0, []types.Type{conditionType, lowPrecType, lowPrecType})
			}
			return newCheckResultWithSuccess(0)
		}
		if retType, hasVector, ok := commonVectorType(source); hasVector {
			if !ok {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			finalTypes := []types.Type{conditionType, retType, retType}
			if needCast || source[0] != retType || source[1] != retType {
				return newCheckResultWithCast(0, finalTypes)
			}
			return newCheckResultWithSuccess(0)
		}
		if source[0].Oid == types.T_binary || source[0].Oid == types.T_varbinary ||
			source[1].Oid == types.T_binary || source[1].Oid == types.T_varbinary {
			binaryIdx := 0
			if source[0].Oid == types.T_any {
				binaryIdx = 1
			}
			otherIdx := 1 - binaryIdx
			if source[otherIdx].Oid == types.T_any {
				retType := source[binaryIdx]
				setMaxWidthFromSource(&retType, source)
				finalTypes := []types.Type{conditionType, retType, retType}
				return newCheckResultWithCast(0, finalTypes)
			}
		}
		if retType, ok := binaryStringCommonType(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, ok := mixedStringNumericToVarchar(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, ok := signedUnsignedIntegerCommonType(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, resultAligned, ok := textStringCommonType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast || !resultAligned
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		minCost := math.MaxInt32
		retType := types.Type{}

		target := make([]types.T, 2)
		decimalReturnRequired := requireDecimalReturn(source)
		for _, rett := range retOperatorIffSupports {
			if decimalReturnRequired && !rett.IsDecimal() {
				continue
			}
			target[0], target[1] = rett, rett

			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				retType = rett.ToType()
				if retType.Oid.IsDecimal() {
					if !setSafeDecimalWidthAndScaleFromSource(&retType, source) {
						continue
					}
				} else if retType.Oid.IsMySQLString() {
					retType = commonConditionalStringType(retType, source)
				}
				retType = commonTemporalType(retType, source)
				minCost = cost
			}
		}

		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		finalTypes := []types.Type{conditionType, retType, retType}
		shouldCast := needCast || retType.Oid.IsMySQLString() ||
			needDecimalMetadataCast(source, retType) || needTemporalMetadataCast(source, retType)
		for i := range inputs {
			if inputs[i].Oid != finalTypes[i].Oid {
				shouldCast = true
			}
		}
		if !shouldCast {
			return newCheckResultWithSuccess(0)
		}
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func legacyCaseBooleanCheck(inputs []types.Type) checkResult {
	l := len(inputs)
	isValue := func(i int) bool { return i%2 == 1 || (l%2 == 1 && i == l-1) }
	if widened, ok := widenMixedLowPrecisionFloats(inputs, isValue); ok {
		return castFromWidened(legacyCaseBooleanCheck(widened), widened)
	}

	needCast := false
	if l >= 2 {
		// Each searched CASE condition is a boolean context. Accept every type
		// that the type system can implicitly convert to BOOL, including the
		// untyped NULL literal, whose cast remains NULL and therefore does not
		// select its branch.
		for i := 0; i < l-1; i += 2 {
			if inputs[i].Oid != types.T_bool {
				if canCast, _ := fixedImplicitTypeCast(inputs[i], types.T_bool); !canCast {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				needCast = true
			}
		}

		// XXX choose a supported Y type.
		var source []types.Type
		minCost := math.MaxInt32
		retType := types.Type{}

		source = make([]types.Type, 0, (l+1)/2)
		for j := 1; j < l; j += 2 {
			source = append(source, inputs[j])
		}
		if l%2 == 1 {
			source = append(source, inputs[l-1])
		}

		if retType, ok := CommonMediumIntType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range inputs {
				isValue := i%2 == 1 || (len(inputs)%2 == 1 && i == len(inputs)-1)
				if !isValue {
					finalTypes[i] = types.T_bool.ToType()
					shouldCast = shouldCast || inputs[i].Oid != types.T_bool
					continue
				}
				finalTypes[i] = retType
				shouldCast = shouldCast || !sameMediumIntCommonType(inputs[i], retType)
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if retType, ok := conditionalTemporalCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = retType
				}
			}
			shouldCast := needCast
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid || inputs[i].Scale != finalTypes[i].Scale {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if lowPrecType, ok := sameLowPrecisionFloatType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = lowPrecType
				}
				if inputs[i].Oid != finalTypes[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if vectorType, hasVector, valid := commonVectorType(source); hasVector {
			if !valid {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
					if inputs[i].Oid != types.T_bool {
						shouldCast = true
					}
				} else {
					finalTypes[i] = vectorType
					if inputs[i] != vectorType {
						shouldCast = true
					}
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if retType, ok := binaryStringCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = retType
				}
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, ok := mixedStringNumericToVarchar(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = retType
				}
			}
			if len(inputs)%2 == 1 {
				finalTypes[len(finalTypes)-1] = retType
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, ok := signedUnsignedIntegerCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = retType
				}
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, resultAligned, ok := textStringCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast || !resultAligned
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = types.T_bool.ToType()
				} else {
					finalTypes[i] = retType
				}
				if finalTypes[i].Oid != inputs[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		target := make([]types.T, len(source))
		decimalReturnRequired := requireDecimalReturn(source)

		for _, rett := range retOperatorCaseSupports {
			if decimalReturnRequired && !rett.IsDecimal() {
				continue
			}
			for i := range target {
				target[i] = rett
			}
			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				retType = rett.ToType()
				if retType.Oid.IsDecimal() {
					if !setSafeDecimalWidthAndScaleFromSource(&retType, source) {
						continue
					}
				} else if retType.Oid.IsMySQLString() {
					retType = commonConditionalStringType(retType, source)
				}
				retType = commonTemporalType(retType, source)
				minCost = cost
			}
		}
		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		finalTypes := make([]types.Type, len(inputs))
		shouldCast := needCast || retType.Oid.IsMySQLString() ||
			needDecimalMetadataCast(source, retType) || needTemporalMetadataCast(source, retType)
		for i := range finalTypes {
			if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
				finalTypes[i] = types.T_bool.ToType()
			} else {
				finalTypes[i] = retType
			}
			if finalTypes[i].Oid != inputs[i].Oid {
				shouldCast = true
			}
		}
		if !shouldCast {
			return newCheckResultWithSuccess(0)
		}

		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func caseCheck(overloads []overload, inputs []types.Type) checkResult {
	return boolValueBranchesAsNumeric(overloads, inputs, caseValueArgument, caseCheckTypes)
}

func caseValueArgument(i, n int) bool {
	return i%2 == 1 || (n%2 == 1 && i == n-1)
}

func caseCheckTypes(_ []overload, inputs []types.Type) checkResult {
	l := len(inputs)
	isValue := func(i int) bool { return i%2 == 1 || (l%2 == 1 && i == l-1) }
	if widened, ok := widenMixedLowPrecisionFloats(inputs, isValue); ok {
		return castFromWidened(caseCheck(nil, widened), widened)
	}

	needCast := false
	if l >= 2 {
		// Each searched CASE condition is a boolean context. Accept every type
		// that the type system can implicitly convert to BOOL, including the
		// untyped NULL literal, whose cast remains NULL and therefore does not
		// select its branch. A JSON condition stays in-domain and evaluates
		// per row with conditional numeric coercion, so admission is decided
		// here rather than through the general implicit-cast table (which keeps
		// no T_json -> T_bool row) and it schedules no cast. A recovered ENUM
		// condition is admitted the same way: its stored ordinal becomes BOOL
		// through the conditional storage-truth CAST contract, never through a
		// general implicit-cast table row that would also leak admission into
		// native-mode statements.
		for i := 0; i < l-1; i += 2 {
			if inputs[i].Oid != types.T_bool {
				if inputs[i].Oid != types.T_json && inputs[i].Oid != types.T_enum {
					if canCast, _ := fixedImplicitTypeCast(inputs[i], types.T_bool); !canCast {
						return newCheckResultWithFailure(failedFunctionParametersWrong)
					}
					needCast = true
				}
			}
		}

		// XXX choose a supported Y type.
		var source []types.Type
		minCost := math.MaxInt32
		retType := types.Type{}

		source = make([]types.Type, 0, (l+1)/2)
		for j := 1; j < l; j += 2 {
			source = append(source, inputs[j])
		}
		if l%2 == 1 {
			source = append(source, inputs[l-1])
		}

		if retType, ok := CommonMediumIntType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range inputs {
				isValue := i%2 == 1 || (len(inputs)%2 == 1 && i == len(inputs)-1)
				if !isValue {
					finalTypes[i] = caseConditionFinalType(inputs, i)
					shouldCast = shouldCast || finalTypes[i].Oid != inputs[i].Oid
					continue
				}
				finalTypes[i] = retType
				shouldCast = shouldCast || !sameMediumIntCommonType(inputs[i], retType)
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if retType, ok := conditionalTemporalCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = retType
				}
			}
			shouldCast := needCast
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid || inputs[i].Scale != finalTypes[i].Scale {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if lowPrecType, ok := sameLowPrecisionFloatType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = lowPrecType
				}
				if inputs[i].Oid != finalTypes[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if vectorType, hasVector, valid := commonVectorType(source); hasVector {
			if !valid {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
					if finalTypes[i].Oid != inputs[i].Oid {
						shouldCast = true
					}
				} else {
					finalTypes[i] = vectorType
					if inputs[i] != vectorType {
						shouldCast = true
					}
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		if retType, ok := binaryStringCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = retType
				}
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, ok := mixedStringNumericToVarchar(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = retType
				}
			}
			if len(inputs)%2 == 1 {
				finalTypes[len(finalTypes)-1] = retType
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, ok := signedUnsignedIntegerCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = retType
				}
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		if retType, resultAligned, ok := textStringCommonType(source); ok {
			finalTypes := make([]types.Type, len(inputs))
			shouldCast := needCast || !resultAligned
			for i := range finalTypes {
				if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
					finalTypes[i] = caseConditionFinalType(inputs, i)
				} else {
					finalTypes[i] = retType
				}
				if finalTypes[i].Oid != inputs[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		target := make([]types.T, len(source))
		decimalReturnRequired := requireDecimalReturn(source)

		for _, rett := range retOperatorCaseSupports {
			if decimalReturnRequired && !rett.IsDecimal() {
				continue
			}
			for i := range target {
				target[i] = rett
			}
			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				retType = rett.ToType()
				if retType.Oid.IsDecimal() {
					if !setSafeDecimalWidthAndScaleFromSource(&retType, source) {
						continue
					}
				} else if retType.Oid.IsMySQLString() {
					retType = commonConditionalStringType(retType, source)
				}
				retType = commonTemporalType(retType, source)
				minCost = cost
			}
		}
		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		finalTypes := make([]types.Type, len(inputs))
		shouldCast := needCast || retType.Oid.IsMySQLString() ||
			needDecimalMetadataCast(source, retType) || needTemporalMetadataCast(source, retType)
		for i := range finalTypes {
			if i%2 == 0 && !(len(inputs)%2 == 1 && i == len(inputs)-1) {
				finalTypes[i] = caseConditionFinalType(inputs, i)
			} else {
				finalTypes[i] = retType
			}
			if finalTypes[i].Oid != inputs[i].Oid {
				shouldCast = true
			}
		}
		if !shouldCast {
			return newCheckResultWithSuccess(0)
		}

		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

// caseConditionTruth adapts a searched CASE condition slot for execution:
// BOOL conditions read their cast value; JSON conditions stay in-domain and
// evaluate per row with the same conditional numeric coercion IF uses, instead
// of the strict JSON-to-BOOL cast that explicit CAST ... AS BOOL keeps.
type caseConditionTruth struct {
	cond vector.FunctionParameterWrapper[bool]
	json *vector.Vector
}

func newCaseConditionTruth(vec *vector.Vector) caseConditionTruth {
	if vec.GetType().Oid == types.T_json {
		return caseConditionTruth{json: vec}
	}
	return caseConditionTruth{cond: vector.GenerateFunctionFixedTypeParameter[bool](vec)}
}

func (c caseConditionTruth) truth(mode SQLCompatibilityMode, row uint64) (bool, error) {
	if c.json != nil {
		return IffConditionTruthyAt(c.json, row, mode)
	}
	v, null := c.cond.GetValue(row)
	return !null && v, nil
}

func caseFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	t := result.GetResultVector().GetType()
	switch t.Oid {
	case types.T_bit:
		return generalCaseFn[uint64](parameters, result, proc, length, selectList)
	case types.T_int8:
		return generalCaseFn[int8](parameters, result, proc, length, selectList)
	case types.T_int16:
		return generalCaseFn[int16](parameters, result, proc, length, selectList)
	case types.T_int32:
		return generalCaseFn[int32](parameters, result, proc, length, selectList)
	case types.T_int64:
		return generalCaseFn[int64](parameters, result, proc, length, selectList)
	case types.T_uint8:
		return generalCaseFn[uint8](parameters, result, proc, length, selectList)
	case types.T_uint16:
		return generalCaseFn[uint16](parameters, result, proc, length, selectList)
	case types.T_uint32:
		return generalCaseFn[uint32](parameters, result, proc, length, selectList)
	case types.T_uint64:
		return generalCaseFn[uint64](parameters, result, proc, length, selectList)
	case types.T_float32:
		return generalCaseFn[float32](parameters, result, proc, length, selectList)
	case types.T_float64:
		return generalCaseFn[float64](parameters, result, proc, length, selectList)
	case types.T_bf16:
		return generalCaseFn[types.BF16](parameters, result, proc, length, selectList)
	case types.T_float16:
		return generalCaseFn[types.Float16](parameters, result, proc, length, selectList)
	case types.T_float8:
		return generalCaseFn[types.Float8](parameters, result, proc, length, selectList)
	case types.T_float4:
		return generalCaseFn[types.Float4](parameters, result, proc, length, selectList)
	case types.T_date:
		return generalCaseFn[types.Date](parameters, result, proc, length, selectList)
	case types.T_time:
		return generalCaseFn[types.Time](parameters, result, proc, length, selectList)
	case types.T_datetime:
		return generalCaseFn[types.Datetime](parameters, result, proc, length, selectList)
	case types.T_timestamp:
		return generalCaseFn[types.Timestamp](parameters, result, proc, length, selectList)
	case types.T_uuid:
		return generalCaseFn[types.Uuid](parameters, result, proc, length, selectList)
	case types.T_bool:
		return generalCaseFn[bool](parameters, result, proc, length, selectList)
	case types.T_decimal64:
		return generalCaseFn[types.Decimal64](parameters, result, proc, length, selectList)
	case types.T_decimal128:
		return generalCaseFn[types.Decimal128](parameters, result, proc, length, selectList)
	case types.T_decimal256:
		return generalCaseFn[types.Decimal256](parameters, result, proc, length, selectList)
	case types.T_enum:
		return generalCaseFn[types.Enum](parameters, result, proc, length, selectList)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_datalink, types.T_json,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
		types.T_array_float8, types.T_array_float4:
		return strCaseFn(parameters, result, proc, length, selectList)
	}
	panic("unreached code")
}

func generalCaseFn[T constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
	types.Decimal64 | types.Decimal128 | types.Decimal256 | types.Timestamp | types.Uuid](vecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// JSON conditions retain their in-domain truth and compatibility checks below.
	if len(vecs) == 3 && vecs[0].GetType().Oid == types.T_bool && vecs[2].IsConstNull() {
		cond := vector.GenerateFunctionFixedTypeParameter[bool](vecs[0])
		value := vector.GenerateFunctionFixedTypeParameter[T](vecs[1])
		rs := vector.MustFunctionResult[T](result)
		var zero T
		nullStart := uint64(0)
		inNull := false
		for i := uint64(0); i < uint64(length); i++ {
			if c, n := cond.GetValue(i); !n && c {
				if v, n := value.GetValue(i); !n {
					if inNull {
						rs.AddNullRange(nullStart, i)
						inNull = false
					}
					rs.AppendMustValue(v)
					continue
				}
			}
			if !inNull {
				nullStart = i
				inNull = true
			}
			rs.AppendMustValue(zero)
		}
		if inNull {
			rs.AddNullRange(nullStart, uint64(length))
		}
		return nil
	}
	// case Xn then Yn else Z
	mode := CompatibilityModeFromProcess(proc)
	xs := make([]caseConditionTruth, 0, len(vecs)/2)
	ys := make([]vector.FunctionParameterWrapper[T], 0, len(vecs)/2)

	l := len(vecs)
	for i := 0; i < l-1; i += 2 {
		xs = append(xs, newCaseConditionTruth(vecs[i]))
	}
	for j := 1; j < l; j += 2 {
		ys = append(ys, vector.GenerateFunctionFixedTypeParameter[T](vecs[j]))
	}

	rs := vector.MustFunctionResult[T](result)

	if len(vecs)%2 == 1 {
		z := vector.GenerateFunctionFixedTypeParameter[T](vecs[len(vecs)-1])
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				truth, err := xs[j].truth(mode, i)
				if err != nil {
					return err
				}
				if truth {
					if err := rs.Append(ys[j].GetValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
			}
			if matchElse {
				if err := rs.Append(z.GetValue(i)); err != nil {
					return err
				}
			}
		}
	} else {
		var dv T // default value

		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				truth, err := xs[j].truth(mode, i)
				if err != nil {
					return err
				}
				if truth {
					if err := rs.Append(ys[j].GetValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
			}
			if matchElse {
				if err := rs.Append(dv, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func strCaseFn(vecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// case Xn then Yn else Z
	mode := CompatibilityModeFromProcess(proc)
	xs := make([]caseConditionTruth, 0, len(vecs)/2)
	ys := make([]vector.FunctionParameterWrapper[types.Varlena], 0, len(vecs)/2)

	l := len(vecs)
	for i := 0; i < l-1; i += 2 {
		xs = append(xs, newCaseConditionTruth(vecs[i]))
	}
	for j := 1; j < l; j += 2 {
		ys = append(ys, vector.GenerateFunctionStrParameter(vecs[j]))
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	if len(vecs)%2 == 1 {
		z := vector.GenerateFunctionStrParameter(vecs[len(vecs)-1])
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				truth, err := xs[j].truth(mode, i)
				if err != nil {
					return err
				}
				if truth {
					if err := rs.AppendBytes(ys[j].GetStrValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
			}
			if matchElse {
				if err := rs.AppendBytes(z.GetStrValue(i)); err != nil {
					return err
				}
			}
		}
	} else {
		for i := uint64(0); i < uint64(length); i++ {
			matchElse := true
			for j := range xs {
				truth, err := xs[j].truth(mode, i)
				if err != nil {
					return err
				}
				if truth {
					if err := rs.AppendBytes(ys[j].GetStrValue(i)); err != nil {
						return err
					}
					matchElse = false
					break
				}
			}
			if matchElse {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var (
	retOperatorIffSupports = []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_uuid,
		types.T_bool, types.T_date, types.T_datetime,
		types.T_bit,
		types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_json,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_timestamp, types.T_time, types.T_datalink,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8,
	}
)

func iffCheck(overloads []overload, inputs []types.Type) checkResult {
	return boolValueBranchesAsNumeric(overloads, inputs, iffValueArgument, iffCheckTypes)
}

func iffValueArgument(i, n int) bool {
	return n == 3 && i > 0
}

func iffCheckTypes(_ []overload, inputs []types.Type) checkResult {
	// iff(x, y, z)
	if len(inputs) == 3 {
		if widened, ok := widenMixedLowPrecisionFloats(inputs, func(i int) bool { return i > 0 }); ok {
			return castFromWidened(iffCheck(nil, widened), widened)
		}
		conditionType := inputs[0]
		needCast := conditionType.Oid == types.T_any
		if needCast {
			conditionType = types.T_bool.ToType()
		} else if conditionType.Oid != types.T_bool &&
			conditionType.Oid != types.T_enum &&
			conditionType.Oid != types.T_json &&
			!conditionType.IsNumeric() && !conditionType.Oid.IsMySQLString() {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}

		source := []types.Type{inputs[1], inputs[2]}
		if retType, ok := CommonMediumIntType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast ||
				!sameMediumIntCommonType(source[0], retType) ||
				!sameMediumIntCommonType(source[1], retType)
			if shouldCast {
				return newCheckResultWithCast(0, finalTypes)
			}
			return newCheckResultWithSuccess(0)
		}
		if retType, ok := conditionalTemporalCommonType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid || inputs[i].Scale != finalTypes[i].Scale {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}
		sameCollatedText := false
		switch source[0].Oid {
		case types.T_char, types.T_varchar, types.T_text:
			sameCollatedText = source[0].Eq(source[1])
		}
		if sameCollatedText {
			// Matching text branches already have a common collation identity.
			// Preserve it instead of rebuilding the same OID with ToType(), which
			// would turn legacy or utf8mb4_bin metadata into general_ci.
			if needCast {
				return newCheckResultWithCast(0, []types.Type{conditionType, source[0], source[1]})
			}
			return newCheckResultWithSuccess(0)
		}
		if lowPrecType, ok := sameLowPrecisionFloatType(source); ok {
			if needCast || source[0].Oid != lowPrecType.Oid || source[1].Oid != lowPrecType.Oid {
				return newCheckResultWithCast(0, []types.Type{conditionType, lowPrecType, lowPrecType})
			}
			return newCheckResultWithSuccess(0)
		}
		if retType, hasVector, ok := commonVectorType(source); hasVector {
			if !ok {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			finalTypes := []types.Type{conditionType, retType, retType}
			if needCast || source[0] != retType || source[1] != retType {
				return newCheckResultWithCast(0, finalTypes)
			}
			return newCheckResultWithSuccess(0)
		}
		if source[0].Oid == types.T_binary || source[0].Oid == types.T_varbinary ||
			source[1].Oid == types.T_binary || source[1].Oid == types.T_varbinary {
			binaryIdx := 0
			if source[0].Oid == types.T_any {
				binaryIdx = 1
			}
			otherIdx := 1 - binaryIdx
			if source[otherIdx].Oid == types.T_any {
				retType := source[binaryIdx]
				setMaxWidthFromSource(&retType, source)
				finalTypes := []types.Type{conditionType, retType, retType}
				return newCheckResultWithCast(0, finalTypes)
			}
		}
		if retType, ok := binaryStringCommonType(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, ok := mixedStringNumericToVarchar(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, ok := signedUnsignedIntegerCommonType(source); ok {
			return newCheckResultWithCast(0, []types.Type{conditionType, retType, retType})
		}
		if retType, resultAligned, ok := textStringCommonType(source); ok {
			finalTypes := []types.Type{conditionType, retType, retType}
			shouldCast := needCast || !resultAligned
			for i := range inputs {
				if inputs[i].Oid != finalTypes[i].Oid {
					shouldCast = true
				}
			}
			if !shouldCast {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithCast(0, finalTypes)
		}

		minCost := math.MaxInt32
		retType := types.Type{}

		target := make([]types.T, 2)
		decimalReturnRequired := requireDecimalReturn(source)
		for _, rett := range retOperatorIffSupports {
			if decimalReturnRequired && !rett.IsDecimal() {
				continue
			}
			target[0], target[1] = rett, rett

			c, cost := tryToMatch(source, target)
			if c == matchFailed {
				continue
			}
			if cost < minCost {
				retType = rett.ToType()
				if retType.Oid.IsDecimal() {
					if !setSafeDecimalWidthAndScaleFromSource(&retType, source) {
						continue
					}
				} else if retType.Oid.IsMySQLString() {
					retType = commonConditionalStringType(retType, source)
				}
				retType = commonTemporalType(retType, source)
				minCost = cost
			}
		}

		if minCost == math.MaxInt32 {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		finalTypes := []types.Type{conditionType, retType, retType}
		shouldCast := needCast || retType.Oid.IsMySQLString() ||
			needDecimalMetadataCast(source, retType) || needTemporalMetadataCast(source, retType)
		for i := range inputs {
			if inputs[i].Oid != finalTypes[i].Oid {
				shouldCast = true
			}
		}
		if !shouldCast {
			return newCheckResultWithSuccess(0)
		}
		return newCheckResultWithCast(0, finalTypes)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func IffConditionTruthyAt(vec *vector.Vector, row uint64, mode SQLCompatibilityMode) (bool, error) {
	if vec.IsConstNull() || vec.GetNulls().Contains(row) {
		return false, nil
	}

	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, int(row)), nil
	case types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, int(row)) != 0, nil
	case types.T_enum:
		// The binder keeps ENUM storage in the condition. MySQL truth for an
		// ENUM is its numeric ordinal, exactly as BIT truth is its payload.
		// Native mode keeps the pre-contract behavior: storage-typed ENUM
		// conditions never bind there (legacy admission), so this stays the
		// base diagnostic.
		if mode != SQLCompatibilityMySQL {
			return false, moerr.NewInvalidArgNoCtx("IF condition", vec.GetType().String())
		}
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, int(row)) != 0, nil
	case types.T_json:
		// MySQL conditional truth coerces a JSON scalar numerically: strings
		// use their leading numeric prefix ("2tail" is true like 2, "abc" and
		// the string "true" are false like 0), numbers compare against zero,
		// and JSON null selects the ELSE branch the same way a SQL NULL
		// condition does. Explicit CAST ... AS BOOL keeps the strict kernel.
		// Native mode keeps the pre-contract behavior: JSON conditions never
		// bind there (legacy admission makes base reject them), so this stays
		// the base diagnostic.
		if mode != SQLCompatibilityMySQL {
			return false, moerr.NewInvalidArgNoCtx("IF condition", vec.GetType().String())
		}
		value, isNull, err := jsonScalarConditionalTruthy(types.DecodeJson(vec.GetBytesAt(int(row))))
		if err != nil {
			return false, err
		}
		if isNull {
			return false, nil
		}
		return value, nil
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, int(row)) != 0, nil
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, int(row)) != 0, nil
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, int(row)) != 0, nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, int(row)) != 0, nil
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, int(row)) != 0, nil
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, int(row)) != 0, nil
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, int(row)) != 0, nil
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, int(row)) != 0, nil
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, int(row)) != 0, nil
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, int(row)) != 0, nil
	case types.T_bf16:
		return vector.GetFixedAtNoTypeCheck[types.BF16](vec, int(row)).ToFloat32() != 0, nil
	case types.T_float16:
		return vector.GetFixedAtNoTypeCheck[types.Float16](vec, int(row)).ToFloat32() != 0, nil
	case types.T_float8:
		return vector.GetFixedAtNoTypeCheck[types.Float8](vec, int(row)).ToFloat32() != 0, nil
	case types.T_float4:
		return vector.GetFixedAtNoTypeCheck[types.Float4](vec, int(row)).ToFloat32() != 0, nil
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, int(row)) != 0, nil
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, int(row)).Compare(types.Decimal128{}) != 0, nil
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, int(row)).Compare(types.Decimal256{}) != 0, nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text:
		value, err := parseBytesToFloat(vec.GetBytesAt(int(row)), vec.GetIsBin(), 64, mode)
		if err != nil {
			return false, err
		}
		return value != 0, nil
	default:
		return false, moerr.NewInvalidArgNoCtx("IF condition", vec.GetType().String())
	}
}

func iffFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rett := result.GetResultVector().GetType()
	switch rett.Oid {
	case types.T_bit:
		return generalIffFn[uint64](parameters, result, proc, length, selectList)
	case types.T_int8:
		return generalIffFn[int8](parameters, result, proc, length, selectList)
	case types.T_int16:
		return generalIffFn[int16](parameters, result, proc, length, selectList)
	case types.T_int32:
		return generalIffFn[int32](parameters, result, proc, length, selectList)
	case types.T_int64:
		return generalIffFn[int64](parameters, result, proc, length, selectList)
	case types.T_uint8:
		return generalIffFn[uint8](parameters, result, proc, length, selectList)
	case types.T_uint16:
		return generalIffFn[uint16](parameters, result, proc, length, selectList)
	case types.T_uint32:
		return generalIffFn[uint32](parameters, result, proc, length, selectList)
	case types.T_uint64:
		return generalIffFn[uint64](parameters, result, proc, length, selectList)
	case types.T_float32:
		return generalIffFn[float32](parameters, result, proc, length, selectList)
	case types.T_float64:
		return generalIffFn[float64](parameters, result, proc, length, selectList)
	case types.T_bf16:
		return generalIffFn[types.BF16](parameters, result, proc, length, selectList)
	case types.T_float16:
		return generalIffFn[types.Float16](parameters, result, proc, length, selectList)
	case types.T_float8:
		return generalIffFn[types.Float8](parameters, result, proc, length, selectList)
	case types.T_float4:
		return generalIffFn[types.Float4](parameters, result, proc, length, selectList)
	case types.T_uuid:
		return generalIffFn[types.Uuid](parameters, result, proc, length, selectList)
	case types.T_bool:
		return generalIffFn[bool](parameters, result, proc, length, selectList)
	case types.T_date:
		return generalIffFn[types.Date](parameters, result, proc, length, selectList)
	case types.T_datetime:
		return generalIffFn[types.Datetime](parameters, result, proc, length, selectList)
	case types.T_decimal64:
		return generalIffFn[types.Decimal64](parameters, result, proc, length, selectList)
	case types.T_decimal128:
		return generalIffFn[types.Decimal128](parameters, result, proc, length, selectList)
	case types.T_decimal256:
		return generalIffFn[types.Decimal256](parameters, result, proc, length, selectList)
	case types.T_time:
		return generalIffFn[types.Time](parameters, result, proc, length, selectList)
	case types.T_timestamp:
		return generalIffFn[types.Timestamp](parameters, result, proc, length, selectList)
	case types.T_enum:
		return generalIffFn[types.Enum](parameters, result, proc, length, selectList)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_datalink, types.T_json,
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
		types.T_array_float8, types.T_array_float4:
		return strIffFn(parameters, result, proc, length, selectList)
	}
	panic("unreached code")
}

func generalIffFn[T constraints.Integer | constraints.Float | bool | types.Date | types.Datetime |
	types.Decimal64 | types.Decimal128 | types.Decimal256 | types.Timestamp | types.Uuid](vecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p2 := vector.GenerateFunctionFixedTypeParameter[T](vecs[1])
	p3 := vector.GenerateFunctionFixedTypeParameter[T](vecs[2])

	rs := vector.MustFunctionResult[T](result)
	mode := CompatibilityModeFromProcess(proc)
	var zero T
	for i := uint64(0); i < uint64(length); i++ {
		if iffRowSkipped(selectList, i) {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		truth, err := IffConditionTruthyAt(vecs[0], i, mode)
		if err != nil {
			return err
		}
		if truth {
			if err := rs.Append(p2.GetValue(i)); err != nil {
				return err
			}
		} else {
			if err := rs.Append(p3.GetValue(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func strIffFn(vecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p2 := vector.GenerateFunctionStrParameter(vecs[1])
	p3 := vector.GenerateFunctionStrParameter(vecs[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	mode := CompatibilityModeFromProcess(proc)
	for i := uint64(0); i < uint64(length); i++ {
		if iffRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		truth, err := IffConditionTruthyAt(vecs[0], i, mode)
		if err != nil {
			return err
		}
		if truth {
			if err := rs.AppendBytes(p2.GetStrValue(i)); err != nil {
				return err
			}
		} else {
			if err := rs.AppendBytes(p3.GetStrValue(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func iffRowSkipped(selectList *FunctionSelectList, row uint64) bool {
	return selectList != nil && (selectList.IgnoreAllRow() ||
		(!selectList.ShouldEvalAllRow() && selectList.Contains(row)))
}

func operatorUnaryPlus[T constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 | types.Decimal256](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(p1.GetValue(i)); err != nil {
			return err
		}
	}
	return nil
}

func unaryMinusMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		switch inputs[0].Oid {
		case types.T_int8, types.T_int16, types.T_int32:
			// Keep overloads 0/1/2 unchanged for serialized legacy plans. New plans
			// use the historical BIGINT overload after an explicit widening cast,
			// so old workers execute the same physical argument/result contract.
			return newCheckResultWithCast(3, []types.Type{types.T_int64.ToType()})
		case types.T_bf16, types.T_float16, types.T_float8, types.T_float4:
			// The low-precision floats widen losslessly to float32 (overload 4).
			return newCheckResultWithCast(4, []types.Type{types.T_float32.ToType()})
		}
	}
	return fixedTypeMatch(overloads, inputs)
}

// unaryPlusMatch widens a low-precision float operand to float32 (overload 8); every
// other type keeps its existing unary-plus overload.
func unaryPlusMatch(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		switch inputs[0].Oid {
		case types.T_bf16, types.T_float16, types.T_float8, types.T_float4:
			return newCheckResultWithCast(8, []types.Type{types.T_float32.ToType()})
		}
	}
	return fixedTypeMatch(overloads, inputs)
}

func operatorUnaryMinus[T constraints.Signed | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[T](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if selectList != nil && (selectList.IgnoreAllRow() || (!selectList.ShouldEvalAllRow() && selectList.Contains(i))) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if !null {
			if signed, ok := any(v).(int64); ok && signed == math.MinInt64 {
				return moerr.NewOutOfRangeNoCtxf("BIGINT", "unary minus value '%d'", signed)
			}
		}
		if err := rs.Append(-v, null); err != nil {
			return err
		}
	}
	return nil
}

func operatorUnaryMinusDecimal64(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	rs := vector.MustFunctionResult[types.Decimal64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Minus(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorUnaryMinusDecimal128(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](parameters[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Minus(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorUnaryMinusDecimal256(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal256](parameters[0])
	rs := vector.MustFunctionResult[types.Decimal256](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Minus(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func funcBitInversion[T constraints.Integer](x T) uint64 {
	// The numeric unary bit inversion is evaluated in the unsigned 64-bit
	// domain, so widen first and complement once. Complementing at the storage
	// width of T leaked that width for unsigned zero values: ~tinyint_unsigned
	// 0 returned 255 instead of 18446744073709551615 (issue #31905). For
	// signed x the two's-complement sign extension makes ^uint64(x) equal to
	// the old uint64(^x), so their behavior is unchanged.
	return ^uint64(x)
}

func operatorUnaryTilde[T constraints.Integer](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(funcBitInversion(v), null); err != nil {
				return err
			}
		}

	}
	return nil
}

func operatorOpIs(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !parameters[1].IsConst() || parameters[1].IsConstNull() {
		return moerr.NewInternalError(proc.Ctx, "second parameter of IS must be TRUE or FALSE")
	}
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	v2, _ := p2.GetValue(0)
	if v2 {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if err := rs.Append(!null1 && v1, false); err != nil {
				return err
			}
		}
	} else {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if err := rs.Append(!null1 && !v1, false); err != nil {
				return err
			}
		}
	}

	return nil
}

func operatorOpIsNot(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !parameters[1].IsConst() || parameters[1].IsConstNull() {
		return moerr.NewInternalError(proc.Ctx, "second parameter of IS NOT must be TRUE or FALSE")
	}
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	v2, _ := p2.GetValue(0)
	if v2 {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if err := rs.Append(null1 || !v1, false); err != nil {
				return err
			}
		}
	} else {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if err := rs.Append(null1 || v1, false); err != nil {
				return err
			}
		}
	}

	return nil
}

func operatorOpIsNull(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[bool](result)

	if parameters[0].IsConst() {
		val := parameters[0].IsConstNull()
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
		return nil
	}

	null := parameters[0].GetNulls()
	if null.IsEmpty() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(false, false); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(null.Contains(i), false); err != nil {
			return err
		}
	}
	return nil
}

func operatorOpIsNotNull(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[bool](result)

	if parameters[0].IsConst() {
		val := !parameters[0].IsConstNull()
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
		return nil
	}

	null := parameters[0].GetNulls()
	if !null.Any() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(true, false); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(!null.Contains(i), false); err != nil {
			return err
		}
	}
	return nil
}

func operatorIsTrue(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return funcIs(parameters, result, length, false, true)
}

func operatorIsFalse(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return funcIs(parameters, result, length, false, false)
}

func operatorIsNotFalse(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return funcIs(parameters, result, length, true, true)
}

func operatorIsNotTrue(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return funcIs(parameters, result, length, true, false)
}

func funcIs(parameters []*vector.Vector, result vector.FunctionResultWrapper, length int, nullValue bool, require bool) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])

	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		if null1 {
			if err := rs.Append(nullValue, false); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v1 == require, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// boolValueBranchesAsNumeric gives CASE/IF/COALESCE MySQL's numeric meaning
// of TRUE/FALSE when BOOL value branches are mixed with numeric ones:
// COALESCE(2, TRUE) is 2 and IF(c, 1.5e0, TRUE) is 1.5e0. BOOL has no
// implicit cast to a number in fixedCanImplicitCastRule (on purpose, see
// fixedTypeMatchWithBoolNumericCast), so the plain checkers picked BOOL as the
// common type and turned every numeric branch into 0/1.
//
// The common type is resolved as if each BOOL value branch were BIGINT, or
// DECIMAL(1,0) when a DECIMAL branch takes part: TRUE/FALSE only ever are
// 1/0, a single integral digit, so a high-scale DECIMAL branch must not be
// widened by the 19 digits a BIGINT stand-in would claim (DECIMAL(65,58)
// mixed with TRUE would otherwise need 77 digits and fall off DECIMAL256).
// The BOOL branches are then cast to the resolved type. Conditions are left
// alone. If the BOOL branches cannot be cast to the chosen type, the original
// resolution is kept; a mixture the numeric domains cannot resolve (e.g.
// DECIMAL(76,76) with TRUE needs 77 digits) is an error, not a silent fold
// back to BOOL.
func boolValueBranchesAsNumeric(
	overloads []overload,
	inputs []types.Type,
	isValue func(i, n int) bool,
	check func([]overload, []types.Type) checkResult,
) checkResult {
	hasBool, hasNumeric, hasDecimal := false, false, false
	for i, input := range inputs {
		if !isValue(i, len(inputs)) {
			continue
		}
		switch {
		case input.Oid == types.T_bool:
			hasBool = true
		case input.Oid == types.T_any:
		case input.IsNumeric() && input.Oid != types.T_bit:
			hasNumeric = true
			hasDecimal = hasDecimal || input.Oid.IsDecimal()
		default:
			return check(overloads, inputs)
		}
	}
	if !hasBool || !hasNumeric {
		return check(overloads, inputs)
	}

	replacement := types.T_int64.ToType()
	if hasDecimal {
		replacement = types.New(types.T_decimal64, 1, 0)
	}
	normalized := append([]types.Type(nil), inputs...)
	for i := range normalized {
		if isValue(i, len(inputs)) && normalized[i].Oid == types.T_bool {
			normalized[i] = replacement
		}
	}
	matched := check(overloads, normalized)
	var finalTypes []types.Type
	switch matched.status {
	case succeedMatched:
		finalTypes = normalized
	case succeedWithCast:
		finalTypes = append([]types.Type(nil), matched.finalType...)
	default:
		return matched
	}
	for i, input := range inputs {
		if !isValue(i, len(inputs)) || input.Oid != types.T_bool {
			continue
		}
		if target := finalTypes[i]; !target.IsNumeric() || target.Oid == types.T_bit ||
			!IfTypeCastSupported(types.T_bool, target.Oid) {
			return check(overloads, inputs)
		}
	}
	return newCheckResultWithCast(matched.idx, finalTypes)
}
