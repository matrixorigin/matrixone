// Copyright 2021 - 2025 Matrix Origin
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
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type leastGreatestResolution struct {
	resultType       types.Type
	castTypes        []types.Type
	overloadID       int
	comparisonMode   leastGreatestComparisonMode
	temporalItemType types.Type
}

type leastGreatestComparisonMode uint8

type leastGreatestOrdered interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

const (
	leastGreatestCompareNormal leastGreatestComparisonMode = iota
	leastGreatestComparePackedDate
)

const (
	leastGreatestNormalOverload = iota
	leastGreatestTemporalOverload
	leastGreatestJSONTemporalOverload
	leastGreatestYearNumericOverload
)

// check input types for least and greatest function.
// It requires at least 1 input. NULL arguments (T_any) are allowed and produce
// NULL results per MySQL behavior.
func leastGreatestCheck(_ []overload, inputs []types.Type) checkResult {
	resolution, ok := resolveLeastGreatestType(inputs)
	if !ok {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if len(resolution.castTypes) > 0 {
		return newCheckResultWithCast(resolution.overloadID, resolution.castTypes)
	}
	return newCheckResultWithSuccess(resolution.overloadID)
}

func leastGreatestReturnType(parameters []types.Type) types.Type {
	resolution, ok := resolveLeastGreatestType(parameters)
	if ok {
		return resolution.resultType
	}
	for _, p := range parameters {
		if p.Oid != types.T_any {
			return p
		}
	}
	return types.T_varchar.ToType()
}

func resolveLeastGreatestType(inputs []types.Type) (leastGreatestResolution, bool) {
	if len(inputs) < 1 {
		return leastGreatestResolution{}, false
	}

	// Collect the non-NULL (non-T_any) argument types. A NULL literal is typed
	// T_any and always evaluates to NULL regardless of the resolved type.
	nonNull := make([]types.Type, 0, len(inputs))
	for i := range inputs {
		if inputs[i].Oid != types.T_any {
			nonNull = append(nonNull, inputs[i])
		}
	}
	// All arguments are NULL literals: nothing to compare, evaluate as varchar.
	if len(nonNull) == 0 {
		return leastGreatestResolution{resultType: types.T_varchar.ToType(), overloadID: leastGreatestNormalOverload}, true
	}

	if allLeastGreatestJSON(nonNull) {
		target := types.T_varchar.ToType()
		return leastGreatestResolution{
			resultType: target,
			castTypes:  leastGreatestCastTypes(inputs, target),
			overloadID: leastGreatestNormalOverload,
		}, true
	}

	if hasLeastGreatestEnum(nonNull) {
		normalized := append([]types.Type(nil), inputs...)
		for i := range normalized {
			if normalized[i].Oid == types.T_enum {
				normalized[i] = types.T_varchar.ToType()
			}
		}
		resolution, ok := resolveLeastGreatestType(normalized)
		if !ok {
			return leastGreatestResolution{}, false
		}
		if len(resolution.castTypes) == 0 {
			resolution.castTypes = leastGreatestCastTypes(inputs, resolution.resultType)
		}
		return resolution, true
	}

	// Fast path: every non-NULL argument already shares the same type Oid. This
	// preserves the original behavior (including any per-type scale handling) for
	// the common case where no promotion is needed.
	baseOid := nonNull[0].Oid
	sameOid := true
	for i := 1; i < len(nonNull); i++ {
		if nonNull[i].Oid != baseOid {
			sameOid = false
			break
		}
	}
	if sameOid {
		if !leastGreatestExecutorSupportsOid(baseOid) {
			return leastGreatestResolution{}, false
		}
		if target, ok := leastGreatestSameOidAlignedType(nonNull); ok {
			return leastGreatestResolution{
				resultType: target,
				castTypes:  leastGreatestCastTypes(inputs, target),
				overloadID: leastGreatestNormalOverload,
			}, true
		}
		return leastGreatestResolution{resultType: nonNull[0], overloadID: leastGreatestNormalOverload}, true
	}

	if hasUnsupportedLeastGreatestMixedType(nonNull) {
		return leastGreatestResolution{}, false
	}

	if hasLeastGreatestJSON(nonNull) && hasLeastGreatestDateBearingTemporal(nonNull) {
		target, ok := leastGreatestJSONDateTemporalType(nonNull)
		if !ok {
			return leastGreatestResolution{}, false
		}
		resolution := leastGreatestPackedDateResolution(inputs, target)
		resolution.overloadID = leastGreatestJSONTemporalOverload
		return resolution, true
	}

	if hasLeastGreatestJSON(nonNull) {
		target, ok := leastGreatestJSONMixedType(nonNull)
		if !ok {
			return leastGreatestResolution{}, false
		}
		return leastGreatestResolution{
			resultType: target,
			castTypes:  leastGreatestCastTypes(inputs, target),
			overloadID: leastGreatestNormalOverload,
		}, true
	}

	if hasLeastGreatestDateBearingTemporal(nonNull) {
		target, ok := leastGreatestDateTemporalMixedType(nonNull)
		if !ok {
			return leastGreatestResolution{}, false
		}
		return leastGreatestPackedDateResolution(inputs, target), true
	}

	if hasLeastGreatestYear(nonNull) {
		if target, ok := leastGreatestCommonNumericOrYearType(nonNull); ok {
			return leastGreatestYearNumericResolution(inputs, target), true
		}
	}

	if target, ok := leastGreatestCommonNumericType(nonNull); ok {
		return leastGreatestResolution{
			resultType: target,
			castTypes:  leastGreatestCastTypes(inputs, target),
			overloadID: leastGreatestNormalOverload,
		}, true
	}

	if target, ok := leastGreatestStringMixedType(nonNull); ok {
		return leastGreatestResolution{
			resultType: target,
			castTypes:  leastGreatestCastTypes(inputs, target),
			overloadID: leastGreatestNormalOverload,
		}, true
	}

	return leastGreatestResolution{}, false
}

func leastGreatestCastTypes(inputs []types.Type, target types.Type) []types.Type {
	castType := make([]types.Type, len(inputs))
	for i := range castType {
		castType[i] = target
	}
	return castType
}

func leastGreatestPackedDateResolution(inputs []types.Type, target types.Type) leastGreatestResolution {
	castTypes := make([]types.Type, len(inputs))
	for i := range inputs {
		if inputs[i].Oid == types.T_any || isLeastGreatestTemporal(inputs[i].Oid) {
			castTypes[i] = inputs[i]
		} else {
			castTypes[i] = target
		}
	}
	return leastGreatestResolution{
		resultType:       target,
		castTypes:        castTypes,
		overloadID:       leastGreatestTemporalOverload,
		comparisonMode:   leastGreatestComparePackedDate,
		temporalItemType: leastGreatestTemporalItemType(inputs),
	}
}

func leastGreatestYearNumericResolution(inputs []types.Type, target types.Type) leastGreatestResolution {
	castTypes := make([]types.Type, len(inputs))
	for i := range inputs {
		if inputs[i].Oid == types.T_year {
			castTypes[i] = inputs[i]
		} else {
			castTypes[i] = target
		}
	}
	return leastGreatestResolution{
		resultType: target,
		castTypes:  castTypes,
		overloadID: leastGreatestYearNumericOverload,
	}
}

func allLeastGreatestJSON(inputs []types.Type) bool {
	for i := range inputs {
		if inputs[i].Oid != types.T_json {
			return false
		}
	}
	return true
}

func hasLeastGreatestJSON(inputs []types.Type) bool {
	for i := range inputs {
		if inputs[i].Oid == types.T_json {
			return true
		}
	}
	return false
}

func hasLeastGreatestYear(inputs []types.Type) bool {
	for i := range inputs {
		if inputs[i].Oid == types.T_year {
			return true
		}
	}
	return false
}

func hasLeastGreatestEnum(inputs []types.Type) bool {
	for i := range inputs {
		if inputs[i].Oid == types.T_enum {
			return true
		}
	}
	return false
}

func leastGreatestExecutorSupportsOid(oid types.T) bool {
	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_uuid, types.T_float32, types.T_float64,
		types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary,
		types.T_array_float32, types.T_array_float64,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp, types.T_year,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_Rowid:
		return true
	default:
		return false
	}
}

func leastGreatestSameOidAlignedType(inputs []types.Type) (types.Type, bool) {
	target := inputs[0]
	switch target.Oid {
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		if !leastGreatestMetadataDiffers(inputs) {
			return types.Type{}, false
		}
		if !setSafeDecimalWidthAndScaleFromSource(&target, inputs) {
			return types.T_float64.ToType(), true
		}
		return target, true
	case types.T_time, types.T_datetime, types.T_timestamp:
		if !leastGreatestMetadataDiffers(inputs) {
			return types.Type{}, false
		}
		setMaxScaleFromSource(&target, inputs)
		return target, true
	default:
		return types.Type{}, false
	}
}

func leastGreatestMetadataDiffers(inputs []types.Type) bool {
	for i := 1; i < len(inputs); i++ {
		if inputs[i].Width != inputs[0].Width || inputs[i].Scale != inputs[0].Scale {
			return true
		}
	}
	return false
}

func hasUnsupportedLeastGreatestMixedType(inputs []types.Type) bool {
	for i := range inputs {
		switch inputs[i].Oid {
		case types.T_interval, types.T_uuid, types.T_Rowid,
			types.T_array_float32, types.T_array_float64,
			types.T_geometry, types.T_geometry32, types.T_datalink:
			return true
		}
	}
	return false
}

func hasLeastGreatestDateBearingTemporal(inputs []types.Type) bool {
	for i := range inputs {
		if isLeastGreatestDateBearingTemporal(inputs[i].Oid) {
			return true
		}
	}
	return false
}

func isLeastGreatestDateBearingTemporal(oid types.T) bool {
	return oid == types.T_date || oid == types.T_datetime || oid == types.T_timestamp
}

func isLeastGreatestTemporal(oid types.T) bool {
	return isLeastGreatestDateBearingTemporal(oid) || oid == types.T_time || oid == types.T_year
}

func leastGreatestTemporalItemType(inputs []types.Type) types.Type {
	var best types.Type
	bestRank := -1
	for i := range inputs {
		rank := leastGreatestTemporalRank(inputs[i].Oid)
		if rank > bestRank {
			bestRank = rank
			best = inputs[i]
		}
	}
	return best
}

func leastGreatestTemporalRank(oid types.T) int {
	switch oid {
	case types.T_datetime:
		return 5
	case types.T_timestamp:
		return 4
	case types.T_date:
		return 3
	case types.T_time:
		return 2
	case types.T_year:
		return 1
	default:
		return -1
	}
}

func leastGreatestJSONDateTemporalType(inputs []types.Type) (types.Type, bool) {
	for i := range inputs {
		switch inputs[i].Oid {
		case types.T_json, types.T_char, types.T_varchar,
			types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_year:
		default:
			if !inputs[i].IsNumeric() {
				return types.Type{}, false
			}
		}
	}
	return types.T_varchar.ToType(), true
}

func leastGreatestDateTemporalMixedType(inputs []types.Type) (types.Type, bool) {
	allTemporal := true
	hasText, hasNonBinary, hasBlob, hasBinary, hasNumeric := false, false, false, false, false
	for i := range inputs {
		switch inputs[i].Oid {
		case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_year:
		case types.T_text:
			allTemporal = false
			hasText = true
		case types.T_char, types.T_varchar:
			allTemporal = false
			hasNonBinary = true
		case types.T_blob:
			allTemporal = false
			hasBlob = true
		case types.T_binary, types.T_varbinary:
			allTemporal = false
			hasBinary = true
		default:
			if !inputs[i].IsNumeric() {
				return types.Type{}, false
			}
			allTemporal = false
			hasNumeric = true
		}
	}
	switch {
	case allTemporal:
		return types.T_datetime.ToType(), true
	case hasText:
		return types.T_text.ToType(), true
	case hasNonBinary || hasNumeric:
		return types.T_varchar.ToType(), true
	case hasBlob:
		return types.T_blob.ToType(), true
	case hasBinary:
		return types.T_varbinary.ToType(), true
	default:
		return types.Type{}, false
	}
}

func leastGreatestJSONMixedType(inputs []types.Type) (types.Type, bool) {
	hasText := false
	for i := range inputs {
		switch inputs[i].Oid {
		case types.T_binary, types.T_varbinary, types.T_blob:
			return types.Type{}, false
		case types.T_text:
			hasText = true
		case types.T_json, types.T_char, types.T_varchar, types.T_time, types.T_year:
		default:
			if !inputs[i].IsNumeric() {
				return types.Type{}, false
			}
		}
	}
	if hasText {
		return types.T_text.ToType(), true
	}
	return types.T_varchar.ToType(), true
}

func leastGreatestCommonNumericOrYearType(inputs []types.Type) (types.Type, bool) {
	numericInputs := make([]types.Type, len(inputs))
	for i := range inputs {
		if inputs[i].Oid == types.T_year {
			numericInputs[i] = types.T_uint16.ToType()
			continue
		}
		if !inputs[i].IsNumeric() {
			return types.Type{}, false
		}
		numericInputs[i] = inputs[i]
	}
	return leastGreatestCommonNumericType(numericInputs)
}

func leastGreatestStringMixedType(inputs []types.Type) (types.Type, bool) {
	hasText, hasNonBinary, hasBlob, hasBinary, hasTemporal, hasNumeric := false, false, false, false, false, false
	for i := range inputs {
		switch inputs[i].Oid {
		case types.T_text:
			hasText = true
		case types.T_char, types.T_varchar:
			hasNonBinary = true
		case types.T_blob:
			hasBlob = true
		case types.T_binary, types.T_varbinary:
			hasBinary = true
		case types.T_date, types.T_datetime, types.T_timestamp, types.T_time, types.T_year:
			hasTemporal = true
		default:
			if !inputs[i].IsNumeric() {
				return types.Type{}, false
			}
			hasNumeric = true
		}
	}
	switch {
	case hasText:
		return types.T_text.ToType(), true
	case hasNonBinary:
		return types.T_varchar.ToType(), true
	case hasTemporal && hasNumeric:
		return types.T_varchar.ToType(), true
	case hasBlob:
		return types.T_blob.ToType(), true
	case hasBinary:
		return types.T_varbinary.ToType(), true
	case hasTemporal:
		return types.T_varchar.ToType(), true
	default:
		return types.Type{}, false
	}
}

// leastGreatestCommonNumericType derives the common type used to compare a set
// of mixed numeric arguments to LEAST/GREATEST. It follows MySQL's promotion
// rules: any floating-point operand makes the result DOUBLE; otherwise any
// DECIMAL operand makes the result a DECIMAL wide enough to hold every operand;
// otherwise the operands are integers and the narrowest integer type that holds
// them all is chosen. It returns (_, false) if any argument is not numeric.
func leastGreatestCommonNumericType(inputs []types.Type) (types.Type, bool) {
	hasFloat, hasDecimal, hasSigned, hasUnsigned := false, false, false, false
	for i := range inputs {
		t := inputs[i]
		switch {
		case t.IsFloat():
			hasFloat = true
		case t.IsDecimal():
			hasDecimal = true
		case t.IsInt():
			hasSigned = true
		case t.IsUInt(), t.Oid == types.T_bit:
			hasUnsigned = true
		default:
			// non-numeric argument: cannot promote
			return types.Type{}, false
		}
	}

	// 1. Any floating-point argument: compare as DOUBLE.
	if hasFloat {
		return types.T_float64.ToType(), true
	}

	// 2. Any DECIMAL argument: widen to a decimal that holds every decimal and
	//    integer operand without losing integral digits or scale, promoting to
	//    DECIMAL128/256 as needed. Fall back to DOUBLE if the required precision
	//    exceeds DECIMAL256.
	if hasDecimal {
		target := types.T_decimal64.ToType()
		for i := range inputs {
			if inputs[i].Oid == types.T_decimal256 {
				target.Oid = types.T_decimal256
				break
			}
			if inputs[i].Oid == types.T_decimal128 {
				target.Oid = types.T_decimal128
			}
		}
		target.Size = int32(target.Oid.TypeLen())
		if !setSafeDecimalWidthAndScaleFromSource(&target, inputs) {
			return types.T_float64.ToType(), true
		}
		return target, true
	}

	// 3. Integer-only operands (including BIT). Choose the narrowest integer type
	//    that holds every operand. Mixing signed and unsigned operands that do
	//    not fit a signed 64-bit integer widens to DECIMAL128 to stay lossless.
	maxWidth := int32(0)
	for i := range inputs {
		w := integerIntegralWidth(inputs[i].Oid)
		if inputs[i].Oid == types.T_bit {
			w = 20 // BIT holds up to a 64-bit unsigned value
		}
		if w > maxWidth {
			maxWidth = w
		}
	}
	switch {
	case hasSigned && hasUnsigned:
		// uint32 (10 digits) and narrower fit losslessly in int64 (19 digits).
		if maxWidth <= 10 {
			return types.T_int64.ToType(), true
		}
		// A uint64/BIT operand alongside a signed operand cannot fit any signed
		// integer; use DECIMAL128, which holds the full unsigned 64-bit range.
		dt := types.T_decimal128.ToType()
		dt.Scale = 0
		dt.Width = 20
		dt.Size = int32(types.T_decimal128.TypeLen())
		return dt, true
	case hasUnsigned:
		return unsignedTypeForWidth(maxWidth), true
	default:
		return signedTypeForWidth(maxWidth), true
	}
}

// signedTypeForWidth returns the narrowest signed integer type whose integral
// width covers w decimal digits.
func signedTypeForWidth(w int32) types.Type {
	switch {
	case w <= 3:
		return types.T_int8.ToType()
	case w <= 5:
		return types.T_int16.ToType()
	case w <= 10:
		return types.T_int32.ToType()
	default:
		return types.T_int64.ToType()
	}
}

// unsignedTypeForWidth returns the narrowest unsigned integer type whose
// integral width covers w decimal digits.
func unsignedTypeForWidth(w int32) types.Type {
	switch {
	case w <= 3:
		return types.T_uint8.ToType()
	case w <= 5:
		return types.T_uint16.ToType()
	case w <= 10:
		return types.T_uint32.ToType()
	default:
		return types.T_uint64.ToType()
	}
}

func leastGreatestFnFixed[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 T) bool) error {
	rs := vector.MustFunctionResult[T](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColWithTypeCheck[T](rsVec)
	rsNull := rsVec.GetNulls()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			return leastGreatestSetNullResult(result, length)
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for np, pv := range parameters {
		p := vector.GenerateFunctionFixedTypeParameter[T](pv)
		if pv.IsConstNull() {
			return leastGreatestSetNullResult(result, length)
		}
		if p.WithAnyNullValue() || rsAnyNull {
			nulls.Or(rsNull, pv.GetNulls(), rsNull)
		}

		for i := 0; i < length; i++ {
			if rsNull.Contains(uint64(i)) {
				continue
			}

			v, _ := p.GetValue(uint64(i))
			if np == 0 {
				rss[i] = v
			} else {
				if compareFn(v, rss[i]) {
					rss[i] = v
				}
			}
		}
	}
	return nil
}

func leastGreatestFnVarlen(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 []byte) bool) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()
	rsNull := rsVec.GetNulls()

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			return leastGreatestSetNullResult(result, length)
		}
		if !selectList.ShouldEvalAllRow() {
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for _, pv := range parameters {
		if pv.IsConstNull() {
			return leastGreatestSetNullResult(result, length)
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		var v []byte
		var isNull bool

		if selectList != nil && selectList.ShouldEvalAllRow() {
			if selectList.Contains(i) {
				rs.AppendBytes(nil, true)
				continue
			}
		}

		for _, pv := range parameters {
			if pv.IsNull(i) {
				isNull = true
				break
			} else {
				vv := pv.GetBytesAt(int(i))
				if v == nil {
					v = vv
				} else {
					if compareFn(vv, v) {
						v = vv
					}
				}
			}
		}
		rs.AppendBytes(v, isNull)
	}

	return nil
}

func leastGreatestYearNumericFn(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(int) bool) error {
	target := *result.GetResultVector().GetType()
	switch target.Oid {
	case types.T_int8:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (int8, error) {
			return int8(y), nil
		}, func(v1, v2 int8) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_int16:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (int16, error) {
			return int16(y), nil
		}, func(v1, v2 int16) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_int32:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (int32, error) {
			return int32(y), nil
		}, func(v1, v2 int32) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_int64:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (int64, error) {
			return y, nil
		}, func(v1, v2 int64) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_uint8:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (uint8, error) {
			return uint8(y), nil
		}, func(v1, v2 uint8) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_uint16:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (uint16, error) {
			return uint16(y), nil
		}, func(v1, v2 uint16) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_uint32:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (uint32, error) {
			return uint32(y), nil
		}, func(v1, v2 uint32) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_uint64:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (uint64, error) {
			return uint64(y), nil
		}, func(v1, v2 uint64) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_float64:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (float64, error) {
			return float64(y), nil
		}, func(v1, v2 float64) bool { return compareFn(compareOrdered(v1, v2)) })
	case types.T_decimal64:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (types.Decimal64, error) {
			return types.ParseDecimal64(strconv.FormatInt(y, 10), target.Width, target.Scale)
		}, func(v1, v2 types.Decimal64) bool { return compareFn(v1.Compare(v2)) })
	case types.T_decimal128:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (types.Decimal128, error) {
			return types.ParseDecimal128(strconv.FormatInt(y, 10), target.Width, target.Scale)
		}, func(v1, v2 types.Decimal128) bool { return compareFn(v1.Compare(v2)) })
	case types.T_decimal256:
		return leastGreatestYearNumericFnFixed(parameters, result, proc, length, selectList, func(y int64) (types.Decimal256, error) {
			return types.ParseDecimal256(strconv.FormatInt(y, 10), target.Width, target.Scale)
		}, func(v1, v2 types.Decimal256) bool { return compareFn(v1.Compare(v2)) })
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported YEAR numeric comparison result type %s", target.Oid.String())
	}
}

func compareOrdered[T leastGreatestOrdered](v1, v2 T) int {
	if v1 < v2 {
		return -1
	}
	if v1 > v2 {
		return 1
	}
	return 0
}

func leastGreatestYearNumericFnFixed[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	yearToTarget func(int64) (T, error),
	compareFn func(v1, v2 T) bool) error {
	rs := vector.MustFunctionResult[T](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColWithTypeCheck[T](rsVec)
	rsNull := rsVec.GetNulls()

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			return leastGreatestSetNullResult(result, length)
		}
		if !selectList.ShouldEvalAllRow() {
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for _, pv := range parameters {
		if pv.IsConstNull() {
			return leastGreatestSetNullResult(result, length)
		}
	}

	yearParams := make([]vector.FunctionParameterWrapper[types.MoYear], len(parameters))
	numericParams := make([]vector.FunctionParameterWrapper[T], len(parameters))
	for i, pv := range parameters {
		if pv.GetType().Oid == types.T_year {
			yearParams[i] = vector.GenerateFunctionFixedTypeParameter[types.MoYear](pv)
		} else {
			numericParams[i] = vector.GenerateFunctionFixedTypeParameter[T](pv)
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if rsNull.Contains(i) {
			continue
		}

		var winner T
		for j, pv := range parameters {
			if pv.IsNull(i) {
				rsNull.Add(i)
				break
			}

			var value T
			if pv.GetType().Oid == types.T_year {
				year, _ := yearParams[j].GetValue(i)
				var err error
				value, err = yearToTarget(year.ToInt64())
				if err != nil {
					return err
				}
			} else {
				value, _ = numericParams[j].GetValue(i)
			}

			if j == 0 || compareFn(value, winner) {
				winner = value
			}
		}
		if !rsNull.Contains(i) {
			rss[i] = winner
		}
	}
	return nil
}

func leastGreatestFnPackedDate(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 types.Datetime) bool) error {
	resolution, ok := resolveLeastGreatestType(vectorTypes(parameters))
	if !ok || resolution.comparisonMode != leastGreatestComparePackedDate {
		return moerr.NewInternalErrorNoCtx("unsupported temporal comparison resolution")
	}
	return leastGreatestFnPackedDateResolved(parameters, result, proc, length, selectList, resolution, compareFn)
}

func leastGreatestFnJSONPackedDate(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	compareFn func(v1, v2 types.Datetime) bool) error {
	resolution := leastGreatestResolution{
		resultType:       types.T_varchar.ToType(),
		comparisonMode:   leastGreatestComparePackedDate,
		temporalItemType: leastGreatestTemporalItemType(vectorTypes(parameters)),
	}
	return leastGreatestFnPackedDateResolved(parameters, result, proc, length, selectList, resolution, compareFn)
}

func leastGreatestFnPackedDateResolved(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	resolution leastGreatestResolution,
	compareFn func(v1, v2 types.Datetime) bool) error {
	rsVec := result.GetResultVector()
	rsNull := rsVec.GetNulls()
	if selectList != nil {
		if selectList.IgnoreAllRow() {
			return leastGreatestSetNullResult(result, length)
		}
		if !selectList.ShouldEvalAllRow() {
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	for _, pv := range parameters {
		if pv.IsConstNull() {
			return leastGreatestSetNullResult(result, length)
		}
	}

	loc := time.Local
	if proc != nil && proc.GetSessionInfo() != nil && proc.GetSessionInfo().TimeZone != nil {
		loc = proc.GetSessionInfo().TimeZone
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := leastGreatestAppendPackedDateResult(result, resolution, types.Datetime(0), loc, true); err != nil {
				return err
			}
			continue
		}
		if rsNull.Contains(i) {
			if err := leastGreatestAppendPackedDateResult(result, resolution, types.Datetime(0), loc, true); err != nil {
				return err
			}
			continue
		}

		var winner types.Datetime
		for j, pv := range parameters {
			if pv.IsNull(i) {
				if err := leastGreatestAppendPackedDateResult(result, resolution, types.Datetime(0), loc, true); err != nil {
					return err
				}
				goto nextRow
			}
			v, err := leastGreatestDatetimeValue(pv, i, resolution.temporalItemType, proc, loc)
			if err != nil {
				return err
			}
			if j == 0 || compareFn(v, winner) {
				winner = v
			}
		}
		if err := leastGreatestAppendPackedDateResult(result, resolution, winner, loc, false); err != nil {
			return err
		}
	nextRow:
	}
	return nil
}

func vectorTypes(parameters []*vector.Vector) []types.Type {
	ts := make([]types.Type, len(parameters))
	for i := range parameters {
		ts[i] = *parameters[i].GetType()
	}
	return ts
}

func leastGreatestSetNullResult(result vector.FunctionResultWrapper, length int) error {
	if result.GetResultVector().GetType().IsVarlen() {
		rs := vector.MustFunctionResult[types.Varlena](result)
		rs.SetNullResult(uint64(length))
		return nil
	}
	nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
	return nil
}

func leastGreatestDatetimeValue(v *vector.Vector, row uint64, temporalItemType types.Type, proc *process.Process, loc *time.Location) (types.Datetime, error) {
	switch v.GetType().Oid {
	case types.T_date:
		p := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		val, _ := p.GetValue(row)
		return val.ToDatetime(), nil
	case types.T_datetime:
		p := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		val, _ := p.GetValue(row)
		return val, nil
	case types.T_timestamp:
		p := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
		val, _ := p.GetValue(row)
		return val.ToDatetime(loc), nil
	case types.T_time:
		p := vector.GenerateFunctionFixedTypeParameter[types.Time](v)
		val, _ := p.GetValue(row)
		return leastGreatestTimeToDatetime(val, v.GetType().Scale, proc, loc), nil
	case types.T_year:
		p := vector.GenerateFunctionFixedTypeParameter[types.MoYear](v)
		val, _ := p.GetValue(row)
		return types.DatetimeFromClock(int32(val.ToInt64()), 1, 1, 0, 0, 0, 0), nil
	case types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary:
		return leastGreatestParsePackedDateBytes(v.GetBytesAt(int(row)), temporalItemType, proc, loc)
	default:
		return types.Datetime(0), moerr.NewInternalErrorNoCtxf("unsupported temporal comparison type %s", v.GetType().Oid.String())
	}
}

// leastGreatestTimeToDatetime follows MySQL's temporal comparison rule: when
// TIME must be compared as a datetime, it is combined with the statement's
// start date in the session time zone. Do not use Time.ToDatetime here: that
// helper uses the instantaneous UTC date, which can differ within a query and
// from the session-local date.
func leastGreatestTimeToDatetime(value types.Time, scale int32, proc *process.Process, loc *time.Location) types.Datetime {
	if loc == nil {
		loc = time.Local
	}

	queryStart := time.Time{}
	if proc != nil {
		queryStart = proc.GetStmtProfile().GetQueryStart()
		if queryStart.IsZero() && proc.GetUnixTime() != 0 {
			queryStart = time.Unix(0, proc.GetUnixTime())
		}
	}
	if queryStart.IsZero() {
		queryStart = time.Now()
	}
	queryStart = queryStart.In(loc)

	base := types.DatetimeFromClock(
		int32(queryStart.Year()),
		uint8(queryStart.Month()),
		uint8(queryStart.Day()),
		0, 0, 0, 0,
	)
	return base + types.Datetime(value.TruncateToScale(scale))
}

func leastGreatestParsePackedDateBytes(v []byte, temporalItemType types.Type, proc *process.Process, loc *time.Location) (types.Datetime, error) {
	s := string(v)
	switch temporalItemType.Oid {
	case types.T_date:
		d, err := types.ParseDateCast(s)
		if err != nil {
			return types.Datetime(0), err
		}
		return d.ToDatetime(), nil
	case types.T_datetime:
		return types.ParseDatetime(s, temporalItemType.Scale)
	case types.T_timestamp:
		ts, err := types.ParseTimestamp(loc, s, temporalItemType.Scale)
		if err != nil {
			return types.Datetime(0), err
		}
		return ts.ToDatetime(loc), nil
	case types.T_time:
		t, err := types.ParseTime(s, temporalItemType.Scale)
		if err != nil {
			return types.Datetime(0), err
		}
		return leastGreatestTimeToDatetime(t, temporalItemType.Scale, proc, loc), nil
	default:
		return types.Datetime(0), moerr.NewInternalErrorNoCtxf("unsupported temporal comparison target %s", temporalItemType.Oid.String())
	}
}

func leastGreatestAppendPackedDateResult(
	result vector.FunctionResultWrapper,
	resolution leastGreatestResolution,
	value types.Datetime,
	loc *time.Location,
	isNull bool) error {
	switch resolution.resultType.Oid {
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return rs.Append(value, isNull)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return rs.Append(value.ToDate(), isNull)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return rs.Append(value.ToTimestamp(loc), isNull)
	case types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return rs.AppendBytes([]byte(leastGreatestFormatPackedDate(value, resolution.temporalItemType, loc)), isNull)
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported temporal comparison result type %s", resolution.resultType.Oid.String())
	}
}

func leastGreatestFormatPackedDate(value types.Datetime, temporalItemType types.Type, loc *time.Location) string {
	switch temporalItemType.Oid {
	case types.T_date:
		return value.ToDate().String()
	case types.T_time:
		return value.ToTime(temporalItemType.Scale).String2(temporalItemType.Scale)
	case types.T_timestamp:
		return value.ToTimestamp(loc).String2(loc, temporalItemType.Scale)
	case types.T_year:
		year, _, _, _ := value.ToDate().Calendar(true)
		return fmt.Sprintf("%04d", year)
	default:
		return value.String2(temporalItemType.Scale)
	}
}

// leastGreatestParamType finds the first non-T_any parameter type.
// If all parameters are T_any (all NULL constants), returns T_varchar.
func leastGreatestParamType(parameters []*vector.Vector) types.Type {
	for _, p := range parameters {
		if p.GetType().Oid != types.T_any {
			return *p.GetType()
		}
	}
	return types.T_varchar.ToType()
}

// leastGreatestRestoreTemporalResultType keeps runtime result metadata aligned
// with the resolver. Some execution paths can allocate the result wrapper
// before the aligned TIME/DATETIME/TIMESTAMP scale reaches it.
func leastGreatestRestoreTemporalResultType(parameters []*vector.Vector, result vector.FunctionResultWrapper) {
	resolution, ok := resolveLeastGreatestType(vectorTypes(parameters))
	if !ok {
		return
	}

	switch resolution.resultType.Oid {
	case types.T_time, types.T_datetime, types.T_timestamp:
		resultType := result.GetResultVector().GetType()
		if resultType.Oid == resolution.resultType.Oid && !resultType.Eq(resolution.resultType) {
			result.GetResultVector().SetType(resolution.resultType)
		}
	}
}

func leastFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	leastGreatestRestoreTemporalResultType(parameters, result)
	paramType := leastGreatestParamType(parameters)
	switch paramType.Oid {
	case types.T_bool:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 bool) bool {
				return !v1 && v2
			})

	case types.T_bit:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 < v2
			})

	case types.T_int8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int8) bool {
				return v1 < v2
			})
	case types.T_int16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int16) bool {
				return v1 < v2
			})
	case types.T_int32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int32) bool {
				return v1 < v2
			})
	case types.T_int64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int64) bool {
				return v1 < v2
			})
	case types.T_uint8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint8) bool {
				return v1 < v2
			})
	case types.T_uint16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint16) bool {
				return v1 < v2
			})
	case types.T_uint32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint32) bool {
				return v1 < v2
			})
	case types.T_uint64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 < v2
			})

	case types.T_uuid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Uuid) bool {
				return types.CompareUuid(v1, v2) < 0
			})

	case types.T_float32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float32) bool {
				return v1 < v2
			})

	case types.T_float64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float64) bool {
				return v1 < v2
			})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) < 0
			})

	case types.T_binary, types.T_varbinary:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) < 0
			})

	case types.T_array_float32:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float32](v1)
				_v2 := types.BytesToArray[float32](v2)

				return types.ArrayCompare[float32](_v1, _v2) < 0
			})

	case types.T_array_float64:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float64](v1)
				_v2 := types.BytesToArray[float64](v2)
				return types.ArrayCompare[float64](_v1, _v2) < 0
			})

	case types.T_date:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Date) bool {
				return v1 < v2
			})

	case types.T_datetime:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Datetime) bool {
				return v1 < v2
			})

	case types.T_time:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Time) bool {
				return v1 < v2
			})

	case types.T_timestamp:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Timestamp) bool {
				return v1 < v2
			})

	case types.T_year:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.MoYear) bool {
				return v1 < v2
			})

	case types.T_decimal64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal64) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_decimal128:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal128) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_decimal256:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal256) bool {
				return v1.Compare(v2) < 0
			})

	case types.T_Rowid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Rowid) bool {
				return v1.LT(&v2)
			})
	}
	panic("unreached code")
}

func leastTemporalFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestFnPackedDate(parameters, result, proc, length, selectList, func(v1, v2 types.Datetime) bool {
		return v1 < v2
	})
}

func leastJSONTemporalFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestFnJSONPackedDate(parameters, result, proc, length, selectList, func(v1, v2 types.Datetime) bool {
		return v1 < v2
	})
}

func leastYearNumericFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestYearNumericFn(parameters, result, proc, length, selectList, func(comparison int) bool {
		return comparison < 0
	})
}

func greatestFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	leastGreatestRestoreTemporalResultType(parameters, result)
	paramType := leastGreatestParamType(parameters)
	switch paramType.Oid {
	case types.T_bool:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 bool) bool {
				return v1 && !v2
			})

	case types.T_bit:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 > v2
			})

	case types.T_int8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int8) bool {
				return v1 > v2
			})
	case types.T_int16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int16) bool {
				return v1 > v2
			})
	case types.T_int32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int32) bool {
				return v1 > v2
			})
	case types.T_int64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 int64) bool {
				return v1 > v2
			})
	case types.T_uint8:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint8) bool {
				return v1 > v2
			})
	case types.T_uint16:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint16) bool {
				return v1 > v2
			})
	case types.T_uint32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint32) bool {
				return v1 > v2
			})
	case types.T_uint64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 uint64) bool {
				return v1 > v2
			})

	case types.T_uuid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Uuid) bool {
				return types.CompareUuid(v1, v2) > 0
			})

	case types.T_float32:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float32) bool {
				return v1 > v2
			})

	case types.T_float64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 float64) bool {
				return v1 > v2
			})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) > 0
			})

	case types.T_binary, types.T_varbinary:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				return bytes.Compare(v1, v2) > 0
			})

	case types.T_array_float32:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float32](v1)
				_v2 := types.BytesToArray[float32](v2)
				return types.ArrayCompare[float32](_v1, _v2) > 0
			})

	case types.T_array_float64:
		return leastGreatestFnVarlen(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 []byte) bool {
				_v1 := types.BytesToArray[float64](v1)
				_v2 := types.BytesToArray[float64](v2)
				return types.ArrayCompare[float64](_v1, _v2) > 0
			})

	case types.T_date:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Date) bool {
				return v1 > v2
			})

	case types.T_datetime:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Datetime) bool {
				return v1 > v2
			})

	case types.T_time:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Time) bool {
				return v1 > v2
			})

	case types.T_timestamp:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Timestamp) bool {
				return v1 > v2
			})

	case types.T_year:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.MoYear) bool {
				return v1 > v2
			})

	case types.T_decimal64:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal64) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_decimal128:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal128) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_decimal256:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Decimal256) bool {
				return v1.Compare(v2) > 0
			})

	case types.T_Rowid:
		return leastGreatestFnFixed(
			parameters,
			result,
			proc,
			length,
			selectList,
			func(v1, v2 types.Rowid) bool {
				return v1.GT(&v2)
			})
	}
	panic("unreached code")
}

func greatestTemporalFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestFnPackedDate(parameters, result, proc, length, selectList, func(v1, v2 types.Datetime) bool {
		return v1 > v2
	})
}

func greatestJSONTemporalFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestFnJSONPackedDate(parameters, result, proc, length, selectList, func(v1, v2 types.Datetime) bool {
		return v1 > v2
	})
}

func greatestYearNumericFn(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	return leastGreatestYearNumericFn(parameters, result, proc, length, selectList, func(comparison int) bool {
		return comparison > 0
	})
}
