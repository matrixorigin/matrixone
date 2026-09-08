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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// comparePreparedJSON applies the SQL category carried by a binary-protocol
// parameter. The adapter represents that parameter as ByteJSON only so the
// prepared plan has a stable physical type; comparison still follows the same
// cast contract as the equivalent typed SQL literal.
func comparePreparedJSON(
	parameters []*vector.Vector,
	result *vector.FunctionResult[bool],
	proc *process.Process,
	length int,
	nullSafe bool,
	cmp func(int) bool,
	selectList *FunctionSelectList,
) error {
	leftIsParam := parameters[0].IsPreparedJSONComparisonParam()
	rightIsParam := parameters[1].IsPreparedJSONComparisonParam()
	if leftIsParam == rightIsParam {
		return moerr.NewInternalError(proc.Ctx, "prepared JSON comparison requires exactly one typed parameter")
	}

	jsonPos, paramPos := 0, 1
	if leftIsParam {
		jsonPos, paramPos = 1, 0
	}
	jsonVector := parameters[jsonPos]
	paramVector := parameters[paramPos]
	paramType := paramVector.GetPrepareParamType()
	rss := vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())
	resultNulls := result.GetResultVector().GetNulls()

	if selectList != nil && selectList.IgnoreAllRow() {
		nulls.AddRange(resultNulls, 0, uint64(length))
		return nil
	}
	if paramType != types.T_any {
		expectedKind, ok := vector.PrepareParamKindForType(paramType)
		if !ok || expectedKind != paramVector.GetPrepareParamKind() {
			return moerr.NewInternalErrorf(
				proc.Ctx,
				"prepared parameter type %s does not match conversion kind %d",
				paramType.String(), paramVector.GetPrepareParamKind(),
			)
		}
	}

	var (
		cachedJSON              bytejson.ByteJson
		cachedJSONNull          bool
		cachedJSONReady         bool
		cachedParam             bytejson.ByteJson
		cachedParamNull         bool
		cachedParamReady        bool
		cachedDecimalParam      bytejson.ParsedNumeric
		cachedDecimalParamReady bool
	)
	if jsonVector.IsConst() {
		var err error
		cachedJSON, cachedJSONNull, err = comparisonJSONAt(proc, jsonVector, 0)
		if err != nil {
			return err
		}
		cachedJSONReady = true
	}
	if paramVector.IsConst() {
		var err error
		cachedParam, cachedParamNull, err = comparisonJSONAt(proc, paramVector, 0)
		if err != nil {
			return err
		}
		cachedParamReady = true
	}
	if cachedParamReady && !cachedParamNull && paramType == types.T_any &&
		paramVector.GetPrepareParamKind() == vector.PrepareParamDecimal &&
		!isJSONLiteralNull(cachedParam) {
		numericParam, paramIsNull, ok := preparedJSONDecimal(cachedParam)
		if !ok || paramIsNull {
			return jsonCastErr(proc.Ctx, types.T_decimal256)
		}
		cachedDecimalParam, ok = bytejson.ParseNumeric(numericParam)
		if !ok {
			return jsonCastErr(proc.Ctx, types.T_decimal256)
		}
		cachedDecimalParamReady = true
	}

	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			resultNulls.Add(uint64(row))
			continue
		}

		jsonValue, jsonNull := cachedJSON, cachedJSONNull
		if !cachedJSONReady {
			var err error
			jsonValue, jsonNull, err = comparisonJSONAt(proc, jsonVector, row)
			if err != nil {
				return err
			}
		}
		paramValue, paramNull := cachedParam, cachedParamNull
		if !cachedParamReady {
			var err error
			paramValue, paramNull, err = comparisonJSONAt(proc, paramVector, row)
			if err != nil {
				return err
			}
		}

		// JSON null participates in the same SQL-null comparison contract as a
		// JSON-to-scalar cast. Account for it before short-circuiting on a NULL
		// parameter so <=> observes two null values, not one.
		jsonNull = jsonNull || isJSONLiteralNull(jsonValue)
		if jsonNull || paramNull {
			if nullSafe {
				rss[row] = jsonNull && paramNull
			} else {
				resultNulls.Add(uint64(row))
			}
			continue
		}

		var comparison int
		var coercedJSONNull, coercedParamNull bool
		var err error
		if paramType == types.T_any {
			kind := paramVector.GetPrepareParamKindAt(row)
			if kind == vector.PrepareParamDecimal && cachedDecimalParamReady {
				comparison, coercedJSONNull, err = comparePreparedJSONDecimalWithParsedParam(
					proc, jsonValue, cachedDecimalParam)
			} else {
				comparison, coercedJSONNull, coercedParamNull, err = comparePreparedJSONScalars(
					proc, jsonValue, paramValue, kind)
			}
		} else {
			comparison, coercedJSONNull, coercedParamNull, err = comparePreparedJSONScalarsAsType(
				proc, jsonValue, paramValue, paramType)
		}
		if err != nil {
			return err
		}
		if coercedJSONNull || coercedParamNull {
			if nullSafe {
				rss[row] = coercedJSONNull && coercedParamNull
			} else {
				resultNulls.Add(uint64(row))
			}
			continue
		}
		rss[row] = cmp(comparison)
	}
	return nil
}

func comparisonJSONAt(
	proc *process.Process,
	value *vector.Vector,
	row int,
) (bytejson.ByteJson, bool, error) {
	if value.IsNull(uint64(row)) {
		return bytejson.ByteJson{}, true, nil
	}
	data := value.GetBytesAt(row)
	if len(data) == 0 {
		return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "empty encoded JSON comparison value")
	}
	bj := types.DecodeJson(data)
	switch bj.Type {
	case bytejson.TpCodeObject, bytejson.TpCodeArray:
		if len(bj.Data) == 0 {
			return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "truncated encoded JSON container")
		}
	case bytejson.TpCodeString, bytejson.TpCodeDecimal,
		bytejson.TpCodeDate, bytejson.TpCodeTime, bytejson.TpCodeDatetime,
		bytejson.TpCodeBlob, bytejson.TpCodeOpaque, bytejson.TpCodeBit:
		if !validComparisonJSONVarlena(bj.Data) {
			return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "truncated encoded JSON comparison value")
		}
	case bytejson.TpCodeLiteral:
		if len(bj.Data) != 1 || bj.Data[0] > bytejson.LiteralFalse {
			return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "invalid encoded JSON literal")
		}
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64:
		if len(bj.Data) != 8 {
			return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "truncated encoded JSON number")
		}
	default:
		return bytejson.ByteJson{}, false, moerr.NewInvalidInput(proc.Ctx, "invalid encoded JSON comparison type")
	}
	return bj, false, nil
}

func validComparisonJSONVarlena(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	payloadLength, prefixLength := uint64(data[0]), 1
	if data[0] >= utf8.RuneSelf {
		var decodedBytes int
		payloadLength, decodedBytes = binary.Uvarint(data)
		if decodedBytes <= 0 {
			return false
		}
		prefixLength = decodedBytes
	}
	return payloadLength == uint64(len(data)-prefixLength)
}

// compareJSONScalarWithBoolean applies the established numeric-to-boolean
// comparison coercion without erasing the JSON scalar category. JSON strings
// are valid values of a different category, so they compare unequal instead
// of becoming SQL NULL or being parsed as boolean text.
func compareJSONScalarWithBoolean(
	ctx context.Context,
	jsonValue bytejson.ByteJson,
	booleanValue bool,
) (comparison int, jsonNull bool, err error) {
	if jsonValue.Type == bytejson.TpCodeString {
		return 1, false, nil
	}
	value, isNull, err := jsonScalarToBool(ctx, jsonValue)
	if err != nil || isNull {
		return 0, isNull, err
	}
	if value == booleanValue {
		return 0, false, nil
	}
	if !value {
		return -1, false, nil
	}
	return 1, false, nil
}

func isJSONLiteralNull(value bytejson.ByteJson) bool {
	return value.Type == bytejson.TpCodeLiteral &&
		len(value.Data) > 0 && value.Data[0] == bytejson.LiteralNull
}

func preparedJSONBoolean(ctx context.Context, value bytejson.ByteJson) (bool, error) {
	if value.Type != bytejson.TpCodeLiteral || len(value.Data) == 0 {
		return false, moerr.NewInternalError(ctx, "prepared boolean parameter is not encoded as a JSON boolean")
	}
	switch value.Data[0] {
	case bytejson.LiteralTrue:
		return true, nil
	case bytejson.LiteralFalse:
		return false, nil
	default:
		return false, moerr.NewInternalError(ctx, "prepared boolean parameter has an invalid JSON literal")
	}
}

func compareJSONBoolean(
	parameters []*vector.Vector,
	result *vector.FunctionResult[bool],
	proc *process.Process,
	length int,
	nullSafe bool,
	cmp func(int) bool,
	selectList *FunctionSelectList,
) error {
	jsonPos, boolPos := 0, 1
	if parameters[0].GetType().Oid == types.T_bool {
		jsonPos, boolPos = 1, 0
	}
	if parameters[jsonPos].GetType().Oid != types.T_json ||
		parameters[boolPos].GetType().Oid != types.T_bool {
		return moerr.NewInternalError(proc.Ctx, "JSON/BOOL comparison requires one operand of each type")
	}

	jsonVector := parameters[jsonPos]
	booleanVector := parameters[boolPos]
	var booleanValues []bool
	if !booleanVector.IsConstNull() {
		booleanValues = vector.MustFixedColNoTypeCheck[bool](booleanVector)
	}
	booleanIsConst := booleanVector.IsConst()
	rss := vector.MustFixedColNoTypeCheck[bool](result.GetResultVector())
	resultNulls := result.GetResultVector().GetNulls()
	if selectList != nil && selectList.IgnoreAllRow() {
		nulls.AddRange(resultNulls, 0, uint64(length))
		return nil
	}

	var cachedJSON bytejson.ByteJson
	var cachedJSONNull bool
	jsonIsConst := jsonVector.IsConst()
	if jsonIsConst {
		var err error
		cachedJSON, cachedJSONNull, err = comparisonJSONAt(proc, jsonVector, 0)
		if err != nil {
			return err
		}
	}
	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			resultNulls.Add(uint64(row))
			continue
		}

		jsonValue, jsonVectorNull := cachedJSON, cachedJSONNull
		if !jsonIsConst {
			var err error
			jsonValue, jsonVectorNull, err = comparisonJSONAt(proc, jsonVector, row)
			if err != nil {
				return err
			}
		}
		booleanRow := row
		if booleanIsConst {
			booleanRow = 0
		}
		booleanNull := booleanVector.IsNull(uint64(row))
		var booleanValue bool
		if !booleanNull {
			booleanValue = booleanValues[booleanRow]
		}
		jsonVectorNull = jsonVectorNull || isJSONLiteralNull(jsonValue)
		if jsonVectorNull || booleanNull {
			if nullSafe {
				rss[row] = jsonVectorNull && booleanNull
			} else {
				resultNulls.Add(uint64(row))
			}
			continue
		}

		comparison, jsonScalarNull, err := compareJSONScalarWithBoolean(proc.Ctx, jsonValue, booleanValue)
		if err != nil {
			return err
		}
		if jsonScalarNull {
			if !nullSafe {
				resultNulls.Add(uint64(row))
			}
			continue
		}
		rss[row] = cmp(comparison)
	}
	return nil
}

func comparePreparedJSONScalars(
	proc *process.Process,
	jsonValue bytejson.ByteJson,
	paramValue bytejson.ByteJson,
	kind vector.PrepareParamKind,
) (comparison int, jsonNull bool, paramNull bool, err error) {
	switch kind {
	case vector.PrepareParamBoolean:
		right, rightErr := preparedJSONBoolean(proc.Ctx, paramValue)
		if rightErr != nil {
			return 0, false, false, rightErr
		}
		comparison, leftNull, leftErr := compareJSONScalarWithBoolean(proc.Ctx, jsonValue, right)
		return comparison, leftNull, false, leftErr

	case vector.PrepareParamInteger:
		left, leftNull, leftOK := preparedJSONInteger(jsonValue)
		right, rightNull, rightOK := preparedJSONInteger(paramValue)
		if !leftOK || !rightOK {
			return 0, false, false, jsonCastErr(proc.Ctx, types.T_int64)
		}
		if leftNull || rightNull {
			return 0, leftNull, rightNull, nil
		}
		return comparePreparedJSONIntegers(left, right), false, false, nil

	case vector.PrepareParamFloat:
		left, leftNull, leftOK := jsonToScalar(jsonValue)
		right, rightNull, rightOK := jsonToScalar(paramValue)
		if !leftOK || !rightOK {
			return 0, false, false, jsonCastErr(proc.Ctx, types.T_float64)
		}
		if leftNull || rightNull {
			return 0, leftNull, rightNull, nil
		}
		return compareFloat64Total(left, right), false, false, nil

	case vector.PrepareParamDecimal:
		left, leftNull, leftOK := preparedJSONDecimal(jsonValue)
		right, rightNull, rightOK := preparedJSONDecimal(paramValue)
		if !leftOK || !rightOK {
			return 0, false, false, jsonCastErr(proc.Ctx, types.T_decimal256)
		}
		if leftNull || rightNull {
			return 0, leftNull, rightNull, nil
		}
		comparison, ok := bytejson.CompareNumeric(left, right)
		if !ok {
			return 0, false, false, jsonCastErr(proc.Ctx, types.T_decimal256)
		}
		return comparison, false, false, nil

	case vector.PrepareParamNone:
		left, leftErr := preparedJSONString(jsonValue)
		if leftErr != nil {
			return 0, false, false, leftErr
		}
		right, rightErr := preparedJSONString(paramValue)
		if rightErr != nil {
			return 0, false, false, rightErr
		}
		return bytes.Compare(left, right), false, false, nil

	default:
		return 0, false, false, moerr.NewInternalErrorf(proc.Ctx, "unsupported prepared parameter kind %d", kind)
	}
}

func comparePreparedJSONDecimalWithParsedParam(
	proc *process.Process,
	jsonValue bytejson.ByteJson,
	paramValue bytejson.ParsedNumeric,
) (comparison int, jsonNull bool, err error) {
	left, leftNull, leftOK := preparedJSONDecimal(jsonValue)
	if !leftOK {
		return 0, false, jsonCastErr(proc.Ctx, types.T_decimal256)
	}
	if leftNull {
		return 0, true, nil
	}
	parsedLeft, ok := bytejson.ParseNumeric(left)
	if !ok {
		return 0, false, jsonCastErr(proc.Ctx, types.T_decimal256)
	}
	comparison, ok = bytejson.CompareParsedNumeric(parsedLeft, paramValue)
	if !ok {
		return 0, false, jsonCastErr(proc.Ctx, types.T_decimal256)
	}
	return comparison, false, nil
}

func comparePreparedJSONScalarsAsType(
	proc *process.Process,
	jsonValue bytejson.ByteJson,
	paramValue bytejson.ByteJson,
	paramType types.T,
) (comparison int, jsonNull bool, paramNull bool, err error) {
	switch paramType {
	case types.T_int8:
		return comparePreparedJSONSignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MinInt8, math.MaxInt8, paramType)
	case types.T_int16:
		return comparePreparedJSONSignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MinInt16, math.MaxInt16, paramType)
	case types.T_int32:
		return comparePreparedJSONSignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MinInt32, math.MaxInt32, paramType)
	case types.T_int64:
		return comparePreparedJSONSignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MinInt64, math.MaxInt64, paramType)

	case types.T_uint8:
		return comparePreparedJSONUnsignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MaxUint8, paramType)
	case types.T_uint16:
		return comparePreparedJSONUnsignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MaxUint16, paramType)
	case types.T_uint32:
		return comparePreparedJSONUnsignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MaxUint32, paramType)
	case types.T_uint64:
		return comparePreparedJSONUnsignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MaxUint64, paramType)

	case types.T_float32:
		left, leftNull, leftErr := preparedJSONFloatScalar(proc.Ctx, jsonValue, paramType)
		if leftErr != nil {
			return 0, false, false, leftErr
		}
		right, rightNull, rightErr := preparedJSONFloatScalar(proc.Ctx, paramValue, paramType)
		if rightErr != nil {
			return 0, false, false, rightErr
		}
		if leftNull || rightNull {
			return 0, leftNull, rightNull, nil
		}
		return compareFloat64Total(left, right), false, false, nil
	case types.T_float64:
		left, leftNull, leftErr := preparedJSONFloatScalarWithLimit(proc.Ctx, jsonValue, paramType, math.MaxFloat64, false)
		if leftErr != nil {
			return 0, false, false, leftErr
		}
		right, rightNull, rightErr := preparedJSONFloatScalarWithLimit(proc.Ctx, paramValue, paramType, math.MaxFloat64, false)
		if rightErr != nil {
			return 0, false, false, rightErr
		}
		if leftNull || rightNull {
			return 0, leftNull, rightNull, nil
		}
		return compareFloat64Total(left, right), false, false, nil
	case types.T_bit, types.T_year:
		return comparePreparedJSONUnsignedScalars(
			proc.Ctx, jsonValue, paramValue, math.MaxUint64, paramType)
	case types.T_char, types.T_varchar, types.T_text,
		types.T_json, types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_enum, types.T_geometry:
		if isJSONLiteralNull(paramValue) {
			return 0, false, true, nil
		}
		return compareJSONOverlapExact(jsonValue, paramValue), false, false, nil

	}

	return 0, false, false, moerr.NewInternalErrorf(
		proc.Ctx, "unsupported prepared parameter type %s", paramType.String())
}

func comparePreparedJSONSignedScalars(
	ctx context.Context,
	leftValue bytejson.ByteJson,
	rightValue bytejson.ByteJson,
	minValue int64,
	maxValue int64,
	paramType types.T,
) (comparison int, leftNull bool, rightNull bool, err error) {
	left, leftNull, leftOK := jsonToInt64Scalar(leftValue)
	if !leftOK || (!leftNull && (left < minValue || left > maxValue)) {
		return 0, false, false, jsonCastErr(ctx, paramType)
	}
	right, rightNull, rightOK := jsonToInt64Scalar(rightValue)
	if !rightOK || (!rightNull && (right < minValue || right > maxValue)) {
		return 0, false, false, jsonCastErr(ctx, paramType)
	}
	if leftNull || rightNull {
		return 0, leftNull, rightNull, nil
	}
	return compareInt64(left, right), false, false, nil
}

func comparePreparedJSONUnsignedScalars(
	ctx context.Context,
	leftValue bytejson.ByteJson,
	rightValue bytejson.ByteJson,
	maxValue uint64,
	paramType types.T,
) (comparison int, leftNull bool, rightNull bool, err error) {
	left, leftNull, leftOK := jsonToUint64Scalar(leftValue)
	if !leftOK || (!leftNull && left > maxValue) {
		return 0, false, false, jsonCastErr(ctx, paramType)
	}
	right, rightNull, rightOK := jsonToUint64Scalar(rightValue)
	if !rightOK || (!rightNull && right > maxValue) {
		return 0, false, false, jsonCastErr(ctx, paramType)
	}
	if leftNull || rightNull {
		return 0, leftNull, rightNull, nil
	}
	return compareUint64(left, right), false, false, nil
}

func preparedJSONFloatScalar(
	ctx context.Context,
	value bytejson.ByteJson,
	paramType types.T,
) (float64, bool, error) {
	return preparedJSONFloatScalarWithLimit(ctx, value, paramType, math.MaxFloat32, true)
}

func preparedJSONFloatScalarWithLimit(
	ctx context.Context,
	value bytejson.ByteJson,
	paramType types.T,
	maxValue float64,
	convertToFloat32 bool,
) (float64, bool, error) {
	result, isNull, ok := jsonToScalar(value)
	if !ok {
		return 0, false, jsonCastErr(ctx, paramType)
	}
	if isNull {
		return 0, true, nil
	}
	if result < -maxValue || result > maxValue {
		return 0, false, jsonCastErr(ctx, paramType)
	}
	if convertToFloat32 {
		result = float64(float32(result))
	}
	return result, false, nil
}

type preparedJSONIntegerValue struct {
	signed   int64
	unsigned uint64
	isSigned bool
}

func preparedJSONInteger(value bytejson.ByteJson) (preparedJSONIntegerValue, bool, bool) {
	if value.Type == bytejson.TpCodeLiteral {
		if len(value.Data) > 0 && value.Data[0] == bytejson.LiteralNull {
			return preparedJSONIntegerValue{}, true, true
		}
		return preparedJSONIntegerValue{}, false, false
	}
	if value.Type == bytejson.TpCodeString {
		text := string(value.GetString())
		if signed, ok := bytejson.NumericTextToInt64(text); ok {
			return preparedJSONIntegerValue{signed: signed, isSigned: true}, false, true
		}
		if unsigned, ok := bytejson.NumericTextToUint64(text); ok {
			return preparedJSONIntegerValue{unsigned: unsigned}, false, true
		}
		return preparedJSONIntegerValue{}, false, false
	}
	if signed, ok := bytejson.NumericToInt64(value); ok {
		return preparedJSONIntegerValue{signed: signed, isSigned: true}, false, true
	}
	if unsigned, ok := bytejson.NumericToUint64(value); ok {
		return preparedJSONIntegerValue{unsigned: unsigned}, false, true
	}
	return preparedJSONIntegerValue{}, false, false
}

// preparedJSONDecimal maps numeric JSON scalars and numeric JSON strings into
// the exact ByteJSON numeric domain. CompareNumeric then shares one equality
// model with JSON containment and overlap instead of independently lowering
// DECIMAL values to float64.
func preparedJSONDecimal(value bytejson.ByteJson) (bytejson.ByteJson, bool, bool) {
	switch value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64:
		return value, false, true
	case bytejson.TpCodeDecimal:
		return value, false, true
	case bytejson.TpCodeString:
		// STRING and DECIMAL share the same length-prefixed payload layout.
		// Reclassify the local descriptor so a per-row numeric string does not
		// allocate a duplicate payload solely for exact comparison.
		value.Type = bytejson.TpCodeDecimal
		return value, false, true
	case bytejson.TpCodeLiteral:
		if len(value.Data) > 0 && value.Data[0] == bytejson.LiteralNull {
			return bytejson.ByteJson{}, true, true
		}
	}
	return bytejson.ByteJson{}, false, false
}

func comparePreparedJSONIntegers(left, right preparedJSONIntegerValue) int {
	if left.isSigned && right.isSigned {
		return compareInt64(left.signed, right.signed)
	}
	if !left.isSigned && !right.isSigned {
		return compareUint64(left.unsigned, right.unsigned)
	}
	if left.isSigned {
		if left.signed < 0 {
			return -1
		}
		return compareUint64(uint64(left.signed), right.unsigned)
	}
	if right.signed < 0 {
		return 1
	}
	return compareUint64(left.unsigned, uint64(right.signed))
}

func preparedJSONString(value bytejson.ByteJson) ([]byte, error) {
	switch value.Type {
	case bytejson.TpCodeString:
		return value.GetString(), nil
	case bytejson.TpCodeLiteral:
		switch value.Data[0] {
		case bytejson.LiteralNull:
			return []byte("null"), nil
		case bytejson.LiteralTrue:
			return []byte("true"), nil
		case bytejson.LiteralFalse:
			return []byte("false"), nil
		}
	}
	return value.MarshalJSON()
}

func otherCompareOperatorSupports(typ1, typ2 types.Type) bool {
	if isDatetimeTimestampComparison(typ1, typ2) {
		return true
	}
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_json:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
	case types.T_year:
	default:
		return false
	}
	return true
}

// jsonOrderingWithStringNotSupported returns true when one operand is JSON and the other
// is a MySQL string type, indicating that ordering comparisons (>, <, >=, <=) are not
// supported for this combination. Only equality/inequality (= and !=) are allowed.
func jsonOrderingWithStringNotSupported(inputs []types.Type) bool {
	if len(inputs) != 2 {
		return false
	}
	return (inputs[0].Oid == types.T_json) != (inputs[1].Oid == types.T_json) &&
		(inputs[0].Oid.IsMySQLString() || inputs[1].Oid.IsMySQLString())
}

func equalAndNotEqualOperatorSupports(typ1, typ2 types.Type) bool {
	if isDatetimeTimestampComparison(typ1, typ2) {
		return true
	}
	if typ1.Oid != typ2.Oid {
		return false
	}
	switch typ1.Oid {
	case types.T_bool:
	case types.T_bit:
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
	case types.T_float32, types.T_float64:
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
	case types.T_char, types.T_varchar:
	case types.T_date, types.T_datetime:
	case types.T_timestamp, types.T_time:
	case types.T_blob, types.T_text, types.T_datalink:
	case types.T_binary, types.T_varbinary:
	case types.T_geometry, types.T_geometry32:
	case types.T_json:
	case types.T_uuid:
	case types.T_Rowid:
	case types.T_array_float32, types.T_array_float64:
	case types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
	case types.T_enum:
	case types.T_year:
	default:
		return false
	}
	return true
}

func compareDatetimeAndTimestamp(
	parameters []*vector.Vector,
	result *vector.FunctionResult[bool],
	proc *process.Process,
	length int,
	cmp func(left, right types.Timestamp) bool,
	selectList *FunctionSelectList,
) error {
	zone := proc.GetSessionInfo().TimeZone
	if parameters[0].GetType().Oid == types.T_datetime {
		timestampScale := parameters[1].GetType().Scale
		return opBinaryFixedFixedToFixed[types.Datetime, types.Timestamp, bool](
			parameters, result, proc, length,
			func(left types.Datetime, right types.Timestamp) bool {
				return cmp(left.ToTimestamp(zone).TruncateToScale(timestampScale), right)
			}, selectList)
	}
	timestampScale := parameters[0].GetType().Scale
	return opBinaryFixedFixedToFixed[types.Timestamp, types.Datetime, bool](
		parameters, result, proc, length,
		func(left types.Timestamp, right types.Datetime) bool {
			return cmp(left, right.ToTimestamp(zone).TruncateToScale(timestampScale))
		}, selectList)
}

func floatArrayEqual[T constraints.Float](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func narrowFloatArrayEqual[T interface{ ToFloat32() float32 }](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ToFloat32() != right[i].ToFloat32() {
			return false
		}
	}
	return true
}

func opBinaryFixedFixedToFixedNullSafe[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	cmpFn func(v1, v2 T) bool,
	selectList *FunctionSelectList,
) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[bool](result)
	p1 := vector.OptGetParamFromWrapper[T](rs, 0, parameters[0])
	p2 := vector.OptGetParamFromWrapper[T](rs, 1, parameters[1])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Result of <=> is never NULL
	rsVec.GetNulls().Reset()

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)

		if null1 && null2 {
			rss[i] = true
		} else if null1 || null2 {
			rss[i] = false
		} else {
			rss[i] = cmpFn(v1, v2)
		}
	}
	return nil
}

func opBinaryBytesBytesToFixedNullSafe(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	cmpFn func(v1, v2 []byte) bool,
	selectList *FunctionSelectList,
) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Result of <=> is never NULL
	rsVec.GetNulls().Reset()

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)

		if null1 && null2 {
			rss[i] = true
		} else if null1 || null2 {
			rss[i] = false
		} else {
			rss[i] = cmpFn(v1, v2)
		}
	}
	return nil
}

func compareJsonBytes(left, right []byte) int {
	return bytejson.CompareByteJson(types.DecodeJson(left), types.DecodeJson(right))
}

func float32ComparisonNormalizers(leftScale, rightScale int32) (
	left types.Float32ScaleNormalizer,
	right types.Float32ScaleNormalizer,
	normalize bool,
) {
	left = types.NewFloat32ScaleNormalizer(leftScale)
	if rightScale == leftScale {
		right = left
	} else {
		right = types.NewFloat32ScaleNormalizer(rightScale)
	}
	return left, right, leftScale > 0 || rightScale > 0
}

func nullSafeEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isJSONBooleanComparison(*paramType, *parameters[1].GetType()) {
		return compareJSONBoolean(parameters, rs, proc, length, true, func(c int) bool { return c == 0 }, selectList)
	}

	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixedNullSafe[bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a == b
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixedNullSafe[uint64](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixedNullSafe[int8](parameters, rs, proc, length, func(a, b int8) bool {
			return a == b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixedNullSafe[int16](parameters, rs, proc, length, func(a, b int16) bool {
			return a == b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixedNullSafe[int32](parameters, rs, proc, length, func(a, b int32) bool {
			return a == b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixedNullSafe[int64](parameters, rs, proc, length, func(a, b int64) bool {
			return a == b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixedNullSafe[uint8](parameters, rs, proc, length, func(a, b uint8) bool {
			return a == b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixedNullSafe[uint16](parameters, rs, proc, length, func(a, b uint16) bool {
			return a == b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixedNullSafe[uint32](parameters, rs, proc, length, func(a, b uint32) bool {
			return a == b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixedNullSafe[uint64](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixedNullSafe[types.Uuid](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a == b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixedNullSafe[float32](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a == b
			}, selectList)
		}
		return opBinaryFixedFixedToFixedNullSafe[float32](parameters, rs, proc, length, func(a, b float32) bool {
			return a == b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixedNullSafe[float64](parameters, rs, proc, length, func(a, b float64) bool {
			return a == b
		}, selectList)
	case types.T_json:
		if parameters[0].IsPreparedJSONComparisonParam() || parameters[1].IsPreparedJSONComparisonParam() {
			return comparePreparedJSON(parameters, rs, proc, length, true, func(c int) bool { return c == 0 }, selectList)
		}
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) == 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Equal(bytes.TrimRight(a, " "), bytes.TrimRight(b, " "))
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_geometry, types.T_geometry32:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Equal(a, b)
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)
			return floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)
			return floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return narrowFloatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return narrowFloatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) == 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixedNullSafe(parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) == 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixedNullSafe[types.Date](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a == b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixedNullSafe[types.Datetime](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a == b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixedNullSafe[types.Time](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a == b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixedNullSafe[types.Timestamp](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a == b
		}, selectList)
	case types.T_decimal64:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal64](parameters, rs, proc, length, func(a, b types.Decimal64) bool {
			return a == b
		}, selectList)
	case types.T_decimal128:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal128](parameters, rs, proc, length, func(a, b types.Decimal128) bool {
			return a == b
		}, selectList)
	case types.T_decimal256:
		return opBinaryFixedFixedToFixedNullSafe[types.Decimal256](parameters, rs, proc, length, func(a, b types.Decimal256) bool {
			return a == b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixedNullSafe[types.Rowid](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		}, selectList)
	case types.T_enum:
		return opBinaryFixedFixedToFixedNullSafe[types.Enum](parameters, rs, proc, length, func(a, b types.Enum) bool {
			return a == b
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixedNullSafe[types.MoYear](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a == b
		}, selectList)
	}
	panic("unreached code")
}

// should convert to c.Numeric next.
func equalFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isJSONBooleanComparison(*paramType, *parameters[1].GetType()) {
		return compareJSONBoolean(parameters, rs, proc, length, false, func(c int) bool { return c == 0 }, selectList)
	}
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left == right
		}, selectList)
	}

	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a == b
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a == b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a == b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a == b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a == b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a == b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a == b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a == b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a == b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a == b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a == b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a == b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a == b
		}, selectList)
	case types.T_json:
		if parameters[0].IsPreparedJSONComparisonParam() || parameters[1].IsPreparedJSONComparisonParam() {
			return comparePreparedJSON(parameters, rs, proc, length, false, func(c int) bool { return c == 0 }, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) == 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Equal(bytes.TrimRight(a, " "), bytes.TrimRight(b, " "))
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink,
		types.T_geometry, types.T_geometry32:
		if parameters[0].GetArea() == nil && parameters[1].GetArea() == nil && (selectList == nil) {
			return compareVarlenaEqual(parameters, rs, proc, length, selectList)
		}
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(v1, v2 string) bool {
			return v1 == v2
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return narrowFloatArrayEqual(types.BytesToArray[types.BF16](v1), types.BytesToArray[types.BF16](v2))
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return narrowFloatArrayEqual(types.BytesToArray[types.Float16](v1), types.BytesToArray[types.Float16](v2))
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[int8](types.BytesToArray[int8](v1), types.BytesToArray[int8](v2)) == 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			return types.ArrayElementCompare[uint8](types.BytesToArray[uint8](v1), types.BytesToArray[uint8](v2)) == 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a == b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a == b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a == b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a == b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a == b
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a == b
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a == b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		}, selectList)
	case types.T_enum:
		return opBinaryFixedFixedToFixed[types.Enum, types.Enum, bool](parameters, rs, proc, length, func(a, b types.Enum) bool {
			return a == b
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a == b
		}, selectList)
	}
	panic("unreached code")
}

type valueDecimalCompareType interface {
	types.Decimal64 | types.Decimal128 | types.Decimal256
}

func valueDecimalCompare[T valueDecimalCompareType](
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b T) bool, scaleValue func(v T, delta int32) (T, error), selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])

	m := p2.GetType().Scale - p1.GetType().Scale

	rsVec := result.GetResultVector()
	rss := vector.MustFixedColWithTypeCheck[bool](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	rsNull := rsVec.GetNulls()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
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
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				x, err := scaleValue(v1, m)
				if err != nil {
					return err
				}
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(x, v2)
				}
			} else {
				y, err := scaleValue(v2, -m)
				if err != nil {
					return err
				}
				for i := uint64(0); i < length; i++ {
					rss[i] = cmpFn(v1, y)
				}
			}
		}
		return nil
	}

	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				x, err := scaleValue(v1, m)
				if err != nil {
					return err
				}
				if p2.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				if p2.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v2, _ := p2.GetValue(i)
						y, err := scaleValue(v2, -m)
						if err != nil {
							return err
						}
						rss[i] = cmpFn(v1, y)
					}
				} else {
					scaleMy := -m
					for i := uint64(0); i < length; i++ {
						v2, _ := p2.GetValue(i)
						y, err := scaleValue(v2, scaleMy)
						if err != nil {
							return err
						}
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}

		return nil
	}

	if c2 {
		v2, null2 := p2.GetValue(0)
		if null2 {
			nulls.AddRange(rsNull, 0, length)
		} else {
			if m >= 0 {
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						x, err := scaleValue(v1, m)
						if err != nil {
							return err
						}
						rss[i] = cmpFn(x, v2)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						x, err := scaleValue(v1, m)
						if err != nil {
							return err
						}
						rss[i] = cmpFn(x, v2)
					}
				}
			} else {
				y, err := scaleValue(v2, -m)
				if err != nil {
					return err
				}
				if p1.WithAnyNullValue() || rsAnyNull {
					nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
					for i := uint64(0); i < length; i++ {
						if rsNull.Contains(i) {
							continue
						}
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				} else {
					for i := uint64(0); i < length; i++ {
						v1, _ := p1.GetValue(i)
						rss[i] = cmpFn(v1, y)
					}
				}
			}
		}
		return nil
	}

	if p1.WithAnyNullValue() || p2.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, parameters[0].GetNulls(), rsNull)
		nulls.Or(rsNull, parameters[1].GetNulls(), rsNull)
		if m >= 0 {
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				x, err := scaleValue(v1, m)
				if err != nil {
					return err
				}
				rss[i] = cmpFn(x, v2)
			}
		} else {
			scaleMy := -m
			for i := uint64(0); i < length; i++ {
				if rsNull.Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				y, err := scaleValue(v2, scaleMy)
				if err != nil {
					return err
				}
				rss[i] = cmpFn(v1, y)
			}
		}
		return nil
	}

	if m >= 0 {
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			x, err := scaleValue(v1, m)
			if err != nil {
				return err
			}
			rss[i] = cmpFn(x, v2)
		}
	} else {
		scaleMy := -m
		for i := uint64(0); i < length; i++ {
			v1, _ := p1.GetValue(i)
			v2, _ := p2.GetValue(i)
			y, err := scaleValue(v2, scaleMy)
			if err != nil {
				return err
			}
			rss[i] = cmpFn(v1, y)
		}
	}
	return nil
}

func valueDec64Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal64) bool, selectList *FunctionSelectList) error {
	return valueDecimalCompare[types.Decimal64](parameters, result, length, cmpFn, func(v types.Decimal64, delta int32) (types.Decimal64, error) {
		scaled, _ := v.Scale(delta)
		return scaled, nil
	}, selectList)
}

func valueDec128Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal128) bool, selectList *FunctionSelectList) error {
	return valueDecimalCompare[types.Decimal128](parameters, result, length, cmpFn, func(v types.Decimal128, delta int32) (types.Decimal128, error) {
		scaled, _ := v.Scale(delta)
		return scaled, nil
	}, selectList)
}

func valueDec256Compare(
	parameters []*vector.Vector, result *vector.FunctionResult[bool], length uint64,
	cmpFn func(a, b types.Decimal256) bool, selectList *FunctionSelectList) error {
	return valueDecimalCompare[types.Decimal256](parameters, result, length, cmpFn, func(v types.Decimal256, delta int32) (types.Decimal256, error) {
		return v.Scale(delta)
	}, selectList)
}

func greatThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left > right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a > b
		}, selectList)
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return x && !y
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a > b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a > b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a > b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a > b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a > b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a > b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a > b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a > b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) > 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a > b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a > b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a > b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) > 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(bytes.TrimRight(a, " "), bytes.TrimRight(b, " ")) > 0
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) > 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) > 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) > 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) > 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) > 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) > 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) > 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a > b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a > b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a > b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a > b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) > 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.GT(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a > b
		}, selectList)
	}
	panic("unreached code")
}

func greatEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left >= right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return x || !y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a >= b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a >= b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a >= b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a >= b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a >= b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a >= b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a >= b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a >= b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a >= b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) >= 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a >= b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a >= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a >= b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) >= 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(bytes.TrimRight(a, " "), bytes.TrimRight(b, " ")) >= 0
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) >= 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) >= 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) >= 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a >= b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a >= b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a >= b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a >= b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) >= 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.GE(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a >= b
		}, selectList)
	}
	panic("unreached code")
}

func notEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isJSONBooleanComparison(*paramType, *parameters[1].GetType()) {
		return compareJSONBoolean(parameters, rs, proc, length, false, func(c int) bool { return c != 0 }, selectList)
	}
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left != right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(a, b bool) bool {
			return a != b
		}, selectList)
	case types.T_bit:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a != b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a != b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a != b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a != b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a != b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a != b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a != b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a != b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(a, b types.Uuid) bool {
			return a != b
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a != b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a != b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a != b
		}, selectList)
	case types.T_json:
		if parameters[0].IsPreparedJSONComparisonParam() || parameters[1].IsPreparedJSONComparisonParam() {
			return comparePreparedJSON(parameters, rs, proc, length, false, func(c int) bool { return c != 0 }, selectList)
		}
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) != 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return !bytes.Equal(bytes.TrimRight(a, " "), bytes.TrimRight(b, " "))
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryStrStrToFixed[bool](parameters, rs, proc, length, func(a, b string) bool {
			return a != b
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return !floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return !floatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return !narrowFloatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return !narrowFloatArrayEqual(_v1, _v2)
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) != 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) != 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a != b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a != b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a != b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a != b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a != b
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a != b
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a != b
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return !a.EQ(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a != b
		}, selectList)
	}
	panic("unreached code")
}

func lessThanFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left < right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return !x && y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a < b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a < b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a < b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a < b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a < b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a < b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a < b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a < b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a < b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) < 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a < b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a < b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a < b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) < 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(bytes.TrimRight(a, " "), bytes.TrimRight(b, " ")) < 0
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) < 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) < 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) < 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) < 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) < 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) < 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) < 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a < b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a < b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a < b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a < b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) < 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.LT(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a < b
		}, selectList)
	}
	panic("unreached code")
}

func lessEqualFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	if isDatetimeTimestampComparison(*paramType, *parameters[1].GetType()) {
		return compareDatetimeAndTimestamp(parameters, rs, proc, length, func(left, right types.Timestamp) bool {
			return left <= right
		}, selectList)
	}
	switch paramType.Oid {
	case types.T_bool:
		return opBinaryFixedFixedToFixed[bool, bool, bool](parameters, rs, proc, length, func(x, y bool) bool {
			return !x || y
		}, selectList)
	case types.T_bit:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a <= b
		}, selectList)
	case types.T_int8:
		return opBinaryFixedFixedToFixed[int8, int8, bool](parameters, rs, proc, length, func(a, b int8) bool {
			return a <= b
		}, selectList)
	case types.T_int16:
		return opBinaryFixedFixedToFixed[int16, int16, bool](parameters, rs, proc, length, func(a, b int16) bool {
			return a <= b
		}, selectList)
	case types.T_int32:
		return opBinaryFixedFixedToFixed[int32, int32, bool](parameters, rs, proc, length, func(a, b int32) bool {
			return a <= b
		}, selectList)
	case types.T_int64:
		return opBinaryFixedFixedToFixed[int64, int64, bool](parameters, rs, proc, length, func(a, b int64) bool {
			return a <= b
		}, selectList)
	case types.T_uint8:
		return opBinaryFixedFixedToFixed[uint8, uint8, bool](parameters, rs, proc, length, func(a, b uint8) bool {
			return a <= b
		}, selectList)
	case types.T_uint16:
		return opBinaryFixedFixedToFixed[uint16, uint16, bool](parameters, rs, proc, length, func(a, b uint16) bool {
			return a <= b
		}, selectList)
	case types.T_uint32:
		return opBinaryFixedFixedToFixed[uint32, uint32, bool](parameters, rs, proc, length, func(a, b uint32) bool {
			return a <= b
		}, selectList)
	case types.T_uint64:
		return opBinaryFixedFixedToFixed[uint64, uint64, bool](parameters, rs, proc, length, func(a, b uint64) bool {
			return a <= b
		}, selectList)
	case types.T_uuid:
		return opBinaryFixedFixedToFixed[types.Uuid, types.Uuid, bool](parameters, rs, proc, length, func(v1, v2 types.Uuid) bool {
			return types.CompareUuid(v1, v2) <= 0
		}, selectList)
	case types.T_float32:
		leftNormalizer, rightNormalizer, normalize := float32ComparisonNormalizers(
			paramType.Scale,
			parameters[1].GetType().Scale,
		)
		if normalize {
			return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
				a = leftNormalizer.Normalize(a)
				b = rightNormalizer.Normalize(b)
				return a <= b
			}, selectList)
		}
		return opBinaryFixedFixedToFixed[float32, float32, bool](parameters, rs, proc, length, func(a, b float32) bool {
			return a <= b
		}, selectList)
	case types.T_float64:
		return opBinaryFixedFixedToFixed[float64, float64, bool](parameters, rs, proc, length, func(a, b float64) bool {
			return a <= b
		}, selectList)
	case types.T_json:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return compareJsonBytes(a, b) <= 0
		}, selectList)
	case types.T_char:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(bytes.TrimRight(a, " "), bytes.TrimRight(b, " ")) <= 0
		}, selectList)
	case types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		}, selectList)
	case types.T_binary, types.T_varbinary:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(a, b []byte) bool {
			return bytes.Compare(a, b) <= 0
		}, selectList)
	case types.T_array_float32:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float32](v1)
			_v2 := types.BytesToArray[float32](v2)

			return types.ArrayCompare[float32](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_float64:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[float64](v1)
			_v2 := types.BytesToArray[float64](v2)

			return types.ArrayCompare[float64](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_bf16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.BF16](v1)
			_v2 := types.BytesToArray[types.BF16](v2)
			return types.ArrayElementCompare[types.BF16](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_float16:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[types.Float16](v1)
			_v2 := types.BytesToArray[types.Float16](v2)
			return types.ArrayElementCompare[types.Float16](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_int8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[int8](v1)
			_v2 := types.BytesToArray[int8](v2)
			return types.ArrayElementCompare[int8](_v1, _v2) <= 0
		}, selectList)
	case types.T_array_uint8:
		return opBinaryBytesBytesToFixed[bool](parameters, rs, proc, length, func(v1, v2 []byte) bool {
			_v1 := types.BytesToArray[uint8](v1)
			_v2 := types.BytesToArray[uint8](v2)
			return types.ArrayElementCompare[uint8](_v1, _v2) <= 0
		}, selectList)
	case types.T_date:
		return opBinaryFixedFixedToFixed[types.Date, types.Date, bool](parameters, rs, proc, length, func(a, b types.Date) bool {
			return a <= b
		}, selectList)
	case types.T_datetime:
		return opBinaryFixedFixedToFixed[types.Datetime, types.Datetime, bool](parameters, rs, proc, length, func(a, b types.Datetime) bool {
			return a <= b
		}, selectList)
	case types.T_time:
		return opBinaryFixedFixedToFixed[types.Time, types.Time, bool](parameters, rs, proc, length, func(a, b types.Time) bool {
			return a <= b
		}, selectList)
	case types.T_timestamp:
		return opBinaryFixedFixedToFixed[types.Timestamp, types.Timestamp, bool](parameters, rs, proc, length, func(a, b types.Timestamp) bool {
			return a <= b
		}, selectList)
	case types.T_decimal64:
		return valueDec64Compare(parameters, rs, uint64(length), func(a, b types.Decimal64) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_decimal128:
		return valueDec128Compare(parameters, rs, uint64(length), func(a, b types.Decimal128) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_decimal256:
		return valueDec256Compare(parameters, rs, uint64(length), func(a, b types.Decimal256) bool {
			return a.Compare(b) <= 0
		}, selectList)
	case types.T_Rowid:
		return opBinaryFixedFixedToFixed[types.Rowid, types.Rowid, bool](parameters, rs, proc, length, func(a, b types.Rowid) bool {
			return a.LE(&b)
		}, selectList)
	case types.T_year:
		return opBinaryFixedFixedToFixed[types.MoYear, types.MoYear, bool](parameters, rs, proc, length, func(a, b types.MoYear) bool {
			return a <= b
		}, selectList)
	}
	panic("unreached code")
}

func operatorOpBitUint64[T1, T2 constraints.Integer](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T2](parameters[1])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if err := rs.Append(fn(uint64(v1), uint64(v2)), null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func operatorOpUint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[uint64, uint64](parameters, result, proc, length, fn)
}

func operatorOpInt64ToUint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[int64, int64](parameters, result, proc, length, fn)
}

func operatorOpUint64Int64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[uint64, int64](parameters, result, proc, length, fn)
}

func operatorOpInt64Uint64Fn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int,
	fn func(uint64, uint64) uint64) error {
	return operatorOpBitUint64[int64, uint64](parameters, result, proc, length, fn)
}

func operatorOpStrFn(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func([]byte, []byte) ([]byte, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			rv, err := fn(v1, v2)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes(rv, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func operatorOpBitAndUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i & i2 })
}

func operatorOpBitAndStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] & i2[j]
		}
		return rv, nil
	})
}

func operatorOpBitXorUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i ^ i2 })
}

func operatorOpBitXorStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] ^ i2[j]
		}
		return rv, nil
	})
}

func operatorOpBitOrUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, func(i uint64, i2 uint64) uint64 { return i | i2 })
}

func operatorOpBitOrStrFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpStrFn(parameters, result, proc, length, func(i []byte, i2 []byte) ([]byte, error) {
		if len(i) != len(i2) {
			return nil, moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length")
		}
		rv := make([]byte, len(i))
		for j := range rv {
			rv[j] = i[j] | i2[j]
		}
		return rv, nil
	})
}

func bitShiftLeft(value, shift uint64) uint64 {
	if shift >= 64 {
		return 0
	}
	return value << shift
}

func bitShiftRight(value, shift uint64) uint64 {
	if shift >= 64 {
		return 0
	}
	return value >> shift
}

func operatorOpBitShiftLeftUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftLeftInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, bitShiftLeft)
}

func operatorOpBitShiftRightUint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightInt64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64ToUint64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightUint64Int64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpUint64Int64Fn(parameters, result, proc, length, bitShiftRight)
}

func operatorOpBitShiftRightInt64Uint64Fn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return operatorOpInt64Uint64Fn(parameters, result, proc, length, bitShiftRight)
}
