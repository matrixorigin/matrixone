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
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	jsonValueNullResponse    int64 = 1
	jsonValueErrorResponse   int64 = 2
	jsonValueDefaultResponse int64 = 3
)

type jsonValueState uint8

const (
	jsonValueSQLNull jsonValueState = iota
	jsonValuePathNull
	jsonValueEmpty
	jsonValueOneValue
	jsonValueMultiple
	jsonValueSourceParseError
	jsonValueHardError
)

type jsonValueExtracted struct {
	state jsonValueState
	value bytejson.ByteJson
	text  string
	path  string
	err   error
}

// jsonValueInputParameter reads a document or path without assuming that the
// planner inserted a string cast. The seven-argument internal overload keeps
// prepared parameters in their runtime domain; a fixed-width value therefore
// needs the same textual representation that the rest of the expression
// engine exposes rather than being reinterpreted as a Varlena descriptor.
type jsonValueInputParameter struct {
	vec      *vector.Vector
	strParam vector.FunctionParameterWrapper[types.Varlena]
}

func newJsonValueInputParameter(v *vector.Vector) jsonValueInputParameter {
	p := jsonValueInputParameter{vec: v}
	if v != nil && v.GetType().IsVarlen() {
		p.strParam = vector.GenerateFunctionStrParameter(v)
	}
	return p
}

func (p jsonValueInputParameter) get(row uint64) ([]byte, bool) {
	if p.vec == nil || p.vec.IsNull(row) {
		return nil, true
	}
	if p.strParam != nil {
		value, null := p.strParam.GetStrValue(row)
		return value, null
	}
	return []byte(p.vec.RowToString(int(row))), false
}

func decodeJSONValueStored(data []byte) (value bytejson.ByteJson, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = bytejson.Null
			err = moerr.NewInvalidInputNoCtx("invalid binary JSON document")
		}
	}()
	if len(data) < 2 {
		return bytejson.Null, moerr.NewInvalidInputNoCtx("invalid binary JSON document")
	}
	if err := value.Unmarshal(data); err != nil {
		return bytejson.Null, err
	}
	// Unmarshal intentionally remains a zero-copy storage decoder. MarshalJSON
	// walks every offset and validates the shape while preserving the same
	// representation for execution.
	if _, err := value.MarshalJSON(); err != nil {
		return bytejson.Null, err
	}
	return value, nil
}

// JsonValue is the public executor entry point for both the persisted
// two-argument form and the planner-only seven-argument contract form.
func JsonValue(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(ivecs) == 7 {
		return jsonValueContract(ivecs, result, proc, length, selectList)
	}
	return jsonValueLegacy(ivecs, result, proc, length, selectList)
}

func jsonValueContract(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(ivecs) != 7 {
		return moerr.NewInvalidArg(jsonValueContext(proc), "json_value arguments", len(ivecs))
	}
	target := *ivecs[2].GetType()
	if target.Oid == types.T_any {
		target = types.NewWithCharset(types.T_varchar, 512, 0, types.CharsetUTF8MB4Bin)
	}
	onEmpty := jsonValueResponseMode(ivecs[3])
	onError := jsonValueResponseMode(ivecs[5])

	switch target.Oid {
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob:
		return jsonValueTextResult(ivecs, result, proc, length, selectList, target, onEmpty, onError)
	case types.T_json:
		return jsonValueJSONResult(ivecs, result, proc, length, selectList, onEmpty, onError)
	case types.T_int8:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt8)
	case types.T_int16:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt16)
	case types.T_int32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt32)
	case types.T_int64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt64)
	case types.T_uint8:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint8)
	case types.T_uint16:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint16)
	case types.T_uint32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint32)
	case types.T_uint64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint64)
	case types.T_float32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueFloat32)
	case types.T_float64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueFloat64)
	case types.T_decimal64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal64)
	case types.T_decimal128:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal128)
	case types.T_decimal256:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal256)
	case types.T_date:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDate)
	case types.T_time:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueTime)
	case types.T_datetime:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDatetime)
	case types.T_year:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueYear)
	default:
		return moerr.NewInvalidArg(jsonValueContext(proc), "json_value RETURNING type", target.String())
	}
}

func jsonValueResponseMode(v *vector.Vector) int64 {
	p := vector.GenerateFunctionFixedTypeParameter[int64](v)
	mode, null := p.GetValue(0)
	if null {
		return jsonValueNullResponse
	}
	return mode
}

func jsonValueContext(proc *process.Process) context.Context {
	if proc == nil || proc.Ctx == nil {
		return context.Background()
	}
	return proc.Ctx
}

func jsonValueExtract(
	doc, pathBytes []byte,
	docType types.T,
) jsonValueExtracted {
	pathString := string(pathBytes)
	path, err := types.ParseStringToPath(pathString)
	if err != nil {
		return jsonValueExtracted{
			state: jsonValueHardError,
			path:  pathString,
			// Keep the two-argument function's historical error contract. A
			// path is a hard error and is never routed through ON ERROR.
			err: moerr.NewInvalidArgNoCtx("json_value", "invalid path expression"),
		}
	}

	var value bytejson.ByteJson
	if docType == types.T_json {
		// T_json values are normally validated by the storage layer, but an
		// expression can still provide an empty/malformed binary payload (for
		// example through a test vector or a remote plan).
		var decodeErr error
		value, decodeErr = decodeJSONValueStored(doc)
		if decodeErr != nil {
			return jsonValueExtracted{state: jsonValueSourceParseError, path: pathString, err: decodeErr}
		}
		if err := bytejson.ValidateJSONDocumentDepth(value); err != nil {
			return jsonValueExtracted{state: jsonValueHardError, path: pathString, err: err}
		}
	} else {
		value, err = types.ParseSliceToByteJsonWithDepthLimit(doc, bytejson.JSONDocumentMaxNestingDepth)
		if err != nil {
			if bytejson.IsJSONDocumentDepthError(err) {
				return jsonValueExtracted{state: jsonValueHardError, path: pathString, err: err}
			}
			return jsonValueExtracted{state: jsonValueSourceParseError, path: pathString, err: err}
		}
	}

	matched, exists := value.QueryWithExists([]*bytejson.Path{&path})
	if !exists {
		return jsonValueExtracted{state: jsonValueEmpty, path: pathString}
	}
	if !path.IsSimple() {
		if matched.Type != bytejson.TpCodeArray {
			return jsonValueExtracted{state: jsonValueMultiple, path: pathString}
		}
		count := matched.GetElemCnt()
		switch {
		case count == 0:
			return jsonValueExtracted{state: jsonValueEmpty, path: pathString}
		case count > 1:
			return jsonValueExtracted{state: jsonValueMultiple, path: pathString}
		default:
			matched = matched.GetArrayElem(0)
		}
	}
	if matched.IsNull() {
		return jsonValueExtracted{state: jsonValueSQLNull, path: pathString}
	}
	text, err := matched.Unquote()
	if err != nil {
		return jsonValueExtracted{state: jsonValueSourceParseError, path: pathString, err: err}
	}
	return jsonValueExtracted{
		state: jsonValueOneValue,
		value: matched,
		text:  text,
		path:  pathString,
	}
}

func jsonValueExecuteCore(
	ivecs []*vector.Vector,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	onEmpty, onError int64,
	appendValue func(jsonValueExtracted) error,
	appendNull func() error,
	appendDefault func(uint64) error,
) error {
	docParam := newJsonValueInputParameter(ivecs[0])
	pathParam := newJsonValueInputParameter(ivecs[1])
	docType := ivecs[0].GetType().Oid

	if selectList != nil && selectList.IgnoreAllRow() {
		for range length {
			if err := appendNull(); err != nil {
				return err
			}
		}
		return nil
	}

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && selectList.Contains(row) {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}
		doc, docNull := docParam.get(row)
		path, pathNull := pathParam.get(row)
		if docNull {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}
		if pathNull {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}

		extracted := jsonValueExtract(doc, path, docType)
		switch extracted.state {
		case jsonValueSQLNull, jsonValuePathNull:
			if err := appendNull(); err != nil {
				return err
			}
		case jsonValueEmpty:
			if err := jsonValueApplyResponse(extracted, onEmpty, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
				return err
			}
		case jsonValueMultiple:
			if err := jsonValueApplyResponse(extracted, onError, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
				return err
			}
		case jsonValueSourceParseError:
			if onError != jsonValueErrorResponse {
				appendJSONValueWarning(proc, extracted.err, false)
			}
			if err := jsonValueApplyResponse(extracted, onError, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
				return err
			}
		case jsonValueHardError:
			return extracted.err
		case jsonValueOneValue:
			if err := appendValue(extracted); err != nil {
				extracted.err = err
				if onError != jsonValueErrorResponse {
					appendJSONValueWarning(proc, err, true)
				}
				if err = jsonValueApplyResponse(extracted, onError, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func jsonValueApplyResponse(
	extracted jsonValueExtracted,
	mode int64,
	row uint64,
	appendNull func() error,
	appendDefault func(uint64) error,
	ctx context.Context,
) error {
	switch mode {
	case jsonValueNullResponse:
		return appendNull()
	case jsonValueDefaultResponse:
		return appendDefault(row)
	case jsonValueErrorResponse:
		if extracted.state == jsonValueEmpty {
			return moerr.NewMissingJSONValue(ctx, extracted.path)
		}
		if extracted.state == jsonValueMultiple {
			return moerr.NewMultipleJSONValues(ctx, extracted.path)
		}
		return extracted.err
	default:
		return moerr.NewInvalidArgNoCtx("json_value response mode", mode)
	}
}

func appendJSONValueWarning(proc *process.Process, err error, conversion bool) {
	if proc == nil || err == nil {
		return
	}
	appender, ok := proc.GetSession().(warningDiagnosticAppender)
	if ok {
		code := moerr.ER_INVALID_JSON_TEXT
		if conversion {
			switch {
			case moerr.IsMoErrCode(err, moerr.ErrOutOfRange):
				code = moerr.ER_WARN_DATA_OUT_OF_RANGE
			case moerr.IsMoErrCode(err, moerr.ErrDataTruncated):
				code = moerr.WARN_DATA_TRUNCATED
			default:
				// Conversion failures from the JSON numeric/temporal parsers are
				// ordinary Go errors. Keep them visible as row-level truncation
				// diagnostics rather than misreporting them as parse failures.
				code = moerr.WARN_DATA_TRUNCATED
			}
		}
		appender.AppendWarningDiagnostic(code, err.Error())
	}
}

func jsonValueTextResult(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	target types.Type,
	onEmpty, onError int64,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	defaults := vector.GenerateFunctionStrParameter(ivecs[4])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) error {
			value, err := jsonValueTextBytes(extracted.text, target)
			if err != nil {
				return err
			}
			return rs.AppendBytes(value, false)
		},
		rs.AppendMustNullForBytesResult,
		func(row uint64) error {
			value, null := defaults.GetStrValue(row)
			return rs.AppendBytes(value, null)
		})
}

func jsonValueJSONResult(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	onEmpty, onError int64,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	defaults := vector.GenerateFunctionStrParameter(ivecs[4])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) error {
			return rs.AppendByteJson(extracted.value, false)
		},
		rs.AppendMustNullForBytesResult,
		func(row uint64) error {
			value, null := defaults.GetStrValue(row)
			if null {
				return rs.AppendMustNullForBytesResult()
			}
			defaultJSON, err := decodeJSONValueStored(value)
			if err != nil {
				return err
			}
			return rs.AppendByteJson(defaultJSON, false)
		})
}

func jsonValueFixedResult[T types.FixedSizeTExceptStrType](
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	target types.Type,
	onEmpty, onError int64,
	convert func(jsonValueExtracted, types.Type) (T, error),
) error {
	rs := vector.MustFunctionResult[T](result)
	defaults := vector.GenerateFunctionFixedTypeParameter[T](ivecs[4])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) error {
			value, err := convert(extracted, target)
			if err != nil {
				return err
			}
			return rs.Append(value, false)
		},
		func() error {
			var zero T
			return rs.Append(zero, true)
		},
		func(row uint64) error {
			value, null := defaults.GetValue(row)
			return rs.Append(value, null)
		})
}

func jsonValueTextBytes(value string, target types.Type) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, moerr.NewInvalidInputNoCtx("invalid UTF-8 JSON_VALUE result")
	}
	if target.Oid == types.T_binary || target.Oid == types.T_varbinary || target.Charset == types.CharsetBinary {
		if target.Width > 0 && len(value) > int(target.Width) {
			return nil, moerr.NewDataTruncatedNoCtx("JSON_VALUE", "result exceeds binary width")
		}
	} else if target.Width > 0 && utf8.RuneCountInString(value) > int(target.Width) {
		return nil, moerr.NewDataTruncatedNoCtx("JSON_VALUE", "result exceeds character width")
	}
	return []byte(value), nil
}

func jsonValueScalarText(extracted jsonValueExtracted) (string, error) {
	if err := jsonValueRequireScalar(extracted); err != nil {
		return "", err
	}
	return strings.TrimSpace(extracted.text), nil
}

func jsonValueRequireScalar(extracted jsonValueExtracted) error {
	if extracted.value.Type == bytejson.TpCodeObject || extracted.value.Type == bytejson.TpCodeArray {
		return moerr.NewInvalidInputNoCtx("JSON_VALUE composite cannot be converted to a scalar")
	}
	return nil
}

func jsonValueNumericText(extracted jsonValueExtracted) (string, error) {
	if err := jsonValueRequireScalar(extracted); err != nil {
		return "", err
	}
	if extracted.value.Type == bytejson.TpCodeLiteral && len(extracted.value.Data) > 0 {
		switch extracted.value.Data[0] {
		case bytejson.LiteralTrue:
			return "1", nil
		case bytejson.LiteralFalse:
			return "0", nil
		}
	}
	return strings.TrimSpace(extracted.text), nil
}

// jsonValueNumericExactAtScale reports whether a decimal spelling can be
// represented by the requested SQL scale without discarding any fractional
// digits. JSON_VALUE routes loss of precision through ON ERROR instead of
// silently inheriting the truncating behavior of an ordinary cast.
func jsonValueNumericExactAtScale(text string, scale int32) bool {
	if scale < 0 {
		scale = 0
	}
	rational, ok := new(big.Rat).SetString(strings.TrimSpace(text))
	if !ok {
		return false
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rational.Mul(rational, new(big.Rat).SetInt(factor))
	return rational.Denom().Cmp(big.NewInt(1)) == 0
}

func parseJSONValueInt8(e jsonValueExtracted, _ types.Type) (int8, error) {
	v, err := parseJSONValueInt64(e, types.T_int8.ToType())
	if err = checkedIntegerRange(err, v, math.MinInt8, math.MaxInt8); err != nil {
		return 0, err
	}
	return int8(v), nil
}

func parseJSONValueInt16(e jsonValueExtracted, _ types.Type) (int16, error) {
	v, err := parseJSONValueInt64(e, types.T_int16.ToType())
	if err = checkedIntegerRange(err, v, math.MinInt16, math.MaxInt16); err != nil {
		return 0, err
	}
	return int16(v), nil
}

func parseJSONValueInt32(e jsonValueExtracted, _ types.Type) (int32, error) {
	v, err := parseJSONValueInt64(e, types.T_int32.ToType())
	if err = checkedIntegerRange(err, v, math.MinInt32, math.MaxInt32); err != nil {
		return 0, err
	}
	return int32(v), nil
}

func parseJSONValueInt64(e jsonValueExtracted, _ types.Type) (int64, error) {
	if err := jsonValueRequireScalar(e); err != nil {
		return 0, err
	}
	numericText, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueNumericExactAtScale(numericText, 0) {
		return 0, fmt.Errorf("invalid SIGNED value %q: fractional precision would be lost", e.text)
	}
	switch e.value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		if value, ok := bytejson.NumericToInt64(e.value); ok {
			return value, nil
		}
	case bytejson.TpCodeString:
		if value, ok := bytejson.NumericTextToInt64(string(e.value.GetString())); ok {
			return value, nil
		}
	case bytejson.TpCodeLiteral:
		if len(e.value.Data) > 0 {
			switch e.value.Data[0] {
			case bytejson.LiteralTrue:
				return 1, nil
			case bytejson.LiteralFalse:
				return 0, nil
			}
		}
	}
	return 0, fmt.Errorf("invalid SIGNED value %q", e.text)
}

func checkedIntegerRange(err error, value, min, max int64) error {
	if err != nil {
		return err
	}
	if value < min || value > max {
		return fmt.Errorf("JSON_VALUE integer %d is out of range", value)
	}
	return nil
}

func parseJSONValueUint8(e jsonValueExtracted, _ types.Type) (uint8, error) {
	v, err := parseJSONValueUint64(e, types.T_uint8.ToType())
	if err = checkedUnsignedRange(err, v, math.MaxUint8); err != nil {
		return 0, err
	}
	return uint8(v), nil
}

func parseJSONValueUint16(e jsonValueExtracted, _ types.Type) (uint16, error) {
	v, err := parseJSONValueUint64(e, types.T_uint16.ToType())
	if err = checkedUnsignedRange(err, v, math.MaxUint16); err != nil {
		return 0, err
	}
	return uint16(v), nil
}

func parseJSONValueUint32(e jsonValueExtracted, _ types.Type) (uint32, error) {
	v, err := parseJSONValueUint64(e, types.T_uint32.ToType())
	if err = checkedUnsignedRange(err, v, math.MaxUint32); err != nil {
		return 0, err
	}
	return uint32(v), nil
}

func parseJSONValueUint64(e jsonValueExtracted, _ types.Type) (uint64, error) {
	if err := jsonValueRequireScalar(e); err != nil {
		return 0, err
	}
	numericText, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueNumericExactAtScale(numericText, 0) {
		return 0, fmt.Errorf("invalid UNSIGNED value %q: fractional precision would be lost", e.text)
	}
	switch e.value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		if value, ok := bytejson.NumericToUint64(e.value); ok {
			return value, nil
		}
	case bytejson.TpCodeString:
		if value, ok := bytejson.NumericTextToUint64(string(e.value.GetString())); ok {
			return value, nil
		}
	case bytejson.TpCodeLiteral:
		if len(e.value.Data) > 0 {
			switch e.value.Data[0] {
			case bytejson.LiteralTrue:
				return 1, nil
			case bytejson.LiteralFalse:
				return 0, nil
			}
		}
	}
	return 0, fmt.Errorf("invalid UNSIGNED value %q", e.text)
}

func checkedUnsignedRange(err error, value, max uint64) error {
	if err != nil {
		return err
	}
	if value > max {
		return fmt.Errorf("JSON_VALUE unsigned integer %d is out of range", value)
	}
	return nil
}

func parseJSONValueFloat32(e jsonValueExtracted, _ types.Type) (float32, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(s, 32)
	if err != nil || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid FLOAT value %q", s)
	}
	return float32(v), nil
}

func parseJSONValueFloat64(e jsonValueExtracted, _ types.Type) (float64, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid DOUBLE value %q", s)
	}
	return v, nil
}

func parseJSONValueDecimal64(e jsonValueExtracted, target types.Type) (types.Decimal64, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return 0, fmt.Errorf("invalid DECIMAL(%d,%d) value %q: fractional precision would be lost", target.Width, target.Scale, s)
	}
	return types.ParseDecimal64(s, target.Width, target.Scale)
}

func parseJSONValueDecimal128(e jsonValueExtracted, target types.Type) (types.Decimal128, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return types.Decimal128{}, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return types.Decimal128{}, fmt.Errorf("invalid DECIMAL(%d,%d) value %q: fractional precision would be lost", target.Width, target.Scale, s)
	}
	return types.ParseDecimal128(s, target.Width, target.Scale)
}

func parseJSONValueDecimal256(e jsonValueExtracted, target types.Type) (types.Decimal256, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return types.Decimal256{}, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return types.Decimal256{}, fmt.Errorf("invalid DECIMAL(%d,%d) value %q: fractional precision would be lost", target.Width, target.Scale, s)
	}
	return types.ParseDecimal256(s, target.Width, target.Scale)
}

func parseJSONValueDate(e jsonValueExtracted, _ types.Type) (types.Date, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	return types.ParseDateCast(s)
}

func parseJSONValueTime(e jsonValueExtracted, target types.Type) (types.Time, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	return types.ParseTime(s, target.Scale)
}

func parseJSONValueDatetime(e jsonValueExtracted, target types.Type) (types.Datetime, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	return types.ParseDatetime(s, target.Scale)
}

func parseJSONValueYear(e jsonValueExtracted, _ types.Type) (types.MoYear, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	if value, parseErr := strconv.ParseInt(s, 10, 64); parseErr == nil {
		return types.ParseMoYearFromInt(value)
	}
	return types.ParseMoYear(s)
}
