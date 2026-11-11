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
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	jsonValueEmpty
	jsonValueOneValue
	jsonValueMultiple
	jsonValueSourceParseError
	jsonValueHardError
)

type jsonValueExtracted struct {
	selected vector.JSONQueryResult
	state    jsonValueState
	value    bytejson.ByteJson
	text     string
	path     string
	err      error
}

// jsonValueInputParameter reads a document or path without assuming that the
// planner inserted a string cast. The seven-argument internal overload keeps
// prepared parameters in their runtime domain; a fixed-width value therefore
// needs the same textual representation that the rest of the expression
// engine exposes rather than being reinterpreted as a Varlena descriptor.
type jsonValueInputParameter struct{ vec *vector.Vector }

func newJsonValueInputParameter(v *vector.Vector) jsonValueInputParameter {
	return jsonValueInputParameter{vec: v}
}
func (p jsonValueInputParameter) get(row uint64) ([]byte, bool, error) {
	if p.vec == nil || p.vec.IsNull(row) {
		return nil, true, nil
	}
	if p.vec.GetType().IsVarlen() {
		value, err := p.vec.GetBytesAtChecked(int(row))
		return value, false, err
	}
	return []byte(p.vec.RowToString(int(row))), false, nil
}

func decodeJSONValueStored(data []byte) (value bytejson.ByteJson, err error) {
	value, err = decodeJSONValueStoredRaw(data)
	if err != nil {
		return bytejson.Null, err
	}
	if err := bytejson.ValidateStoredJSONDocument(value); err != nil {
		return bytejson.Null, err
	}
	return value, nil
}

func decodeJSONValueStoredRaw(data []byte) (bytejson.ByteJson, error) {
	if len(data) < 2 {
		return bytejson.Null, moerr.NewInvalidInputNoCtx("invalid binary JSON document")
	}
	var value bytejson.ByteJson
	if err := value.Unmarshal(data); err != nil {
		return bytejson.Null, err
	}
	return value, nil
}

// Vector admission validates the stored layout, but vector bytes can be
// changed through borrowed aliases afterward. Recheck the current bytes,
// including canonical key and string rules, before following any offsets.
func decodeJSONValueStoredAdmitted(data []byte) (bytejson.ByteJson, error) {
	return decodeJSONValueStored(data)
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
	ctx := jsonValueEvaluationContext(proc)
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	if len(ivecs) == 7 {
		err = jsonValueContract(ivecs, result, proc, length, selectList)
	} else {
		err = jsonValueLegacy(ivecs, result, proc, length, selectList)
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func jsonValueContract(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	readers ...vector.JSONReader,
) error {
	if len(ivecs) != 7 {
		return moerr.NewInvalidArg(jsonValueContext(proc), "json_value arguments", len(ivecs))
	}
	target := *ivecs[2].GetType()
	implicitText := target.Oid == types.T_any ||
		(target.Oid == types.T_varchar && target.Width == 512 && target.Charset == types.CharsetUTF8MB4Bin)
	if implicitText {
		target = types.NewWithCharset(types.T_varchar, 512, 0, types.CharsetUTF8MB4Bin)
	}
	onEmpty := jsonValueResponseMode(ivecs[3])
	onError := jsonValueResponseMode(ivecs[5])

	// typeswitch:partial JSON_VALUE supports FLOAT/DOUBLE, not low-precision float targets.
	switch target.Oid {
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob:
		return jsonValueTextResult(ivecs, result, proc, length, selectList, target, implicitText, onEmpty, onError, readers...)
	case types.T_json:
		return jsonValueJSONResult(ivecs, result, proc, length, selectList, onEmpty, onError, readers...)
	case types.T_int8:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt8, readers...)
	case types.T_int16:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt16, readers...)
	case types.T_int32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt32, readers...)
	case types.T_int64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueInt64, readers...)
	case types.T_uint8:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint8, readers...)
	case types.T_uint16:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint16, readers...)
	case types.T_uint32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint32, readers...)
	case types.T_uint64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueUint64, readers...)
	case types.T_float32:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueFloat32, readers...)
	case types.T_float64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueFloat64, readers...)
	case types.T_decimal64:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal64, readers...)
	case types.T_decimal128:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal128, readers...)
	case types.T_decimal256:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDecimal256, readers...)
	case types.T_date:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDate, readers...)
	case types.T_time:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueTime, readers...)
	case types.T_datetime:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueDatetime, readers...)
	case types.T_year:
		return jsonValueFixedResult(ivecs, result, proc, length, selectList, target, onEmpty, onError, parseJSONValueYear, readers...)
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

// Binding can reuse a Process whose previous pipeline has already ended.
// The live binding request owns cancellation then; execution uses its pipeline.
func jsonValueEvaluationContext(proc *process.Process) context.Context {
	if proc == nil || proc.Base == nil || proc.GetBaseProcessRunningStatus() {
		return jsonValueContext(proc)
	}
	if ctx := proc.GetTopContext(); ctx != nil {
		return ctx
	}
	return context.Background()
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
	return jsonValueExtractWithStoredValidation(doc, pathBytes, docType, true)
}

func jsonValueExtractAdmitted(
	doc, pathBytes []byte,
	docType types.T,
) jsonValueExtracted {
	return jsonValueExtractWithStoredValidation(doc, pathBytes, docType, false)
}

func jsonValueExtractWithStoredValidation(
	doc, pathBytes []byte,
	docType types.T,
	validateStored bool,
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

	return jsonValueExtractAtPath(doc, docType, validateStored, path, pathString)
}

func jsonValueExtractAtPath(doc []byte, docType types.T, validateStored bool, path bytejson.Path, pathString string) jsonValueExtracted {
	var err error

	var value bytejson.ByteJson
	if docType == types.T_json {
		// T_json values are normally validated by the storage layer, but an
		// expression can still provide an empty/malformed binary payload (for
		// example through a test vector or a remote plan).
		var decodeErr error
		if validateStored {
			value, decodeErr = decodeJSONValueStored(doc)
		} else {
			value, decodeErr = decodeJSONValueStoredAdmitted(doc)
		}
		if decodeErr != nil {
			return jsonValueExtracted{state: jsonValueHardError, path: pathString, err: decodeErr}
		}
	} else {
		value, err = types.ParseSliceToByteJsonWithDepthLimit(doc, bytejson.JSONDocumentMaxNestingDepth)
		if err != nil {
			// Invalid source JSON is a statement error. ON ERROR applies to
			// extraction/conversion failures, not malformed input documents.
			return jsonValueExtracted{state: jsonValueHardError, path: pathString, err: err}
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
	appendValue func(jsonValueExtracted) (conversionErr, outputErr error),
	appendNull func() error,
	appendDefault func(uint64, bool) error,
	textResult *vector.FunctionResult[types.Varlena],
	textTarget types.Type,
	textImplicit bool,
	readers ...vector.JSONReader,
) error {
	var docParam jsonValueInputParameter
	if len(readers) == 0 {
		docParam = newJsonValueInputParameter(ivecs[0])
	}
	pathParam := newJsonValueInputParameter(ivecs[1])
	docType := types.T_json
	if len(readers) == 0 {
		docType = ivecs[0].GetType().Oid
	}

	if selectList != nil && selectList.IgnoreAllRow() {
		for range length {
			if err := appendNull(); err != nil {
				return err
			}
		}
		return nil
	}

	var cursor vector.JSONCursor
	if len(readers) != 0 && textResult == nil {
		var err error
		cursor, err = readers[0].OpenCursor()
		if err != nil {
			return err
		}
		defer cursor.Close()
	}

	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && selectList.Contains(row) {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}
		var doc []byte
		var docNull bool
		var docErr error
		if len(readers) == 0 {
			doc, docNull, docErr = docParam.get(row)
		}
		path, pathNull, pathErr := pathParam.get(row)
		if pathErr != nil {
			return pathErr
		}
		if pathNull {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}
		if docErr != nil {
			return docErr
		}
		// Contract paths are validated even for NULL documents. Keep that
		// ordering and reuse this parse for extraction within the same row.
		pathText := string(path)
		parsedPath, pathParseErr := types.ParseStringToPath(pathText)
		if pathParseErr != nil {
			return moerr.NewInvalidArgNoCtx("json_value", "invalid path expression")
		}
		if docNull {
			if err := appendNull(); err != nil {
				return err
			}
			continue
		}

		var extracted jsonValueExtracted
		if len(readers) != 0 {
			if textResult != nil {
				selected, selectionErr := readers[0].QueryTextAtPath(int(row), parsedPath, false)
				if selectionErr == nil && selected.Exists && !selected.InputNull && !selected.ValueNull && !selected.Multiple && !selected.TextError {
					var conversionErr error
					if !textImplicit {
						conversionErr = jsonValueRequireScalarType(bytejson.TpCode(selected.Type))
					}
					value := selected.Text
					if conversionErr == nil {
						value, conversionErr = jsonValueText(value, textTarget)
					}
					if conversionErr == nil {
						if err := textResult.AppendBytes(util.UnsafeStringToBytes(value), false); err != nil {
							return err
						}
						continue
					}
					if onError != jsonValueErrorResponse {
						appendJSONValueWarning(proc, conversionErr, true)
					}
					extracted = jsonValueExtracted{state: jsonValueOneValue, path: pathText, err: conversionErr}
					if err := jsonValueApplyResponse(extracted, onError, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
						return err
					}
					continue
				}
				extracted = jsonValueExtractReaderTextSelection(selected, selectionErr, pathText)
			} else {
				extracted = jsonValueExtractReaderAtPath(cursor, int(row), parsedPath, pathText, false, true)
			}
		} else {
			extracted = jsonValueExtractAtPath(doc, docType, false, parsedPath, pathText)
		}
		processErr := func() error {
			switch extracted.state {
			case jsonValueSQLNull:
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
				err, outputErr := appendValue(extracted)
				// ON ERROR is a value-conversion policy, not an allocation fallback.
				// Never publish a NULL/default after failing to append the result.
				if outputErr != nil {
					return outputErr
				}
				if err != nil {
					extracted.err = err
					if onError != jsonValueErrorResponse {
						appendJSONValueWarning(proc, err, true)
					}
					if err = jsonValueApplyResponse(extracted, onError, row, appendNull, appendDefault, jsonValueContext(proc)); err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if processErr != nil {
			return processErr
		}
	}
	return nil
}

func jsonValueApplyResponse(
	extracted jsonValueExtracted,
	mode int64,
	row uint64,
	appendNull func() error,
	appendDefault func(uint64, bool) error,
	ctx context.Context,
) error {
	switch mode {
	case jsonValueNullResponse:
		return appendNull()
	case jsonValueDefaultResponse:
		return appendDefault(row, extracted.state == jsonValueEmpty)
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
	implicitText bool,
	onEmpty, onError int64,
	readers ...vector.JSONReader,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	emptyDefault := vector.GenerateFunctionStrParameter(ivecs[4])
	errorDefault := vector.GenerateFunctionStrParameter(ivecs[6])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) (error, error) {
			if !implicitText {
				if err := jsonValueRequireScalar(extracted); err != nil {
					return err, nil
				}
			}
			value, err := jsonValueText(extracted.text, target)
			if err != nil {
				return err, nil
			}
			// AppendBytes validates and copies synchronously into caller-owned
			// storage; the temporary view of value must not escape this call.
			return nil, rs.AppendBytes(util.UnsafeStringToBytes(value), false)
		},
		rs.AppendMustNullForBytesResult,
		func(row uint64, onEmpty bool) error {
			defaults := errorDefault
			if onEmpty {
				defaults = emptyDefault
			}
			value, null := defaults.GetStrValue(row)
			return rs.AppendBytes(value, null)
		}, rs, target, implicitText, readers...)
}

func jsonValueJSONResult(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	onEmpty, onError int64,
	readers ...vector.JSONReader,
) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	emptyDefault := vector.GenerateFunctionStrParameter(ivecs[4])
	errorDefault := vector.GenerateFunctionStrParameter(ivecs[6])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) (error, error) {
			if extracted.selected != nil {
				return nil, extracted.selected.AppendResult(result)
			}
			return nil, rs.AppendByteJson(extracted.value, false)
		},
		rs.AppendMustNullForBytesResult,
		func(row uint64, onEmpty bool) error {
			defaults := errorDefault
			if onEmpty {
				defaults = emptyDefault
			}
			value, null := defaults.GetStrValue(row)
			if null {
				return rs.AppendMustNullForBytesResult()
			}
			defaultJSON, err := decodeJSONValueStored(value)
			if err != nil {
				return err
			}
			return rs.AppendByteJson(defaultJSON, false)
		}, nil, types.Type{}, false, readers...)
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
	readers ...vector.JSONReader,
) error {
	rs := vector.MustFunctionResult[T](result)
	emptyDefault := vector.GenerateFunctionFixedTypeParameter[T](ivecs[4])
	errorDefault := vector.GenerateFunctionFixedTypeParameter[T](ivecs[6])
	return jsonValueExecuteCore(ivecs, proc, length, selectList, onEmpty, onError,
		func(extracted jsonValueExtracted) (error, error) {
			value, err := convert(extracted, target)
			if err != nil {
				return err, nil
			}
			return nil, rs.Append(value, false)
		},
		func() error {
			var zero T
			return rs.Append(zero, true)
		},
		func(row uint64, onEmpty bool) error {
			defaults := errorDefault
			if onEmpty {
				defaults = emptyDefault
			}
			value, null := defaults.GetValue(row)
			return rs.Append(value, null)
		}, nil, types.Type{}, false, readers...)
}

func jsonValueText(value string, target types.Type) (string, error) {
	if !utf8.ValidString(value) {
		return "", moerr.NewInvalidInputNoCtx("invalid UTF-8 JSON_VALUE result")
	}
	if target.Oid == types.T_binary || target.Oid == types.T_varbinary || target.Charset == types.CharsetBinary {
		if target.Width > 0 && len(value) > int(target.Width) {
			return "", moerr.NewDataTruncatedNoCtx("JSON_VALUE", "result exceeds binary width")
		}
	} else if target.Width > 0 && len(value) > int(target.Width) && utf8.RuneCountInString(value) > int(target.Width) {
		return "", moerr.NewDataTruncatedNoCtx("JSON_VALUE", "result exceeds character width")
	}
	return value, nil
}

func jsonValueScalarText(extracted jsonValueExtracted) (string, error) {
	if err := jsonValueRequireScalar(extracted); err != nil {
		return "", err
	}
	return strings.TrimSpace(extracted.text), nil
}

func jsonValueRequireScalar(extracted jsonValueExtracted) error {
	return jsonValueRequireScalarType(extracted.value.Type)
}

func jsonValueRequireScalarType(typ bytejson.TpCode) error {
	if typ == bytejson.TpCodeObject || typ == bytejson.TpCodeArray {
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
		return 0, moerr.NewDataTruncatedf(moerr.Context(), "SIGNED", "value %q loses fractional precision", e.text)
	}
	switch e.value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		if value, ok := e.numericInt64(); ok {
			return value, nil
		}
	case bytejson.TpCodeString:
		if value, ok := bytejson.NumericTextToInt64(numericText); ok {
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
	return 0, moerr.NewOutOfRangeNoCtxf("SIGNED", "JSON_VALUE value %q", e.text)
}

func checkedIntegerRange(err error, value, min, max int64) error {
	if err != nil {
		return err
	}
	if value < min || value > max {
		return moerr.NewOutOfRangeNoCtxf("SIGNED", "JSON_VALUE integer %d", value)
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
		return 0, moerr.NewDataTruncatedf(moerr.Context(), "UNSIGNED", "value %q loses fractional precision", e.text)
	}
	switch e.value.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		if value, ok := e.numericUint64(); ok {
			return value, nil
		}
	case bytejson.TpCodeString:
		if value, ok := bytejson.NumericTextToUint64(numericText); ok {
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
	return 0, moerr.NewOutOfRangeNoCtxf("UNSIGNED", "JSON_VALUE value %q", e.text)
}

func checkedUnsignedRange(err error, value, max uint64) error {
	if err != nil {
		return err
	}
	if value > max {
		return moerr.NewOutOfRangeNoCtxf("UNSIGNED", "JSON_VALUE integer %d", value)
	}
	return nil
}

func parseJSONValueFloat32(e jsonValueExtracted, _ types.Type) (float32, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(s, 32)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, moerr.NewOutOfRangeNoCtxf("FLOAT", "JSON_VALUE value %q", s)
	}
	return float32(v), nil
}

func parseJSONValueFloat64(e jsonValueExtracted, _ types.Type) (float64, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, moerr.NewOutOfRangeNoCtxf("DOUBLE", "JSON_VALUE value %q", s)
	}
	return v, nil
}

func parseJSONValueDecimal64(e jsonValueExtracted, target types.Type) (types.Decimal64, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return 0, moerr.NewDataTruncatedf(moerr.Context(), "DECIMAL", "value %q loses fractional precision at scale %d", s, target.Scale)
	}
	return types.ParseDecimal64(s, target.Width, target.Scale)
}

func parseJSONValueDecimal128(e jsonValueExtracted, target types.Type) (types.Decimal128, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return types.Decimal128{}, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return types.Decimal128{}, moerr.NewDataTruncatedf(moerr.Context(), "DECIMAL", "value %q loses fractional precision at scale %d", s, target.Scale)
	}
	return types.ParseDecimal128(s, target.Width, target.Scale)
}

func parseJSONValueDecimal256(e jsonValueExtracted, target types.Type) (types.Decimal256, error) {
	s, err := jsonValueNumericText(e)
	if err != nil {
		return types.Decimal256{}, err
	}
	if !jsonValueNumericExactAtScale(s, target.Scale) {
		return types.Decimal256{}, moerr.NewDataTruncatedf(moerr.Context(), "DECIMAL", "value %q loses fractional precision at scale %d", s, target.Scale)
	}
	return types.ParseDecimal256(s, target.Width, target.Scale)
}

func parseJSONValueDate(e jsonValueExtracted, _ types.Type) (types.Date, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	value, err := types.ParseDateCastWithoutTime(s)
	if err != nil {
		return 0, err
	}
	if value == types.ZeroDate {
		return 0, moerr.NewInvalidInputNoCtxf("invalid DATE value %s", s)
	}
	return value, nil
}

func jsonValueFractionalPrecisionExact(text string, scale int32) bool {
	if scale < 0 {
		return false
	}
	dot := strings.IndexByte(strings.TrimSpace(text), '.')
	if dot < 0 {
		return true
	}
	fraction := strings.TrimSpace(text)[dot+1:]
	return int32(len(fraction)) <= scale
}

func rejectJSONValueZeroDatetime(text string) error {
	text = strings.TrimSpace(text)
	if !strings.ContainsAny(text, "-/") {
		return nil
	}
	value, err := types.ParseDatetime(text, 6)
	if err == nil && value == types.ZeroDatetime {
		return moerr.NewInvalidInputNoCtxf("invalid DATETIME value %s", text)
	}
	return nil
}

func parseJSONValueTime(e jsonValueExtracted, target types.Type) (types.Time, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueFractionalPrecisionExact(s, target.Scale) {
		return 0, moerr.NewDataTruncatedf(moerr.Context(), "TIME", "value %q loses fractional precision at scale %d", s, target.Scale)
	}
	if err := rejectJSONValueZeroDatetime(s); err != nil {
		return 0, err
	}
	return types.ParseTimeWithoutDate(s, target.Scale)
}

func parseJSONValueDatetime(e jsonValueExtracted, target types.Type) (types.Datetime, error) {
	s, err := jsonValueScalarText(e)
	if err != nil {
		return 0, err
	}
	if !jsonValueFractionalPrecisionExact(s, target.Scale) {
		return 0, moerr.NewDataTruncatedf(moerr.Context(), "DATETIME", "value %q loses fractional precision at scale %d", s, target.Scale)
	}
	value, err := types.ParseDatetime(s, target.Scale)
	if err != nil {
		return 0, err
	}
	if value == types.ZeroDatetime {
		return 0, moerr.NewInvalidInputNoCtxf("invalid DATETIME value %s", s)
	}
	return value, nil
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

func (e jsonValueExtracted) numericInt64() (int64, bool) {
	if e.selected != nil {
		return e.selected.Int64()
	}
	return bytejson.NumericToInt64(e.value)
}
func (e jsonValueExtracted) numericUint64() (uint64, bool) {
	if e.selected != nil {
		return e.selected.Uint64()
	}
	return bytejson.NumericToUint64(e.value)
}

// JsonValueWithReader evaluates an explicitly retained immutable input. The
// reader is borrowed for this call; the caller owns its lifetime and mapping.
// Ordinary JsonValue continues to validate every mutable binary input.
// The closed reader supplies document type, NULL state and row mapping. Its
// document parameter slot may be nil; ordinary mutable input still uses JsonValue.
func JsonValueWithReader(reader vector.JSONReader, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !vector.IsPrivateJSONReader(reader) && !objectio.IsPrivateJSONCacheReader(reader) {
		return moerr.NewInvalidArg(jsonValueContext(proc), "json_value reader input", length)
	}
	return jsonValueWithAdmittedReader(reader, ivecs, result, proc, length, selectList)
}

// jsonValueWithAdmittedReader is reachable in production only after the
// concrete owner check above. Tests also use it to inject mid-query cancellation.
func jsonValueWithAdmittedReader(reader vector.JSONReader, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if reader == nil || reader.Length() != length || (len(ivecs) != 2 && len(ivecs) != 7) || (ivecs[0] != nil && ivecs[0].GetType().Oid != types.T_json) {
		return moerr.NewInvalidArg(jsonValueContext(proc), "json_value reader input", length)
	}
	ctx := jsonValueEvaluationContext(proc)
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	if len(ivecs) == 7 {
		err = jsonValueContract(ivecs, result, proc, length, selectList, reader)
	} else {
		err = jsonValueLegacyReader(reader, ivecs, result, proc, length, selectList)
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func jsonValueLegacyReader(reader vector.JSONReader, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	writer := vector.MustFunctionResult[types.Varlena](result)
	pathParam := newJsonValueInputParameter(ivecs[1])
	legacyFloat := LegacyFloatStringFormatting(jsonValueContext(proc))
	for row := 0; row < length; row++ {
		if selectList != nil && (selectList.IgnoreAllRow() || selectList.Contains(uint64(row))) {
			if err := writer.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		path, null, pathErr := pathParam.get(uint64(row))
		if pathErr != nil {
			return moerr.NewInvalidArg(jsonValueContext(proc), "json_value", "invalid path expression")
		}
		if null {
			if err := writer.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		selected, err := reader.QueryText(row, path, legacyFloat)
		if err != nil {
			if selected.InvalidPath {
				return moerr.NewInvalidArgNoCtx("json_value", "invalid path expression")
			}
			return err
		}
		if !selected.Exists || selected.InputNull || selected.ValueNull || selected.Multiple {
			if err := writer.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		// The writer copies before returning, so this read-only view cannot
		// escape or expose the retained document backing.
		if err := writer.AppendBytes(util.UnsafeStringToBytes(selected.Text), false); err != nil {
			return err
		}
	}
	return nil
}

// Map non-success text selections to the shared response/error machinery.
func jsonValueExtractReaderTextSelection(selected vector.JSONTextSelection, err error, pathText string) jsonValueExtracted {
	extracted := jsonValueExtracted{path: pathText}
	if err != nil {
		extracted.err = err
		if selected.TextError {
			extracted.state = jsonValueSourceParseError
		} else {
			extracted.state = jsonValueHardError
		}
		return extracted
	}
	switch {
	case selected.InputNull || selected.ValueNull:
		extracted.state = jsonValueSQLNull
	case selected.Multiple:
		extracted.state = jsonValueMultiple
	case !selected.Exists:
		extracted.state = jsonValueEmpty
	default:
		extracted.state = jsonValueOneValue
		extracted.text = selected.Text
		extracted.value.Type = bytejson.TpCode(selected.Type)
	}
	return extracted
}

func jsonValueExtractReaderAtPath(cursor vector.JSONCursor, row int, path bytejson.Path, pathText string, legacyFloat bool, typed bool) jsonValueExtracted {
	err := cursor.ResetQuery(row, path)
	if err != nil {
		return jsonValueExtracted{state: jsonValueHardError, err: err}
	}
	selected := cursor
	e := jsonValueExtracted{}
	if typed {
		e.selected, e.path = selected, pathText
	}
	if typed && selected.SQLNull() {
		e.state = jsonValueSQLNull
		return e
	}
	if !selected.Exists() {
		e.state = jsonValueEmpty
		return e
	}
	if !path.IsSimple() {
		count := selected.SelectSingle()
		if count == 0 {
			e.state = jsonValueEmpty
			return e
		}
		if count > 1 {
			e.state = jsonValueMultiple
			return e
		}
	}
	if selected.IsJSONNull() {
		e.state = jsonValueSQLNull
		return e
	}
	if typed {
		e.value.Type = selected.Type()
		if literal, ok := selected.Literal(); ok {
			e.value.Data = []byte{literal}
		}
	}
	if legacyFloat {
		e.text, e.err = selected.TextWithLegacyFloat()
	} else {
		e.text, e.err = selected.Text()
	}
	if e.err != nil {
		e.state = jsonValueSourceParseError
	} else {
		e.state = jsonValueOneValue
	}
	return e
}
