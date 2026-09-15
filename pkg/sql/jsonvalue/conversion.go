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

package jsonvalue

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/internal/bytejsonvalidate"
)

// ConversionStatus describes the value classification at the JSON_TABLE
// conversion boundary. Missing is produced only by path iteration; a JSON
// null is a real match and is therefore kept distinct from Missing.
type ConversionStatus uint8

const (
	StatusMissing ConversionStatus = iota
	StatusSuccess
	StatusJSONNull
	StatusComposite
	StatusConversionError
	StatusRangeError
	StatusTruncated
	StatusStatementError
)

// Names used by the first Foundation API are aliases so callers can migrate
// without losing the more precise distinctions above.
const (
	StatusConverted  = StatusSuccess
	StatusNull       = StatusJSONNull
	StatusInvalid    = StatusConversionError
	StatusOutOfRange = StatusRangeError
)

func (s ConversionStatus) String() string {
	switch s {
	case StatusMissing:
		return "missing"
	case StatusSuccess:
		return "success"
	case StatusJSONNull:
		return "json-null"
	case StatusComposite:
		return "composite"
	case StatusConversionError:
		return "conversion-error"
	case StatusRangeError:
		return "range-error"
	case StatusTruncated:
		return "truncated"
	case StatusStatementError:
		return "statement-error"
	default:
		return "unknown"
	}
}

// WarningDescriptor describes a successful conversion that also needs a
// runtime diagnostic. The descriptor is returned to the owning executor; this
// layer never writes a session warning or performs warning deduplication.
type WarningDescriptor struct {
	Code    uint16
	Message string
}

// Result is safe to inspect before appending to a vector. Value is one of the
// concrete Go values accepted by vector.AppendAny for the target SQL type.
type Result struct {
	Value   any
	Status  ConversionStatus
	Err     error
	Warning *WarningDescriptor
}

// ConversionOptions carries session-dependent conversion inputs. The
// location is intentionally supplied at execution time rather than captured
// in a plan or a constant default.
type ConversionOptions struct {
	Location *time.Location
}

// ErrMissingJSONTableValue is returned by AppendResult when the caller tries
// to append a result for which the path produced no match. JSON_TABLE owns the
// ON EMPTY decision, so this package never silently appends SQL NULL here.
var ErrMissingJSONTableValue = errors.New("JSON_TABLE path produced no match")

// ConvertScalar converts one ByteJson value to a MatrixOne SQL scalar type.
// The caller owns ON EMPTY and ON ERROR policy. In particular, this function
// never receives or invents a missing value, and it never turns a JSON null
// into a conversion error.
func ConvertScalar(value bytejson.ByteJson, target types.Type) Result {
	return ConvertScalarWithContext(context.Background(), value, target, ConversionOptions{})
}

// ConvertScalarWithLocation is a convenience for timestamp conversion when
// execution has a session timezone available.
func ConvertScalarWithLocation(value bytejson.ByteJson, target types.Type, location *time.Location) Result {
	return ConvertScalarWithContext(context.Background(), value, target, ConversionOptions{Location: location})
}

// ConvertScalarWithContext is the cancellation-aware conversion entry point.
// It performs only bounded local work and never starts a worker or waits on an
// external resource.
func ConvertScalarWithContext(ctx context.Context, value bytejson.ByteJson, target types.Type, options ConversionOptions) (result Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Result{
				Status: StatusStatementError,
				Err:    fmt.Errorf("invalid ByteJson value: %v", recovered),
			}
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	if isJSONNull(value) {
		return Result{Status: StatusJSONNull}
	}

	if target.Oid == types.T_json {
		encoded, err := safeMarshal(value)
		if err != nil {
			return Result{Status: StatusStatementError, Err: err}
		}
		return Result{Value: encoded, Status: StatusSuccess}
	}
	if value.Type == bytejson.TpCodeObject || value.Type == bytejson.TpCodeArray {
		return Result{
			Status: StatusComposite,
			Err:    conversionError(target, "object or array"),
		}
	}

	text, err := safeScalarText(value)
	if err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}

	switch target.Oid {
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_datalink:
		out, truncated := truncateText(text, target)
		result = Result{Value: []byte(out), Status: StatusSuccess}
		if truncated {
			result.Status = StatusTruncated
			result.Warning = &WarningDescriptor{
				Code:    moerr.WARN_DATA_TRUNCATED,
				Message: fmt.Sprintf("JSON_TABLE value truncated for %s", target.DescString()),
			}
		}
		return result
	case types.T_bool:
		v, err := boolValue(value, text)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return convertSignedInteger(value, text, target)
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return convertUnsignedInteger(value, text, target)
	case types.T_float32, types.T_float64:
		return convertFloat(text, target)
	case types.T_decimal64:
		v, err := types.ParseDecimal64(text, target.Width, target.Scale)
		if err != nil {
			return Result{Status: decimalFailureStatus(text), Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_decimal128:
		v, err := types.ParseDecimal128(text, target.Width, target.Scale)
		if err != nil {
			return Result{Status: decimalFailureStatus(text), Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_decimal256:
		v, err := types.ParseDecimal256(text, target.Width, target.Scale)
		if err != nil {
			return Result{Status: decimalFailureStatus(text), Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_date:
		v, err := types.ParseDateCast(text)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_time:
		v, err := types.ParseTime(text, target.Scale)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_datetime:
		v, err := types.ParseDatetime(text, target.Scale)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_timestamp:
		location := options.Location
		if location == nil {
			location = time.UTC
		}
		v, err := types.ParseTimestamp(location, text, target.Scale)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_year:
		v, err := types.ParseMoYear(text)
		if err != nil {
			return Result{Status: StatusConversionError, Err: err}
		}
		return Result{Value: v, Status: StatusSuccess}
	case types.T_bit:
		v, ok := numericToUint(value, text)
		if !ok {
			return numericFailure(value, text, target)
		}
		if target.Width > 0 && target.Width < 64 && v >= uint64(1)<<target.Width {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: v, Status: StatusSuccess}
	default:
		return Result{
			Status: StatusConversionError,
			Err:    moerr.NewNotSupportedf(nil, "JSON scalar conversion to %s", target),
		}
	}
}

// ConvertPathMatches consumes one path match at a time. Missing is returned
// without inventing a null. For non-JSON targets the second match is enough to
// classify ON ERROR, so remaining matches are deliberately left unconsumed.
// JSON targets use the final output cell as the only incremental array
// builder, bounded by types.MaxBlobLen.
func ConvertPathMatches(iterator *bytejson.PathIterator, target types.Type) Result {
	return ConvertPathMatchesWithLimitContext(context.Background(), iterator, target, types.MaxBlobLen)
}

// ConvertPathMatchesContext is the default-limit cancellation-aware form.
func ConvertPathMatchesContext(ctx context.Context, iterator *bytejson.PathIterator, target types.Type) Result {
	return ConvertPathMatchesWithLimitContext(ctx, iterator, target, types.MaxBlobLen)
}

// ConvertPathMatchesWithLimit is useful to deterministic unit tests and
// controlled callers that need a stricter cell budget than the repository
// default. Production JSON_TABLE uses ConvertPathMatches or its context form.
func ConvertPathMatchesWithLimit(iterator *bytejson.PathIterator, target types.Type, maxBytes int) Result {
	return ConvertPathMatchesWithLimitContext(context.Background(), iterator, target, maxBytes)
}

// ConvertPathMatchesWithLimitContext is the bounded implementation shared by
// the convenience forms above.
func ConvertPathMatchesWithLimitContext(
	ctx context.Context,
	iterator *bytejson.PathIterator,
	target types.Type,
	maxBytes int,
) Result {
	if iterator == nil {
		return Result{Status: StatusStatementError, Err: errors.New("nil JSON_TABLE path iterator")}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	first, ok, err := iterator.NextContext(ctx)
	if err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	if !ok {
		return Result{Status: StatusMissing}
	}

	second, ok, err := iterator.NextContext(ctx)
	if err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	if !ok {
		if target.Oid == types.T_json {
			return convertJSONValueWithLimit(ctx, first, maxBytes)
		}
		return ConvertScalarWithContext(ctx, first, target, ConversionOptions{})
	}
	if target.Oid != types.T_json {
		return Result{
			Status: StatusConversionError,
			Err:    fmt.Errorf("JSON_TABLE path returned multiple values for %s", target.DescString()),
		}
	}

	builder, err := bytejson.NewJSONTableArrayBuilder(maxBytes)
	if err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	defer builder.Close()
	if err = builder.AppendContext(ctx, first); err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	if err = builder.AppendContext(ctx, second); err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	for {
		value, matched, nextErr := iterator.NextContext(ctx)
		if nextErr != nil {
			return Result{Status: StatusStatementError, Err: nextErr}
		}
		if !matched {
			break
		}
		if err = builder.AppendContext(ctx, value); err != nil {
			return Result{Status: StatusStatementError, Err: err}
		}
	}
	array, err := builder.Build()
	if err != nil {
		return Result{Status: StatusStatementError, Err: err}
	}
	// ConvertScalar only serializes the already-built final cell here; it does
	// not re-walk the matches or build a second slice. Keep the final check as
	// the admission boundary in case the storage representation changes.
	return convertJSONValueWithLimit(ctx, array, maxBytes)
}

func convertJSONValueWithLimit(ctx context.Context, value bytejson.ByteJson, maxBytes int) Result {
	if maxBytes <= 0 {
		return Result{
			Status: StatusStatementError,
			Err:    fmt.Errorf("invalid JSON_TABLE JSON cell limit %d", maxBytes),
		}
	}
	if isJSONNull(value) {
		return Result{Status: StatusJSONNull}
	}
	encoded, err := bytejson.MarshalStorageCompatibleWithLimit(ctx, value, maxBytes)
	if err != nil {
		return Result{
			Status: StatusStatementError,
			Err:    err,
		}
	}
	return Result{Value: encoded, Status: StatusSuccess}
}

// AppendResult atomically appends a successful, null, or truncating result to
// vec. Error, missing, composite, and statement-error statuses are returned
// without changing the vector. The checkpoint also protects against future
// vector append paths that publish metadata before reporting allocation
// failure.
func AppendResult(vec *vector.Vector, result Result, mp *mpool.MPool) (err error) {
	if vec == nil {
		return errors.New("nil destination vector")
	}
	if mp == nil {
		return errors.New("nil destination vector mpool")
	}
	if vec.IsConst() {
		return errors.New("cannot append JSON_TABLE value to a constant vector")
	}
	switch result.Status {
	case StatusJSONNull:
		// handled below
	case StatusSuccess, StatusTruncated:
		if !appendValueCompatible(vec.GetType().Oid, result.Value) {
			return fmt.Errorf("JSON_TABLE conversion result %s is incompatible with %s", result.Status, vec.GetType().DescString())
		}
	case StatusMissing:
		return ErrMissingJSONTableValue
	default:
		if result.Err != nil {
			return result.Err
		}
		return fmt.Errorf("JSON_TABLE conversion did not produce a value: %s", result.Status)
	}

	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			vec.RollbackAppend(checkpoint, 1)
		}
	}()
	if result.Status == StatusJSONNull {
		return vector.AppendAny(vec, nil, true, mp)
	}
	return vector.AppendAny(vec, result.Value, false, mp)
}

// AppendConvertedResult is an explicit alias used by executor call sites.
func AppendConvertedResult(vec *vector.Vector, result Result, mp *mpool.MPool) error {
	return AppendResult(vec, result, mp)
}

func isJSONNull(value bytejson.ByteJson) bool {
	return value.Type == bytejson.TpCodeLiteral && len(value.Data) == 1 && value.Data[0] == bytejson.LiteralNull
}

func safeMarshal(value bytejson.ByteJson) (data []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid ByteJson value: %v", recovered)
			data = nil
		}
	}()
	if err := validateJSONValue(value); err != nil {
		return nil, err
	}
	return value.Marshal()
}

func validateJSONValue(value bytejson.ByteJson) error {
	if validJSONValue(value) {
		return nil
	}
	return fmt.Errorf("invalid ByteJson value of type %#x", value.Type)
}

func validJSONValue(value bytejson.ByteJson) bool {
	switch value.Type {
	case bytejson.TpCodeLiteral:
		if len(value.Data) != 1 {
			return false
		}
		switch value.Data[0] {
		case bytejson.LiteralNull, bytejson.LiteralTrue, bytejson.LiteralFalse:
			return true
		default:
			return false
		}
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64:
		return len(value.Data) == 8
	case bytejson.TpCodeFloat64:
		if len(value.Data) != 8 {
			return false
		}
		floating := math.Float64frombits(binary.LittleEndian.Uint64(value.Data))
		return !math.IsNaN(floating) && !math.IsInf(floating, 0)
	case bytejson.TpCodeString, bytejson.TpCodeDecimal,
		bytejson.TpCodeDate, bytejson.TpCodeTime, bytejson.TpCodeDatetime,
		bytejson.TpCodeBlob, bytejson.TpCodeOpaque, bytejson.TpCodeBit:
		_, ok := bytejsonvalidate.UvarintPayload(value.Data)
		return ok
	case bytejson.TpCodeArray, bytejson.TpCodeObject:
		return bytejsonvalidate.Container(byte(value.Type), value.Data, validJSONScalar)
	default:
		return false
	}
}

func validJSONScalar(tp byte, data []byte) bool {
	return validJSONValue(bytejson.ByteJson{Type: bytejson.TpCode(tp), Data: data})
}

func safeScalarText(value bytejson.ByteJson) (text string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid ByteJson scalar: %v", recovered)
			text = ""
		}
	}()
	if value.Type == bytejson.TpCodeLiteral && len(value.Data) == 1 {
		switch value.Data[0] {
		case bytejson.LiteralTrue:
			return "true", nil
		case bytejson.LiteralFalse:
			return "false", nil
		}
	}
	return value.Unquote()
}

func convertSignedInteger(value bytejson.ByteJson, text string, target types.Type) Result {
	v, ok := numericToInt(value, text)
	if !ok {
		return numericFailure(value, text, target)
	}
	switch target.Oid {
	case types.T_int8:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: int8(v), Status: StatusSuccess}
	case types.T_int16:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: int16(v), Status: StatusSuccess}
	case types.T_int32:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: int32(v), Status: StatusSuccess}
	default:
		return Result{Value: v, Status: StatusSuccess}
	}
}

func convertUnsignedInteger(value bytejson.ByteJson, text string, target types.Type) Result {
	v, ok := numericToUint(value, text)
	if !ok {
		return numericFailure(value, text, target)
	}
	switch target.Oid {
	case types.T_uint8:
		if v > math.MaxUint8 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: uint8(v), Status: StatusSuccess}
	case types.T_uint16:
		if v > math.MaxUint16 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: uint16(v), Status: StatusSuccess}
	case types.T_uint32:
		if v > math.MaxUint32 {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: uint32(v), Status: StatusSuccess}
	default:
		return Result{Value: v, Status: StatusSuccess}
	}
}

func convertFloat(text string, target types.Type) Result {
	v, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		status := StatusConversionError
		if errors.Is(err, strconv.ErrRange) {
			status = StatusRangeError
		}
		return Result{Status: status, Err: conversionError(target, text)}
	}
	if target.Oid == types.T_float32 {
		f := float32(v)
		if math.IsInf(float64(f), 0) {
			return Result{Status: StatusRangeError, Err: conversionError(target, text)}
		}
		return Result{Value: f, Status: StatusSuccess}
	}
	return Result{Value: v, Status: StatusSuccess}
}

func numericToInt(value bytejson.ByteJson, text string) (int64, bool) {
	switch value.Type {
	case bytejson.TpCodeLiteral:
		if len(value.Data) == 1 {
			switch value.Data[0] {
			case bytejson.LiteralTrue:
				return 1, true
			case bytejson.LiteralFalse:
				return 0, true
			}
		}
		return 0, false
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return bytejson.NumericToInt64(value)
	default:
		return bytejson.NumericTextToInt64(text)
	}
}

func numericToUint(value bytejson.ByteJson, text string) (uint64, bool) {
	switch value.Type {
	case bytejson.TpCodeLiteral:
		if len(value.Data) == 1 {
			switch value.Data[0] {
			case bytejson.LiteralTrue:
				return 1, true
			case bytejson.LiteralFalse:
				return 0, true
			}
		}
		return 0, false
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return bytejson.NumericToUint64(value)
	default:
		return bytejson.NumericTextToUint64(text)
	}
}

func numericFailure(value bytejson.ByteJson, text string, target types.Type) Result {
	status := StatusConversionError
	if value.Type == bytejson.TpCodeInt64 || value.Type == bytejson.TpCodeUint64 ||
		value.Type == bytejson.TpCodeFloat64 || value.Type == bytejson.TpCodeDecimal {
		status = StatusRangeError
	} else if textLooksNumeric(text) {
		status = StatusRangeError
	}
	return Result{Status: status, Err: conversionError(target, text)}
}

func decimalFailureStatus(text string) ConversionStatus {
	if textLooksNumeric(text) {
		return StatusRangeError
	}
	return StatusConversionError
}

func textLooksNumeric(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	v, err := strconv.ParseFloat(trimmed, 64)
	if err == nil {
		return !math.IsNaN(v)
	}
	return errors.Is(err, strconv.ErrRange)
}

func boolValue(value bytejson.ByteJson, text string) (bool, error) {
	if value.Type == bytejson.TpCodeLiteral && len(value.Data) == 1 {
		switch value.Data[0] {
		case bytejson.LiteralTrue:
			return true, nil
		case bytejson.LiteralFalse:
			return false, nil
		}
	}
	if value.Type == bytejson.TpCodeInt64 {
		return value.GetInt64() != 0, nil
	}
	if value.Type == bytejson.TpCodeUint64 {
		return value.GetUint64() != 0, nil
	}
	if value.Type == bytejson.TpCodeFloat64 {
		floating := value.GetFloat64()
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return false, conversionError(types.T_bool.ToType(), text)
		}
		return floating != 0, nil
	}
	if parsed, err := types.ParseBool(strings.TrimSpace(text)); err == nil {
		return parsed, nil
	}
	return false, conversionError(types.T_bool.ToType(), text)
}

func truncateText(text string, target types.Type) (string, bool) {
	if target.Width <= 0 {
		return text, false
	}
	limit := int(target.Width)
	if target.Oid == types.T_char || target.Oid == types.T_varchar || target.Oid == types.T_text {
		if utf8.RuneCountInString(text) <= limit {
			return text, false
		}
		end := 0
		for i := range text {
			if limit == 0 {
				break
			}
			end = i
			limit--
		}
		if limit == 0 {
			_, size := utf8.DecodeRuneInString(text[end:])
			end += size
		}
		return text[:end], true
	}
	if len(text) <= limit {
		return text, false
	}
	return text[:limit], true
}

func appendValueCompatible(oid types.T, value any) bool {
	switch oid {
	case types.T_bool:
		_, ok := value.(bool)
		return ok
	case types.T_bit, types.T_uint64:
		_, ok := value.(uint64)
		return ok
	case types.T_uint8:
		_, ok := value.(uint8)
		return ok
	case types.T_uint16:
		_, ok := value.(uint16)
		return ok
	case types.T_uint32:
		_, ok := value.(uint32)
		return ok
	case types.T_int8:
		_, ok := value.(int8)
		return ok
	case types.T_int16:
		_, ok := value.(int16)
		return ok
	case types.T_int32:
		_, ok := value.(int32)
		return ok
	case types.T_int64:
		_, ok := value.(int64)
		return ok
	case types.T_float32:
		_, ok := value.(float32)
		return ok
	case types.T_float64:
		_, ok := value.(float64)
		return ok
	case types.T_decimal64:
		_, ok := value.(types.Decimal64)
		return ok
	case types.T_decimal128:
		_, ok := value.(types.Decimal128)
		return ok
	case types.T_decimal256:
		_, ok := value.(types.Decimal256)
		return ok
	case types.T_date:
		_, ok := value.(types.Date)
		return ok
	case types.T_time:
		_, ok := value.(types.Time)
		return ok
	case types.T_datetime:
		_, ok := value.(types.Datetime)
		return ok
	case types.T_timestamp:
		_, ok := value.(types.Timestamp)
		return ok
	case types.T_year:
		_, ok := value.(types.MoYear)
		return ok
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json, types.T_datalink:
		_, ok := value.([]byte)
		return ok
	default:
		return false
	}
}

func conversionError(target types.Type, value string) error {
	return moerr.NewInvalidInputNoCtxf("cannot convert JSON value %q to %s", value, target.DescString())
}
