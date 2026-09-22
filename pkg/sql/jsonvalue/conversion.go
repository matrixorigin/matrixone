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
var ErrMissingJSONTableValue = moerr.NewInvalidInputNoCtx("JSON_TABLE path produced no match")

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
				Err:    moerr.NewInvalidInputNoCtxf("invalid ByteJson value: %v", recovered),
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
	if err := validateJSONValue(value); err != nil {
		// Keep the established conversion classification for a well-shaped
		// floating-point payload whose value is NaN or Inf. Other malformed
		// representations must not fall through to an empty scalar string.
		if value.Type != bytejson.TpCodeFloat64 || len(value.Data) != 8 || isTextConversionTarget(target.Oid) {
			return Result{Status: StatusStatementError, Err: err}
		}
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
		return convertDecimal(text, target, func(input string) (any, error) {
			return types.ParseDecimal64(input, target.Width, target.Scale)
		})
	case types.T_decimal128:
		return convertDecimal(text, target, func(input string) (any, error) {
			return types.ParseDecimal128(input, target.Width, target.Scale)
		})
	case types.T_decimal256:
		return convertDecimal(text, target, func(input string) (any, error) {
			return types.ParseDecimal256(input, target.Width, target.Scale)
		})
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
			Err:    moerr.NewNotSupportedf(ctx, "JSON scalar conversion to %s", target),
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
		return Result{Status: StatusStatementError, Err: moerr.NewInvalidStateNoCtx("nil JSON_TABLE path iterator")}
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
			Err:    moerr.NewInvalidInputNoCtxf("JSON_TABLE path returned multiple values for %s", target.DescString()),
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
			Err:    moerr.NewInvalidInputNoCtxf("invalid JSON_TABLE JSON cell limit %d", maxBytes),
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
		return moerr.NewInvalidStateNoCtx("nil destination vector")
	}
	if mp == nil {
		return moerr.NewInvalidStateNoCtx("nil destination vector mpool")
	}
	if vec.IsConst() {
		return moerr.NewInvalidStateNoCtx("cannot append JSON_TABLE value to a constant vector")
	}
	switch result.Status {
	case StatusJSONNull:
		// handled below
	case StatusSuccess, StatusTruncated:
		if !appendValueCompatible(vec.GetType().Oid, result.Value) {
			return moerr.NewInvalidInputNoCtxf("JSON_TABLE conversion result %s is incompatible with %s", result.Status, vec.GetType().DescString())
		}
	case StatusMissing:
		return ErrMissingJSONTableValue
	default:
		if result.Err != nil {
			return result.Err
		}
		return moerr.NewInternalErrorNoCtxf("JSON_TABLE conversion did not produce a value: %s", result.Status)
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
			err = moerr.NewInvalidInputNoCtxf("invalid ByteJson value: %v", recovered)
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
	return moerr.NewInvalidInputNoCtxf("invalid ByteJson value of type %#x", value.Type)
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
			err = moerr.NewInvalidInputNoCtxf("invalid ByteJson scalar: %v", recovered)
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

func isTextConversionTarget(oid types.T) bool {
	switch oid {
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_datalink:
		return true
	default:
		return false
	}
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
	if textLooksNumeric(text) || textLooksHexadecimal(text) {
		return StatusRangeError
	}
	return StatusConversionError
}

func textLooksHexadecimal(text string) bool {
	start, ok := hexadecimalPayloadStart(text)
	if !ok {
		return false
	}
	digits := 0
	for i := start; i < len(text); i++ {
		if text[i] == ' ' {
			continue
		}
		if _, ok := hexadecimalDigit(text[i]); !ok {
			return false
		}
		digits++
	}
	return digits > 0
}

func hexadecimalPayloadStart(text string) (int, bool) {
	i := 0
	for i < len(text) && text[i] == ' ' {
		i++
	}
	if i < len(text) && (text[i] == '-' || text[i] == '+') {
		i++
		for i < len(text) && text[i] == ' ' {
			i++
		}
	}
	if i+2 > len(text) || text[i] != '0' || text[i+1] != 'x' {
		return 0, false
	}
	return i + 2, true
}

func hexadecimalDigit(ch byte) (byte, bool) {
	switch {
	case ch >= '0' && ch <= '9':
		return ch - '0', true
	case ch >= 'a' && ch <= 'f':
		return ch - 'a' + 10, true
	case ch >= 'A' && ch <= 'F':
		return ch - 'A' + 10, true
	default:
		return 0, false
	}
}

type decimalScan struct {
	negative      bool
	digits        []byte
	digitCount    int64
	suffixNonZero bool
	point         int64
	exponent      int64
	exponentSign  int64
	exponentHuge  bool
}

// convertDecimal parses a bounded canonical decimal representation before
// calling the existing Decimal implementation. The existing parser rounds
// while it reads its fixed-width coefficient, so passing a long input through
// it can round once while parsing and again while applying the target scale.
// Keeping only the target-width prefix plus one rounding digit avoids that
// double rounding while keeping retained analysis state bounded by the
// destination type.
func convertDecimal(text string, target types.Type, parse func(string) (any, error)) Result {
	canonical, truncated, known, outOfRange := canonicalDecimalInput(text, target)
	if outOfRange {
		return Result{Status: StatusRangeError, Err: decimalRangeError(text, target)}
	}
	input := text
	if known {
		input = canonical
	}
	value, err := parse(input)
	if err != nil {
		return Result{Status: decimalFailureStatus(text), Err: err}
	}
	isHexadecimal, withinHexWidth := decimalHexValueWithinWidth(text, target)
	if !known {
		if isHexadecimal && !withinHexWidth {
			return Result{Status: StatusRangeError, Err: decimalRangeError(text, target)}
		}
		if !decimalValueWithinWidth(value, target) {
			return Result{Status: StatusRangeError, Err: decimalRangeError(text, target)}
		}
	}
	// A successful non-hex input outside the bounded decimal grammar is accepted
	// by the legacy parser. It is unsafe to treat that representation as exact,
	// so preserve the warning contract conservatively rather than silently
	// losing diagnostics. Hexadecimal input is checked separately as an exact
	// integer representation and does not need this fallback warning.
	if !known && !isHexadecimal {
		truncated = true
	}
	return decimalResult(value, target, truncated)
}

func decimalResult(value any, target types.Type, truncated bool) Result {
	result := Result{Value: value, Status: StatusSuccess}
	if truncated {
		result.Status = StatusTruncated
		result.Warning = &WarningDescriptor{
			Code:    moerr.WARN_DATA_TRUNCATED,
			Message: fmt.Sprintf("JSON_TABLE decimal value truncated for %s", target.DescString()),
		}
	}
	return result
}

func decimalWidthLimit(oid types.T) int32 {
	switch oid {
	case types.T_decimal64:
		return 18
	case types.T_decimal128:
		return 38
	case types.T_decimal256:
		return 76
	default:
		return 0
	}
}

func decimalRangeError(text string, target types.Type) error {
	width := target.Width
	if limit := decimalWidthLimit(target.Oid); width > limit {
		width = limit
	}
	switch target.Oid {
	case types.T_decimal64:
		return moerr.NewInvalidInputNoCtxf("%s beyond the range, can't be converted to Decimal64(%d,%d).", text, width, target.Scale)
	case types.T_decimal128:
		return moerr.NewInvalidInputNoCtxf("%s beyond the range, can't be converted to Decimal128(%d,%d).", text, width, target.Scale)
	case types.T_decimal256:
		return moerr.NewInvalidInputNoCtxf("%s beyond the range, can't be converted to Decimal256(%d,%d).", text, width, target.Scale)
	default:
		return conversionError(target, text)
	}
}

func decimalValueWithinWidth(value any, target types.Type) bool {
	width := target.Width
	if limit := decimalWidthLimit(target.Oid); width > limit {
		width = limit
	}
	if width < 0 {
		return false
	}

	var formatted string
	switch value := value.(type) {
	case types.Decimal64:
		formatted = value.Format(0)
	case types.Decimal128:
		formatted = value.Format(0)
	case types.Decimal256:
		formatted = value.Format(0)
	default:
		return false
	}
	magnitude := strings.TrimLeft(strings.TrimPrefix(formatted, "-"), "0")
	return len(magnitude) <= int(width)
}

func decimalHexValueWithinWidth(text string, target types.Type) (isHexadecimal, within bool) {
	start, ok := hexadecimalPayloadStart(text)
	if !ok {
		return false, true
	}
	width := target.Width
	if limit := decimalWidthLimit(target.Oid); width > limit {
		width = limit
	}
	integerDigits := width - target.Scale
	if integerDigits < 0 {
		integerDigits = 0
	}

	var magnitude types.Decimal256
	seenNonZero := false
	var significantDigits int32
	for i := start; i < len(text); i++ {
		if text[i] == ' ' {
			continue
		}
		digit, ok := hexadecimalDigit(text[i])
		if !ok {
			return true, false
		}
		if !seenNonZero && digit == 0 {
			continue
		}
		seenNonZero = true
		significantDigits++
		if significantDigits > integerDigits {
			return true, false
		}
		var err error
		magnitude, err = magnitude.Mul256(types.Decimal256{B0_63: 16})
		if err != nil {
			return true, false
		}
		magnitude, err = magnitude.Add256(types.Decimal256{B0_63: uint64(digit)})
		if err != nil {
			return true, false
		}
	}
	if !seenNonZero {
		return true, true
	}
	limit, ok := decimal256PowerOfTen(integerDigits)
	if !ok || magnitude.Sign() {
		return true, false
	}
	return true, magnitude.Less(limit)
}

func decimal256PowerOfTen(width int32) (types.Decimal256, bool) {
	if width < 0 || width > 76 {
		return types.Decimal256{}, false
	}
	result := types.Decimal256{B0_63: 1}
	for width >= 19 {
		var err error
		result, err = result.Mul256(types.Decimal256{B0_63: types.Pow10[19]})
		if err != nil {
			return types.Decimal256{}, false
		}
		width -= 19
	}
	if width > 0 {
		var err error
		result, err = result.Mul256(types.Decimal256{B0_63: types.Pow10[width]})
		if err != nil {
			return types.Decimal256{}, false
		}
	}
	return result, true
}

// canonicalDecimalInput returns a short decimal token whose value is already
// rounded to target.Scale. The scan accepts the decimal syntax understood by
// the JSON_TABLE conversion boundary, including spaces ignored by the legacy
// decimal parser and an optional leading plus sign. It never allocates based
// on an exponent or retains more than width+1 significant digits.
func canonicalDecimalInput(text string, target types.Type) (canonical string, truncated, known, outOfRange bool) {
	widthLimit := decimalWidthLimit(target.Oid)
	if widthLimit == 0 || target.Width <= 0 || target.Scale < 0 || target.Scale > widthLimit {
		return "", false, false, false
	}
	width := target.Width
	if width > widthLimit {
		width = widthLimit
	}
	if width <= 0 {
		return "", false, false, false
	}

	keepLimit := int64(width) + 1
	scan, ok := scanDecimal(text, keepLimit)
	if !ok {
		return "", false, false, false
	}
	known = true
	if scan.digitCount == 0 {
		return "0", false, true, false
	}
	if scan.exponentHuge {
		if scan.exponentSign > 0 {
			return "", false, true, true
		}
		return "0", true, true, false
	}

	cut := scan.point + int64(target.Scale)
	if cut > int64(width) {
		return "", false, true, true
	}

	var coefficient []byte
	roundUp := false
	if cut <= 0 {
		truncated = true
		if cut == 0 && scan.digits[0] >= '5' {
			roundUp = true
		}
		coefficient = []byte{'0'}
	} else if cut < scan.digitCount {
		// cut <= width < keepLimit, so the first discarded digit is always
		// retained by scanDecimal.
		coefficient = append([]byte(nil), scan.digits[:cut]...)
		for _, digit := range scan.digits[cut:] {
			if digit != '0' {
				truncated = true
				break
			}
		}
		if scan.suffixNonZero {
			truncated = true
		}
		roundUp = scan.digits[cut] >= '5'
	} else {
		coefficient = append([]byte(nil), scan.digits...)
		for int64(len(coefficient)) < cut {
			coefficient = append(coefficient, '0')
		}
	}
	if roundUp {
		coefficient = incrementDecimalDigits(coefficient)
	}
	if int32(len(coefficient)) > width {
		return "", false, true, true
	}

	canonical = formatScaledDecimal(coefficient, target.Scale, scan.negative)
	return canonical, truncated, true, false
}

// scanDecimal retains only significant digits needed to form a target-width
// coefficient. It records whether non-zero digits were omitted so warning
// classification never depends on an unbounded intermediate number.
func scanDecimal(text string, keepLimit int64) (decimalScan, bool) {
	text = strings.TrimSpace(text)
	var scan decimalScan
	scan.exponentSign = 1
	firstNonZero := int64(-1)
	digitIndex := int64(0)
	digitsBeforeDot := int64(0)
	seenDot := false
	seenExponent := false
	inExponent := false
	exponentSignSeen := false
	exponentDigitsSeen := false
	const maxExponent = int64(1 << 60)

	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == ' ' {
			continue
		}
		if i == 0 && (ch == '-' || ch == '+') {
			scan.negative = ch == '-'
			continue
		}
		if !inExponent {
			switch {
			case ch >= '0' && ch <= '9':
				if !seenDot {
					digitsBeforeDot++
				}
				if firstNonZero < 0 && ch != '0' {
					firstNonZero = digitIndex
				}
				if firstNonZero >= 0 {
					significantIndex := digitIndex - firstNonZero
					scan.digitCount++
					if significantIndex < keepLimit {
						scan.digits = append(scan.digits, ch)
					} else if ch != '0' {
						scan.suffixNonZero = true
					}
				}
				digitIndex++
			case ch == '.':
				if seenDot {
					return decimalScan{}, false
				}
				seenDot = true
			case ch == 'e':
				if seenExponent || digitIndex == 0 {
					return decimalScan{}, false
				}
				seenExponent = true
				inExponent = true
			default:
				return decimalScan{}, false
			}
			continue
		}

		if !exponentSignSeen && !exponentDigitsSeen && (ch == '-' || ch == '+') {
			scan.exponentSign = 1
			if ch == '-' {
				scan.exponentSign = -1
			}
			exponentSignSeen = true
			continue
		}
		if ch < '0' || ch > '9' {
			return decimalScan{}, false
		}
		exponentDigitsSeen = true
		if scan.exponentHuge {
			continue
		}
		digit := int64(ch - '0')
		if scan.exponent > (maxExponent-digit)/10 {
			scan.exponentHuge = true
			continue
		}
		scan.exponent = scan.exponent*10 + digit
	}
	if digitIndex == 0 {
		return decimalScan{}, false
	}
	// ParseDecimal accepts a missing exponent number (for example, "1e+")
	// as a zero exponent. Keep that established behavior while rejecting a
	// second sign after exponent digits.
	if firstNonZero < 0 {
		return scan, true
	}
	scan.point = digitsBeforeDot - firstNonZero
	if scan.exponentHuge {
		scan.exponent = maxExponent
	} else if scan.exponent != 0 {
		if scan.exponentSign > 0 {
			if scan.point > maxExponent-scan.exponent {
				scan.exponentHuge = true
			} else {
				scan.point += scan.exponent
			}
		} else {
			if scan.point < -maxExponent+scan.exponent {
				scan.exponentHuge = true
				scan.exponentSign = -1
			} else {
				scan.point -= scan.exponent
			}
		}
	}
	return scan, true
}

func incrementDecimalDigits(digits []byte) []byte {
	for i := len(digits) - 1; i >= 0; i-- {
		if digits[i] < '9' {
			digits[i]++
			return digits
		}
		digits[i] = '0'
	}
	return append([]byte{'1'}, digits...)
}

func formatScaledDecimal(coefficient []byte, scale int32, negative bool) string {
	for len(coefficient) > 1 && coefficient[0] == '0' {
		coefficient = coefficient[1:]
	}
	var value string
	if scale == 0 {
		value = string(coefficient)
	} else if int32(len(coefficient)) <= scale {
		value = "0." + strings.Repeat("0", int(scale)-len(coefficient)) + string(coefficient)
	} else {
		point := len(coefficient) - int(scale)
		value = string(coefficient[:point]) + "." + string(coefficient[point:])
	}
	if negative && value != "0" {
		return "-" + value
	}
	return value
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
