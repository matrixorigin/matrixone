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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func jsonValueTestExtracted(t *testing.T, raw string) jsonValueExtracted {
	t.Helper()
	value, err := types.ParseStringToByteJson(raw)
	require.NoError(t, err)
	text, err := value.Unquote()
	require.NoError(t, err)
	return jsonValueExtracted{state: jsonValueOneValue, value: value, text: text}
}

func TestJSONValueScalarConvertersCoverSupportedTargets(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		target types.Type
		run    func(jsonValueExtracted, types.Type) (any, error)
		want   any
	}{
		{"int16", `"123"`, types.T_int16.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueInt16(e, target)
		}, int16(123)},
		{"int32", `"123"`, types.T_int32.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueInt32(e, target)
		}, int32(123)},
		{"uint8", `"123"`, types.T_uint8.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueUint8(e, target)
		}, uint8(123)},
		{"uint16", `"123"`, types.T_uint16.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueUint16(e, target)
		}, uint16(123)},
		{"uint32", `"123"`, types.T_uint32.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueUint32(e, target)
		}, uint32(123)},
		{"float32", `"1.25"`, types.T_float32.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueFloat32(e, target)
		}, float32(1.25)},
		{"float64", `"1.25"`, types.T_float64.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueFloat64(e, target)
		}, float64(1.25)},
		{"decimal64", `"12.34"`, types.New(types.T_decimal64, 10, 2), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueDecimal64(e, target)
		}, func() types.Decimal64 {
			value, err := types.ParseDecimal64("12.34", 10, 2)
			require.NoError(t, err)
			return value
		}()},
		{"decimal128", `"12.34"`, types.New(types.T_decimal128, 30, 2), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueDecimal128(e, target)
		}, func() types.Decimal128 {
			value, err := types.ParseDecimal128("12.34", 30, 2)
			require.NoError(t, err)
			return value
		}()},
		{"decimal256", `"12.34"`, types.New(types.T_decimal256, 60, 2), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueDecimal256(e, target)
		}, func() types.Decimal256 {
			value, err := types.ParseDecimal256("12.34", 60, 2)
			require.NoError(t, err)
			return value
		}()},
		{"date", `"2024-01-02"`, types.T_date.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueDate(e, target)
		}, nil},
		{"time", `"12:34:56.123"`, types.New(types.T_time, 3, 3), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueTime(e, target)
		}, nil},
		{"datetime", `"2024-01-02 12:34:56.123"`, types.New(types.T_datetime, 3, 3), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueDatetime(e, target)
		}, nil},
		{"year", `"2024"`, types.T_year.ToType(), func(e jsonValueExtracted, target types.Type) (any, error) {
			return parseJSONValueYear(e, target)
		}, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.run(jsonValueTestExtracted(t, tc.raw), tc.target)
			require.NoError(t, err)
			if tc.want != nil {
				require.Equal(t, tc.want, got)
			} else {
				require.NotNil(t, got)
			}
		})
	}
}

func TestJSONValueNumericConvertersCoverSourceDomains(t *testing.T) {
	intCases := []struct {
		name string
		e    jsonValueExtracted
		want int64
	}{
		{"signed", jsonValueTestExtracted(t, "12"), 12},
		{"unsigned", jsonValueTestExtracted(t, "12"), 12},
		{"float", jsonValueTestExtracted(t, "12.0"), 12},
		{"decimal", jsonValueExtracted{state: jsonValueOneValue, value: newTypedByteJson(bytejson.TpCodeDecimal, "12"), text: "12"}, 12},
		{"string", jsonValueTestExtracted(t, `" 12 "`), 12},
		{"true", jsonValueTestExtracted(t, "true"), 1},
		{"false", jsonValueTestExtracted(t, "false"), 0},
	}
	for _, tc := range intCases {
		t.Run("signed/"+tc.name, func(t *testing.T) {
			got, err := parseJSONValueInt64(tc.e, types.T_int64.ToType())
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
		t.Run("unsigned/"+tc.name, func(t *testing.T) {
			got, err := parseJSONValueUint64(tc.e, types.T_uint64.ToType())
			require.NoError(t, err)
			require.Equal(t, uint64(tc.want), got)
		})
	}

	_, err := parseJSONValueInt64(jsonValueTestExtracted(t, `{"a":1}`), types.T_int64.ToType())
	require.Error(t, err)
	_, err = parseJSONValueUint64(jsonValueTestExtracted(t, `"not-a-number"`), types.T_uint64.ToType())
	require.Error(t, err)
	_, err = parseJSONValueInt64(jsonValueTestExtracted(t, "1.5"), types.T_int64.ToType())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDataTruncated))
}

func TestJSONValueTemporalConversionBoundaries(t *testing.T) {
	_, err := parseJSONValueDate(jsonValueTestExtracted(t, `"0000-00-00"`), types.T_date.ToType())
	require.Error(t, err)

	_, err = parseJSONValueTime(
		jsonValueTestExtracted(t, `"12:34:56.1234"`),
		types.New(types.T_time, 0, 3),
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDataTruncated))

	_, err = parseJSONValueDatetime(
		jsonValueTestExtracted(t, `"2024-01-02 12:34:56.1234"`),
		types.New(types.T_datetime, 0, 3),
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDataTruncated))

	_, err = parseJSONValueDatetime(
		jsonValueTestExtracted(t, `"0000-00-00 00:00:00"`),
		types.New(types.T_datetime, 0, 3),
	)
	require.Error(t, err)
}

func TestJSONValueTextAndScalarBoundaries(t *testing.T) {
	require.Equal(t, []byte("abc"), func() []byte {
		value, err := jsonValueTextBytes("abc", types.New(types.T_varchar, 3, 0))
		require.NoError(t, err)
		return value
	}())
	_, err := jsonValueTextBytes("abcd", types.New(types.T_varchar, 3, 0))
	require.Error(t, err)
	_, err = jsonValueTextBytes("abc", types.New(types.T_binary, 2, 0))
	require.Error(t, err)
	_, err = jsonValueTextBytes(string([]byte{0xff}), types.T_varchar.ToType())
	require.Error(t, err)

	composite := jsonValueTestExtracted(t, `{"a":1}`)
	_, err = jsonValueScalarText(composite)
	require.Error(t, err)
	_, err = jsonValueNumericText(composite)
	require.Error(t, err)
}

func TestJSONValueResponseAndWarningBoundaries(t *testing.T) {
	empty := jsonValueExtracted{state: jsonValueEmpty, path: "$.missing"}
	multiple := jsonValueExtracted{state: jsonValueMultiple, path: "$.*"}
	var nulls, defaults int
	var defaultWasEmpty bool
	appendNull := func() error {
		nulls++
		return nil
	}
	appendDefault := func(_ uint64, isEmpty bool) error {
		defaults++
		defaultWasEmpty = isEmpty
		return nil
	}
	require.NoError(t, jsonValueApplyResponse(empty, jsonValueNullResponse, 0, appendNull, appendDefault, context.Background()))
	require.NoError(t, jsonValueApplyResponse(empty, jsonValueDefaultResponse, 0, appendNull, appendDefault, context.Background()))
	require.True(t, defaultWasEmpty)
	require.NoError(t, jsonValueApplyResponse(multiple, jsonValueDefaultResponse, 0, appendNull, appendDefault, context.Background()))
	require.False(t, defaultWasEmpty)
	require.Equal(t, 1, nulls)
	require.Equal(t, 2, defaults)
	missingErr := jsonValueApplyResponse(empty, jsonValueErrorResponse, 0, appendNull, appendDefault, context.Background())
	require.True(t, moerr.IsMoErrCode(missingErr, moerr.ErrMissingJSONValue))
	multipleErr := jsonValueApplyResponse(multiple, jsonValueErrorResponse, 0, appendNull, appendDefault, context.Background())
	require.True(t, moerr.IsMoErrCode(multipleErr, moerr.ErrMultipleJSONValues))
	require.Error(t, jsonValueApplyResponse(empty, 99, 0, appendNull, appendDefault, context.Background()))

	require.NotNil(t, jsonValueContext(nil))
	ctx := context.WithValue(context.Background(), "json-value", "ctx")
	require.Same(t, ctx, jsonValueContext(&process.Process{Ctx: ctx}))

	session := &numericWarningSession{}
	proc := &process.Process{Session: session}
	appendJSONValueWarning(nil, errors.New("ignored"), false)
	appendJSONValueWarning(proc, errors.New("invalid json"), false)
	appendJSONValueWarning(proc, moerr.NewOutOfRangeNoCtxf("DOUBLE", "value"), true)
	appendJSONValueWarning(proc, moerr.NewDataTruncatedNoCtxf("DOUBLE", "value"), true)
	appendJSONValueWarning(proc, errors.New("conversion"), true)
	require.Len(t, session.warnings, 4)
	require.Equal(t, moerr.ER_INVALID_JSON_TEXT, session.warnings[0].code)
	require.Equal(t, moerr.ER_WARN_DATA_OUT_OF_RANGE, session.warnings[1].code)
	require.Equal(t, moerr.WARN_DATA_TRUNCATED, session.warnings[2].code)
	require.Equal(t, moerr.WARN_DATA_TRUNCATED, session.warnings[3].code)
}

func TestDecodeJSONValueStoredRejectsShortInput(t *testing.T) {
	_, err := decodeJSONValueStored([]byte{byte(bytejson.TpCodeArray)})
	require.Error(t, err)
}
