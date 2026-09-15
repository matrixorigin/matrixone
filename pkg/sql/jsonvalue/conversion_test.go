// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func parseConversionValue(t *testing.T, text string) bytejson.ByteJson {
	t.Helper()
	value, err := bytejson.ParseFromString(text)
	require.NoError(t, err)
	return value
}

func conversionIterator(t *testing.T, document, pathText string) *bytejson.PathIterator {
	t.Helper()
	root := parseConversionValue(t, document)
	path, err := bytejson.ParseJsonPath(pathText)
	require.NoError(t, err)
	return bytejson.NewPathIterator(root, &path)
}

func TestConvertScalarReportsIndependentStatusClasses(t *testing.T) {
	cases := []struct {
		name       string
		document   string
		target     types.Type
		wantStatus ConversionStatus
		wantValue  any
	}{
		{
			name:       "integer success",
			document:   `42`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  int64(42),
		},
		{
			name:       "boolean coerces to integer",
			document:   `true`,
			target:     types.T_int8.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  int8(1),
		},
		{
			name:       "unsigned integer success",
			document:   `18446744073709551615`,
			target:     types.T_uint64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  uint64(18446744073709551615),
		},
		{
			name:       "unsigned integer range failure",
			document:   `-1`,
			target:     types.T_uint64.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "float success",
			document:   `1.25`,
			target:     types.T_float64.ToType(),
			wantStatus: StatusSuccess,
			wantValue:  float64(1.25),
		},
		{
			name:       "json null",
			document:   `null`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusJSONNull,
		},
		{
			name:       "composite target scalar",
			document:   `[1]`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusComposite,
		},
		{
			name:       "text conversion failure",
			document:   `"not-a-number"`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusConversionError,
		},
		{
			name:       "integer range failure",
			document:   `9223372036854775808`,
			target:     types.T_int64.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "narrow integer range failure",
			document:   `129`,
			target:     types.T_int8.ToType(),
			wantStatus: StatusRangeError,
		},
		{
			name:       "date success",
			document:   `"2024-02-03"`,
			target:     types.T_date.ToType(),
			wantStatus: StatusSuccess,
		},
		{
			name:       "decimal success",
			document:   `"12.34"`,
			target:     types.New(types.T_decimal64, 10, 2),
			wantStatus: StatusSuccess,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := ConvertScalar(parseConversionValue(t, test.document), test.target)
			require.Equal(t, test.wantStatus, result.Status)
			if test.wantValue != nil {
				require.Equal(t, test.wantValue, result.Value)
			}
			if test.wantStatus != StatusSuccess && test.wantStatus != StatusJSONNull {
				require.Error(t, result.Err)
			}
		})
	}
}

func TestConvertScalarTruncationAndTimestampLocation(t *testing.T) {
	short := ConvertScalar(
		parseConversionValue(t, `"四文字"`),
		types.New(types.T_varchar, 2, 0),
	)
	require.Equal(t, StatusTruncated, short.Status)
	require.Equal(t, []byte("四文"), short.Value)
	require.NotNil(t, short.Warning)
	require.Equal(t, uint16(1265), short.Warning.Code)

	dateTime := parseConversionValue(t, `"2024-02-03 04:05:06"`)
	utc := ConvertScalarWithLocation(dateTime, types.T_timestamp.ToType(), time.UTC)
	plusEight := ConvertScalarWithLocation(dateTime, types.T_timestamp.ToType(), time.FixedZone("UTC+8", 8*60*60))
	require.Equal(t, StatusSuccess, utc.Status)
	require.Equal(t, StatusSuccess, plusEight.Status)
	require.NotEqual(t, utc.Value, plusEight.Value, "timestamp conversion receives execution timezone")

	invalidDate := ConvertScalar(parseConversionValue(t, `"2024-99-99"`), types.T_date.ToType())
	require.Equal(t, StatusConversionError, invalidDate.Status)
	require.Error(t, invalidDate.Err)
}

func TestConvertScalarMalformedByteJsonFailsClosed(t *testing.T) {
	malformed := bytejson.ByteJson{Type: bytejson.TpCodeInt64, Data: []byte{1}}
	result := ConvertScalar(malformed, types.T_int64.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
	result = ConvertScalar(malformed, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)

	unknown := bytejson.ByteJson{Type: bytejson.TpCode(0xff), Data: []byte{1}}
	result = ConvertScalar(unknown, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)

	malformedComposite := bytejson.ByteJson{Type: bytejson.TpCodeArray, Data: []byte{1}}
	result = ConvertScalar(malformedComposite, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.Error(t, result.Err)
}

func TestConvertPathMatchesMalformedRootJSONFailsClosed(t *testing.T) {
	path, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	for _, value := range []bytejson.ByteJson{
		{Type: bytejson.TpCodeInt64, Data: []byte{1}},
		{Type: bytejson.TpCode(0xff), Data: []byte{1}},
	} {
		iterator := bytejson.NewPathIterator(value, &path)
		result := ConvertPathMatches(iterator, types.T_json.ToType())
		iterator.Close()
		require.Equal(t, StatusStatementError, result.Status)
		require.Error(t, result.Err)
	}
}

func TestConvertPathMatchesDistinguishesMissingNullAndMultipleValues(t *testing.T) {
	missing := conversionIterator(t, `{"present":null}`, `$.missing`)
	defer missing.Close()
	result := ConvertPathMatches(missing, types.T_int64.ToType())
	require.Equal(t, StatusMissing, result.Status)
	mp := mpool.MustNewZero()
	missingVector := vector.NewVec(types.T_int64.ToType())
	defer missingVector.Free(mp)
	require.ErrorIs(t, AppendResult(missingVector, result, mp), ErrMissingJSONTableValue)

	null := conversionIterator(t, `{"present":null}`, `$.present`)
	defer null.Close()
	result = ConvertPathMatches(null, types.T_int64.ToType())
	require.Equal(t, StatusJSONNull, result.Status)
	require.NoError(t, result.Err)

	multiple := conversionIterator(t, `[1,2,3]`, `$[*]`)
	defer multiple.Close()
	result = ConvertPathMatches(multiple, types.T_int64.ToType())
	require.Equal(t, StatusConversionError, result.Status)
	require.Error(t, result.Err)
	// The non-JSON ON ERROR decision consumes only the first two matches. It
	// must not eagerly collect or silently discard the third value.
	remaining, matched, err := multiple.Next()
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "3", remaining.String())
}

func TestConvertPathMatchesBuildsBoundedJSONCell(t *testing.T) {
	multiple := conversionIterator(t, `[1,2]`, `$[*]`)
	defer multiple.Close()
	result := ConvertPathMatches(multiple, types.T_json.ToType())
	require.Equal(t, StatusSuccess, result.Status)
	encoded, ok := result.Value.([]byte)
	require.True(t, ok)
	decoded := types.DecodeJson(encoded)
	require.Equal(t, `[1, 2]`, decoded.String())

	limited := conversionIterator(t, `[1,2]`, `$[*]`)
	defer limited.Close()
	result = ConvertPathMatchesWithLimit(limited, types.T_json.ToType(), 34)
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)

	cancelled := conversionIterator(t, `[1,2]`, `$[*]`)
	defer cancelled.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result = ConvertPathMatchesContext(ctx, cancelled, types.T_json.ToType())
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, context.Canceled)
}

func TestConvertPathMatchesEnforcesJSONCellLimitForSingleMatch(t *testing.T) {
	for _, document := range []string{
		`"0123456789"`,
		`[1,2,3]`,
		`{"value":"0123456789"}`,
	} {
		t.Run(document, func(t *testing.T) {
			value := parseConversionValue(t, document)
			encoded, err := value.Marshal()
			require.NoError(t, err)
			require.Greater(t, len(encoded), 1)

			exact := conversionIterator(t, document, `$`)
			defer exact.Close()
			result := ConvertPathMatchesWithLimit(exact, types.T_json.ToType(), len(encoded))
			require.Equal(t, StatusSuccess, result.Status)
			require.Equal(t, encoded, result.Value)

			under := conversionIterator(t, document, `$`)
			defer under.Close()
			result = ConvertPathMatchesWithLimit(under, types.T_json.ToType(), len(encoded)-1)
			require.Equal(t, StatusStatementError, result.Status)
			require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
		})
	}

	null := conversionIterator(t, `null`, `$`)
	defer null.Close()
	result := ConvertPathMatchesWithLimit(null, types.T_json.ToType(), 1)
	require.Equal(t, StatusJSONNull, result.Status)
	require.NoError(t, result.Err)
}

func TestConvertPathMatchesEnforcesDefaultJSONCellLimitForSingleMatch(t *testing.T) {
	payloadLength := types.MaxBlobLen
	data := make([]byte, binary.MaxVarintLen64+payloadLength)
	prefixLength := binary.PutUvarint(data, uint64(payloadLength))
	data = data[:prefixLength+payloadLength]
	value := bytejson.ByteJson{Type: bytejson.TpCodeBlob, Data: data}
	require.Greater(t, len(data)+1, types.MaxBlobLen)

	path, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	iterator := bytejson.NewPathIterator(value, &path)
	result := ConvertPathMatches(iterator, types.T_json.ToType())
	iterator.Close()
	require.Equal(t, StatusStatementError, result.Status)
	require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
}

func TestConvertPathMatchesRejectsLargeOpaqueBeforeEncoding(t *testing.T) {
	for _, test := range []struct {
		name          string
		payloadLength int
	}{
		{name: "1MiB", payloadLength: 1 << 20},
		{name: "8MiB", payloadLength: 8 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Build the source before measuring conversion. This isolates the
			// rejected storage encoding from the caller-owned input allocation.
			data := make([]byte, binary.MaxVarintLen64+test.payloadLength)
			prefixLength := binary.PutUvarint(data, uint64(test.payloadLength))
			data = data[:prefixLength+test.payloadLength]
			value := bytejson.ByteJson{Type: bytejson.TpCodeOpaque, Data: data}
			path, err := bytejson.ParseJsonPath(`$`)
			require.NoError(t, err)
			iterator := bytejson.NewPathIterator(value, &path)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result := ConvertPathMatchesWithLimit(iterator, types.T_json.ToType(), 14)
			runtime.ReadMemStats(&after)
			iterator.Close()

			require.Equal(t, StatusStatementError, result.Status)
			require.ErrorIs(t, result.Err, bytejson.ErrJSONTableCellLimit)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(test.payloadLength/2))
			heapDelta := uint64(0)
			if after.HeapAlloc > before.HeapAlloc {
				heapDelta = after.HeapAlloc - before.HeapAlloc
			}
			t.Logf("payload=%d total_alloc_delta=%d heap_alloc_delta=%d", test.payloadLength, after.TotalAlloc-before.TotalAlloc, heapDelta)
			require.Less(t, heapDelta, uint64(test.payloadLength/2))
		})
	}
}

func TestConvertPathMatchesUsesSharedOpaqueAdmissionForSingleAndMulti(t *testing.T) {
	data := make([]byte, binary.MaxVarintLen64+1<<20)
	prefixLength := binary.PutUvarint(data, uint64(1<<20))
	data = data[:prefixLength+1<<20]
	opaque := bytejson.ByteJson{Type: bytejson.TpCodeOpaque, Data: data}

	singlePath, err := bytejson.ParseJsonPath(`$`)
	require.NoError(t, err)
	single := bytejson.NewPathIterator(opaque, &singlePath)
	singleResult := ConvertPathMatchesWithLimit(single, types.T_json.ToType(), 14)
	single.Close()
	require.Equal(t, StatusStatementError, singleResult.Status)
	require.ErrorIs(t, singleResult.Err, bytejson.ErrJSONTableCellLimit)

	array, err := bytejson.CreateByteJSON([]any{opaque})
	require.NoError(t, err)
	multiPath, err := bytejson.ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	multi := bytejson.NewPathIterator(array, &multiPath)
	multiResult := ConvertPathMatchesWithLimit(multi, types.T_json.ToType(), 14)
	multi.Close()
	require.Equal(t, StatusStatementError, multiResult.Status)
	require.ErrorIs(t, multiResult.Err, bytejson.ErrJSONTableCellLimit)
}

func TestConvertUint32AppendResultRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	vectorValue := vector.NewVec(types.T_uint32.ToType())
	defer vectorValue.Free(mp)

	for _, test := range []struct {
		document string
		want     uint32
	}{
		{document: `0`, want: 0},
		{document: `4294967295`, want: math.MaxUint32},
	} {
		result := ConvertScalar(parseConversionValue(t, test.document), types.T_uint32.ToType())
		require.Equal(t, StatusSuccess, result.Status)
		require.IsType(t, uint32(0), result.Value)
		require.NoError(t, AppendResult(vectorValue, result, mp))
		require.Equal(t, test.want, vector.GetFixedAtNoTypeCheck[uint32](vectorValue, vectorValue.Length()-1))
	}

	null := ConvertScalar(parseConversionValue(t, `null`), types.T_uint32.ToType())
	require.Equal(t, StatusJSONNull, null.Status)
	require.NoError(t, AppendResult(vectorValue, null, mp))
	require.True(t, vectorValue.IsNull(uint64(vectorValue.Length()-1)))

	over := ConvertScalar(parseConversionValue(t, `4294967296`), types.T_uint32.ToType())
	require.Equal(t, StatusRangeError, over.Status)
	require.Error(t, over.Err)
	require.Error(t, AppendResult(vectorValue, over, mp))
	require.Equal(t, 3, vectorValue.Length())
}

func TestAppendResultRejectsErrorsWithoutMutatingVector(t *testing.T) {
	mp := mpool.MustNewZero()
	vectorValue := vector.NewVec(types.T_int32.ToType())
	defer vectorValue.Free(mp)
	require.NoError(t, vector.AppendAny(vectorValue, int32(7), false, mp))

	bad := Result{Status: StatusConversionError, Err: errors.New("conversion failed")}
	require.ErrorIs(t, AppendResult(vectorValue, bad, mp), bad.Err)
	require.Equal(t, 1, vectorValue.Length())
	require.Equal(t, int32(7), vector.GetFixedAtNoTypeCheck[int32](vectorValue, 0))

	require.NoError(t, AppendResult(vectorValue, Result{Status: StatusSuccess, Value: int32(9)}, mp))
	require.NoError(t, AppendResult(vectorValue, Result{Status: StatusJSONNull}, mp))
	require.Equal(t, 3, vectorValue.Length())
	require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](vectorValue, 1))
	require.True(t, vectorValue.IsNull(2))

	missing := Result{Status: StatusMissing}
	require.ErrorIs(t, AppendResult(vectorValue, missing, mp), ErrMissingJSONTableValue)
	require.Equal(t, 3, vectorValue.Length())
}
