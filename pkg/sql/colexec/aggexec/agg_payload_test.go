// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-20
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPayloadFieldIteratorAndErrors(t *testing.T) {
	payload := appendPayloadField(nil, []byte("alpha"), false)
	payload = appendPayloadField(payload, nil, true)

	var seen []string
	err := payloadFieldIterator(payload, 2, func(i int, isNull bool, data []byte) error {
		if isNull {
			seen = append(seen, "null")
			return nil
		}
		seen = append(seen, string(data))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "null"}, seen)

	stopErr := errors.New("stop")
	err = payloadFieldIterator(payload, 2, func(i int, isNull bool, data []byte) error {
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)

	cases := []struct {
		name    string
		payload []byte
		errMsg  string
	}{
		{name: "truncated-null-flag", payload: nil, errMsg: "truncated null flag"},
		{name: "truncated-size", payload: []byte{1}, errMsg: "truncated size"},
		{name: "truncated-field-bytes", payload: append([]byte{1}, []byte{4, 0, 0, 0, 'x'}...), errMsg: "truncated field bytes"},
		{name: "trailing-bytes", payload: append(appendPayloadField(nil, []byte("x"), false), 'z'), errMsg: "trailing bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := payloadFieldIterator(tc.payload, 1, func(i int, isNull bool, data []byte) error {
				return nil
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestEncodeGroupConcatPayloadAndFieldBytes(t *testing.T) {
	mp := mpool.MustNewZero()

	textVec, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("const"), 2, mp)
	require.NoError(t, err)
	intVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(intVec, []int64{7, 9}, nil, mp))

	payload, err := encodeGroupConcatPayload([]*vector.Vector{textVec, intVec}, 1, []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()})
	require.NoError(t, err)

	var got []string
	err = payloadFieldIterator(payload, 2, func(i int, isNull bool, data []byte) error {
		require.False(t, isNull)
		if i == 0 {
			got = append(got, string(data))
		} else {
			got = append(got, string(append([]byte(nil), data...)))
			require.Equal(t, intVec.GetRawBytesAt(1), data)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "const", got[0])

	nullVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(nullVec, nil, true, mp))
	payload, err = encodeGroupConcatPayload([]*vector.Vector{nullVec}, 0, []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)
	require.Nil(t, payload)

	require.Equal(t, textVec.GetBytesAt(0), groupConcatFieldBytes(textVec, 0, types.T_varchar.ToType()))
	require.Equal(t, intVec.GetRawBytesAt(0), groupConcatFieldBytes(intVec, 0, types.T_int64.ToType()))

	textVec.Free(mp)
	intVec.Free(mp)
	nullVec.Free(mp)
}

func TestAppendGroupConcatDataCoversTypes(t *testing.T) {
	d64, err := types.ParseDecimal64("12.34", 10, 2)
	require.NoError(t, err)
	d128, err := types.ParseDecimal128("56.78", 20, 2)
	require.NoError(t, err)
	bj, err := bytejson.CreateByteJSONWithCheck(map[string]any{"k": "v"})
	require.NoError(t, err)
	jsonBytes, err := bj.Marshal()
	require.NoError(t, err)

	dateVal := types.Date(3)
	timeVal := types.Time(1234567)
	datetimeVal := types.Datetime(8)
	timestampVal := types.Timestamp(9)
	intervalVal := types.Day
	tsVal := types.BuildTS(11, 12)
	rowidVal := types.Rowid{1, 2, 3}
	blockidVal := types.Blockid{4, 5, 6}

	cases := []struct {
		name    string
		typ     types.Type
		data    []byte
		want    string
		wantErr string
	}{
		{name: "bit", typ: types.T_bit.ToType(), data: types.EncodeUint64(ptr(uint64(7))), want: "7"},
		{name: "bool", typ: types.T_bool.ToType(), data: []byte{1}, want: "true"},
		{name: "int8", typ: types.T_int8.ToType(), data: []byte{0xfe}, want: "-2"},
		{name: "int16", typ: types.T_int16.ToType(), data: types.EncodeInt16(ptr(int16(-3))), want: "-3"},
		{name: "int32", typ: types.T_int32.ToType(), data: types.EncodeInt32(ptr(int32(-4))), want: "-4"},
		{name: "int64", typ: types.T_int64.ToType(), data: types.EncodeInt64(ptr(int64(-5))), want: "-5"},
		{name: "uint8", typ: types.T_uint8.ToType(), data: []byte{6}, want: "6"},
		{name: "uint16", typ: types.T_uint16.ToType(), data: types.EncodeUint16(ptr(uint16(7))), want: "7"},
		{name: "uint32", typ: types.T_uint32.ToType(), data: types.EncodeUint32(ptr(uint32(8))), want: "8"},
		{name: "uint64", typ: types.T_uint64.ToType(), data: types.EncodeUint64(ptr(uint64(9))), want: "9"},
		{name: "float32", typ: types.T_float32.ToType(), data: types.EncodeFloat32(ptr(float32(1.5))), want: "1.5"},
		{name: "float64", typ: types.T_float64.ToType(), data: types.EncodeFloat64(ptr(2.5)), want: "2.5"},
		{name: "decimal64", typ: types.New(types.T_decimal64, 10, 2), data: types.EncodeDecimal64(&d64), want: d64.Format(2)},
		{name: "decimal128", typ: types.New(types.T_decimal128, 20, 2), data: types.EncodeDecimal128(&d128), want: d128.Format(2)},
		{name: "date", typ: types.T_date.ToType(), data: types.EncodeDate(&dateVal), want: dateVal.String()},
		{name: "time", typ: types.T_time.ToType(), data: types.EncodeTime(&timeVal), want: timeVal.String()},
		{name: "datetime", typ: types.T_datetime.ToType(), data: types.EncodeDatetime(&datetimeVal), want: datetimeVal.String()},
		{name: "timestamp", typ: types.T_timestamp.ToType(), data: types.EncodeTimestamp(&timestampVal), want: timestampVal.String()},
		{name: "text", typ: types.T_text.ToType(), data: []byte("hello"), want: "hello"},
		{name: "json", typ: types.T_json.ToType(), data: jsonBytes, want: types.DecodeJson(jsonBytes).String()},
		{name: "interval", typ: types.Type{Oid: types.T_interval}, data: []byte{byte(intervalVal)}, want: intervalVal.String()},
		{name: "ts", typ: types.T_TS.ToType(), data: tsVal[:], want: tsVal.ToString()},
		{name: "rowid", typ: types.T_Rowid.ToType(), data: rowidVal[:], want: rowidVal.String()},
		{name: "blockid", typ: types.T_Blockid.ToType(), data: blockidVal[:], want: fmt.Sprint(blockidVal)},
		{name: "too-long", typ: types.T_text.ToType(), data: make([]byte, math.MaxUint16+1), wantErr: "too long"},
		{name: "unsupported", typ: types.T_decimal256.ToType(), data: []byte{1}, wantErr: "unsupported type"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := appendGroupConcatData(nil, tc.typ, tc.data)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}

	got, err := appendGroupConcatData([]byte("prefix-"), types.T_varchar.ToType(), []byte("tail"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(got), "prefix-tail"))
}

func ptr[T any](v T) *T {
	return &v
}
