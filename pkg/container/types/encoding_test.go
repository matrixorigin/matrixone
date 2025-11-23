// Copyright 2025 Matrix Origin
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

package types

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

type binaryStub struct {
	data []byte
}

func (b binaryStub) MarshalBinary() ([]byte, error) {
	return append([]byte(nil), b.data...), nil
}

func (b *binaryStub) UnmarshalBinary(data []byte) error {
	b.data = append(b.data[:0], data...)
	return nil
}

func TestEncodeDecodeSlice(t *testing.T) {
	ints := []int32{1, -2, 3}
	raw := EncodeSlice(ints)
	intElemSize := int(reflect.TypeOf(ints[0]).Size())
	require.Len(t, raw, len(ints)*intElemSize)

	reDecoded := DecodeSlice[int32](append([]byte(nil), raw...))
	require.Equal(t, ints, reDecoded)

	withCap := make([]int16, 2, 4)
	withCap[0], withCap[1] = 5, -7
	rawCap := EncodeSliceWithCap(withCap)
	int16ElemSize := int(reflect.TypeOf(withCap[0]).Size())
	require.Len(t, rawCap, len(withCap)*int16ElemSize)

	reDecodedCap := DecodeSlice[int16](append([]byte(nil), rawCap...))
	require.Equal(t, withCap, reDecodedCap)

	require.Nil(t, EncodeSlice[int32](nil))
	require.Nil(t, EncodeSliceWithCap[int32](nil))
	require.Nil(t, DecodeSlice[int32](nil))
}

func TestDecodeSlicePanics(t *testing.T) {
	require.Panics(t, func() {
		DecodeSlice[int16]([]byte{1, 2, 3})
	})
}

func TestPrimitiveEncodeDecode(t *testing.T) {
	d64, err := Decimal64FromFloat64(12.34, 18, 2)
	require.NoError(t, err)
	d128, err := Decimal128FromFloat64(56.78, 38, 4)
	require.NoError(t, err)

	var uuidVal Uuid
	copy(uuidVal[:], []byte("1234567890abcdef"))

	tsVal := BuildTS(42, 7)
	var blockID Blockid
	copy(blockID[:], []byte("block-id-for-test"))
	rowid := NewRowid(&blockID, 9)

	tests := []struct {
		name   string
		encode func() []byte
		decode func([]byte) any
		expect any
	}{
		{
			name: "bool",
			encode: func() []byte {
				v := true
				return EncodeBool(&v)
			},
			decode: func(b []byte) any { return DecodeBool(b) },
			expect: true,
		},
		{
			name: "int8",
			encode: func() []byte {
				v := int8(-8)
				return EncodeInt8(&v)
			},
			decode: func(b []byte) any { return DecodeInt8(b) },
			expect: int8(-8),
		},
		{
			name: "int16",
			encode: func() []byte {
				v := int16(-16)
				return EncodeInt16(&v)
			},
			decode: func(b []byte) any { return DecodeInt16(b) },
			expect: int16(-16),
		},
		{
			name: "int32",
			encode: func() []byte {
				v := int32(-32)
				return EncodeInt32(&v)
			},
			decode: func(b []byte) any { return DecodeInt32(b) },
			expect: int32(-32),
		},
		{
			name: "int64",
			encode: func() []byte {
				v := int64(-64)
				return EncodeInt64(&v)
			},
			decode: func(b []byte) any { return DecodeInt64(b) },
			expect: int64(-64),
		},
		{
			name: "uint8",
			encode: func() []byte {
				v := uint8(8)
				return EncodeUint8(&v)
			},
			decode: func(b []byte) any { return DecodeUint8(b) },
			expect: uint8(8),
		},
		{
			name: "uint16",
			encode: func() []byte {
				v := uint16(16)
				return EncodeUint16(&v)
			},
			decode: func(b []byte) any { return DecodeUint16(b) },
			expect: uint16(16),
		},
		{
			name: "uint32",
			encode: func() []byte {
				v := uint32(32)
				return EncodeUint32(&v)
			},
			decode: func(b []byte) any { return DecodeUint32(b) },
			expect: uint32(32),
		},
		{
			name: "uint64",
			encode: func() []byte {
				v := uint64(64)
				return EncodeUint64(&v)
			},
			decode: func(b []byte) any { return DecodeUint64(b) },
			expect: uint64(64),
		},
		{
			name: "float32",
			encode: func() []byte {
				v := float32(3.14)
				return EncodeFloat32(&v)
			},
			decode: func(b []byte) any { return DecodeFloat32(b) },
			expect: float32(3.14),
		},
		{
			name: "float64",
			encode: func() []byte {
				v := float64(6.28)
				return EncodeFloat64(&v)
			},
			decode: func(b []byte) any { return DecodeFloat64(b) },
			expect: float64(6.28),
		},
		{
			name: "date",
			encode: func() []byte {
				v := Date(20240507)
				return EncodeDate(&v)
			},
			decode: func(b []byte) any { return DecodeDate(b) },
			expect: Date(20240507),
		},
		{
			name: "time",
			encode: func() []byte {
				v := Time(123456789)
				return EncodeTime(&v)
			},
			decode: func(b []byte) any { return DecodeTime(b) },
			expect: Time(123456789),
		},
		{
			name: "datetime",
			encode: func() []byte {
				v := Datetime(987654321)
				return EncodeDatetime(&v)
			},
			decode: func(b []byte) any { return DecodeDatetime(b) },
			expect: Datetime(987654321),
		},
		{
			name: "timestamp",
			encode: func() []byte {
				v := Timestamp(1024)
				return EncodeTimestamp(&v)
			},
			decode: func(b []byte) any { return DecodeTimestamp(b) },
			expect: Timestamp(1024),
		},
		{
			name: "enum",
			encode: func() []byte {
				v := Enum(5)
				return EncodeEnum(&v)
			},
			decode: func(b []byte) any { return DecodeEnum(b) },
			expect: Enum(5),
		},
		{
			name:   "decimal64",
			encode: func() []byte { return EncodeDecimal64(&d64) },
			decode: func(b []byte) any { return DecodeDecimal64(b) },
			expect: d64,
		},
		{
			name:   "decimal128",
			encode: func() []byte { return EncodeDecimal128(&d128) },
			decode: func(b []byte) any { return DecodeDecimal128(b) },
			expect: d128,
		},
		{
			name:   "uuid",
			encode: func() []byte { return EncodeUuid(&uuidVal) },
			decode: func(b []byte) any { return DecodeUuid(b) },
			expect: uuidVal,
		},
		{
			name: "ts",
			encode: func() []byte {
				ts := tsVal
				return EncodeTxnTS(&ts)
			},
			decode: func(b []byte) any { return DecodeFixed[TS](b) },
			expect: tsVal,
		},
		{
			name: "rowid",
			encode: func() []byte {
				r := rowid
				return EncodeFixed(r)
			},
			decode: func(b []byte) any { return DecodeFixed[Rowid](b) },
			expect: rowid,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.encode()
			require.NotEmpty(t, raw)
			got := tc.decode(raw)
			require.Equal(t, tc.expect, got)
		})
	}
}

func TestEncodeDecodeBinaryInterfaces(t *testing.T) {
	payload := []byte("matrixone")
	stub := binaryStub{data: payload}

	raw, err := Encode(stub)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	var decoded binaryStub
	require.NoError(t, Decode(raw, &decoded))
	require.Equal(t, payload, decoded.data)
}

func TestEncodeDecodeJson(t *testing.T) {
	var bj bytejson.ByteJson
	require.NoError(t, bj.UnmarshalJSON([]byte(`"hello"`)))

	raw, err := EncodeJson(bj)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	decoded := DecodeJson(raw)
	require.Equal(t, bj.Type, decoded.Type)
	require.Equal(t, bj.Data, decoded.Data)
}

func TestEncodeBlockID(t *testing.T) {
	var block Blockid
	copy(block[:], []byte("block-identifier-test"))
	raw := EncodeBlockID(&block)
	require.Equal(t, len(block), len(raw))
	require.Equal(t, block[:], raw)
}

func TestEncodeDecodeType(t *testing.T) {
	tp := &Type{
		Oid:   T_int32,
		Size:  4,
		Width: 10,
		Scale: 2,
	}
	raw := EncodeType(tp)
	require.Len(t, raw, TSize)
	decoded := DecodeType(raw)
	require.Equal(t, *tp, decoded)
}

func TestEncodeDecodeStringSlice(t *testing.T) {
	strs := []string{"alpha", "beta", "gamma"}
	raw := EncodeStringSlice(strs)
	require.NotEmpty(t, raw)
	require.Equal(t, []string{"alpha", "beta", "gamma"}, DecodeStringSlice(raw))

	empty := EncodeStringSlice(nil)
	require.Equal(t, []byte{0, 0, 0, 0}, empty)
	require.Nil(t, DecodeStringSlice(empty))
}

func TestEncodeDecodeValue(t *testing.T) {
	d64, err := Decimal64FromFloat64(78.9, 18, 1)
	require.NoError(t, err)
	d128, err := Decimal128FromFloat64(42.03, 38, 2)
	require.NoError(t, err)

	var uuidVal Uuid
	copy(uuidVal[:], []byte("unique-identifier"))
	ts := BuildTS(7, 11)
	var block Blockid
	copy(block[:], []byte("block-identifier"))
	rowid := NewRowid(&block, 99)

	byteVal := []byte("payload")

	cases := []struct {
		name string
		typ  T
		val  any
		cmp  func(any) any
	}{
		{"bool", T_bool, true, func(v any) any { return v }},
		{"bit", T_bit, uint64(0b1010), func(v any) any { return v }},
		{"int8", T_int8, int8(-8), func(v any) any { return v }},
		{"int16", T_int16, int16(-16), func(v any) any { return v }},
		{"int32", T_int32, int32(-32), func(v any) any { return v }},
		{"int64", T_int64, int64(-64), func(v any) any { return v }},
		{"uint8", T_uint8, uint8(8), func(v any) any { return v }},
		{"uint16", T_uint16, uint16(16), func(v any) any { return v }},
		{"uint32", T_uint32, uint32(32), func(v any) any { return v }},
		{"uint64", T_uint64, uint64(64), func(v any) any { return v }},
		{"float32", T_float32, float32(12.5), func(v any) any { return v }},
		{"float64", T_float64, float64(18.75), func(v any) any { return v }},
		{"decimal64", T_decimal64, d64, func(v any) any { return v }},
		{"decimal128", T_decimal128, d128, func(v any) any { return v }},
		{"date", T_date, Date(202405), func(v any) any { return v }},
		{"time", T_time, Time(1122334455), func(v any) any { return v }},
		{"timestamp", T_timestamp, Timestamp(778899), func(v any) any { return v }},
		{"datetime", T_datetime, Datetime(556677), func(v any) any { return v }},
		{"uuid", T_uuid, uuidVal, func(v any) any { return v }},
		{"ts", T_TS, ts, func(v any) any { return v }},
		{"rowid", T_Rowid, rowid, func(v any) any { return v }},
		{"enum", T_enum, Enum(3), func(v any) any { return v }},
		{"char", T_char, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"varchar", T_varchar, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"blob", T_blob, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"json", T_json, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"text", T_text, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"binary", T_binary, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"varbinary", T_varbinary, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"array_float32", T_array_float32, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"array_float64", T_array_float64, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
		{"datalink", T_datalink, byteVal, func(v any) any { return append([]byte(nil), v.([]byte)...) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := EncodeValue(tc.val, tc.typ)
			decoded := DecodeValue(encoded, tc.typ)
			switch tc.typ {
			case T_char, T_varchar, T_blob, T_json, T_text, T_binary, T_varbinary, T_array_float32, T_array_float64, T_datalink:
				require.Equal(t, tc.cmp(tc.val), decoded)
			default:
				require.Equal(t, tc.cmp(tc.val), decoded)
			}
		})
	}
}

func TestCompareValues(t *testing.T) {
	d64a, _ := Decimal64FromFloat64(10, 18, 0)
	d64b, _ := Decimal64FromFloat64(12, 18, 0)
	d128a, _ := Decimal128FromFloat64(100, 38, 0)
	d128b, _ := Decimal128FromFloat64(101, 38, 0)

	var uuidA, uuidB Uuid
	copy(uuidA[:], []byte("aaaaaaaaaaaaaaaa"))
	copy(uuidB[:], []byte("bbbbbbbbbbbbbbbb"))

	tsA := BuildTS(1, 0)
	tsB := BuildTS(2, 0)

	var block Blockid
	copy(block[:], []byte("block-identifier"))
	rowA := NewRowid(&block, 1)
	rowB := NewRowid(&block, 2)

	require.Equal(t, -1, CompareValues(false, true, T_bool))
	int8a, int8b := int8(1), int8(2)
	int16a, int16b := int16(1), int16(2)
	int32a, int32b := int32(1), int32(2)
	int64a, int64b := int64(1), int64(2)
	uint8a, uint8b := uint8(1), uint8(2)
	uint16a, uint16b := uint16(1), uint16(2)
	uint32a, uint32b := uint32(1), uint32(2)
	uint64a, uint64b := uint64(1), uint64(2)
	float32a, float32b := float32(1.2), float32(3.4)
	float64a, float64b := float64(1.2), float64(3.4)

	require.Equal(t, int(int8a-int8b), CompareValues(int8a, int8b, T_int8))
	require.Equal(t, int(int16a-int16b), CompareValues(int16a, int16b, T_int16))
	require.Equal(t, int(int32a-int32b), CompareValues(int32a, int32b, T_int32))
	require.Equal(t, int(int64a-int64b), CompareValues(int64a, int64b, T_int64))
	require.Equal(t, int(uint8a-uint8b), CompareValues(uint8a, uint8b, T_uint8))
	require.Equal(t, int(uint16a-uint16b), CompareValues(uint16a, uint16b, T_uint16))
	require.Equal(t, int(uint32a-uint32b), CompareValues(uint32a, uint32b, T_uint32))
	require.Equal(t, int(uint64a-uint64b), CompareValues(uint64a, uint64b, T_uint64))
	require.Equal(t, int(float32a-float32b), CompareValues(float32a, float32b, T_float32))
	require.Equal(t, int(float64a-float64b), CompareValues(float64a, float64b, T_float64))
	require.Equal(t, -1, CompareValues(d64a, d64b, T_decimal64))
	require.Equal(t, -1, CompareValues(d128a, d128b, T_decimal128))
	require.Equal(t, -1, CompareValues(Date(1), Date(2), T_date))
	require.Equal(t, -1, CompareValues(Time(1), Time(2), T_time))
	require.Equal(t, -1, CompareValues(Timestamp(1), Timestamp(2), T_timestamp))
	require.Equal(t, -1, CompareValues(Datetime(1), Datetime(2), T_datetime))
	require.Equal(t, -1, CompareValues(uuidA, uuidB, T_uuid))
	require.Equal(t, -1, CompareValues(tsA, tsB, T_TS))
	require.Equal(t, -1, CompareValues(rowA, rowB, T_Rowid))
	require.Equal(t, bytes.Compare([]byte("a"), []byte("b")), CompareValues([]byte("a"), []byte("b"), T_char))
	enumA, enumB := Enum(1), Enum(2)
	require.Equal(t, int(enumA-enumB), CompareValues(enumA, enumB, T_enum))
	require.Equal(t, int(int32a-int32a), CompareValues(int32a, int32a, T_int32))
}

func TestCompareValuesNULL(t *testing.T) {
	require.Equal(t, int(0), CompareValues(nil, nil, T(255)))
	require.Equal(t, int(-1), CompareValues(nil, 1, T(255)))
	require.Equal(t, int(1), CompareValues(1, nil, T(255)))
}

func TestEncodeValueUnsupported(t *testing.T) {
	require.Panics(t, func() {
		EncodeValue(struct{}{}, T(255))
	})
}

func TestDecodeValueUnsupported(t *testing.T) {
	require.Panics(t, func() {
		DecodeValue(nil, T(255))
	})
}

func TestWriteValues(t *testing.T) {
	buf := &bytes.Buffer{}
	d64, err := Decimal64FromFloat64(12.34, 18, 2)
	require.NoError(t, err)
	d128, err := Decimal128FromFloat64(56.78, 38, 4)
	require.NoError(t, err)
	var (
		uuidVal Uuid
		block   Blockid
	)
	copy(uuidVal[:], []byte("uuid-for-write-val"))
	copy(block[:], []byte("block-for-write-va"))
	row := NewRowid(&block, 42)
	ts := BuildTS(9, 1)
	values := []any{
		[]byte("raw"),
		bool(true),
		int8(-8),
		int16(-16),
		int32(-32),
		int64(-64),
		uint8(8),
		uint16(16),
		uint32(32),
		uint64(64),
		float32(3.14),
		float64(6.28),
		Date(20240101),
		Time(123456789),
		Datetime(20240101112233),
		Timestamp(2048),
		d64,
		d128,
		uuidVal,
		ts,
		row,
	}

	n, err := WriteValues(buf, values...)
	require.NoError(t, err)
	require.Equal(t, int64(len(buf.Bytes())), n)
	require.Greater(t, n, int64(0))

	require.Panics(t, func() {
		_, _ = WriteValues(buf, "unsupported")
	})
}

type errWriter struct {
	failAt int
	count  int
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.count == e.failAt {
		return 0, moerr.NewInternalErrorNoCtx("write failed")
	}
	e.count++
	return len(p), nil
}

func TestWriteValuesErrorPropagation(t *testing.T) {
	w := &errWriter{failAt: 2}
	var block Blockid

	copy(block[:], []byte("block-for-error-te"))
	row := NewRowid(&block, 1)

	_, err := WriteValues(w, []byte("raw"), bool(true), row)
	require.Error(t, err)
}

func TestWriteValuesErrorPerType(t *testing.T) {
	cases := []any{
		bool(true),
		int8(1),
		int16(2),
		int32(3),
		int64(4),
		uint8(5),
		uint16(6),
		uint32(7),
		uint64(8),
		float32(1.23),
		float64(4.56),
		Date(20240102),
		Time(987654321),
		Datetime(20240202101010),
		Timestamp(4096),
		func() Decimal64 {
			v, _ := Decimal64FromFloat64(1.23, 18, 2)
			return v
		}(),
		func() Decimal128 {
			v, _ := Decimal128FromFloat64(4.56, 38, 2)
			return v
		}(),
		func() Uuid {
			var id Uuid
			copy(id[:], []byte("uuid-write-error"))
			return id
		}(),
		func() TS {
			return BuildTS(1, 2)
		}(),
		func() Rowid {
			var blk Blockid
			copy(blk[:], []byte("block-write-error"))
			return NewRowid(&blk, 7)
		}(),
	}

	for _, val := range cases {
		_, err := WriteValues(&errWriter{failAt: 0}, val)
		require.Error(t, err, "expected error when encoding %T", val)
	}
}

func TestInt32Uint32Encoding(t *testing.T) {
	vals := []int32{-100, -1, 0, 1, 123456}
	for _, v := range vals {
		encoded := Int32ToUint32(v)
		decoded := Uint32ToInt32(encoded)
		require.Equal(t, v, decoded)
	}
}
