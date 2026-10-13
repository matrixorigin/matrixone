// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetvariant

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/stretchr/testify/require"
)

func TestEncodeVariantRoundTrip(t *testing.T) {
	for _, text := range []string{
		"null", "true", "false", `"你好"`, `""`,
		"[]", "{}", "-9223372036854775808", "18446744073709551615", "1.00", "-1.001", "1e37",
		"0e99999", "1.000000000000000000000000000000000000000e-1",
		"99999999999999999999999999999999999999", "-99999999999999999999999999999999999999",
		`{"agent":{"cost":9007199254740993.001,"id":18446744073709551615},"events":[true,null,"工具",{}]}`,
		`{"A":2,"a":1,"":3,"nested":{"present":null}}`,
	} {
		t.Run(text, func(t *testing.T) {
			bj, err := bytejson.ParseFromStringExact(text)
			require.NoError(t, err)
			metadata, payload, err := Encode(context.Background(), bj)
			require.NoError(t, err)
			got, err := Decode(context.Background(), metadata, payload)
			require.NoError(t, err)
			require.Zero(t, bytejson.CompareByteJson(bj, got), "must preserve logical equality")
		})
	}
	for _, v := range []bytejson.ByteJson{
		typed(bytejson.TpCodeDate, "1969-12-31"),
		typed(bytejson.TpCodeDatetime, "1969-12-31 23:59:59.999999"),
		typed(bytejson.TpCodeDatetime, "9999-12-31 23:59:59.999999"),
		typed(bytejson.TpCodeTime, "23:59:59.999999"),
		typed(bytejson.TpCodeOpaque, string([]byte{0, 255, 1})),
	} {
		for _, persisted := range []bool{false, true} {
			bj := v
			if persisted {
				raw, err := v.Marshal()
				require.NoError(t, err)
				require.NoError(t, bj.Unmarshal(raw))
			}
			metadata, payload, err := Encode(context.Background(), bj)
			require.NoError(t, err)
			got, err := Decode(context.Background(), metadata, payload)
			require.NoError(t, err)
			require.Zero(t, bytejson.CompareByteJson(v, got))
		}
	}
	// A known wire representation provides an independent decimal encoder check.
	bj, err := bytejson.ParseFromStringExact("-1.00")
	require.NoError(t, err)
	_, payload, err := Encode(context.Background(), bj)
	require.NoError(t, err)
	require.Equal(t, "20029cffffff", hex.EncodeToString(payload))
}

func TestEncodeVariantRejectsLossyMappings(t *testing.T) {
	for _, text := range []string{"1e38", "1e-39", "1e999999", "1e-999999", "123456789012345678901234567890123456789"} {
		bj, err := bytejson.ParseFromStringExact(text)
		require.NoError(t, err)
		_, _, err = Encode(context.Background(), bj)
		require.Error(t, err, text)
	}
	for _, v := range []bytejson.ByteJson{
		{}, {Type: bytejson.TpCodeInt64, Data: []byte{0}},
		typed(bytejson.TpCodeDate, "0000-01-01"),
		typed(bytejson.TpCodeTime, "838:59:59"),
		typed(bytejson.TpCodeDatetime, "2026-10-12 01:02:03.123456789"),
		typed(bytejson.TpCodeBit, "x"),
	} {
		_, _, err := Encode(context.Background(), v)
		require.Error(t, err)
	}
	f, err := bytejson.CreateByteJSON(math.Inf(1))
	require.NoError(t, err)
	_, _, err = Encode(context.Background(), f)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = Encode(ctx, bytejson.Null)
	require.ErrorIs(t, err, context.Canceled)
}

func TestEncodeVariantRejectsExpansionBeforeWriting(t *testing.T) {
	// The MO payload is only about 5 MiB, but one million individually
	// addressable entries exceed the shared encoder/decoder working budget.
	array := make([]any, 1000000)
	bj, err := bytejson.CreateByteJSON(array)
	require.NoError(t, err)
	metadata, payload, err := Encode(context.Background(), bj)
	require.ErrorContains(t, err, "expansion")
	require.Nil(t, metadata)
	require.Nil(t, payload)
}

func TestEncodeVariantCompactGolden(t *testing.T) {
	bj, err := bytejson.ParseFromStringExact(`{"a":1}`)
	require.NoError(t, err)
	metadata, value, err := Encode(context.Background(), bj)
	require.NoError(t, err)
	require.Equal(t, "1101000161", hex.EncodeToString(metadata))
	require.Equal(t, "02010000020c01", hex.EncodeToString(value))
	// Independent standard-format golden bytes: 12 bytes instead of the
	// previous 14-byte dictionary plus 26-byte fixed-width object payload.
	require.Equal(t, 12, len(metadata)+len(value))
	for _, tc := range []struct {
		number string
		tag    byte
		size   int
	}{
		{"-128", 3, 2}, {"127", 3, 2}, {"128", 4, 3}, {"-129", 4, 3},
		{"32767", 4, 3}, {"32768", 5, 5}, {"-32769", 5, 5},
		{"2147483647", 5, 5}, {"2147483648", 6, 9},
		{"0.000000001", 8, 6}, {"0.0000000001", 9, 10},
		{"0.000000000000000001", 9, 10}, {"1e-38", 10, 18},
		{"999999999.00", 9, 10}, {"18446744073709551615", 10, 18},
	} {
		original, err := bytejson.ParseFromStringExact(tc.number)
		require.NoError(t, err)
		metadata, value, err := Encode(context.Background(), original)
		require.NoError(t, err, tc.number)
		require.Equal(t, tc.tag<<2, value[0], tc.number)
		require.Len(t, value, tc.size, tc.number)
		got, err := Decode(context.Background(), metadata, value)
		require.NoError(t, err)
		require.Zero(t, bytejson.CompareByteJson(original, got))
	}
}

func TestEncodeVariantCompactContainerBoundaries(t *testing.T) {
	for _, count := range []int{0, 1, 127, 128, 255, 256, 257, 1000} {
		fields := make(map[string]any, count)
		for i := 0; i < count; i++ {
			fields[fmt.Sprintf("key_%04d", i)] = []any{int64(i), int64(-i), nil}
		}
		original, err := bytejson.CreateByteJSON(fields)
		require.NoError(t, err)
		metadata, value, err := Encode(context.Background(), original)
		require.NoError(t, err)
		got, err := Decode(context.Background(), metadata, value)
		require.NoError(t, err)
		require.Zero(t, bytejson.CompareByteJson(original, got), count)
		large := (value[0]>>2)&16 != 0
		require.Equal(t, count > 255, large)
		idWidth := int((value[0]>>4)&3) + 1
		require.Equal(t, variantUintWidth(uint32(max(0, count-1))), idWidth)
	}
	// Offset widths cross 8-, 16- and 24-bit boundaries. The compacted
	// parent table must point at the relocated child, including a 16 MiB case.
	for _, length := range []int{0, 63, 64, 250, 255, 256, 65530, 65535, 65536, 1 << 24} {
		text := strings.Repeat("x", length)
		original, err := bytejson.CreateByteJSON([]any{text})
		require.NoError(t, err)
		metadata, value, err := Encode(context.Background(), original)
		require.NoError(t, err)
		width := int((value[0]>>2)&3) + 1
		payloadLength := length + 1
		if length >= 64 {
			payloadLength += 4
		}
		require.Equal(t, variantUintWidth(uint32(payloadLength)), width)
		got, err := Decode(context.Background(), metadata, value)
		require.NoError(t, err)
		require.Equal(t, text, string(got.GetArrayElem(0).GetString()))
	}
	// Dictionary width is determined by both count and UTF-8 byte offsets.
	fields := map[string]any{strings.Repeat("a", 32768): int64(1), strings.Repeat("b", 32768): int64(2)}
	original, err := bytejson.CreateByteJSON(fields)
	require.NoError(t, err)
	metadata, value, err := Encode(context.Background(), original)
	require.NoError(t, err)
	require.Equal(t, byte(0x91), metadata[0])
	got, err := Decode(context.Background(), metadata, value)
	require.NoError(t, err)
	require.Zero(t, bytejson.CompareByteJson(original, got))
}
