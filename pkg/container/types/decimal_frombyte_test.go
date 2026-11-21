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

package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The FromByte decoders are the IsBin hex/bit literal entry: the byte payload
// spells one unsigned big-endian integer N and DECIMAL(width, scale) stores the
// value N, so the coefficient is N*10^scale (#31816). The expected strings and
// coefficients below are hand-derived values, not decoder output; the text
// parser is an independent oracle because it never runs the binary route.

func TestParseDecimal64FromByteUnsignedBEContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
		want    string
	}{
		{"x41 scale0", "\x41", 3, 0, "65"},
		{"x41 scale1", "\x41", 3, 1, "65.0"},
		{"x41 scale2", "\x41", 5, 2, "65.00"},
		{"x0100 scale3", "\x01\x00", 6, 3, "256.000"},
		{"bit1000001 scale0", "\x41", 5, 0, "65"},
		{"x96 insert shape", "\x00\x00\x00\x96", 10, 2, "150.00"},
		{"x96 small precision", "\x00\x00\x00\x96", 5, 2, "150.00"},
		{"eight zero bytes then 96", strings.Repeat("\x00", 8) + "\x96", 10, 2, "150.00"},
		{"single zero byte", "\x00", 10, 2, "0.00"},
		{"long all-zero payload", strings.Repeat("\x00", 512), 10, 2, "0.00"},
		{"2^56 scale1", "\x01" + strings.Repeat("\x00", 7), 18, 1, "72057594037927936.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDecimal64FromByte(tc.payload, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(tc.scale),
				"canonical text at the declared scale")
			fromText, err := ParseDecimal64(tc.want, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, fromText, got, "text parse of the same digits is an independent oracle")
		})
	}

	// Width clamps like the text parse; a negative scale is treated as 0.
	t.Run("clamped and defensive targets", func(t *testing.T) {
		got, err := ParseDecimal64FromByte("\x00\x00\x00\x96", 40, 2)
		require.NoError(t, err)
		require.Equal(t, Decimal64(15000), got, "width clamps to 18")
		got, err = ParseDecimal64FromByte("\x96", 4, -3)
		require.NoError(t, err)
		require.Equal(t, Decimal64(150), got, "negative scale is treated as 0")
		zero, err := ParseDecimal64FromByte("\x00", -5, 19)
		require.NoError(t, err)
		require.Equal(t, Decimal64(0), zero, "zero passes any clamped target")
	})

	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
	}{
		{"empty payload", "", 10, 2},
		{"nine significant bytes", "\x01" + strings.Repeat("\x00", 8), 18, 0},
		{"high bit 8-byte scale0", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xCD", 18, 0},
		{"high bit 8-byte scale2", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xCD", 10, 2},
		{"2^63 boundary", "\x80" + strings.Repeat("\x00", 7), 18, 0},
		{"max int64 past declared 18 digits", "\x7F" + strings.Repeat("\xFF", 7), 18, 0},
		{"2^56 past declared width 10", "\x01" + strings.Repeat("\x00", 7), 10, 0},
		{"2^56 scaled past declared 18 digits", "\x01" + strings.Repeat("\x00", 7), 18, 2},
		{"overprecision x96 at (3,2)", "\x96", 3, 2},
		{"scale beyond carrier digits", "\x96", 10, 19},
		{"negative width with value", "\x96", -5, 0},
	} {
		t.Run(tc.name+" errors", func(t *testing.T) {
			_, err := ParseDecimal64FromByte(tc.payload, tc.width, tc.scale)
			require.Error(t, err, "out of target range, never a wrapped negative")
		})
	}

	t.Run("reuse after failure", func(t *testing.T) {
		_, err := ParseDecimal64FromByte("", 10, 2)
		require.Error(t, err)
		got, err := ParseDecimal64FromByte("\x41", 3, 1)
		require.NoError(t, err)
		require.Equal(t, "65.0", got.Format(1))
	})
}

func TestParseDecimal128FromByteUnsignedBEContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
		want    string
	}{
		{"x96 scale2", "\x00\x00\x00\x96", 20, 2, "150.00"},
		{"x96 256-shape width", "\x00\x00\x00\x96", 38, 2, "150.00"},
		{"2^64-307 wide value", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xCD", 38, 2, "18446744073709551309.00"},
		{"12-byte value at scale1", "\x01" + strings.Repeat("\x00", 11), 38, 1, "309485009821345068724781056.0"},
		{"sixteen zero bytes then 96", strings.Repeat("\x00", 16) + "\x96", 20, 2, "150.00"},
		{"long all-zero payload", strings.Repeat("\x00", 512), 38, 2, "0.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDecimal128FromByte(tc.payload, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(tc.scale),
				"canonical text at the declared scale")
			fromText, err := ParseDecimal128(tc.want, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, fromText, got, "text parse of the same digits is an independent oracle")
		})
	}

	t.Run("clamped and defensive targets", func(t *testing.T) {
		got, err := ParseDecimal128FromByte("\x00\x00\x00\x96", 40, 2)
		require.NoError(t, err)
		require.Equal(t, Decimal128{15000, 0}, got, "width clamps to 38")
		got, err = ParseDecimal128FromByte("\x96", 4, -3)
		require.NoError(t, err)
		require.Equal(t, Decimal128{150, 0}, got, "negative scale is treated as 0")
		zero, err := ParseDecimal128FromByte("\x00", -5, 39)
		require.NoError(t, err)
		require.Equal(t, Decimal128{0, 0}, zero, "zero passes any clamped target")
	})

	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
	}{
		{"empty payload", "", 20, 2},
		{"seventeen significant bytes", "\x01" + strings.Repeat("\x00", 16), 38, 0},
		{"sixteen 0xff bytes", strings.Repeat("\xFF", 16), 38, 0},
		{"2^127 boundary", "\x80" + strings.Repeat("\x00", 15), 38, 0},
		{"2^127-1 past declared 38 digits", "\x7F" + strings.Repeat("\xFF", 15), 38, 0},
		{"2^127-1 scaled at (38,2)", "\x7F" + strings.Repeat("\xFF", 15), 38, 2},
		{"8-byte value past declared width 20", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xCD", 20, 2},
		{"overprecision x96 at (3,2)", "\x96", 3, 2},
		{"scale beyond carrier digits", "\x96", 38, 39},
		{"negative width with value", "\x96", -5, 0},
	} {
		t.Run(tc.name+" errors", func(t *testing.T) {
			_, err := ParseDecimal128FromByte(tc.payload, tc.width, tc.scale)
			require.Error(t, err, "out of target range, never a wrapped negative")
		})
	}

	t.Run("reuse after failure", func(t *testing.T) {
		_, err := ParseDecimal128FromByte("", 20, 2)
		require.Error(t, err)
		got, err := ParseDecimal128FromByte("\x00\x00\x00\x96", 20, 2)
		require.NoError(t, err)
		require.Equal(t, "150.00", got.Format(2))
	})
}

func TestParseDecimal256FromByteUnsignedBEContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
		want    string
	}{
		{"x96 scale2 38", "\x00\x00\x00\x96", 38, 2, "150.00"},
		{"x96 scale2 76", "\x00\x00\x00\x96", 76, 2, "150.00"},
		{"2^64-307 wide value", "\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xCD", 76, 2, "18446744073709551309.00"},
		{"sixteen 0xff bytes", strings.Repeat("\xFF", 16), 76, 2, "340282366920938463463374607431768211455.00"},
		{"thirty-two zero bytes then 96", strings.Repeat("\x00", 32) + "\x96", 76, 2, "150.00"},
		{"long all-zero payload", strings.Repeat("\x00", 512), 76, 2, "0.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDecimal256FromByte(tc.payload, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.Format(tc.scale),
				"canonical text at the declared scale")
			fromText, err := ParseDecimal256(tc.want, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, fromText, got, "text parse of the same digits is an independent oracle")
		})
	}

	t.Run("clamped and defensive targets", func(t *testing.T) {
		got, err := ParseDecimal256FromByte("\x00\x00\x00\x96", 80, 2)
		require.NoError(t, err)
		fromText, err := ParseDecimal256("150.00", 76, 2)
		require.NoError(t, err)
		require.Equal(t, fromText, got, "width clamps to 76 and matches the text parse")
		got, err = ParseDecimal256FromByte("\x96", 4, -3)
		require.NoError(t, err)
		require.Equal(t, Decimal256{150, 0, 0, 0}, got, "negative scale is treated as 0")
		zero, err := ParseDecimal256FromByte("\x00", -5, 77)
		require.NoError(t, err)
		require.Equal(t, Decimal256{}, zero, "zero passes any clamped target")
	})

	for _, tc := range []struct {
		name    string
		payload string
		width   int32
		scale   int32
	}{
		{"empty payload", "", 38, 2},
		{"thirty-three significant bytes", "\x01" + strings.Repeat("\x00", 32), 76, 0},
		{"thirty-two 0xff bytes", strings.Repeat("\xFF", 32), 76, 0},
		{"2^255 boundary", "\x80" + strings.Repeat("\x00", 31), 76, 0},
		{"2^255-1 past declared 76 digits", "\x7F" + strings.Repeat("\xFF", 31), 76, 0},
		{"sixteen 0xff bytes past declared width 40", strings.Repeat("\xFF", 16), 40, 2},
		{"overprecision x96 at (3,2)", "\x96", 3, 2},
		{"scale beyond carrier digits", "\x96", 76, 77},
		{"negative width with value", "\x96", -5, 0},
	} {
		t.Run(tc.name+" errors", func(t *testing.T) {
			_, err := ParseDecimal256FromByte(tc.payload, tc.width, tc.scale)
			require.Error(t, err, "out of target range, never a wrapped negative")
		})
	}

	t.Run("reuse after failure", func(t *testing.T) {
		_, err := ParseDecimal256FromByte("", 38, 2)
		require.Error(t, err)
		got, err := ParseDecimal256FromByte("\x00\x00\x00\x96", 38, 2)
		require.NoError(t, err)
		require.Equal(t, "150.00", got.Format(2))
	})
}
