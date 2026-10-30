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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A HEX/BIT payload is an unsigned integer in numeric context: x'41' is 65,
// so DECIMAL(5,1) holds 65.0 (MySQL), not the coefficient 65 read as 6.5.
func TestParseDecimalFromByteHonorsScale(t *testing.T) {
	for _, tc := range []struct {
		payload      string
		width, scale int32
		want         string
	}{
		{"\x41", 5, 1, "65.0"},
		{"\x41", 10, 2, "65.00"},
		{"\x41", 10, 0, "65"},
		{"\x00\x96", 10, 2, "150.00"},
		{"\x01\x00", 10, 3, "256.000"},
		{"\xff\xff\xff\xff\xff\xff\xff\xff", 20, 0, "18446744073709551615"},
	} {
		if tc.width <= 18 {
			d, err := ParseDecimal64FromByte(tc.payload, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, tc.want, d.Format(tc.scale), "Decimal64(%d,%d) %x", tc.width, tc.scale, tc.payload)
		}
		d128, err := ParseDecimal128FromByte(tc.payload, 38, tc.scale)
		require.NoError(t, err)
		require.Equal(t, tc.want, d128.Format(tc.scale), "Decimal128 %x", tc.payload)
		d256, err := ParseDecimal256FromByte(tc.payload, 65, tc.scale)
		require.NoError(t, err)
		require.Equal(t, tc.want, d256.Format(tc.scale), "Decimal256 %x", tc.payload)
	}

	// The 8-byte all-ones payload is unsigned, never a negative two's complement.
	d, err := ParseDecimal128FromByte("\xff\xff\xff\xff\xff\xff\xfe\xcd", 38, 0)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551309", d.Format(0))

	// Wider than the declared precision is an error, not a silently wider value.
	_, err = ParseDecimal64FromByte("\xff\xff", 3, 0)
	require.Error(t, err)
	_, err = ParseDecimal64FromByte("\x41", 3, 2)
	require.Error(t, err)
	_, err = ParseDecimal64FromByte("", 10, 2)
	require.Error(t, err)
}
