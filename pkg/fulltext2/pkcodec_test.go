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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestPkCodecAllTypesRoundTrip is the single-source-of-truth guardrail: EVERY supported
// pk type must survive encodePk → decodePk unchanged. A pk type added to one codec switch
// but forgotten in another (the class of bug that once stalled CDC on a BIT pk) fails here
// instead of in production. Add every new pk type to this table.
func TestPkCodecAllTypesRoundTrip(t *testing.T) {
	u1, _ := types.ParseUuid("12345678-1234-1234-1234-1234567890ab")
	cases := []struct {
		typ types.T
		val any
	}{
		{types.T_int64, int64(-1 << 50)},
		{types.T_uint64, uint64(1<<64 - 1)},
		{types.T_bit, uint64(255)},
		{types.T_int32, int32(-123456)},
		{types.T_uint32, uint32(4000000000)},
		{types.T_int16, int16(-30000)},
		{types.T_uint16, uint16(60000)},
		{types.T_int8, int8(-128)},
		{types.T_uint8, uint8(200)},
		{types.T_date, types.Date(19000)},
		{types.T_datetime, types.Datetime(-42)},
		{types.T_time, types.Time(987654321)},
		{types.T_timestamp, types.Timestamp(1234567890)},
		{types.T_decimal64, types.Decimal64(999999)},
		{types.T_decimal128, types.Decimal128{B0_63: 7, B64_127: 11}},
		{types.T_varchar, []byte("hello pk")},
		{types.T_char, []byte("c")},
		{types.T_text, []byte("")},
		{types.T_blob, []byte{0, 1, 2, 255}},
		{types.T_json, []byte(`{"k":1}`)},
		{types.T_uuid, u1},
	}
	for _, c := range cases {
		b, err := encodePk(int32(c.typ), c.val)
		require.NoErrorf(t, err, "encodePk %v", c.typ)
		got, err := decodePk(int32(c.typ), b)
		require.NoErrorf(t, err, "decodePk %v", c.typ)
		// byte-slice pks: compare as strings so an empty pk's nil-vs-[]byte{} (which
		// are semantically identical, and identical once appended to a vector) matches.
		if wantB, ok := c.val.([]byte); ok {
			require.Equalf(t, string(wantB), string(got.([]byte)), "round-trip %v", c.typ)
		} else {
			require.Equalf(t, c.val, got, "round-trip %v", c.typ)
		}

		// fixedPkByteWidth must agree with the encoded length for fixed types.
		if w, fixed := fixedPkByteWidth(int32(c.typ)); fixed {
			require.Equalf(t, w, len(b), "fixed width mismatch for %v", c.typ)
		}
	}
}
