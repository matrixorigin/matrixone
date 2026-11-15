// Copyright 2021 Matrix Origin
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
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnumMemberEncoding(t *testing.T) {
	cases := []struct {
		members  []string
		payload  string
		encoding uint32
	}{
		{[]string{"a,b", "c"}, "2#3:a,b1:c", 1},
		{[]string{""}, "1#0:", 1},
		{[]string{"a", "", "c"}, "a,,c", 0},
		{[]string{"2#3:a", "b1:c"}, "2#3:a,b1:c", 0},
		{[]string{"red", "green"}, "red,green", 0},
		{[]string{" :#,", "q'\\\x00\r\n", "雪,\xff"}, "", 1},
		{[]string{strings.Repeat("x", 65536)}, "", 0},
	}
	for _, tc := range cases {
		payload, encoding, err := EncodeEnumMembers(tc.members)
		require.NoError(t, err)
		require.Equal(t, tc.encoding, encoding)
		if tc.payload != "" {
			require.Equal(t, tc.payload, payload)
		}
		decoded, err := DecodeEnumMembers(payload, encoding)
		require.NoError(t, err)
		require.Equal(t, EnumMembers(tc.members), decoded)
		for i, want := range tc.members {
			got, err := decoded.Value(Enum(i + 1))
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	}
	require.False(t, EnumMembersEqual("2#3:a,b1:c", 0, "2#3:a,b1:c", 1))
	require.True(t, EnumMembersEqual("", 0, "1#0:", 1))
	require.False(t, EnumMembersEqual("a,b", 0, "b,a", 0))
	require.False(t, EnumMembersEqual("bad", 1, "bad", 1))
	require.False(t, EnumMembersEqual("x", 256, "x", 0))
	_, _, err := EncodeEnumMembers(nil)
	require.Error(t, err)
	_, _, err = EncodeEnumMembers(make([]string, MaxEnumLen+1))
	require.Error(t, err)
	max := make([]string, MaxEnumLen)
	payload, encoding, err := EncodeEnumMembers(max)
	require.NoError(t, err)
	members, err := DecodeEnumMembers(payload, encoding)
	require.NoError(t, err)
	ordinal, err := members.ParseValue(MaxEnumLen)
	require.NoError(t, err)
	require.Equal(t, Enum(MaxEnumLen), ordinal)
}

func TestEnumMemberEncodingRejectsMalformed(t *testing.T) {
	for _, payload := range []string{
		"", "0#", "01#1:a", "-1#1:a", "1 #1:a", "65536#",
		"1#", "1#01:a", "1#-1:a", "1#2:a", "1#0:x",
		"1#4294967296:a", "1#999999999999999999999999999999999:a",
		"2#1:a", "1#1:a1:b", "99999999999999999999999999#",
	} {
		_, err := DecodeEnumMembers(payload, 1)
		require.Error(t, err, "payload=%q", payload)
	}
	for _, encoding := range []uint32{2, 255, 256, ^uint32(0)} {
		_, err := DecodeEnumMembers("a,b", encoding)
		require.Error(t, err)
	}
	members, err := DecodeEnumMembers("2#3:a,b1:c", 0)
	require.NoError(t, err)
	require.Equal(t, EnumMembers{"2#3:a", "b1:c"}, members)
}

func TestEnumTypeEncodingRoundTrip(t *testing.T) {
	require.Equal(t, uintptr(16), reflect.TypeOf(Type{}).Size())
	typ := T_enum.ToType()
	typ.SetEnumEncoding(1)
	encoded, err := typ.Marshal()
	require.NoError(t, err)
	require.Len(t, encoded, 20)
	require.Equal(t, uint16(1), binary.BigEndian.Uint16(encoded[6:8]))
	var decoded Type
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, typ, decoded)
	raw := append([]byte(nil), EncodeType(&typ)...)
	require.Len(t, raw, 16)
	require.Equal(t, byte(1), raw[3])
	require.Equal(t, typ, DecodeType(raw))
	legacy := T_enum.ToType()
	require.True(t, typ.Eq(legacy))
	for _, tag := range []uint32{2, 255, 256, ^uint32(0)} {
		invalid := typ
		invalid.SetEnumEncoding(tag)
		require.Greater(t, invalid.GetEnumEncoding(), uint32(1))
		_, err := invalid.Marshal()
		require.Error(t, err)
	}
	for _, tag := range []uint16{2, 256, 257, 65535} {
		corrupt := append([]byte(nil), encoded...)
		binary.BigEndian.PutUint16(corrupt[6:8], tag)
		before := decoded
		require.Error(t, decoded.Unmarshal(corrupt))
		require.Equal(t, before, decoded)
	}
}

func FuzzEnumMemberEncoding(f *testing.F) {
	f.Add("a,b", "c")
	f.Add("", "")
	f.Add(" :#\x00,\xff", "雪")
	f.Fuzz(func(t *testing.T, a, b string) {
		if len(a)+len(b) > 1<<20 {
			t.Skip()
		}
		payload, encoding, err := EncodeEnumMembers([]string{a, b})
		require.NoError(t, err)
		members, err := DecodeEnumMembers(payload, encoding)
		require.NoError(t, err)
		require.Equal(t, EnumMembers{a, b}, members)
	})
}
