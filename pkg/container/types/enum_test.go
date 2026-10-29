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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEnum(t *testing.T) {
	cases := []struct {
		name  string
		enums []string
		value string
		want  Enum
	}{
		{
			name:  "test01",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "xs",
			want:  1,
		},
		{
			name:  "test02",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "s",
			want:  2,
		},
		{
			name:  "test03",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "m",
			want:  3,
		},
		{
			name:  "test04",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "l",
			want:  4,
		},
		{
			name:  "test05",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "xl",
			want:  5,
		},
		{
			name:  "test06",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "1",
			want:  1,
		},
		{
			name:  "test07",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "2",
			want:  2,
		},
		{
			name:  "test08",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "3",
			want:  3,
		},
		{
			name:  "test09",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "4",
			want:  4,
		},
		{
			name:  "test10",
			enums: []string{"xs", "s", "m", "l", "xl"},
			value: "5",
			want:  5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseEnum(strings.Join(c.enums, ","), c.value)
			require.NoError(t, err)
			require.Equal(t, c.want, got)
		})
	}
}

func TestParseEnumIndex(t *testing.T) {
	cases := []struct {
		name  string
		enums []string
		index Enum
		want  string
	}{
		{
			name:  "test01",
			enums: []string{"xs", "s", "m", "l", "xl"},
			want:  "xs",
			index: 1,
		},
		{
			name:  "test02",
			enums: []string{"xs", "s", "m", "l", "xl"},
			want:  "s",
			index: 2,
		},
		{
			name:  "test03",
			enums: []string{"xs", "s", "m", "l", "xl"},
			want:  "m",
			index: 3,
		},
		{
			name:  "test04",
			enums: []string{"xs", "s", "m", "l", "xl"},
			want:  "l",
			index: 4,
		},
		{
			name:  "test05",
			enums: []string{"xs", "s", "m", "l", "xl"},
			want:  "xl",
			index: 5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, _ := ParseEnumIndex(strings.Join(c.enums, ","), c.index)
			require.Equal(t, c.want, got)
		})
	}
}

func TestEmptyEnumDomain(t *testing.T) {
	index, err := ParseEnum("", "")
	require.NoError(t, err)
	require.Equal(t, Enum(1), index)
	index, err = ParseEnumName("", "")
	require.NoError(t, err)
	require.Equal(t, Enum(1), index)
	index, err = ParseEnumValue("", 1)
	require.NoError(t, err)
	require.Equal(t, Enum(1), index)
	label, err := ParseEnumIndex("", 1)
	require.NoError(t, err)
	require.Equal(t, "", label)
	for _, value := range []string{"bad", "2", "65537", "18446744073709551615"} {
		_, err := ParseEnum("", value)
		require.Error(t, err, value)
	}
	for _, index := range []Enum{0, 2} {
		_, err := ParseEnumIndex("", index)
		require.Error(t, err)
		_, err = ParseEnumValue("", uint16(index))
		require.Error(t, err)
	}
}

func TestEnumAssignmentParsing(t *testing.T) {
	const domain = "l01,l02,l03,l04,l05,l06,l07,l08,l09,l10"
	for _, tc := range []struct {
		input     string
		want      Enum
		truncated bool
	}{
		{"l01 ", 1, false}, {"L01", 1, false}, {" l01", 0, true},
		{"08", 8, false}, {"010", 10, false}, {"+2", 2, false}, {" 2", 2, false},
		{"2 ", 2, false}, {"00002", 2, false}, {"000002", 0, true},
		{"00002  ", 2, false}, {" 00002", 0, true},
		{"0", 0, false}, {"00", 0, false}, {"+0", 0, false}, {"-0", 0, false},
		{"-1", 0, true}, {"11", 0, true}, {"1_0", 0, true}, {"0x2", 0, true},
		{"0b10", 0, true}, {"0o2", 0, true}, {"2e0", 0, true}, {"2.0", 0, true},
		{"", 0, true}, {" ", 0, true}, {"18446744073709551615", 0, true},
		{"18446744073709551616", 0, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, truncated, err := ParseEnumAssignment(domain, tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.truncated, truncated)
			strict, strictErr := ParseEnum(domain, tc.input)
			if tc.truncated {
				require.Error(t, strictErr)
				require.Zero(t, strict)
			} else {
				require.NoError(t, strictErr)
				require.Equal(t, got, strict)
			}
		})
	}
	for _, prefix := range []string{" ", "\t", "\n", "\r", "\v", "\f"} {
		got, truncated, err := ParseEnumAssignment(domain, prefix+"2")
		require.NoError(t, err)
		require.False(t, truncated)
		require.Equal(t, Enum(2), got)
		if prefix != " " {
			_, truncated, err = ParseEnumAssignment(domain, "2"+prefix)
			require.NoError(t, err)
			require.True(t, truncated)
		}
	}
	for _, input := range []string{"\u00a02", "2\u00a0", "l01\t", "\nl01"} {
		_, truncated, err := ParseEnumAssignment(domain, input)
		require.NoError(t, err)
		require.True(t, truncated)
	}
	for _, tc := range []struct {
		def, input string
		want       Enum
	}{
		{"02,x,y,2,z", "02", 1}, {"02,x,y,2,z", "2 ", 4},
		{"", " ", 1}, {",a", "", 1},
		{"a ,a", "a ", 1}, {"a ,a", "a", 2}, {"a ,a", "a  ", 2},
	} {
		got, truncated, err := ParseEnumAssignment(tc.def, tc.input)
		require.NoError(t, err)
		require.False(t, truncated)
		require.Equal(t, tc.want, got)
		name, err := ParseEnumName(tc.def, tc.input)
		require.NoError(t, err)
		require.Equal(t, tc.want, name)
	}
	for _, tc := range []struct {
		number    uint64
		want      Enum
		truncated bool
	}{
		{0, 0, true}, {1, 1, false}, {10, 10, false}, {11, 0, true},
		{65537, 0, true}, {^uint64(0), 0, true},
	} {
		got, truncated, err := ParseEnumNumberAssignment(domain, tc.number)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
		require.Equal(t, tc.truncated, truncated)
	}
	invalid := strings.Repeat("x,", MaxEnumLen)
	_, truncated, err := ParseEnumAssignment(invalid, "x")
	require.Error(t, err)
	require.False(t, truncated)
	_, _, err = ParseEnumNumberAssignment(invalid, 1)
	require.Error(t, err)
	for _, count := range []int{0, MaxEnumLen + 1} {
		_, truncated, err = ParseEnumNumericText("1", count)
		require.Error(t, err)
		require.False(t, truncated)
	}
}
