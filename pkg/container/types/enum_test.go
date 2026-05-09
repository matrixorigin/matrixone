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
			got, _ := ParseEnum(strings.Join(c.enums, ","), c.value)
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

func TestEncodeDecodeEnumValues(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"simple", []string{"xs", "s", "m", "l", "xl"}},
		{"single", []string{"only"}},
		{"with comma", []string{"a,b", "c", "d,e,f"}},
		{"empty member", []string{"", "a", "b"}},
		{"unicode", []string{"日本語", "中文", "한국어"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeEnumValues(tt.values)
			decoded, err := DecodeEnumValues(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.values, decoded)
		})
	}
}

func TestEncodeEnumValuesBackwardCompat(t *testing.T) {
	values := []string{"xs", "s", "m", "l", "xl"}
	encoded := EncodeEnumValues(values)
	require.Equal(t, "xs,s,m,l,xl", encoded)
}

func TestDecodeEnumValuesEmpty(t *testing.T) {
	_, err := DecodeEnumValues("")
	require.Error(t, err)
}

func TestParseEnumWithComma(t *testing.T) {
	values := []string{"a,b", "c", "d"}
	encoded := EncodeEnumValues(values)
	got, err := ParseEnum(encoded, "a,b")
	require.NoError(t, err)
	require.Equal(t, Enum(1), got)

	got, err = ParseEnum(encoded, "c")
	require.NoError(t, err)
	require.Equal(t, Enum(2), got)
}

func TestParseEnumIndexWithComma(t *testing.T) {
	values := []string{"a,b", "c", "d"}
	encoded := EncodeEnumValues(values)
	got, err := ParseEnumIndex(encoded, 1)
	require.NoError(t, err)
	require.Equal(t, "a,b", got)
}
