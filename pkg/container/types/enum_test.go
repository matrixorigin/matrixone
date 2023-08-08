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
		want  uint16
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
		index uint16
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
