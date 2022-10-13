// Copyright 2022 Matrix Origin
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

var (
	jsonKases = []struct {
		json  string
		valid bool
	}{
		{
			json:  `{"a":1,"b":2,"c":3}`,
			valid: true,
		},
		{
			json:  `{"a":1,"b":2,"c":3`,
			valid: false,
		},
		{
			json:  `{}`,
			valid: true,
		},
		{
			json:  `[]`,
			valid: true,
		},
		{
			json:  `[1,2,3]`,
			valid: true,
		},
		{
			json:  `[1,2,3`,
			valid: false,
		},
		{
			json:  `[`,
			valid: false,
		},
		{
			json:  `{`,
			valid: false,
		},
		{
			json:  `1`,
			valid: true,
		},
		{
			json:  `1,2,3`,
			valid: false,
		},
		{
			json:  `"1,2,3"`,
			valid: true,
		},
		{
			json:  `"1,2,3`,
			valid: false,
		},
		{
			json:  `true`,
			valid: true,
		},
		{
			json:  `false`,
			valid: true,
		},
		{
			json:  `null`,
			valid: true,
		},
	}
	pathKases = []struct {
		path  string
		valid bool
	}{
		{"$", true},
		{"$.", false},
		{"$.*", true},
		{"$.a", true},
		{"$..a.", false},
		{"$.a.b", true},
		{"$.a..b", false},
		{"$[1]", true},
		{"$[1].", false},
		{"$[1].a", true},
		{"$[1]..a", false},
		{"$[1].a.b", true},
		{"$[1].a..b", false},
		{"$[-1]", false},
		{"$[1][2]", true},
		{"$[1][2].", false},
		{"$[1][2].a", true},
		{"$[1][2]..a", false},
		{"$[*]", true},
		{"$[*].", false},
		{"$[*].a", true},
		{`$[*]."a"`, true},
	}
)

func TestParseStringToByteJson(t *testing.T) {
	for _, kase := range jsonKases {
		t.Run(kase.json, func(t *testing.T) {
			b, err := ParseStringToByteJson(kase.json)
			if kase.valid {
				require.Nil(t, err)
				require.JSONEq(t, kase.json, b.String())
			} else {
				require.NotNil(t, err)
			}
		})
	}
}
func TestParseSliceToByteJson(t *testing.T) {
	for _, kase := range jsonKases {
		t.Run(kase.json, func(t *testing.T) {
			b, err := ParseSliceToByteJson([]byte(kase.json))
			if kase.valid {
				require.Nil(t, err)
				require.JSONEq(t, kase.json, b.String())
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestParseStringToPath(t *testing.T) {
	for _, kase := range pathKases {
		t.Run(kase.path, func(t *testing.T) {
			_, err := ParseStringToPath(kase.path)
			if kase.valid {
				if err != nil {
					t.Errorf("%s is valid, but error: %s", kase.path, err)
				}
				require.Nil(t, err)
			} else {
				if err == nil {
					t.Errorf("%s is invalid, but no error", kase.path)
				}
				require.NotNil(t, err)
			}
		})
	}
}
