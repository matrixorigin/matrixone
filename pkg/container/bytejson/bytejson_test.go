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

package bytejson

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiteral(t *testing.T) {
	j := []string{"true", "false", "null"}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.Equal(t, x, bj.String())
	}
}

func TestNumber(t *testing.T) {
	// generate max int64
	j := []string{
		"9223372036854775807",
		"-9223372036854775808",
		"1",
		"-1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to int64
		now, err := strconv.ParseInt(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetInt64())
	}

	// generate max uint64
	j = []string{
		"18446744073709551615",
		"0",
		"1",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to uint64
		now, err := strconv.ParseUint(x, 10, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetUint64())
	}

	//generate max float64
	j = []string{
		"1.7976931348623157e+308",
		"-1.7976931348623157e+308",
		"4.940656458412465441765687928682213723651e-324",
		"0.112131431",
		"1.13353411",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		// transform string to float64
		now, err := strconv.ParseFloat(x, 64)
		require.Nil(t, err)
		require.Equal(t, now, bj.GetFloat64())
	}
}

func TestObject(t *testing.T) {
	j := []string{
		`{"a":1}`,
		`{"a": 1, "b": 2, "c": true, "d": false, "e": null, "f": "string", "g": [1, 2, 3], "h": {"a": 1, "b": 2}, "i": 1.1, "j": 1.1e+10, "k": 1.1e-10}`,
		`{"a":{}}`,
		`{"a":{"b":{"c":{"d":[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456]}}}}`,
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.JSONEq(t, x, bj.String())
	}
}
func TestArray(t *testing.T) {
	j := []string{
		`[`,
		`[{]`,
		`[{}]`,
		`["1"]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`[null,false,true,123,"abc",[1,2,3],{"a":1,"b":2,"c":3,"d":4,"e":5},123.456,1.1e+10,1.1e-10]`,
	}
	for i, x := range j {
		bj, err := ParseFromString(x)
		if i > 1 {
			require.Nil(t, err)
			require.JSONEq(t, x, bj.String())
		} else {
			require.NotNil(t, err)
		}
	}
}

func TestQuery(t *testing.T) {
	kases := []struct {
		jsonStr string
		pathStr string
		outStr  string
	}{
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.a",
			outStr:  "\"1\"",
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.b",
			outStr:  "\"2\"",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[0]",
			outStr:  "1",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[2]",
			outStr:  "3",
		},
		{
			jsonStr: `[1,2,3]`,
			pathStr: "$[*]",
			outStr:  "[1,2,3]",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}]}`,
			pathStr: "$.a[3].b",
			outStr:  "4",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}]}`,
			pathStr: "$.a[3].c",
			outStr:  "null",
		},
		{
			jsonStr: `{"a":[1,2,3,{"b":4}],"c":5}`,
			pathStr: "$.*",
			outStr:  `[[1,2,3,{"b":4}],5]`,
		},
		{
			jsonStr: `{"a":[1,2,3,{"a":4}]}`,
			pathStr: "$**.a",
			outStr:  `[[1,2,3,{"a":4}],4]`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[0]",
			outStr:  `{"a":1}`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[0].a",
			outStr:  `1`,
		},
		{
			jsonStr: `{"a":1}`,
			pathStr: "$[1]",
			outStr:  `null`,
		},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		path, err := ParseJsonPath(kase.pathStr)
		require.Nil(t, err)
		out := bj.Query(&path)
		require.JSONEq(t, kase.outStr, out.String())
	}
}
func TestUnnest(t *testing.T) {
	kases := []struct {
		jsonStr   string
		pathStr   string
		mode      string
		recursive bool
		outer     bool
		outStr    []string
		valid     bool
	}{
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "other",
			valid:   false,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "both",
			pathStr: "$",
			outStr: []string{
				`key: a, path: $.a, value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			pathStr: "$.a",
			mode:    "both",
			valid:   true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "object",
			outStr: []string{
				`key: a, path: $.a, value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": "1", "b": "2", "c": "3"}`,
			mode:    "array",
			valid:   true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "array",
			outStr: []string{
				`path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "object",
			valid:   true,
		},
		{
			jsonStr: `[1,2,3]`,
			mode:    "both",
			outStr: []string{
				`path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			outer:   true,
			outStr: []string{
				`path: $, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "array",
			recursive: true,
			pathStr:   "$.a",
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			pathStr: "$.b",
			valid:   true,
			outer:   true,
			outStr: []string{
				`path: $.b, this: {"c": 4, "d": [5, 6, 7]}`,
			},
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			pathStr: "$.b.d",
			outStr: []string{
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			pathStr:   "$.b",
			recursive: true,
			outStr: []string{
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$.*",
			recursive: true,
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: c, path: $.b.c, value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			pathStr: "$.a**.a",
			outStr: []string{
				`key: b, path: $.a[0].a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			pathStr: "$**.a",
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.b.a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$**.a",
			recursive: true,
			outStr: []string{
				`path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.a[3].b, value: 4, this: {"b": 4}`,
				`key: b, path: $.b.a.b, value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
	}
	filterMap := map[string]struct{}{
		"index": {},
		"this":  {},
		"value": {},
		"path":  {},
		"key":   {},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		var path Path
		if len(kase.pathStr) > 0 {
			path, err = ParseJsonPath(kase.pathStr)
			require.Nil(t, err)
		}
		out, err := bj.Unnest(&path, kase.outer, kase.recursive, kase.mode, filterMap)
		if !kase.valid {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		for i, o := range out {
			require.Equal(t, kase.outStr[i], o.String())
		}
	}

}

func TestByteJson_Unquote(t *testing.T) {
	kases := []struct {
		jsonStr string
		outStr  string
		valid   bool
	}{
		{
			jsonStr: `"a"`,
			outStr:  "a",
			valid:   true,
		},
		{
			jsonStr: `"a\"b"`,
			outStr:  `a"b`,
			valid:   true,
		},
		{
			jsonStr: `"a\b"`,
			outStr:  "a\b",
			valid:   true,
		},
		{
			jsonStr: `"a\r"`,
			outStr:  "a\r",
			valid:   true,
		},
		{
			jsonStr: `"a\t"`,
			outStr:  `a	`,
			valid:   true,
		},
		{
			jsonStr: `"a\n"`,
			outStr: `a
`,
			valid: true,
		},
		{
			jsonStr: `"\u554a\u554a\u5361\u5361"`,
			outStr:  `啊啊卡卡`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\uff0c\u006d\u006f"`,
			outStr:  `你好，mo`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\uff0cmo"`,
			outStr:  `你好，mo`,
			valid:   true,
		},
		{
			jsonStr: `"\u4f60\u597d\ufc"`,
			valid:   false,
		},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		if !kase.valid {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		out, err := bj.Unquote()
		require.Nil(t, err)
		require.Equal(t, kase.outStr, out)
	}
}
