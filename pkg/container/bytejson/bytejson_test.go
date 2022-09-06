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
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
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
		"{\"a\":\"1a\"}",
		"{\"a\": \"1\", \"b\": \"2\"}",
		"{\"a\": \"1\", \"b\": \"2\", \"c\": \"3\"}",
		"{\"result\":[[\"卫衣女\",\"80928.08611854467\"],[\"卫衣男\",\"74626.82297478059\"],[\"卫衣外套\",\"83628.47912479182\"],[\"卫衣套装\",\"62563.97733227019\"],[\"卫衣裙\",\"267198.7776653171\"],[\"卫衣裙女夏\",\"43747.754850631354\"],[\"卫衣连衣裙女\",\"279902.456837801\"],[\"卫衣套装女夏\",\"52571.576083414795\"],[\"卫衣女夏\",\"116244.16040484588\"],[\"卫衣oversize小众高街潮牌ins\",\"59134.02621045128\"]]}",
	}
	for _, x := range j {
		bj, err := ParseFromString(x)
		require.Nil(t, err)
		require.JSONEq(t, x, bj.String())
	}
}
func TestArray(t *testing.T) {
	j := []string{
		"[",
		"[{]",
		"[{}]",
		"[\"1\"]",
		"[\"1\", \"2\"]",
		"[\"1\", \"2\", \"3\"]",
		"[[\"卫衣女\",\"80928.08611854467\"],[\"卫衣男\",\"74626.82297478059\"],[\"卫衣外套\",\"83628.47912479182\"],[\"卫衣套装\",\"62563.97733227019\"],[\"卫衣裙\",\"267198.7776653171\"],[\"卫衣裙女夏\",\"43747.754850631354\"],[\"卫衣连衣裙女\",\"279902.456837801\"],[\"卫衣套装女夏\",\"52571.576083414795\"],[\"卫衣女夏\",\"116244.16040484588\"],[\"卫衣oversize小众高街潮牌ins\",\"59134.02621045128\"]]",
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
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		path, err := ParseJsonPath(kase.pathStr)
		require.Nil(t, err)
		out := bj.Query(path)
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
				`key: a, path: $.a, index: , value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, index: , value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, index: , value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
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
				`key: a, path: $.a, index: , value: "1", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: b, path: $.b, index: , value: "2", this: {"a": "1", "b": "2", "c": "3"}`,
				`key: c, path: $.c, index: , value: "3", this: {"a": "1", "b": "2", "c": "3"}`,
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
				`key: , path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`key: , path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`key: , path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
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
				`key: , path: $[0], index: 0, value: 1, this: [1, 2, 3]`,
				`key: , path: $[1], index: 1, value: 2, this: [1, 2, 3]`,
				`key: , path: $[2], index: 2, value: 3, this: [1, 2, 3]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			outStr: []string{
				`key: a, path: $.a, index: , value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, index: , value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			outStr: []string{
				`key: a, path: $.a, index: , value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, index: , value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			outer:   true,
			outStr: []string{
				`key: , path: $, index: , value: , this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, index: , value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: , path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`key: , path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`key: , path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: b, path: $.b, index: , value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, index: , value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, index: , value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`key: , path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`key: , path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`key: , path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			recursive: true,
			outStr: []string{
				`key: a, path: $.a, index: , value: [1, 2, 3], this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: b, path: $.b, index: , value: {"c": 4, "d": [5, 6, 7]}, this: {"a": [1, 2, 3], "b": {"c": 4, "d": [5, 6, 7]}}`,
				`key: c, path: $.b.c, index: , value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, index: , value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "array",
			recursive: true,
			pathStr:   "$.a",
			outStr: []string{
				`key: , path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`key: , path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`key: , path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
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
				`key: , path: $.b, index: , value: , this: {"c": 4, "d": [5, 6, 7]}`,
			},
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:    "array",
			pathStr: "$.b.d",
			outStr: []string{
				`key: , path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`key: , path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`key: , path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "object",
			pathStr:   "$.b",
			recursive: true,
			outStr: []string{
				`key: c, path: $.b.c, index: , value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, index: , value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3], "b": {"c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$.*",
			recursive: true,
			outStr: []string{
				`key: , path: $.a[0], index: 0, value: 1, this: [1, 2, 3]`,
				`key: , path: $.a[1], index: 1, value: 2, this: [1, 2, 3]`,
				`key: , path: $.a[2], index: 2, value: 3, this: [1, 2, 3]`,
				`key: c, path: $.b.c, index: , value: 4, this: {"c": 4, "d": [5, 6, 7]}`,
				`key: d, path: $.b.d, index: , value: [5, 6, 7], this: {"c": 4, "d": [5, 6, 7]}`,
				`key: , path: $.b.d[0], index: 0, value: 5, this: [5, 6, 7]`,
				`key: , path: $.b.d[1], index: 1, value: 6, this: [5, 6, 7]`,
				`key: , path: $.b.d[2], index: 2, value: 7, this: [5, 6, 7]`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "object",
			pathStr: "$.a**.a",
			outStr: []string{
				`key: b, path: $.a[0].a.b, index: , value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr: `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:    "both",
			pathStr: "$**.a",
			outStr: []string{
				`key: , path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.b.a.b, index: , value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
		{
			jsonStr:   `{"a": [1,2,3,{"b":4}], "b": {"a": {"b": 1}, "c": 4, "d": [5, 6, 7]}}`,
			mode:      "both",
			pathStr:   "$**.a",
			recursive: true,
			outStr: []string{
				`key: , path: $.a[0], index: 0, value: 1, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[1], index: 1, value: 2, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[2], index: 2, value: 3, this: [1, 2, 3, {"b": 4}]`,
				`key: , path: $.a[3], index: 3, value: {"b": 4}, this: [1, 2, 3, {"b": 4}]`,
				`key: b, path: $.a[3].b, index: , value: 4, this: {"b": 4}`,
				`key: b, path: $.b.a.b, index: , value: 1, this: {"b": 1}`,
			},
			valid: true,
		},
	}
	for _, kase := range kases {
		bj, err := ParseFromString(kase.jsonStr)
		require.Nil(t, err)
		var path Path
		if len(kase.pathStr) > 0 {
			path, err = ParseJsonPath(kase.pathStr)
			require.Nil(t, err)
		}
		out, err := bj.Unnest(path, kase.outer, kase.recursive, kase.mode)
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
