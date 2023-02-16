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

package multi

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	kases = []struct {
		index        int
		json         string
		path         string
		want         string
		pathNullList []bool
	}{
		{
			index: 0,
			json:  `{"a":1,"b":2,"c":3}`,
			path:  `$.a`,
			want:  `1`,
		},
		{
			index: 1,
			json:  `{"a":1,"b":2,"c":3}`,
			path:  `$.b`,
			want:  `2`,
		},
		{
			index: 2,
			json:  `{"a":{"q":[1,2,3]}}`,
			path:  `$.a.q[1]`,
			want:  `2`,
		},
		{
			index: 3,
			json:  `[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6}]`,
			path:  `$[1].a`,
			want:  `4`,
		},
		{
			index: 4,
			json:  `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path:  `$.a.q[1]`,
			want:  `{"a":2}`,
		},
		{
			index: 5,
			json:  `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path:  `$.a.q`,
			want:  `[{"a":1},{"a":2},{"a":3}]`,
		},
		{
			index: 6,
			json:  `[1,2,3]`,
			path:  "$[*]",
			want:  "[1,2,3]",
		},
		{
			index: 7,
			json:  `{"a":[1,2,3,{"b":4}]}`,
			path:  "$.a[3].b",
			want:  "4",
		},
		{
			index: 8,
			json:  `{"a":[1,2,3,{"b":4}]}`,
			path:  "$.a[3].c",
			want:  "null",
		},
		{
			index: 9,
			json:  `{"a":[1,2,3,{"b":4}],"c":5}`,
			path:  "$.*",
			want:  `[[1,2,3,{"b":4}],5]`,
		},
		{
			index: 10,
			json:  `{"a":[1,2,3,{"a":4}]}`,
			path:  "$**.a",
			want:  `[[1,2,3,{"a":4}],4]`,
		},
		{
			index: 11,
			json:  `{"a":[1,2,3,{"a":4}]}`,
			path:  "$.a[*].a",
			want:  `4`,
		},
		{
			index:        12,
			json:         `{"a":[1,2,3,{"a":4}]}`,
			pathNullList: []bool{true},
			want:         "null",
		},
	}
)

func TestJsonExtract(t *testing.T) {
	proc := testutil.NewProc()
	for _, kase := range kases {
		inputs := []testutil.FunctionTestInput{
			testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{kase.json}, nil),
			testutil.NewFunctionTestInput(types.T_varchar.ToType(), []string{kase.path}, kase.pathNullList),
		}
		want := make([]string, 1)
		if kase.want != "null" {
			bj, _ := types.ParseStringToByteJson(kase.want)
			dt, _ := bj.Marshal()
			want[0] = string(dt)
		}
		expect := testutil.NewFunctionTestResult(types.T_varchar.ToType(), false, want, nil)
		kaseNow := testutil.NewFunctionTestCase(proc,
			inputs, expect, JsonExtract)
		s, info := kaseNow.Run()
		require.True(t, s, fmt.Sprintf("case %d, err info is '%s'", kase.index, info))
	}
}
