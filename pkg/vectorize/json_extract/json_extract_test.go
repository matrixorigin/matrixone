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

package json_extract

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQVarchar(t *testing.T) {
	kases := []struct {
		json string
		path string
		want string
	}{
		{
			json: `{"a":1,"b":2,"c":3}`,
			path: `$.a`,
			want: `1`,
		},
		{
			json: `{"a":1,"b":2,"c":3}`,
			path: `$.b`,
			want: `2`,
		},
		{
			json: `{"a":{"q":[1,2,3]}}`,
			path: `$.a.q[1]`,
			want: `2`,
		},
		{
			json: `[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6}]`,
			path: `$[1].a`,
			want: `4`,
		},
		{
			json: `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path: `$.a.q[1]`,
			want: `{"a":2}`,
		},
		{
			json: `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path: `$.a.q`,
			want: `[{"a":1},{"a":2},{"a":3}]`,
		},
		{
			json: `[1,2,3]`,
			path: "$[*]",
			want: "[1,2,3]",
		},
		{
			json: `{"a":[1,2,3,{"b":4}]}`,
			path: "$.a[3].b",
			want: "4",
		},
		{
			json: `{"a":[1,2,3,{"b":4}]}`,
			path: "$.a[3].c",
			want: "null",
		},
		{
			json: `{"a":[1,2,3,{"b":4}],"c":5}`,
			path: "$.*",
			want: `[[1,2,3,{"b":4}],5]`,
		},
		{
			json: `{"a":[1,2,3,{"a":4}]}`,
			path: "$**.a",
			want: `[[1,2,3,{"a":4}],4]`,
		},
		{
			json: `{"a":[1,2,3,{"a":4}]}`,
			path: "$.a[*].a",
			want: `4`,
		},
	}
	for _, kase := range kases {
		ph, err := types.ParseStringToPath(kase.path)
		require.Nil(t, err)
		q, err := qVarcharOne([]byte(kase.json), &ph)
		require.Nil(t, err)
		require.JSONEq(t, kase.want, string(q))
	}
}

func TestQJson(t *testing.T) {
	kases := []struct {
		json string
		path string
		want string
	}{
		{
			json: `{"a":1,"b":2,"c":3}`,
			path: `$.a`,
			want: `1`,
		},
		{
			json: `{"a":1,"b":2,"c":3}`,
			path: `$.b`,
			want: `2`,
		},
		{
			json: `{"a":{"q":[1,2,3]}}`,
			path: `$.a.q[1]`,
			want: `2`,
		},
		{
			json: `[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6}]`,
			path: `$[1].a`,
			want: `4`,
		},
		{
			json: `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path: `$.a.q[1]`,
			want: `{"a":2}`,
		},
		{
			json: `{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`,
			path: `$.a.q`,
			want: `[{"a":1},{"a":2},{"a":3}]`,
		},
		{
			json: `[1,2,3]`,
			path: "$[*]",
			want: "[1,2,3]",
		},
		{
			json: `{"a":[1,2,3,{"b":4}]}`,
			path: "$.a[3].b",
			want: "4",
		},
		{
			json: `{"a":[1,2,3,{"b":4}]}`,
			path: "$.a[3].c",
			want: "null",
		},
		{
			json: `{"a":[1,2,3,{"b":4}],"c":5}`,
			path: "$.*",
			want: `[[1,2,3,{"b":4}],5]`,
		},
		{
			json: `{"a":[1,2,3,{"a":4}]}`,
			path: "$**.a",
			want: `[[1,2,3,{"a":4}],4]`,
		},
		{
			json: `{"a":[1,2,3,{"a":4}]}`,
			path: "$.a[*].a",
			want: `4`,
		},
	}
	for _, kase := range kases {
		byteJson, err := types.ParseStringToByteJson(kase.json)
		require.Nil(t, err)
		json, err := byteJson.Marshal()
		require.Nil(t, err)
		ph, err := types.ParseStringToPath(kase.path)
		require.Nil(t, err)
		q, err := qJsonOne(json, &ph)
		require.Nil(t, err)
		require.JSONEq(t, kase.want, string(q))
	}
}
