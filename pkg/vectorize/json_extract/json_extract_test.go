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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var (
	kases = []struct {
		json    string
		path    string
		want    string
		jsonErr bool
		pathErr bool
	}{
		{
			json:    `{"a":1,"b":2,"c":3`,
			path:    `$.a`,
			want:    ``,
			jsonErr: true,
			pathErr: false,
		},
		{
			json:    `{"a":1,"b":2,"c":3}`,
			path:    `$.`,
			want:    ``,
			jsonErr: false,
			pathErr: true,
		},
		{
			json:    `{"a":1,"b":2,"c":3}`,
			path:    `$.b`,
			want:    `2`,
			jsonErr: false,
			pathErr: false,
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
)

func TestByStringOne(t *testing.T) {
	for _, kase := range kases {
		ph, err := types.ParseStringToPath(kase.path)
		if kase.pathErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		q, err := byStringOne([]byte(kase.json), &ph)
		if kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		if kase.want == "null" {
			require.True(t, q.IsNull())
			continue
		}
		require.JSONEq(t, kase.want, q.String())
	}
}

func TestByString(t *testing.T) {
	for _, kase := range kases {
		jBytes := [][]byte{[]byte(kase.json)}
		pBytes := [][]byte{[]byte(kase.path)}
		result, err := byString(jBytes, pBytes, nil)
		if kase.pathErr || kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		if kase.want == "null" {
			require.True(t, result[0].IsNull())
			continue
		}
		require.JSONEq(t, kase.want, result[0].String())
	}
}

func TestByJsonOne(t *testing.T) {
	for _, kase := range kases {
		byteJson, err := types.ParseStringToByteJson(kase.json)
		if kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		json, err := byteJson.Marshal()
		require.Nil(t, err)
		ph, err := types.ParseStringToPath(kase.path)
		if kase.pathErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		q, err := byJsonOne(json, &ph)
		require.Nil(t, err)
		if kase.want == "null" {
			require.True(t, q.IsNull())
			continue
		}
		require.JSONEq(t, kase.want, q.String())
	}
}

func TestByJson(t *testing.T) {
	for _, kase := range kases {
		json, err := types.ParseStringToByteJson(kase.json)
		if kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		jsonBytes, err := types.EncodeJson(json)
		require.Nil(t, err)
		jBytes := [][]byte{jsonBytes}
		pBytes := [][]byte{[]byte(kase.path)}
		result, err := byJson(jBytes, pBytes, nil)
		if kase.pathErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		if kase.want == "null" {
			require.True(t, result[0].IsNull())
			continue
		}
		require.JSONEq(t, kase.want, result[0].String())
	}
}
