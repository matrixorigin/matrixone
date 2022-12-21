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

package binary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

var (
	procs = testutil.NewProc()
	kases = []struct {
		json string
		path string
		want interface{}
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
)

func makeTestVector1(json, path string) []*vector.Vector {
	vec := make([]*vector.Vector, 2)
	vec[0] = vector.New(types.T_varchar.ToType())
	vec[1] = vector.New(types.T_varchar.ToType())
	err := vec[0].Append([]byte(json), false, procs.Mp())
	if err != nil {
		panic(err)
	}
	err = vec[1].Append([]byte(path), false, procs.Mp())
	if err != nil {
		panic(err)
	}
	return vec
}
func makeTestVector2(json, path string) []*vector.Vector {
	vec := make([]*vector.Vector, 2)
	vec[0] = vector.New(types.T_json.ToType())
	vec[1] = vector.New(types.T_varchar.ToType())
	bjson, err := types.ParseStringToByteJson(json)
	if err != nil {
		panic(err)
	}
	bjsonSlice, err := types.EncodeJson(bjson)
	if err != nil {
		panic(err)
	}
	err = vec[0].Append(bjsonSlice, false, procs.Mp())
	if err != nil {
		panic(err)
	}
	err = vec[1].Append([]byte(path), false, procs.Mp())
	if err != nil {
		panic(err)
	}
	return vec
}

func TestJsonExtractByString(t *testing.T) {

	for _, kase := range kases {
		t.Run(kase.path, func(t *testing.T) {
			vec := makeTestVector1(kase.json, kase.path)
			gotvec, err := JsonExtract(vec, procs)
			require.Nil(t, err)
			got := vector.GetBytesVectorValues(gotvec)
			switch value := kase.want.(type) {
			case []string:
				for i := range value {
					bjson := types.DecodeJson(got[i])
					require.JSONEq(t, value[i], bjson.String())
				}
			default:
				if kase.want == "null" {
					require.Equal(t, []byte{}, got[0])
					break
				}
				bjson := types.DecodeJson(got[0])
				require.JSONEq(t, kase.want.(string), bjson.String())
			}
			vec[0].MakeScalar(1)
			_, err = JsonExtract(vec, procs)
			require.NoError(t, err)
			vec[1].MakeScalar(1)
			_, err = JsonExtract(vec, procs)
			require.NoError(t, err)
			vec[0].Nsp.Set(0)
			_, err = JsonExtract(vec, procs)
			require.NoError(t, err)
		})
	}
}

func TestJsonExtractByJson(t *testing.T) {
	for _, kase := range kases {
		t.Run(kase.path, func(t *testing.T) {
			vec := makeTestVector2(kase.json, kase.path)
			got, err := JsonExtract(vec, procs)
			require.Nil(t, err)
			bytes := vector.MustBytesCols(got)
			switch value := kase.want.(type) {
			case []string:
				for i := range value {
					bjson := types.DecodeJson(bytes[i])
					require.JSONEq(t, value[i], bjson.String())
				}
			default:
				if kase.want == "null" {
					require.Equal(t, []byte{}, bytes[0])
					break
				}
				bjson := types.DecodeJson(bytes[0])
				require.JSONEq(t, kase.want.(string), bjson.String())
			}
		})
	}
}
