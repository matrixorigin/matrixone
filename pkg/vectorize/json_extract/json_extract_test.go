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
		json    []string
		path    []string
		want    []string
		jsonErr bool
		pathErr bool
	}{
		{
			json:    []string{`{"a":1,"b":2,"c":3`},
			path:    []string{`$.a`},
			jsonErr: true,
			pathErr: false,
		},
		{
			json:    []string{`{"a":1,"b":2,"c":3}`},
			path:    []string{`$.`},
			jsonErr: false,
			pathErr: true,
		},
		{
			json:    []string{`{"a":1,"b":2,"c":3}`},
			path:    []string{`$.b`},
			want:    []string{`2`},
			jsonErr: false,
			pathErr: false,
		},
		{
			json: []string{`{"a":{"q":[1,2,3]}}`},
			path: []string{`$.a.q[1]`},
			want: []string{`2`},
		},
		{
			json: []string{`[{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6}]`},
			path: []string{`$[1].a`},
			want: []string{`4`},
		},
		{
			json: []string{`{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`},
			path: []string{`$.a.q[1]`},
			want: []string{`{"a":2}`},
		},
		{
			json: []string{`{"a":{"q":[{"a":1},{"a":2},{"a":3}]}}`},
			path: []string{`$.a.q`},
			want: []string{`[{"a":1},{"a":2},{"a":3}]`},
		},
		{
			json: []string{`[1,2,3]`},
			path: []string{"$[*]"},
			want: []string{"[1,2,3]"},
		},
		{
			json: []string{`{"a":[1,2,3,{"b":4}]}`},
			path: []string{"$.a[3].b"},
			want: []string{"4"},
		},
		{
			json: []string{`{"a":[1,2,3,{"b":4}]}`},
			path: []string{"$.a[3].c"},
			want: []string{"null"},
		},
		{
			json: []string{`{"a":[1,2,3,{"b":4}],"c":5}`},
			path: []string{"$.*"},
			want: []string{`[[1,2,3,{"b":4}],5]`},
		},
		{
			json: []string{`{"a":[1,2,3,{"a":4}]}`},
			path: []string{"$**.a"},
			want: []string{`[[1,2,3,{"a":4}],4]`},
		},
		{
			json: []string{`{"a":[1,2,3,{"a":4}]}`},
			path: []string{"$.a[*].a"},
			want: []string{`4`},
		},
	}
	multiKases = []struct {
		json []string
		path []string
		want []string
	}{
		{
			json: []string{`{"a":1,"b":2,"c":3}`, `{"a":1,"b":2,"c":3}`},
			path: []string{`$.a`},
			want: []string{`1`, `1`},
		},
		{
			json: []string{`{"a":1,"b":2,"c":3}`, `{"a":1,"b":2,"c":3}`},
			path: []string{`$.a`, `$.b`},
			want: []string{`1`, `2`},
		},
		{
			json: []string{`{"a":1,"b":2,"c":3}`},
			path: []string{`$.a`, `$.b`},
			want: []string{`1`, `2`},
		},
	}
)

func TestByStringOne(t *testing.T) {
	for _, kase := range kases {
		ph, err := types.ParseStringToPath(kase.path[0])
		if kase.pathErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		q, err := byStringOne([]byte(kase.json[0]), &ph)
		if kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		if kase.want[0] == "null" {
			require.True(t, q.IsNull())
			continue
		}
		require.JSONEq(t, kase.want[0], q.String())
	}
}

func TestByString(t *testing.T) {
	for _, kase := range multiKases {
		jbytes := make([][]byte, 0, len(kase.json))
		for _, j := range kase.json {
			jbytes = append(jbytes, []byte(j))
		}
		pbytes := make([][]byte, 0, len(kase.path))
		for _, p := range kase.path {
			pbytes = append(pbytes, []byte(p))
		}
		qs, err := byString(jbytes, pbytes, nil)
		require.Nil(t, err)
		for i, q := range qs {
			if kase.want[i] == "null" {
				require.True(t, q.IsNull())
				continue
			}
			require.JSONEq(t, kase.want[i], q.String())
		}
	}
}

func TestByJsonOne(t *testing.T) {
	for _, kase := range kases {
		byteJson, err := types.ParseStringToByteJson(kase.json[0])
		if kase.jsonErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		json, err := byteJson.Marshal()
		require.Nil(t, err)
		ph, err := types.ParseStringToPath(kase.path[0])
		if kase.pathErr {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		q, err := byJsonOne(json, &ph)
		require.Nil(t, err)
		if kase.want[0] == "null" {
			require.True(t, q.IsNull())
			continue
		}
		require.JSONEq(t, kase.want[0], q.String())
	}
}

func TestByJson(t *testing.T) {
	for _, kase := range multiKases {
		jbytes := make([][]byte, 0, len(kase.json))
		for _, j := range kase.json {
			tmp, err := types.ParseStringToByteJson(j)
			require.Nil(t, err)
			bytes, err := tmp.Marshal()
			require.Nil(t, err)
			jbytes = append(jbytes, bytes)
		}
		pbytes := make([][]byte, 0, len(kase.path))
		for _, p := range kase.path {
			pbytes = append(pbytes, []byte(p))
		}
		qs, err := byJson(jbytes, pbytes, nil)
		require.Nil(t, err)
		for i, q := range qs {
			if kase.want[i] == "null" {
				require.True(t, q.IsNull())
				continue
			}
			require.JSONEq(t, kase.want[i], q.String())
		}
	}
}
