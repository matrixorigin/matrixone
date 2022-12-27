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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

var (
	multiKases = []struct {
		json []string
		path []string
		want []string
	}{
		{
			json: []string{`{"a":1,"b":2,"c":3}`, `{"a":1,"b":2,"c":3}`, `{"c":3}`},
			path: []string{`$.a`},
			want: []string{`1`, `1`, `null`},
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

func computeJson(json []byte, path *bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(path), nil
}

func TestJsonExtract(t *testing.T) {
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
		nsps := []*nulls.Nulls{nulls.NewWithSize(len(kase.json)), nulls.NewWithSize(len(kase.json))}
		maxLen := len(kase.json)
		if maxLen < len(kase.path) {
			maxLen = len(kase.path)
		}
		rnsp := nulls.NewWithSize(maxLen)
		qs := make([]*bytejson.ByteJson, maxLen)
		qs, err := jsonExtract(jbytes, pbytes, nsps, qs, rnsp, computeJson)
		require.NoError(t, err)
		for i, q := range qs {
			if kase.want[i] == "null" {
				require.True(t, rnsp.Contains(uint64(i)))
				continue
			}
			require.JSONEq(t, kase.want[i], q.String())
		}
		nsps[0].Set(0)
		_, err = jsonExtract(jbytes, pbytes, nsps, qs, rnsp, computeJson)
		require.NoError(t, err)
		nsps[1].Set(0)
		_, err = jsonExtract(jbytes, pbytes, nsps, qs, rnsp, computeJson)
		require.NoError(t, err)
	}
}
