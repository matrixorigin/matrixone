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

package json_unquote

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStringBatch(t *testing.T) {
	xs := [][]byte{
		[]byte(`"hello"`),
		[]byte(`"world"`),
	}
	rs := make([]string, len(xs))
	nsp := nulls.NewWithSize(2)
	rs, err := StringBatch(xs, rs, nsp)
	require.NoError(t, err)
	require.Equal(t, "hello", rs[0])
	require.Equal(t, "world", rs[1])
	nsp.Set(0)
	_, err = StringBatch(xs, rs, nsp)
	require.NoError(t, err)
	xs = append(xs, []byte(`hello`))
	rs = make([]string, len(xs))
	nsp = nulls.NewWithSize(3)
	_, err = StringBatch(xs, rs, nsp)
	require.Error(t, err)
}
func TestJsonBatch(t *testing.T) {
	xs := [][]byte{
		[]byte(`"hello"`),
		[]byte(`"world"`),
	}
	for i := range xs {
		bj, _ := types.ParseSliceToByteJson(xs[i])
		xs[i], _ = bj.Marshal()
	}
	rs := make([]string, len(xs))
	nsp := nulls.NewWithSize(2)
	rs, err := JsonBatch(xs, rs, nsp)
	require.NoError(t, err)
	require.Equal(t, "hello", rs[0])
	require.Equal(t, "world", rs[1])

	nsp.Set(0)
	_, err = JsonBatch(xs, rs, nsp)
	require.NoError(t, err)

}
