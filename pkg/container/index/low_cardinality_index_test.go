// Copyright 2021 Matrix Origin
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

package index

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestInsertWithNulls(t *testing.T) {
	idx, err := newTestIndex(types.T_varchar.ToType())
	require.NoError(t, err)

	// test data = ["a", "b", NULL, "a", "c", NULL, "c", "b", "a", NULL]
	v := vector.New(types.T_varchar.ToType())
	require.NoError(t, v.Append([]byte("a"), false, idx.m))
	require.NoError(t, v.Append([]byte("b"), false, idx.m))
	require.NoError(t, v.Append([]byte(""), true, idx.m))
	require.NoError(t, v.Append([]byte("a"), false, idx.m))
	require.NoError(t, v.Append([]byte("c"), false, idx.m))
	require.NoError(t, v.Append([]byte(""), true, idx.m))
	require.NoError(t, v.Append([]byte("c"), false, idx.m))
	require.NoError(t, v.Append([]byte("b"), false, idx.m))
	require.NoError(t, v.Append([]byte("a"), false, idx.m))
	require.NoError(t, v.Append([]byte(""), true, idx.m))

	// dict = ["a"->1, "b"->2, "c"->3]
	require.NoError(t, idx.InsertBatch(v))

	require.Equal(t, []string{"a", "b", "c"}, vector.GetStrVectorValues(idx.dict.GetUnique()))
	require.Equal(t, []uint16{1, 2, 0, 1, 3, 0, 3, 2, 1, 0}, vector.MustTCols[uint16](idx.poses))
}

func TestEncode(t *testing.T) {
	idx, err := newTestIndex(types.T_varchar.ToType())
	require.NoError(t, err)

	v0 := vector.New(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("My"),
		[]byte("name"),
		[]byte("is"),
		[]byte("Tom"),
	}, idx.m))

	err = idx.InsertBatch(v0)
	require.NoError(t, err)

	v1 := vector.New(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v1, [][]byte{
		[]byte("Jack"),
		[]byte("is"),
		[]byte("My"),
		[]byte("friend"),
		[]byte("name"),
		[]byte("Tom"),
	}, idx.m))

	enc := vector.New(types.T_uint16.ToType())
	err = idx.Encode(enc, v1)
	require.NoError(t, err)
	col := vector.MustTCols[uint16](enc)
	require.Equal(t, uint16(0), col[0])
	require.Equal(t, uint16(4), col[1])
	require.Equal(t, uint16(2), col[2])
	require.Equal(t, uint16(0), col[3])
	require.Equal(t, uint16(3), col[4])
	require.Equal(t, uint16(5), col[5])
}

func newTestIndex(typ types.Type) (*LowCardinalityIndex, error) {
	return New(typ, mpool.MustNewZero())
}
