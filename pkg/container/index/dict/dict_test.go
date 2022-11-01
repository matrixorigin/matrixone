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

package dict

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCardinality(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_int64})
	require.NoError(t, err)
	defer dict.Free()

	dict.unique.Col = []int64{5, 2, 3, 7}
	require.Equal(t, uint64(4), dict.Cardinality())
}

func TestInsertBatchFixedLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_int64})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_int64})
	v0.Col = []int64{5, 2, 3, 7, 3, 2}

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(3), ips[4])
	require.Equal(t, uint16(2), ips[5])

	v1 := vector.New(types.Type{Oid: types.T_int64})
	v1.Col = []int64{4, 2, 1, 5}

	ips, err = dict.InsertBatch(v1)
	require.NoError(t, err)
	require.Equal(t, uint16(5), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(6), ips[2])
	require.Equal(t, uint16(1), ips[3])
}

func TestFindBatchFixedLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_int32})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_int32})
	v0.Col = []int32{5, 2, 3, 7, 1, 4}

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(5), ips[4])
	require.Equal(t, uint16(6), ips[5])

	v1 := vector.New(types.Type{Oid: types.T_int32})
	v1.Col = []int32{7, 3, 8, 4, 6, 3}

	poses := dict.FindBatch(v1)
	require.Equal(t, uint16(4), poses[0])
	require.Equal(t, uint16(3), poses[1])
	require.Equal(t, uint16(0), poses[2])
	require.Equal(t, uint16(6), poses[3])
	require.Equal(t, uint16(0), poses[4])
	require.Equal(t, uint16(3), poses[5])
}

func TestFindDataFixedLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_int32})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_int32})
	v0.Col = []int32{5, 3, 1, 7, 1, 3}

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(3), ips[4])
	require.Equal(t, uint16(2), ips[5])

	data := dict.FindData(1)
	require.Equal(t, int32(5), vector.MustTCols[int32](data)[0])

	data = dict.FindData(2)
	require.Equal(t, int32(3), vector.MustTCols[int32](data)[0])

	data = dict.FindData(3)
	require.Equal(t, int32(1), vector.MustTCols[int32](data)[0])

	data = dict.FindData(4)
	require.Equal(t, int32(7), vector.MustTCols[int32](data)[0])
}

func TestInsertLargeDataFixedLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_int32})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_int32})
	v0.Col = make([]int32, 100000)

	i := 0
	for j := 1; j <= 10000; j++ {
		for cnt := 0; cnt < 10; cnt++ {
			vector.MustTCols[int32](v0)[i] = int32(j)
			i++
		}
	}

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)

	i = 0
	for j := 1; j <= 10000; j++ {
		for cnt := 0; cnt < 10; cnt++ {
			require.Equal(t, uint16(j), ips[i])
			i++
		}
	}
}

func TestInsertBatchVarLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_varchar})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	require.NoError(t, vector.AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("My"),
		[]byte("name"),
		[]byte("is"),
		[]byte("Tom"),
	}, dict.m))

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(5), ips[4])

	v1 := vector.New(types.Type{Oid: types.T_varchar})
	require.NoError(t, vector.AppendBytes(v1, [][]byte{
		[]byte("Tom"),
		[]byte("is"),
		[]byte("My"),
		[]byte("friend"),
	}, dict.m))

	ips, err = dict.InsertBatch(v1)
	require.NoError(t, err)
	require.Equal(t, uint16(5), ips[0])
	require.Equal(t, uint16(4), ips[1])
	require.Equal(t, uint16(2), ips[2])
	require.Equal(t, uint16(6), ips[3])
}

func TestFindBatchVarLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_varchar})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	require.NoError(t, vector.AppendBytes(v0, [][]byte{
		[]byte("hello"),
		[]byte("My"),
		[]byte("name"),
		[]byte("is"),
		[]byte("Tom"),
	}, dict.m))

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(5), ips[4])

	v1 := vector.New(types.Type{Oid: types.T_varchar})
	require.NoError(t, vector.AppendBytes(v1, [][]byte{
		[]byte("Jack"),
		[]byte("is"),
		[]byte("My"),
		[]byte("friend"),
		[]byte("name"),
	}, dict.m))

	poses := dict.FindBatch(v1)
	require.Equal(t, uint16(0), poses[0])
	require.Equal(t, uint16(4), poses[1])
	require.Equal(t, uint16(2), poses[2])
	require.Equal(t, uint16(0), poses[3])
	require.Equal(t, uint16(3), poses[4])
}

func TestFindDataVarLen(t *testing.T) {
	dict, err := newTestDict(types.Type{Oid: types.T_varchar})
	require.NoError(t, err)
	defer dict.Free()

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	require.NoError(t, vector.AppendBytes(v0, [][]byte{
		[]byte("thisisalonglonglonglongstring"),
		[]byte("My"),
		[]byte("name"),
		[]byte("is"),
		[]byte("Tom"),
		[]byte("is"),
		[]byte("Tom"),
	}, dict.m))

	ips, err := dict.InsertBatch(v0)
	require.NoError(t, err)
	require.Equal(t, uint16(1), ips[0])
	require.Equal(t, uint16(2), ips[1])
	require.Equal(t, uint16(3), ips[2])
	require.Equal(t, uint16(4), ips[3])
	require.Equal(t, uint16(5), ips[4])
	require.Equal(t, uint16(4), ips[5])
	require.Equal(t, uint16(5), ips[6])

	data := dict.FindData(1)
	require.Equal(t, []byte("thisisalonglonglonglongstring"), data.GetBytes(0))

	data = dict.FindData(2)
	require.Equal(t, []byte("My"), data.GetBytes(0))

	data = dict.FindData(3)
	require.Equal(t, []byte("name"), data.GetBytes(0))

	data = dict.FindData(4)
	require.Equal(t, []byte("is"), data.GetBytes(0))

	data = dict.FindData(5)
	require.Equal(t, []byte("Tom"), data.GetBytes(0))
}

func newTestDict(typ types.Type) (*Dict, error) {
	return New(typ, mpool.MustNewZero())
}
