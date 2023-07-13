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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestARTIndexNumeric(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_int32.ToType()
	idx := NewSimpleARTMap()

	var err error
	var rows []uint32
	val := int32(0)
	_, err = idx.Search(types.EncodeInt32(&val))
	require.Error(t, err)

	var vecs []containers.Vector
	for i := 0; i < 10; i++ {
		vec := containers.MockVector2(typ, 100, i*100)
		vecs = append(vecs, vec)
		defer vec.Close()
	}

	val = int32(55)
	_, err = idx.Search(types.EncodeInt32(&val))
	require.Error(t, err)

	err = idx.BatchInsert(vecs[0].GetDownstreamVector(), 0, 100, uint32(0))
	require.NoError(t, err)

	val = int32(55)
	rows, err = idx.Search(types.EncodeInt32(&val))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

	val = int32(100)
	_, err = idx.Search(types.EncodeInt32(&val))
	require.ErrorIs(t, err, ErrNotFound)

	err = idx.BatchInsert(vecs[0].GetDownstreamVector(), 0, 100, uint32(100))
	require.NoError(t, err)

	err = idx.BatchInsert(vecs[1].GetDownstreamVector(), 0, 100, uint32(100))
	require.NoError(t, err)

	val = int32(123)
	rows, err = idx.Search(types.EncodeInt32(&val))
	require.NoError(t, err)
	require.Equal(t, uint32(123), rows[0])

	val = int32(233)
	_, err = idx.Search(types.EncodeInt32(&val))
	require.ErrorIs(t, err, ErrNotFound)

	val = int32(55)
	buf := types.EncodeInt32(&val)
	bufval := make([]byte, len(buf))
	copy(bufval, buf)
	err = idx.Insert(bufval, uint32(55))
	require.NoError(t, err)

	val = int32(55)
	rows, err = idx.Search(types.EncodeInt32(&val))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

}

func TestArtIndexString(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_varchar.ToType()
	idx := NewSimpleARTMap()

	var err error
	var rows []uint32
	_, err = idx.Search([]byte(strconv.Itoa(0)))
	require.Error(t, err)

	var vecs []containers.Vector
	for i := 0; i < 10; i++ {
		vec := containers.MockVector2(typ, 100, i*100)
		vecs = append(vecs, vec)
		defer vec.Close()
	}

	_, err = idx.Search([]byte(strconv.Itoa(55)))
	require.ErrorIs(t, err, ErrNotFound)

	err = idx.BatchInsert(vecs[0].GetDownstreamVector(), 0, 100, uint32(0))
	require.NoError(t, err)
	t.Log(idx.String())

	rows, err = idx.Search([]byte(strconv.Itoa(55)))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

	_, err = idx.Search([]byte(strconv.Itoa(100)))
	require.ErrorIs(t, err, ErrNotFound)

	err = idx.BatchInsert(vecs[0].GetDownstreamVector(), 0, 100, uint32(100))
	require.NoError(t, err)

	err = idx.BatchInsert(vecs[1].GetDownstreamVector(), 0, 100, uint32(100))
	require.NoError(t, err)

	rows, err = idx.Search([]byte(strconv.Itoa(123)))
	require.NoError(t, err)
	require.Equal(t, uint32(123), rows[0])

	_, err = idx.Search([]byte(strconv.Itoa(233)))
	require.ErrorIs(t, err, ErrNotFound)
}

func BenchmarkArt(b *testing.B) {
	tr := NewSimpleARTMap()
	b.Run("tree-insert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := uint32(i)
			tr.Insert(types.EncodeUint32(&j), uint32(i))
		}
	})
	buf := []byte("hello")
	b.Run("tree-search", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tr.Search(buf)
		}
	})
}
