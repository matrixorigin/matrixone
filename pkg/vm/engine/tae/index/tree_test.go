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

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestARTIndexNumeric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.Type{Oid: types.T_int32}
	idx := NewSimpleARTMap(typ)

	var err error
	var rows []uint32
	_, err = idx.Search(int32(0))
	require.Error(t, err)

	var vecs []containers.Vector
	for i := 0; i < 10; i++ {
		vec := containers.MockVector2(typ, 100, i*100)
		vecs = append(vecs, vec)
		defer vec.Close()
	}

	_, err = idx.Search(int32(55))
	require.Error(t, err)

	ctx := new(KeysCtx)
	ctx.Count = 100
	ctx.Keys = vecs[0]
	err = idx.BatchInsert(ctx, uint32(0))
	require.NoError(t, err)

	rows, err = idx.Search(int32(55))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

	_, err = idx.Search(int32(100))
	require.ErrorIs(t, err, ErrNotFound)

	ctx = new(KeysCtx)
	ctx.Count = 100
	ctx.Keys = vecs[0]
	err = idx.BatchInsert(ctx, uint32(100))
	require.NoError(t, err)

	ctx.Keys = vecs[1]
	err = idx.BatchInsert(ctx, uint32(100))
	require.NoError(t, err)

	rows, err = idx.Search(int32(123))
	require.NoError(t, err)
	require.Equal(t, uint32(123), rows[0])

	_, err = idx.Search(int32(233))
	require.ErrorIs(t, err, ErrNotFound)

	err = idx.Insert(int32(55), uint32(55))
	require.NoError(t, err)

	rows, err = idx.Search(int32(55))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

}

func TestArtIndexString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.Type{Oid: types.T_varchar}
	idx := NewSimpleARTMap(typ)

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

	ctx := new(KeysCtx)
	ctx.Keys = vecs[0]
	ctx.Count = 100
	err = idx.BatchInsert(ctx, uint32(0))
	require.NoError(t, err)
	t.Log(idx.String())

	rows, err = idx.Search([]byte(strconv.Itoa(55)))
	require.NoError(t, err)
	require.Equal(t, uint32(55), rows[0])

	_, err = idx.Search([]byte(strconv.Itoa(100)))
	require.ErrorIs(t, err, ErrNotFound)

	err = idx.BatchInsert(ctx, uint32(100))
	require.NoError(t, err)

	ctx.Keys = vecs[1]
	err = idx.BatchInsert(ctx, uint32(100))
	require.NoError(t, err)

	rows, err = idx.Search([]byte(strconv.Itoa(123)))
	require.NoError(t, err)
	require.Equal(t, uint32(123), rows[0])

	_, err = idx.Search([]byte(strconv.Itoa(233)))
	require.ErrorIs(t, err, ErrNotFound)
}
