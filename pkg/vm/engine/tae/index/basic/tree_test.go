package basic

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestARTIndexNumeric(t *testing.T) {
	typ := types.Type{Oid: types.T_int32}
	idx := NewSimpleARTMap(typ, nil)

	var res bool
	var err error
	var row uint32
	res, err = idx.ContainsKey(int32(0))
	require.NoError(t, err)
	require.False(t, res)

	var batches []*vector.Vector
	for i := 0; i < 10; i++ {
		batch := common.MockVec(typ, 100, i * 100)
		batches = append(batches, batch)
	}

	row, err = idx.Search(int32(55))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.Delete(int32(55))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.BatchInsert(batches[0], 0, 100, uint32(0), false)
	require.NoError(t, err)

	row, err = idx.Search(int32(55))
	require.NoError(t, err)
	require.Equal(t, uint32(55), row)

	row, err = idx.Search(int32(100))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.Delete(int32(55))
	require.NoError(t, err)

	row, err = idx.Search(int32(55))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.BatchInsert(batches[0], 0, 100, uint32(100), false)
	require.ErrorIs(t, err, errors.ErrKeyDuplicate)

	err = idx.BatchInsert(batches[1], 0, 100, uint32(100), false)
	require.NoError(t, err)

	row, err = idx.Search(int32(123))
	require.NoError(t, err)
	require.Equal(t, uint32(123), row)

	row, err = idx.Search(int32(233))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.Insert(int32(55), uint32(55))
	require.NoError(t, err)

	row, err = idx.Search(int32(55))
	require.NoError(t, err)
	require.Equal(t, uint32(55), row)

	err = idx.Update(int32(55), uint32(114514))
	require.NoError(t, err)

	row, err = idx.Search(int32(55))
	require.NoError(t, err)
	require.Equal(t, uint32(114514), row)

	updated := make([]uint32, 0)
	for i := 0; i < 100; i++ {
		updated = append(updated, uint32(i + 10000))
	}
	err = idx.BatchUpdate(batches[0], updated, 0)
	require.NoError(t, err)

	row, err = idx.Search(int32(67))
	require.NoError(t, err)
	require.Equal(t, uint32(10067), row)
}

func TestArtIndexString(t *testing.T) {
	typ := types.Type{Oid: types.T_varchar}
	idx := NewSimpleARTMap(typ, nil)

	var res bool
	var err error
	var row uint32
	res, err = idx.ContainsKey([]byte(strconv.Itoa(0)))
	require.NoError(t, err)
	require.False(t, res)

	var batches []*vector.Vector
	for i := 0; i < 10; i++ {
		batch := common.MockVec(typ, 100, i * 100)
		batches = append(batches, batch)
	}

	row, err = idx.Search([]byte(strconv.Itoa(55)))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.Delete([]byte(strconv.Itoa(55)))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.BatchInsert(batches[0], 0, 100, uint32(0), false)
	require.NoError(t, err)

	row, err = idx.Search([]byte(strconv.Itoa(55)))
	require.NoError(t, err)
	require.Equal(t, uint32(55), row)

	row, err = idx.Search([]byte(strconv.Itoa(100)))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.Delete([]byte(strconv.Itoa(55)))
	require.NoError(t, err)

	row, err = idx.Search([]byte(strconv.Itoa(55)))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)

	err = idx.BatchInsert(batches[0], 0, 100, uint32(100), false)
	require.ErrorIs(t, err, errors.ErrKeyDuplicate)

	err = idx.BatchInsert(batches[1], 0, 100, uint32(100), false)
	require.NoError(t, err)

	row, err = idx.Search([]byte(strconv.Itoa(123)))
	require.NoError(t, err)
	require.Equal(t, uint32(123), row)

	row, err = idx.Search([]byte(strconv.Itoa(233)))
	require.ErrorIs(t, err, errors.ErrKeyNotFound)
}
