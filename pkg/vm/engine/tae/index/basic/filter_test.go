package basic

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestStaticFilterNumeric(t *testing.T) {
	typ := types.Type{Oid: types.T_int32}
	data := common.MockVec(typ, 40000, 0)
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var positive *roaring.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey(int32(1209))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(int32(5555))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(int32(40000))
	require.NoError(t, err)
	require.False(t, res)

	res, err = sf.MayContainsKey(int16(0))
	require.ErrorIs(t, err, errors.ErrTypeMismatch)

	query := common.MockVec(typ, 2000, 1000)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())
	require.True(t, exist)

	visibility := roaring.NewBitmap()
	visibility.AddRange(uint64(0), uint64(1000))
	exist, positive, err = sf.MayContainsAnyKeys(query, visibility)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), positive.GetCardinality())
	require.True(t, exist)

	query = common.MockVec(typ, 20000, 40000)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	sf1, err := NewBinaryFuseFilter(common.MockVec(typ, 0, 0))
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = common.MockVec(typ, 40000, 0)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
	require.True(t, exist)
}

func TestStaticFilterString(t *testing.T) {
	typ := types.Type{Oid: types.T_varchar}
	data := common.MockVec(typ, 40000, 0)
	sf, err := NewBinaryFuseFilter(data)
	require.NoError(t, err)
	var positive *roaring.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(1209)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(40000)))
	require.NoError(t, err)
	require.False(t, res)

	query := common.MockVec(typ, 2000, 1000)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), positive.GetCardinality())
	require.True(t, exist)

	query = common.MockVec(typ, 20000, 40000)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	sf1, err := NewBinaryFuseFilter(common.MockVec(typ, 0, 0))
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = common.MockVec(typ, 40000, 0)
	exist, positive, err = sf.MayContainsAnyKeys(query, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(40000), positive.GetCardinality())
	require.True(t, exist)
}
