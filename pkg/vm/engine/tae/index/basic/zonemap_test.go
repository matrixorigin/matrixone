package basic

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestZoneMapNumeric(t *testing.T) {
	typ := types.Type{Oid: types.T_int32}
	zm := NewZoneMap(typ, nil)
	var res bool
	var err error
	var ans *roaring.Bitmap
	res, err = zm.MayContainsKey(int32(0))
	require.NoError(t, err)
	require.False(t, res)

	rows := 1000
	vec := common.MockVec(typ, rows, 0)
	err = zm.BatchUpdate(vec, 0, -1)
	require.NoError(t, err)

	res, err = zm.MayContainsKey(int32(0))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey(int32(999))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey(int32(555))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey(int32(1000))
	require.NoError(t, err)
	require.False(t, res)

	res, err = zm.MayContainsKey(int32(-1))
	require.NoError(t, err)
	require.False(t, res)

	rows = 500
	vec = common.MockVec(typ, rows, 700)
	err = zm.BatchUpdate(vec, 0, -1)
	require.NoError(t, err)

	res, err = zm.MayContainsKey(int32(1001))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey(int32(1199))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey(int32(1200))
	require.NoError(t, err)
	require.False(t, res)

	rows = 500
	vec = common.MockVec(typ, rows, -200)
	err = zm.BatchUpdate(vec, 0, -1)
	require.NoError(t, err)

	res, err = zm.MayContainsKey(int32(-201))
	require.NoError(t, err)
	require.False(t, res)

	res, err = zm.MayContainsKey(int32(-100))
	require.NoError(t, err)
	require.True(t, res)

	buf, err := zm.Marshal()
	zm1 := ZoneMap{}
	err = zm1.Unmarshal(buf)
	require.NoError(t, err)

	rows = 500
	typ1 := typ
	typ1.Oid = types.T_int64
	vec = common.MockVec(typ1, rows, 2000)
	err = zm.BatchUpdate(vec, 0, -1)
	require.Error(t, err)

	res, err = zm1.MayContainsKey(int32(1234))
	require.NoError(t, err)
	require.False(t, res)

	res, err = zm1.MayContainsKey(int32(1199))
	require.NoError(t, err)
	require.True(t, res)

	typ1.Oid = types.T_int32
	vec = common.MockVec(typ1, rows, 3000)
	res, ans, err = zm1.MayContainsAnyKeys(vec)
	require.NoError(t, err)
	require.False(t, res)
	require.Equal(t, uint64(0), ans.GetCardinality())

	vec = common.MockVec(typ1, rows, 0)
	res, ans, err = zm1.MayContainsAnyKeys(vec)
	require.NoError(t, err)
	require.True(t, res)
	require.Equal(t, uint64(rows), ans.GetCardinality())

	err = zm1.Update(int32(999999))
	require.NoError(t, err)

	res, err = zm1.MayContainsKey(int32(99999))
	require.NoError(t, err)
	require.True(t, res)
}

func TestZoneMapString(t *testing.T) {
	typ := types.Type{Oid: types.T_char}
	zm := NewZoneMap(typ, nil)
	var res bool
	var err error
	res, err = zm.MayContainsKey([]byte(strconv.Itoa(0)))
	require.NoError(t, err)
	require.False(t, res)

	rows := 1000
	vec := common.MockVec(typ, rows, 0)
	err = zm.BatchUpdate(vec, 0, -1)
	require.NoError(t, err)

	res, err = zm.MayContainsKey([]byte(strconv.Itoa(500)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey([]byte(strconv.Itoa(9999)))
	require.NoError(t, err)
	require.False(t, res)

	res, err = zm.MayContainsKey([]byte("/"))
	require.NoError(t, err)
	require.False(t, res)

	err = zm.Update([]byte("z"))
	require.NoError(t, err)

	res, err = zm.MayContainsKey([]byte(strconv.Itoa(999999)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey([]byte("abcdefghijklmn"))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey([]byte("ydasdasda"))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey([]byte("z1"))
	require.NoError(t, err)
	require.False(t, res)

	buf, err := zm.Marshal()
	zm1 := ZoneMap{}
	err = zm1.Unmarshal(buf)
	require.NoError(t, err)

	res, err = zm.MayContainsKey([]byte("z1"))
	require.NoError(t, err)
	require.False(t, res)

	res, err = zm.MayContainsKey([]byte("z"))
	require.NoError(t, err)
	require.True(t, res)

	res, err = zm.MayContainsKey([]byte("/"))
	require.NoError(t, err)
	require.False(t, res)
}
