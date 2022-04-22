package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBlockZoneMapIndex(t *testing.T) {
	bufManager := buffer.NewNodeManager(1024*1024, nil)
	file := dataio.MockIndexFile()
	cType := common.Plain
	typ := types.Type{Oid: types.T_int32}
	pkColIdx := uint16(0)
	var err error
	var meta *common.IndexMeta
	var res bool
	var ans *roaring.Bitmap

	writer := NewBlockZoneMapIndexWriter()
	err = writer.Init(file, cType, pkColIdx)
	require.NoError(t, err)

	keys := common.MockVec(typ, 1000, 0)
	err = writer.AddValues(keys)
	require.NoError(t, err)

	meta, err = writer.Finalize()
	require.NoError(t, err)

	reader := NewBlockZoneMapIndexReader()
	err = reader.Init(bufManager, file, meta)
	require.NoError(t, err)

	//t.Log(bufManager.String())

	res, err = reader.MayContainsKey(int32(500))
	require.NoError(t, err)
	require.True(t, res)

	res, err = reader.MayContainsKey(int32(1000))
	require.NoError(t, err)
	require.False(t, res)

	keys = common.MockVec(typ, 100, 1000)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.False(t, res)
	require.Equal(t, uint64(0), ans.GetCardinality())

	keys = common.MockVec(typ, 100, 0)
	res, ans, err = reader.MayContainsAnyKeys(keys)
	require.NoError(t, err)
	require.True(t, res)
	require.Equal(t, uint64(100), ans.GetCardinality())

	//t.Log(bufManager.String())
}
