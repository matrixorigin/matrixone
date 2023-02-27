package dataio

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/desginio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

const (
	ModuleName = "DataIO"
)

func newBatch() *batch.Batch {
	mp := mpool.MustNewZero()
	types := []types.Type{
		{Oid: types.T_int32},
		{Oid: types.T_int16},
		{Oid: types.T_int32},
		{Oid: types.T_int64},
		{Oid: types.T_uint16},
		{Oid: types.T_uint32},
		{Oid: types.T_uint8},
		{Oid: types.T_uint64},
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}

func TestDataIO(t *testing.T) {
	defer testutils.AfterTest(t)()
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	bat := newBatch()
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c)
	assert.Nil(t, err)
	writer, _ := desginio.NewDataWriter(service, name)
	writer.SetPrimaryKey(0)
	_, err = writer.WriteBatch(bat)
	_, err = writer.WriteBatch(bat)
	_, err = writer.WriteBatch(bat)
	blocks, _, err := writer.Sync(context.Background())
	writer = nil
	blocks[0].GetExtent()
	reader, _ := desginio.NewDataReader(service, name)
	idxs := []uint16{0, 2, 4}
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 4
	zoneMaps, err := reader.LoadZoneMaps(context.Background(), idxs, blocks[0].GetExtent(), nil)
	zoneMap := zoneMaps[0]
	res := zoneMap.Contains(int32(500))
	require.True(t, res)
	res = zoneMap.Contains(int32(79999))
	require.True(t, res)
	res = zoneMap.Contains(int32(100000))
	require.False(t, res)

	bloomFilter, err := reader.LoadBloomFilter(context.Background(), idxs[0], blocks[0].GetExtent(), nil)
	res, _ = bloomFilter.MayContainsKey(int32(500))

	bat1, err := reader.LoadColumns(context.Background(), idxs, blocks[0].GetExtent(), nil)
	assert.NotNil(t, bat1)
	bats, err := reader.LoadAllData(context.Background(), blocks[0].GetExtent(), nil)
	assert.Equal(t, 3, len(bats))
	block, err := reader.LoadMeta(context.Background(), blocks[0].GetExtent(), nil)
	meta, err := block.GetColumn(0)
	bf, err := meta.GetIndex(context.Background(), objectio.BloomFilterType, nil)
	bf.(index.StaticFilter).MayContainsKey(int32(500))
	zm, err := meta.GetIndex(context.Background(), objectio.ZoneMapType, nil)
	res = zm.(*index.ZoneMap).Contains(int32(500))
	require.True(t, res)
}
