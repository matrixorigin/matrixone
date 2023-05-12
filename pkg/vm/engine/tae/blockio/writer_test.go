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

package blockio

import (
	"context"
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ModuleName = "BlockIO"
)

func TestWriter_WriteBlockAndZoneMap(t *testing.T) {
	defer testutils.AfterTest(t)()
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c, nil)
	assert.Nil(t, err)
	writer, _ := NewBlockWriterNew(service, name, 0, nil)

	schema := catalog.MockSchemaAll(13, 2)
	bats := catalog.MockBatch(schema, 40000*2).Split(2)

	_, err = writer.WriteBatch(containers.ToCNBatch(bats[0]))
	assert.Nil(t, err)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[1]))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	fd := blocks[0]
	col := fd.MustGetColumn(2)
	colZoneMap := col.ZoneMap()
	zm := index.DecodeZM(colZoneMap)

	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(39999))
	require.True(t, res)
	res = zm.Contains(int32(40000))
	require.False(t, res)

	mp := mpool.MustNewZero()
	metaloc := EncodeLocation(writer.GetName(), blocks[0].GetExtent(), 40000, blocks[0].GetID())
	require.NoError(t, err)
	reader, err := NewObjectReader(service, metaloc)
	require.NoError(t, err)
	meta, err := reader.LoadObjectMeta(context.TODO(), mp)
	require.NoError(t, err)
	blkMeta1 := meta.GetBlockMeta(0)
	blkMeta2 := meta.GetBlockMeta(1)
	for i := uint16(0); i < meta.BlockHeader().ColumnCount(); i++ {
		offset := blkMeta1.ColumnMeta(i).Location().Offset()
		length := blkMeta1.ColumnMeta(i).Location().Length() + blkMeta2.ColumnMeta(i).Location().Length()
		oSize := blkMeta1.ColumnMeta(i).Location().OriginSize() + blkMeta2.ColumnMeta(i).Location().OriginSize()
		assert.Equal(t, offset, meta.MustGetColumn(i).Location().Offset())
		assert.Equal(t, length, meta.MustGetColumn(i).Location().Length())
		assert.Equal(t, oSize, meta.MustGetColumn(i).Location().OriginSize())
	}
	header := meta.BlockHeader()
	require.Equal(t, uint32(80000), header.Rows())
	t.Log(meta.MustGetColumn(0).Ndv(), meta.MustGetColumn(1).Ndv(), meta.MustGetColumn(2).Ndv())
	zm = meta.MustGetColumn(2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.False(t, zm.Contains(int32(100000)))
	zm = meta.GetColumnMeta(0, 2).ZoneMap()
	require.True(t, zm.Contains(int32(39999)))
	require.False(t, zm.Contains(int32(40000)))
	zm = meta.GetColumnMeta(1, 2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.True(t, zm.Contains(int32(79999)))
	require.False(t, zm.Contains(int32(80000)))
}

func TestWriter_WriteBlockAfterAlter(t *testing.T) {
	defer testutils.AfterTest(t)()
	dir := testutils.InitTestEnv(ModuleName, t)
	dir = path.Join(dir, "/local")
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(c, nil)
	assert.Nil(t, err)

	schema := catalog.MockSchemaAll(13, 2)
	schema.ApplyAlterTable(api.NewAddColumnReq(0, 0, "xyz", types.NewProtoType(types.T_int32), 1))
	schema.ApplyAlterTable(api.NewRemoveColumnReq(0, 0, 6, 5))

	seqnums := make([]uint16, 0, len(schema.ColDefs))
	for _, col := range schema.ColDefs {
		seqnums = append(seqnums, col.SeqNum)
	}
	t.Log(seqnums)
	bats := containers.MockBatchWithAttrs(
		schema.AllTypes(),
		schema.AllNames(),
		40000*2,
		schema.GetSingleSortKey().Idx, nil).Split(2)

	writer, _ := NewBlockWriterNew(service, name, 1, seqnums)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[0]))
	assert.Nil(t, err)
	_, err = writer.WriteBatch(containers.ToCNBatch(bats[1]))
	assert.Nil(t, err)
	blocks, _, err := writer.Sync(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	fd := blocks[0]
	colx := fd.MustGetColumn(13 /* xyz seqnum*/)
	assert.Equal(t, uint8(types.T_int32), colx.DataType())
	assert.Equal(t, uint16(1), colx.Idx())
	colx = fd.MustGetColumn(14 /* rowid seqnum*/)
	assert.Equal(t, uint8(types.T_Rowid), colx.DataType())
	assert.Equal(t, uint16(13), colx.Idx())
	assert.NoError(t, err)

	col := fd.MustGetColumn(2 /*pk seqnum*/)
	assert.Nil(t, err)
	colZoneMap := col.ZoneMap()
	zm := index.DecodeZM(colZoneMap)

	require.NoError(t, err)
	res := zm.Contains(int32(500))
	require.True(t, res)
	res = zm.Contains(int32(39999))
	require.True(t, res)
	res = zm.Contains(int32(40000))
	require.False(t, res)

	mp := mpool.MustNewZero()
	metaloc := EncodeLocation(writer.GetName(), blocks[0].GetExtent(), 40000, blocks[0].GetID())
	require.NoError(t, err)
	reader, err := NewObjectReader(service, metaloc)
	require.NoError(t, err)
	meta, err := reader.LoadObjectMeta(context.TODO(), mp)
	require.Equal(t, uint16(15), meta.BlockHeader().MetaColumnCount())
	require.Equal(t, uint16(14), meta.BlockHeader().ColumnCount())
	require.NoError(t, err)
	header := meta.BlockHeader()
	require.Equal(t, uint32(80000), header.Rows())
	t.Log(meta.MustGetColumn(0).Ndv(), meta.MustGetColumn(1).Ndv(), meta.MustGetColumn(2).Ndv())
	zm = meta.MustGetColumn(2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.False(t, zm.Contains(int32(100000)))
	zm = meta.GetColumnMeta(0, 2).ZoneMap()
	require.True(t, zm.Contains(int32(39999)))
	require.False(t, zm.Contains(int32(40000)))
	zm = meta.GetColumnMeta(1, 2).ZoneMap()
	require.True(t, zm.Contains(int32(40000)))
	require.True(t, zm.Contains(int32(79999)))
	require.False(t, zm.Contains(int32(80000)))
}

func TestMergeDeleteRows(t *testing.T) {
	require.Equal(t, mergeDeleteRows([]int64{1, 2, 3}, nil), []int64{1, 2, 3})
	require.Equal(t, mergeDeleteRows(nil, []int64{1, 2, 3}), []int64{1, 2, 3})
	require.Equal(t, mergeDeleteRows([]int64{2, 3, 7, 8, 9}, []int64{1, 2, 3}), []int64{1, 2, 3, 7, 8, 9})
	require.Equal(t, mergeDeleteRows([]int64{2}, []int64{1, 2, 3}), []int64{1, 2, 3})
	require.Equal(t, mergeDeleteRows([]int64{1, 2, 3}, []int64{1, 2, 3}), []int64{1, 2, 3})
	require.Equal(t, mergeDeleteRows([]int64{1, 2, 3}, []int64{1}), []int64{1, 2, 3})
	require.Equal(t, mergeDeleteRows([]int64{1, 2, 3}, []int64{3}), []int64{1, 2, 3})
}
