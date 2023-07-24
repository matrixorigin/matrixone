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

package objectio

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "ObjectIo"
)

func GetDefaultTestPath(module string, name string) string {
	return filepath.Join("/tmp", module, name)
}

func MakeDefaultTestPath(module string, name string) string {
	path := GetDefaultTestPath(module, name)
	os.MkdirAll(path, os.FileMode(0755))
	return path
}

func RemoveDefaultTestPath(module string, name string) {
	path := GetDefaultTestPath(module, name)
	os.RemoveAll(path)
}

func InitTestEnv(module string, name string) string {
	RemoveDefaultTestPath(module, name)
	return MakeDefaultTestPath(module, name)
}

func TestNewObjectWriter(t *testing.T) {
	ctx := context.Background()

	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	bat2 := newBatch2(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i := range bat.Vecs {
		zbuf := make([]byte, 64)
		zbuf[31] = 1
		zbuf[63] = 10
		fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	_, err = objectWriter.WriteWithoutSeqnum(bat2)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(blocks))
	assert.Nil(t, objectWriter.buffer)

	objectReader, _ := NewObjectReaderWithStr(name, service)
	extents := make([]Extent, 3)
	for i, blk := range blocks {
		extents[i] = NewExtent(1, blk.GetExtent().Offset(), blk.GetExtent().Length(), blk.GetExtent().OriginSize())
	}
	pool, err := mpool.NewMPool("objectio_test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	nb0 := pool.CurrNB()
	objectReader.CacheMetaExtent(&extents[0])
	metaHeader, err := objectReader.ReadMeta(context.Background(), pool)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	assert.Equal(t, uint32(3), meta.BlockCount())
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	typs := []types.Type{types.T_int8.ToType(), types.T_int32.ToType(), types.T_int64.ToType()}
	vec, err := objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)

	obj, err := Decode(vec.Entries[0].ObjectBytes)
	assert.Nil(t, err)
	vector1 := obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedCol[int8](vector1)[3])

	obj, err = Decode(vec.Entries[1].ObjectBytes)
	assert.Nil(t, err)
	vector2 := obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedCol[int32](vector2)[3])

	obj, err = Decode(vec.Entries[2].ObjectBytes)
	assert.Nil(t, err)
	vector3 := obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAt[int64](vector3, 3))

	blk := blocks[0].MustGetColumn(idxs[0])
	buf := blk.ZoneMap()
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
	assert.True(t, nb0 == pool.CurrNB())

	fs := NewObjectFS(service, dir)
	dirs, err := fs.ListDir("")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(dirs))
	objectReader, err = NewObjectReaderWithStr(name, service)
	assert.Nil(t, err)
	metaHeader, err = objectReader.ReadAllMeta(context.Background(), pool)
	assert.Nil(t, err)
	meta, _ = metaHeader.DataMeta()
	assert.Equal(t, uint32(3), meta.BlockCount())
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), meta.BlockCount())
	idxs = make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	vec, err = objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)

	obj, err = Decode(vec.Entries[0].ObjectBytes)
	assert.Nil(t, err)
	vector1 = obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedCol[int8](vector1)[3])

	obj, err = Decode(vec.Entries[1].ObjectBytes)
	assert.Nil(t, err)
	vector2 = obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedCol[int32](vector2)[3])

	obj, err = Decode(vec.Entries[2].ObjectBytes)
	assert.Nil(t, err)
	vector3 = obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAt[int64](vector3, 3))
	blk = blocks[0].MustGetColumn(idxs[0])
	buf = blk.ZoneMap()
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
	assert.True(t, nb0 == pool.CurrNB())
	buf1, err := objectReader.ReadExtent(context.Background(), meta.BlockHeader().ZoneMapArea())
	assert.Nil(t, err)
	zma := ZoneMapArea(buf1)
	buf = zma.GetZoneMap(0, 0)
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
}

func getObjectMeta(ctx context.Context, t *testing.B) ObjectDataMeta {
	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	for y := 0; y < 1; y++ {
		fd, err := objectWriter.Write(bat)
		assert.Nil(t, err)
		for i := range bat.Vecs {
			zbuf := make([]byte, 64)
			zbuf[31] = 1
			zbuf[63] = 10
			fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
		}
	}
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	objectReader, _ := NewObjectReaderWithStr(name, service)
	ext := blocks[0].BlockHeader().MetaLocation()
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	return meta
}

func BenchmarkMetadata(b *testing.B) {
	ctx := context.Background()
	meta := getObjectMeta(ctx, b)
	b.Run("GetBlockMeta", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.GetBlockMeta(0)
		}
	})
	b.Log(meta.GetBlockMeta(0).GetID())
	b.Run("GetColumnMeta", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.Length()
		}
	})
	b.Run("BlockCount", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			meta.BlockCount()
		}
	})
	b.Log(meta.BlockCount())
}

func TestNewObjectReader(t *testing.T) {
	ctx := context.Background()

	dir := InitTestEnv(ModuleName, t.Name())
	dir = path.Join(dir, "/local")
	id := 1
	name := fmt.Sprintf("%d.blk", id)
	mp := mpool.MustNewZero()
	bat := newBatch(mp)
	defer bat.Clean(mp)
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	fd, err := objectWriter.Write(bat)
	assert.Nil(t, err)
	for i := range bat.Vecs {
		zbuf := make([]byte, 64)
		zbuf[31] = 1
		zbuf[63] = 10
		fd.ColumnMeta(uint16(i)).SetZoneMap(zbuf)
	}
	_, err = objectWriter.Write(bat)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	objectReader, _ := NewObjectReaderWithStr(name, service)
	ext := blocks[0].BlockHeader().MetaLocation()
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	assert.Equal(t, 2, meta.BlockCount())
}

func newBatch(mp *mpool.MPool) *batch.Batch {
	types := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint8.ToType(),
		types.T_uint64.ToType(),
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}

func newBatch2(mp *mpool.MPool) *batch.Batch {
	types := []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
	}
	return testutil.NewBatch(types, false, int(40000*2), mp)
}
