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
	"math"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		Cache:   fileservice.DisabledCacheConfig,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	defer service.Close(ctx)

	objectWriter, err := NewObjectWriterSpecial(WriterNormal, name, service)
	assert.Nil(t, err)
	objectWriter.SetAppendable()
	//objectWriter.pkColIdx = 3
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
	require.Equal(t, objectWriter.objStats.Size(), blocks[0].GetExtent().End()+FooterSize)

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
	oSize := uint32(0)
	for i := uint32(0); i < 3; i++ {
		blockMeta := meta.GetBlockMeta(i)
		for y := uint16(0); y < blockMeta.GetColumnCount(); y++ {
			oSize += blockMeta.MustGetColumn(y).Location().OriginSize()
		}
	}
	oSize += meta.BlockHeader().BFExtent().OriginSize()
	oSize += meta.BlockHeader().ZoneMapArea().OriginSize()
	// 24 is the size of empty bf and zm
	oSize += HeaderSize + FooterSize + 24 + extents[0].OriginSize()
	require.Equal(t, objectWriter.objStats.OriginSize(), oSize)
	assert.Equal(t, uint32(3), meta.BlockCount())
	assert.True(t, meta.BlockHeader().Appendable())
	assert.Equal(t, uint16(math.MaxUint16), meta.BlockHeader().SortKey())
	idxs := make([]uint16, 3)
	idxs[0] = 0
	idxs[1] = 2
	idxs[2] = 3
	typs := []types.Type{types.T_int8.ToType(), types.T_int32.ToType(), types.T_int64.ToType()}
	vec1, err := objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)
	defer vec1.Release()

	obj, err := Decode(vec1.Entries[0].CachedData.Bytes())
	assert.Nil(t, err)
	vector1 := obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedColWithTypeCheck[int8](vector1)[3])

	obj, err = Decode(vec1.Entries[1].CachedData.Bytes())
	assert.Nil(t, err)
	vector2 := obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](vector2)[3])

	obj, err = Decode(vec1.Entries[2].CachedData.Bytes())
	assert.Nil(t, err)
	vector3 := obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](vector3, 3))

	blk := blocks[0].MustGetColumn(idxs[0])
	buf := blk.ZoneMap()
	assert.Equal(t, uint8(0x1), buf[31])
	assert.Equal(t, uint8(0xa), buf[63])
	assert.True(t, nb0 == pool.CurrNB())

	dirs, err := fileservice.SortedList(service.List(ctx, ""))
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
	vec2, err := objectReader.ReadOneBlock(context.Background(), idxs, typs, 0, pool)
	assert.Nil(t, err)
	defer vec2.Release()

	obj, err = Decode(vec2.Entries[0].CachedData.Bytes())
	assert.Nil(t, err)
	vector1 = obj.(*vector.Vector)
	assert.Equal(t, int8(3), vector.MustFixedColWithTypeCheck[int8](vector1)[3])

	obj, err = Decode(vec2.Entries[1].CachedData.Bytes())
	assert.Nil(t, err)
	vector2 = obj.(*vector.Vector)
	assert.Equal(t, int32(3), vector.MustFixedColWithTypeCheck[int32](vector2)[3])

	obj, err = Decode(vec2.Entries[2].CachedData.Bytes())
	assert.Nil(t, err)
	vector3 = obj.(*vector.Vector)
	assert.Equal(t, int64(3), vector.GetFixedAtWithTypeCheck[int64](vector3, 3))
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
	defer service.Close(ctx)

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
	_, _, err = objectWriter.WriteSubBlock(bat, 2)
	assert.Nil(t, err)
	_, _, err = objectWriter.WriteSubBlock(bat, 26)
	assert.Nil(t, err)
	ts := time.Now()
	option := WriteOptions{
		Type: WriteTS,
		Val:  ts,
	}
	blocks, err := objectWriter.WriteEnd(context.Background(), option)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(blocks))
	assert.Nil(t, objectWriter.buffer)
	objectReader, _ := NewObjectReaderWithStr(name, service)
	ext := blocks[0].BlockHeader().MetaLocation()
	objectReader.CacheMetaExtent(&ext)
	metaHeader, err := objectReader.ReadMeta(context.Background(), nil)
	assert.Nil(t, err)
	meta, _ := metaHeader.DataMeta()
	assert.Equal(t, uint32(2), meta.BlockCount())
	meta, _ = metaHeader.SubMeta(0)
	assert.Equal(t, uint32(1), meta.BlockCount())
	meta, _ = metaHeader.SubMeta(24)
	assert.Equal(t, uint32(1), meta.BlockCount())
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
	return NewBatch(types, false, int(40000*2), mp)
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
	return NewBatch(types, false, int(40000*2), mp)
}

func TestWriteArena(t *testing.T) {
	t.Run("alloc within capacity", func(t *testing.T) {
		a := NewArena(1024)
		b1 := a.Alloc(100)
		require.Len(t, b1, 100)
		require.Equal(t, 100, a.usedOffset)

		b2 := a.Alloc(200)
		require.Len(t, b2, 200)
		require.Equal(t, 300, a.usedOffset)

		// b1 and b2 should be from the same backing array
		require.Equal(t, &a.data[0], &b1[0])
		require.Equal(t, &a.data[100], &b2[0])
	})

	t.Run("alloc overflow falls back to exact make", func(t *testing.T) {
		a := NewArena(64)
		b1 := a.Alloc(32)
		require.Equal(t, 32, a.usedOffset)

		// exceeds remaining capacity — falls back to plain make; arena unchanged
		b2 := a.Alloc(64)
		require.Len(t, b2, 64)
		require.Equal(t, 32, a.usedOffset) // not advanced — overflow took fallback path
		require.Equal(t, 64, len(a.data))  // backing array not yet grown
		_ = b1
	})

	t.Run("reset reuses capacity", func(t *testing.T) {
		a := NewArena(256)
		a.Alloc(100)
		a.Alloc(100)
		require.Equal(t, 200, a.usedOffset)

		a.Reset()
		require.Equal(t, 0, a.usedOffset)
		require.Equal(t, 256, len(a.data)) // backing array retained (no overflow)

		b := a.Alloc(50)
		require.Equal(t, &a.data[0], &b[0]) // reuses from beginning
	})

	t.Run("compress buf grows and persists", func(t *testing.T) {
		a := NewArena(64)

		cb1 := a.CompressBuf(100)
		require.Len(t, cb1, 100)
		require.GreaterOrEqual(t, cap(a.compressBuf), 100)

		// smaller request reuses same buffer
		cb2 := a.CompressBuf(50)
		require.Len(t, cb2, 50)
		require.Equal(t, &cb1[0], &cb2[0])

		// larger request grows
		cb3 := a.CompressBuf(200)
		require.Len(t, cb3, 200)

		// Reset does not clear compressBuf
		a.Reset()
		require.GreaterOrEqual(t, len(a.compressBuf), 200)
	})

	t.Run("zero-size arena falls back to exact make", func(t *testing.T) {
		a := NewArena(0)
		b := a.Alloc(10)
		require.Len(t, b, 10)
		require.Equal(t, 0, len(a.data))   // not grown yet — happens on Reset
		require.Equal(t, 0, a.usedOffset)  // not advanced

		// after Reset, the arena grows to accommodate future rounds
		a.Reset()
		require.GreaterOrEqual(t, len(a.data), 10)
		require.Equal(t, 0, a.usedOffset)
	})

	t.Run("reset grows arena to eliminate overflow in future rounds", func(t *testing.T) {
		a := NewArena(8)
		// First round: only the first 8-byte slot fits; rest are exact-make fallbacks.
		var round1 [][]byte
		for i := 0; i < 8; i++ {
			s := a.Alloc(8)
			require.Len(t, s, 8)
			round1 = append(round1, s)
		}
		// All returned slices are distinct and writable.
		for i, s := range round1 {
			s[0] = byte(i)
		}
		require.Equal(t, 8, a.usedOffset) // only first alloc landed in arena

		// Reset: totalRequested=64 > len(data)=8 → grow to nextPow2(64)=64.
		a.Reset()
		require.GreaterOrEqual(t, len(a.data), 64)
		require.Equal(t, 0, a.usedOffset)

		// Second round: all 8 allocations fit in the arena — no fallback.
		for i := 0; i < 8; i++ {
			s := a.Alloc(8)
			require.Len(t, s, 8)
			require.Equal(t, &a.data[i*8], &s[0]) // served from arena, not fallback
		}
		require.Equal(t, 64, a.usedOffset)
	})

	t.Run("serial buf is reused across Reset calls", func(t *testing.T) {
		a := NewArena(256)
		// First use: grow serialBuf to 100 bytes.
		a.serialBuf.Grow(100)
		cap1 := a.serialBuf.Cap()
		require.GreaterOrEqual(t, cap1, 100)

		// Reset does not clear serialBuf capacity.
		a.Reset()
		require.GreaterOrEqual(t, a.serialBuf.Cap(), cap1)
		require.Equal(t, 0, a.serialBuf.Len())

		// Second use: no growth needed — same backing array.
		ptr1 := &a.serialBuf
		a.serialBuf.Grow(50)
		require.Equal(t, ptr1, &a.serialBuf) // pointer identity — same struct
		require.GreaterOrEqual(t, a.serialBuf.Cap(), cap1)
	})
}
