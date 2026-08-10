// Copyright 2026 Matrix Origin
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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TestChunkedColumnRoundTripAndRangedRead(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	const rows = 640
	source := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < rows; i++ {
		value := strings.Repeat(string(rune('a'+i%20)), 20<<10)
		require.NoError(t, vector.AppendBytes(source, []byte(value), false, mp))
	}
	defer source.Free(mp)

	encoded, chunked, err := encodeChunkedColumn(source)
	require.NoError(t, err)
	require.True(t, chunked)
	totalRows, metas, err := parseColumnChunkHeader(encoded, uint32(len(encoded)))
	require.NoError(t, err)
	require.Equal(t, uint32(rows), totalRows)
	require.Greater(t, len(metas), 1)

	cacheData, err := constructorFactory(int64(source.Size()), compress.Lz4Chunked)(
		ctx, bytes.NewReader(encoded), encoded, fileservice.DefaultCacheDataAllocator(),
	)
	require.NoError(t, err)
	decodedObject, err := DecodeCached(cacheData)
	require.NoError(t, err)
	decoded := decodedObject.(*vector.Vector)
	require.Equal(t, source.Length(), decoded.Length())
	require.Equal(t, source.GetStringAt(0), decoded.GetStringAt(0))
	require.Equal(t, source.GetStringAt(rows-1), decoded.GetStringAt(rows-1))
	decoded.Free(mp)
	cacheData.Release()

	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	const name = "chunked-column"
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(encoded)), Data: encoded}},
	}))
	extent := NewExtent(compress.Lz4Chunked, 0, uint32(len(encoded)), uint32(source.Size()))
	windowStart := int(metas[1].rowStart) - 2
	window, err := readChunkedColumnWindow(
		ctx, name, extent, windowStart, 7, fileservice.SkipAllCache, fs, mp,
	)
	require.NoError(t, err)
	defer window.Free(mp)
	require.Equal(t, 7, window.Length())
	for i := 0; i < window.Length(); i++ {
		require.Equal(t, source.GetStringAt(windowStart+i), window.GetStringAt(i))
	}
	_, err = readChunkedColumnWindow(
		ctx, name, extent, -1, 1, fileservice.SkipAllCache, fs, mp,
	)
	require.Error(t, err)

	const invalidName = "invalid-chunked-column"
	invalid := make([]byte, columnChunkHeaderSize)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: invalidName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(invalid)), Data: invalid}},
	}))
	_, err = readChunkedColumnWindow(
		ctx, invalidName,
		NewExtent(compress.Lz4Chunked, 0, uint32(len(invalid)), uint32(len(invalid))),
		0, 1, fileservice.SkipAllCache, fs, mp,
	)
	require.Error(t, err)
	_, err = readChunkedColumnWindow(
		ctx, "missing-chunked-column",
		NewExtent(compress.Lz4Chunked, 0, columnChunkHeaderSize, columnChunkHeaderSize),
		0, 1, fileservice.SkipAllCache, fs, mp,
	)
	require.Error(t, err)

	const malformedName = "malformed-chunked-column"
	malformed := make([]byte, columnChunkHeaderSize+columnChunkEntrySize)
	copy(malformed, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(malformed[8:12], 1)
	binary.LittleEndian.PutUint32(malformed[12:16], 1)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: malformedName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(malformed)), Data: malformed}},
	}))
	_, err = readChunkedColumnWindow(
		ctx, malformedName,
		NewExtent(compress.Lz4Chunked, 0, uint32(len(malformed)), uint32(len(malformed))),
		0, 1, fileservice.SkipAllCache, fs, mp,
	)
	require.Error(t, err)
}

func TestChunkedColumnRejectsMalformedMetadata(t *testing.T) {
	_, _, err := parseColumnChunkHeader([]byte("short"), 5)
	require.Error(t, err)
	_, err = chunkedColumnHeaderReadSize([]byte("short"), columnChunkHeaderSize)
	require.Error(t, err)

	prefix := make([]byte, columnChunkHeaderSize)
	copy(prefix, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(prefix[12:16], math.MaxUint32)
	_, err = chunkedColumnHeaderReadSize(prefix, columnChunkHeaderSize)
	require.ErrorContains(t, err, "header is too large")

	header := make([]byte, columnChunkHeaderSize+columnChunkEntrySize)
	copy(header, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 1)
	encodeColumnChunkMeta(header[columnChunkHeaderSize:], columnChunkMeta{
		rowStart: 1, rowCount: 1, offset: uint32(len(header)), length: 1,
		originSize: 1, algorithm: compress.None,
	})
	_, _, err = parseColumnChunkHeader(header, uint32(len(header)+1))
	require.Error(t, err)

	// Row counts must not wrap around uint32 and accidentally match totalRows.
	header = make([]byte, columnChunkHeaderSize+2*columnChunkEntrySize)
	copy(header, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], 1)
	binary.LittleEndian.PutUint32(header[12:16], 2)
	encodeColumnChunkMeta(header[columnChunkHeaderSize:], columnChunkMeta{
		rowCount: math.MaxUint32,
		offset:   uint32(len(header)), algorithm: uint8(compress.None),
	})
	encodeColumnChunkMeta(header[columnChunkHeaderSize+columnChunkEntrySize:], columnChunkMeta{
		rowStart: math.MaxUint32, rowCount: 2,
		offset: uint32(len(header)), algorithm: uint8(compress.None),
	})
	_, _, err = parseColumnChunkHeader(header, uint32(len(header)))
	require.Error(t, err)
}

func TestChunkedColumnSharedAreaFallsBackToLegacyWriter(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	constant, err := vector.NewConstBytes(
		types.T_text.ToType(), bytes.Repeat([]byte("x"), columnChunkTargetBytes+1), 3, mp,
	)
	require.NoError(t, err)
	defer constant.Free(mp)
	shared := vector.NewVec(types.T_text.ToType())
	require.NoError(t, shared.UnionBatch(constant, 0, constant.Length(), nil, mp))
	require.False(t, shared.VarlenaAreaIsDisjoint())
	fullSize, err := shared.MarshalBinarySize()
	require.NoError(t, err)
	require.Greater(t, fullSize+IOEntryHeaderSize, columnChunkTargetBytes)

	encoded, chunked, err := encodeChunkedColumn(shared)
	require.NoError(t, err)
	require.False(t, chunked)
	require.Nil(t, encoded)

	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	objectID := NewObjectid()
	objectName := BuildObjectNameWithObjectID(&objectID)
	writer, err := NewObjectWriter(
		objectName, fs, 0, []uint16{0}, nil,
	)
	require.NoError(t, err)
	writer.SetChunkedColumnPolicy(func() bool { return true })
	bat := batch.NewWithSize(1)
	bat.SetVector(0, shared)
	bat.SetRowCount(shared.Length())
	defer bat.Clean(mp)

	block, err := writer.Write(bat)
	require.NoError(t, err)
	extent := block.MustGetColumn(0).Location()
	require.Equal(t, uint8(compress.Lz4), extent.Alg())
	require.Less(t, extent.OriginSize(), uint32(2*columnChunkTargetBytes),
		"shared payload must remain O(source bytes), not O(rows*source bytes)")
	blocks, err := writer.WriteEnd(context.Background())
	require.NoError(t, err)
	require.Len(t, blocks, 1)

	reader, err := NewObjectReaderWithStr(objectName.String(), fs)
	require.NoError(t, err)
	metaExtent := blocks[0].GetExtent()
	reader.CacheMetaExtent(&metaExtent)
	read, err := reader.ReadOneBlock(
		context.Background(), []uint16{0}, []types.Type{types.T_text.ToType()}, 0, mp,
	)
	require.NoError(t, err)
	defer read.Release()
	decoded, err := DecodeCached(read.Entries[0].CachedData)
	require.NoError(t, err)
	got := decoded.(*vector.Vector)
	require.Equal(t, 3, got.Length())
	for row := range got.Length() {
		require.Equal(t, constant.GetBytesAt(0), got.GetBytesAt(row))
	}
	window, err := MaterializeCachedVectorWindow(read.Entries[0].CachedData, 0, 3, mp)
	require.NoError(t, err)
	defer window.Free(mp)
	require.Less(t, window.Allocated(), 2*columnChunkTargetBytes,
		"read-side materialization must preserve the compact shared area")
	for row := range window.Length() {
		require.Equal(t, constant.GetBytesAt(0), window.GetBytesAt(row))
	}
}
