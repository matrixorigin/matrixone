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
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func referenceCompressedBytes(t *testing.T, source []byte) []byte {
	t.Helper()
	out := make([]byte, lz4.CompressBlockBound(len(source)))
	var encoder lz4.Compressor
	n, err := encoder.CompressBlock(source, out)
	require.NoError(t, err)
	return out[:n]
}

func checkCompressedExtent(t *testing.T, extent Extent, offset uint32, input, output []byte) {
	t.Helper()
	require.Equal(t, uint8(compress.Lz4), extent.Alg())
	require.Equal(t, offset, extent.Offset())
	require.EqualValues(t, len(input), extent.OriginSize())
	require.EqualValues(t, len(output), extent.Length())
	require.Equal(t, referenceCompressedBytes(t, input), output)
	if len(output) != 0 {
		decoded := make([]byte, len(input))
		n, err := lz4.UncompressBlock(output, decoded)
		require.NoError(t, err)
		require.Equal(t, len(input), n)
		require.Equal(t, input, decoded)
	}
}

func TestWriteWithCompressDirectArena(t *testing.T) {
	first := bytes.Repeat([]byte("first column "), 4000)
	second := bytes.Repeat([]byte("second column "), 4000)
	arena := NewArena(2 * lz4.CompressBlockBound(len(first)+len(second)))
	t.Cleanup(arena.FreeBuffers)
	writer := &objectWriterV1{arena: arena}

	out1, ext1, err := writer.WriteWithCompress(19, first)
	require.NoError(t, err)
	firstSnapshot := bytes.Clone(out1)
	out2, ext2, err := writer.WriteWithCompress(42, second)
	require.NoError(t, err)
	checkCompressedExtent(t, ext1, 19, first, out1)
	checkCompressedExtent(t, ext2, 42, second, out2)
	require.Equal(t, firstSnapshot, out1, "later writes must not overwrite earlier output")
	require.Nil(t, arena.compressBuf, "the arena tail replaces compression scratch")
	require.Equal(t, len(out1)+len(out2), arena.usedOffset)
	require.Equal(t, arena.usedOffset, arena.totalRequested)

	arena.Reset()
	again, ext3, err := writer.WriteWithCompress(0, first)
	require.NoError(t, err)
	checkCompressedExtent(t, ext3, 0, first, again)
	require.Nil(t, arena.compressBuf)
}

func TestWriteWithCompressDirectArenaBoundaryAndFallback(t *testing.T) {
	input := bytes.Repeat([]byte("value with overlap "), 2048)
	bound := lz4.CompressBlockBound(len(input))
	for _, tc := range []struct {
		name        string
		size        int
		overlap     bool
		noArena     bool
		wantScratch bool
	}{
		{name: "exact bound", size: bound},
		{name: "insufficient tail", size: bound - 1, wantScratch: true},
		{name: "overlapping input", size: bound, overlap: true, wantScratch: true},
		{name: "no arena", noArena: true, wantScratch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var arena *WriteArena
			if !tc.noArena {
				arena = NewArena(tc.size)
				t.Cleanup(arena.FreeBuffers)
			}
			writer := &objectWriterV1{arena: arena}
			source := input
			if tc.overlap {
				copy(arena.data, input)
				source = arena.data[:len(input)]
			}
			out, ext, err := writer.WriteWithCompress(13, source)
			require.NoError(t, err)
			checkCompressedExtent(t, ext, 13, input, out)
			if tc.noArena {
				require.NotNil(t, writer.compressBuf)
			} else if tc.wantScratch {
				require.NotNil(t, arena.compressBuf)
			} else {
				require.Nil(t, arena.compressBuf)
			}
		})
	}
}

func TestWriteWithCompressIndependentConcurrentArenas(t *testing.T) {
	input := bytes.Repeat([]byte("concurrent compression "), 512)
	want := referenceCompressedBytes(t, input)
	const workers = 4
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arena := NewArena(lz4.CompressBlockBound(len(input)))
			defer arena.FreeBuffers()
			writer := &objectWriterV1{arena: arena}
			for i := 0; i < 32; i++ {
				arena.Reset()
				got, _, err := writer.WriteWithCompress(0, input)
				if err != nil || !bytes.Equal(got, want) || arena.compressBuf != nil {
					errs <- fmt.Errorf("iteration %d: error=%v, output matches=%v, scratch=%v", i, err, bytes.Equal(got, want), arena.compressBuf != nil)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestDirectArenaWriterRoundTripMultipleColumns(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	integerType, textType := types.T_int64.ToType(), types.T_text.ToType()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(integerType)
	bat.Vecs[1] = vector.NewVec(textType)
	t.Cleanup(func() { bat.Clean(mp) })
	values := [][]byte{bytes.Repeat([]byte("alpha"), 800), nil, bytes.Repeat([]byte("beta"), 700)}
	for i, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], value, value == nil, mp))
	}
	bat.SetRowCount(len(values))

	arena := NewArena(1 << 20)
	t.Cleanup(arena.FreeBuffers)
	objectID := NewObjectid()
	name := BuildObjectNameWithObjectID(&objectID)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0, 1}, arena)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Nil(t, arena.compressBuf)

	// The storage service owns written bytes independently of arena reuse.
	arena.Reset()
	clear(arena.Alloc(64 << 10))
	reader, err := NewObjectReaderWithStr(name.String(), fs)
	require.NoError(t, err)
	metaExtent := blocks[0].GetExtent()
	reader.CacheMetaExtent(&metaExtent)
	read, err := reader.ReadOneBlock(ctx, []uint16{0, 1}, []types.Type{integerType, textType}, 0, mp)
	require.NoError(t, err)
	defer read.Release()
	for column := range 2 {
		decoded, err := DecodeCached(read.Entries[column].CachedData)
		require.NoError(t, err)
		got := decoded.(*vector.Vector)
		require.Equal(t, len(values), got.Length())
		for i, value := range values {
			if column == 0 {
				require.Equal(t, int64(i), vector.MustFixedColNoTypeCheck[int64](got)[i])
			} else if value == nil {
				require.True(t, got.IsNull(uint64(i)))
			} else {
				require.Equal(t, value, got.GetBytesAt(i))
			}
		}
	}
}
