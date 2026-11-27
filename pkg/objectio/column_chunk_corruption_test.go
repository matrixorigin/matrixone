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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func twoChunkTestColumn(t *testing.T, secondType types.Type) []byte {
	t.Helper()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	first := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(first, int64(1), false, mp))
	second := vector.NewVec(secondType)
	switch secondType.Oid {
	case types.T_int64:
		require.NoError(t, vector.AppendFixed(second, int64(2), false, mp))
	case types.T_int32:
		require.NoError(t, vector.AppendFixed(second, int32(2), false, mp))
	default:
		t.Fatal("unsupported fixture type")
	}
	parts := [][]byte{marshalLegacyTestColumn(t, first), marshalLegacyTestColumn(t, second)}
	first.Free(mp)
	second.Free(mp)
	headerSize := columnChunkHeaderSize + 2*columnChunkEntrySize
	data := make([]byte, headerSize+len(parts[0])+len(parts[1]))
	copy(data, columnChunkMagic[:])
	binary.LittleEndian.PutUint32(data[8:12], 2)
	binary.LittleEndian.PutUint32(data[12:16], 2)
	offset := headerSize
	for i, part := range parts {
		encodeColumnChunkMeta(data[columnChunkHeaderSize+i*columnChunkEntrySize:], columnChunkMeta{
			rowStart: uint32(i), rowCount: 1, offset: uint32(offset), length: uint32(len(part)),
			originSize: uint32(len(part)), algorithm: compress.None,
		})
		copy(data[offset:], part)
		offset += len(part)
	}
	return data
}

func TestChunkedColumnCorruptSecondChunkDoesNotReturnPartialVector(t *testing.T) {
	allocator := fileservice.DefaultCacheDataAllocator()
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	valid := twoChunkTestColumn(t, types.T_int64.ToType())
	decoded, err := decodeChunkedColumn(ctx, valid, allocator)
	require.NoError(t, err)
	vec, err := DecodeCached(decoded)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](vec.(*vector.Vector)))
	vec.(*vector.Vector).Free(mp)
	decoded.Release()

	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "different type", data: twoChunkTestColumn(t, types.T_int32.ToType()), want: "payload type mismatch"},
		{name: "second payload truncated", data: func() []byte {
			bad := bytes.Clone(valid)
			_, metas, err := parseColumnChunkHeader(bad, uint32(len(bad)))
			require.NoError(t, err)
			clear(bad[metas[1].offset : metas[1].offset+metas[1].length])
			return bad
		}(), want: ""},
		{name: "second vector corrupt", data: func() []byte {
			bad := bytes.Clone(valid)
			_, metas, err := parseColumnChunkHeader(bad, uint32(len(bad)))
			require.NoError(t, err)
			clear(bad[metas[1].offset+IOEntryHeaderSize : metas[1].offset+metas[1].length])
			return bad
		}(), want: ""},
		{name: "second payload row count", data: func() []byte {
			bad := bytes.Clone(valid)
			binary.LittleEndian.PutUint32(bad[8:12], 3)
			binary.LittleEndian.PutUint32(bad[columnChunkHeaderSize+columnChunkEntrySize+4:], 2)
			return bad
		}(), want: "payload row count mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeChunkedColumn(ctx, tc.data, allocator)
			require.Error(t, err)
			require.Nil(t, got)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestChunkedColumnHeaderRejectsIncompleteAndMismatchedRows(t *testing.T) {
	valid := twoChunkTestColumn(t, types.T_int64.ToType())
	_, _, err := parseColumnChunkHeader(valid[:columnChunkHeaderSize], uint32(len(valid)))
	require.Error(t, err)
	bad := bytes.Clone(valid)
	binary.LittleEndian.PutUint32(bad[8:12], 3)
	_, _, err = parseColumnChunkHeader(bad, uint32(len(bad)))
	require.ErrorContains(t, err, "row count mismatch")
	_, _, err = parseColumnChunkHeader(valid, uint32(len(valid)+1))
	require.ErrorContains(t, err, "row count mismatch")
}

func TestChunkedColumnWindowReleasesEarlierRowsOnLateFailure(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	valid := twoChunkTestColumn(t, types.T_int64.ToType())
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "two-chunks", Entries: []fileservice.IOEntry{{Data: valid, Size: int64(len(valid))}},
	}))
	ext := NewExtent(compress.Lz4Chunked, 0, uint32(len(valid)), uint32(len(valid)))
	window, err := readChunkedColumnWindow(ctx, "two-chunks", ext, 0, 2,
		fileservice.SkipAllCache, fs, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](window))
	window.Free(mp)

	readErr := errors.New("second chunk unavailable")
	failing := &countingColumnFS{FileService: fs, failAt: 4, failErr: readErr}
	window, err = readChunkedColumnWindow(ctx, "two-chunks", ext, 0, 2,
		fileservice.SkipAllCache, failing, mp)
	require.Nil(t, window)
	require.ErrorIs(t, err, readErr)
	require.Zero(t, mp.CurrNB())

	mixed := twoChunkTestColumn(t, types.T_int32.ToType())
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "mixed-chunks", Entries: []fileservice.IOEntry{{Data: mixed, Size: int64(len(mixed))}},
	}))
	window, err = readChunkedColumnWindow(ctx, "mixed-chunks",
		NewExtent(compress.Lz4Chunked, 0, uint32(len(mixed)), uint32(len(mixed))), 0, 2,
		fileservice.SkipAllCache, fs, mp)
	require.Nil(t, window)
	require.ErrorContains(t, err, "payload type mismatch")
	require.Zero(t, mp.CurrNB())

	window, err = readChunkedColumnWindow(ctx, "no-cache-data", ext, 0, 2,
		fileservice.SkipAllCache, &partialReadErrorFS{}, mp)
	require.Nil(t, window)
	require.ErrorContains(t, err, "cache data is unavailable")
	require.Zero(t, mp.CurrNB())
}
