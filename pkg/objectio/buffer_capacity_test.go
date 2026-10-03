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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type captureObjectEntriesFS struct {
	fileservice.FileService
	entries         fileservice.IOVector
	failure         error
	alreadyExists   bool
	writes, deletes int
}

func (fs *captureObjectEntriesFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.entries = v
	fs.writes++
	if fs.alreadyExists && fs.writes == 1 {
		return moerr.NewFileAlreadyExistsNoCtx(v.FilePath)
	}
	if fs.failure != nil {
		return fs.failure
	}
	return fs.FileService.Write(ctx, v)
}
func (fs *captureObjectEntriesFS) Delete(ctx context.Context, paths ...string) error {
	fs.deletes++
	return fs.FileService.Delete(ctx, paths...)
}

func TestObjectBufferAppendOffsets(t *testing.T) {
	b := NewObjectBuffer("object")
	require.NotNil(t, b.GetData().Entries)
	for i := 0; i < 300; i++ {
		payload := []byte{byte(i), byte(i >> 8)}
		offset, size := b.Write(payload)
		require.Equal(t, 2*i, offset)
		require.Equal(t, 2, size)
	}
	require.Len(t, b.GetData().Entries, 300)
	for i, entry := range b.GetData().Entries {
		require.Equal(t, int64(2*i), entry.Offset)
		require.Equal(t, int64(2), entry.Size)
		require.Equal(t, []byte{byte(i), byte(i >> 8)}, entry.Data)
	}
}

func TestObjectWriterReservesPreparedEntries(t *testing.T) {
	for _, width := range []int{1, 8, 300} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			ctx := context.Background()
			base, err := fileservice.NewMemoryFS("test", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { base.Close(ctx) })
			fs := &captureObjectEntriesFS{FileService: base, alreadyExists: width == 8}
			if fs.alreadyExists {
				require.NoError(t, base.Write(ctx, fileservice.IOVector{FilePath: "object", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte{0}}}}))
			}
			writer, err := NewObjectWriterSpecial(WriterNormal, "object", fs)
			require.NoError(t, err)
			mp := mpool.MustNewZero()
			t.Cleanup(func() { mpool.DeleteMPool(mp) })
			bat := batch.NewWithSize(width)
			t.Cleanup(func() { bat.Clean(mp) })
			for i := range bat.Vecs {
				bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(bat.Vecs[i], int64(i), false, mp))
			}
			bat.SetRowCount(1)
			_, err = writer.Write(bat)
			require.NoError(t, err)
			blocks, err := writer.WriteEnd(ctx)
			require.NoError(t, err)
			require.Len(t, blocks, 1)
			// Two schema groups, one block's columns, and header/meta/footer.
			require.Len(t, fs.entries.Entries, width+7)
			if width == 1 {
				require.Less(t, cap(fs.entries.Entries), 256)
			}
			offset := int64(0)
			for _, entry := range fs.entries.Entries {
				require.Equal(t, offset, entry.Offset)
				require.Equal(t, int64(len(entry.Data)), entry.Size)
				offset += entry.Size
			}
			stats := writer.GetDataStats()
			require.Equal(t, uint32(offset), stats.Size())
			reader, err := NewObjectReaderWithStr("object", base)
			require.NoError(t, err)
			extent := blocks[0].GetExtent()
			reader.CacheMetaExtent(&extent)
			meta, err := reader.ReadMeta(ctx, mp)
			require.NoError(t, err)
			require.Equal(t, uint16(width), meta.MustDataMeta().GetBlockMeta(0).BlockHeader().ColumnCount())
			indexes := make([]uint16, width)
			columnTypes := make([]types.Type, width)
			for i := range indexes {
				indexes[i] = uint16(i)
				columnTypes[i] = types.T_int64.ToType()
			}
			data, err := reader.ReadOneBlock(ctx, indexes, columnTypes, 0, mp)
			require.NoError(t, err)
			t.Cleanup(data.Release)
			for i, entry := range data.Entries {
				decoded, err := DecodeCached(entry.CachedData)
				require.NoError(t, err)
				require.Equal(t, int64(i), vector.GetFixedAtWithTypeCheck[int64](decoded.(*vector.Vector), 0))
			}
			if fs.alreadyExists {
				require.Equal(t, 2, fs.writes)
				require.Equal(t, 1, fs.deletes)
			} else {
				require.Equal(t, 1, fs.writes)
				require.Zero(t, fs.deletes)
			}
		})
	}
	t.Run("sync failure", func(t *testing.T) {
		failure := moerr.NewInternalErrorNoCtx("write failed")
		fs := &captureObjectEntriesFS{failure: failure}
		writer, err := NewObjectWriterSpecial(WriterNormal, "object", fs)
		require.NoError(t, err)
		_, err = writer.WriteEnd(context.Background())
		require.ErrorIs(t, err, failure)
		require.Nil(t, writer.buffer)
		require.Equal(t, 1, fs.writes)
		require.Len(t, fs.entries.Entries, 7)
	})
}
