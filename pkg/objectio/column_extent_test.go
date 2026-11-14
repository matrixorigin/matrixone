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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// The object meta's extent of a column, of fixed or variable width, is the run of that
// column's block extents, one after another: one read of it holds every block's column.
// Without per-column object metadata the extent is empty.
func TestObjectColumnExtentIsItsBlocksInOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs := newBlockTopNTestFS(t)
	ctx := context.Background()

	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0, 1, 2}, nil)
	require.NoError(t, err)
	rowsPerBlock := []int{100, 3000, 7}
	cats := make([][]string, 0, len(rowsPerBlock))
	for blk, rows := range rowsPerBlock {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_array_float32.ToType())
		blkCats := make([]string, 0, rows)
		for row := range rows {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(blk*10000+row), false, mp))
			// Short values are stored inline, long ones in the vector's area.
			cat := fmt.Sprintf("c%d", row%13)
			if row%3 == 0 {
				cat = strings.Repeat(fmt.Sprintf("long-%d-%d|", blk, row), 1+row%5)
			}
			isNull := row%17 == 0
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(cat), isNull, mp))
			if isNull {
				cat = ""
			}
			blkCats = append(blkCats, cat)
			require.NoError(t, vector.AppendArray(bat.Vecs[2], []float32{float32(row), 1, 2, 3}, false, mp))
		}
		cats = append(cats, blkCats)
		bat.SetRowCount(rows)
		_, err := writer.Write(bat)
		require.NoError(t, err)
		bat.Clean(mp)
	}
	// Table data writers record per-column object metadata; the writer sets its extents.
	writer.WriteObjectMeta(ctx, uint32(100+3000+7), []ColumnMeta{BuildObjectColumnMeta(), BuildObjectColumnMeta(), BuildObjectColumnMeta()})
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	location := BuildLocation(name, blocks[0].GetExtent(), uint32(rowsPerBlock[0]), 0)
	objMeta, err := FastLoadObjectMeta(ctx, &location, false, fs)
	require.NoError(t, err)
	meta := objMeta.MustDataMeta()
	require.Equal(t, uint32(len(rowsPerBlock)), meta.BlockCount())

	for _, seq := range []uint16{0, 1, 2} {
		column := meta.MustGetColumn(seq).Location()
		if seq > 0 {
			before := meta.MustGetColumn(seq - 1).Location()
			require.Equal(t, before.Offset()+before.Length(), column.Offset(), "column %d starts where the one before ends", seq)
		}
		next := column.Offset()
		var total uint32
		for blk := range rowsPerBlock {
			ext := meta.GetBlockMeta(uint32(blk)).ColumnMeta(seq).Location()
			require.Equal(t, next, ext.Offset(), "column %d block %d starts where the one before ends", seq, blk)
			next += ext.Length()
			total += ext.Length()
		}
		require.Equal(t, column.Length(), total, "column %d", seq)

		whole := readRaw(t, fs, name.String(), int64(column.Offset()), int64(column.Length()))
		for blk := range rowsPerBlock {
			ext := meta.GetBlockMeta(uint32(blk)).ColumnMeta(seq).Location()
			at := ext.Offset() - column.Offset()
			require.True(t, bytes.Equal(whole[at:at+ext.Length()], readRaw(t, fs, name.String(), int64(ext.Offset()), int64(ext.Length()))),
				"column %d block %d", seq, blk)
		}
	}

	id = NewObjectid()
	bare := BuildObjectNameWithObjectID(&id)
	bareWriter, err := NewObjectWriter(bare, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, mp))
	bat.SetRowCount(1)
	_, err = bareWriter.Write(bat)
	require.NoError(t, err)
	bat.Clean(mp)
	bareBlocks, err := bareWriter.WriteEnd(ctx)
	require.NoError(t, err)
	bareLocation := BuildLocation(bare, bareBlocks[0].GetExtent(), 1, 0)
	bareMeta, err := FastLoadObjectMeta(ctx, &bareLocation, false, fs)
	require.NoError(t, err)
	require.Zero(t, bareMeta.MustDataMeta().MustGetColumn(0).Location().Length())

	for blk, want := range cats {
		read, err := ReadOneBlock(ctx, &meta, name.String(), uint16(blk), []uint16{1}, []types.Type{types.T_varchar.ToType()}, mp, fs, fileservice.SkipFullFilePreloads)
		require.NoError(t, err)
		vec := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, MustVectorToCached(vec, read.Entries[0].CachedData))
		require.Equal(t, len(want), vec.Length())
		for row, cat := range want {
			if row%17 == 0 {
				require.True(t, vec.IsNull(uint64(row)), "block %d row %d", blk, row)
				continue
			}
			require.Equal(t, cat, vec.GetStringAt(row), "block %d row %d", blk, row)
		}
		read.Release()
	}
}

func readRaw(t *testing.T, fs fileservice.FileService, path string, offset, size int64) []byte {
	t.Helper()
	vec := &fileservice.IOVector{FilePath: path, Policy: fileservice.SkipAllCache, Entries: []fileservice.IOEntry{{Offset: offset, Size: size}}}
	require.NoError(t, fs.Read(context.Background(), vec))
	defer vec.Release()
	return bytes.Clone(vec.Entries[0].Data)
}
