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

package objecttool

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectReaderAccessorsAndBounds(t *testing.T) {
	reader := &ObjectReader{
		info: &ObjectInfo{
			Path:       "dir/object",
			BlockCount: 2,
			RowCount:   10,
			ColCount:   1,
		},
		cols: []ColInfo{
			{Idx: 0, SeqNum: 3, Type: types.T_int64.ToType()},
		},
	}

	require.Same(t, reader.info, reader.Info())
	require.Equal(t, reader.cols, reader.Columns())
	require.Equal(t, uint32(2), reader.BlockCount())
	require.NoError(t, reader.Close())

	bat, release, err := reader.ReadBlock(context.Background(), 2)
	require.Error(t, err)
	assert.Nil(t, bat)
	assert.Nil(t, release)
	assert.Contains(t, err.Error(), "block index 2 out of range")

	vec, release, err := reader.ReadBlockCommitTS(context.Background(), 2)
	require.Error(t, err)
	assert.Nil(t, vec)
	assert.Nil(t, release)
	assert.Contains(t, err.Error(), "block index 2 out of range")
}

type staticCacheData []byte

func (d staticCacheData) Bytes() []byte            { return d }
func (d staticCacheData) Slice(n int) fscache.Data { return d[:n] }
func (d staticCacheData) Retain()                  {}
func (d staticCacheData) Release()                 {}

func TestDecodeBlockBatchCleansPartialBatchOnCorruptColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, make([]byte, 4096), false, mp))
	body, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)
	header := objectio.IOEntryHeader{Type: objectio.IOET_ColData, Version: objectio.IOET_ColumnData_CurrVer}
	valid := append(append([]byte(nil), objectio.EncodeIOEntryHeader(&header)...), body...)
	corrupt := append(append([]byte(nil), objectio.EncodeIOEntryHeader(&header)...), []byte("corrupt")...)

	bat, err := decodeBlockBatch(context.Background(), []fileservice.IOEntry{
		{CachedData: staticCacheData(valid)},
		{CachedData: staticCacheData(corrupt)},
	}, 2, mp)
	require.Error(t, err)
	require.Nil(t, bat)
}

func TestValidateCommitTSVectorFreesWrongType(t *testing.T) {
	mp := mpool.MustNewNoLock("object-reader-wrong-type")
	defer mpool.DeleteMPool(mp)
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, make([]byte, 4096), false, mp))
	require.Positive(t, vec.Allocated())

	got, err := validateCommitTSVector(context.Background(), vec, mp)
	require.ErrorContains(t, err, "type mismatch")
	require.Nil(t, got)
	require.Zero(t, vec.Allocated())
	require.Zero(t, mp.CurrNB())
}

func TestDecodeCommitTSVectorReturnsErrorForTruncatedHeader(t *testing.T) {
	mp := mpool.MustNewNoLock("object-reader-corrupt-commit-ts")
	defer mpool.DeleteMPool(mp)

	vec, err := decodeCommitTSVector(context.Background(), []byte{1, 2, 3}, mp)
	require.ErrorContains(t, err, "decode object column")
	require.Nil(t, vec)
	require.Zero(t, mp.CurrNB())
}

func TestObjectReaderCloseDeletesMPoolOnceAfterBlockRelease(t *testing.T) {
	const tag = "object-reader-close-test"
	mp := mpool.MustNewNoLock(tag)
	reader := &ObjectReader{mp: mp}

	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, make([]byte, 4096), false, mp))
	require.NotEqual(t, "[]", mpool.ReportMemUsage(tag))
	vec.Free(mp)
	require.Zero(t, mp.CurrNB())

	require.NoError(t, reader.Close())
	require.Equal(t, "[]", mpool.ReportMemUsage(tag))
	require.NoError(t, reader.Close())
	require.Equal(t, "[]", mpool.ReportMemUsage(tag))
}

func TestOpenWithKindRejectsInvalidOfflineKind(t *testing.T) {
	reader, err := OpenWithKind(context.Background(), "/tmp/object", "definitely-not-a-kind")
	require.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "create file service")
}

func TestObjectReaderReadsLocalObject(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filename := "reader.obj"
	writeObjectReaderTestFile(t, dir, filename)

	reader, err := Open(ctx, filepath.Join(dir, filename))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
	})

	info := reader.Info()
	require.Equal(t, filepath.Join(dir, filename), info.Path)
	require.Equal(t, uint32(1), info.BlockCount)
	require.Equal(t, uint64(3), info.RowCount)
	require.Equal(t, uint16(2), info.ColCount)

	cols := reader.Columns()
	require.Len(t, cols, 2)
	require.Equal(t, uint16(0), cols[0].Idx)
	require.Equal(t, types.T_int32, cols[0].Type.Oid)
	require.Equal(t, uint16(1), cols[1].Idx)
	require.Equal(t, types.T_varchar, cols[1].Type.Oid)

	require.NotNil(t, reader.Meta())
	require.Equal(t, uint32(1), reader.BlockCount())

	bat, release, err := reader.ReadBlock(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Equal(t, 3, bat.RowCount())
	require.Equal(t, int32(10), vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])[0])
	release()

	commitTS, releaseCommitTS, err := reader.ReadBlockCommitTS(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, commitTS)
	require.Equal(t, types.T_TS, commitTS.GetType().Oid)
	releaseCommitTS()
	require.Zero(t, reader.mp.CurrNB())
	require.NoError(t, reader.Close())
	require.NoError(t, reader.Close())
}

func writeObjectReaderTestFile(t *testing.T, dir string, filename string) {
	t.Helper()
	ctx := context.Background()
	mp := mpool.MustNewZero()
	service, err := fileservice.NewFileService(ctx, fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
		Cache:   fileservice.DisabledCacheConfig,
	}, nil)
	require.NoError(t, err)
	defer service.Close(ctx)

	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterNormal, filename, service)
	require.NoError(t, err)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for _, row := range []struct {
		id   int32
		name string
	}{
		{10, "ten"},
		{20, "twenty"},
		{30, "thirty"},
	} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row.id, false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row.name), false, mp))
	}
	bat.SetRowCount(3)
	defer bat.Clean(mp)

	_, err = writer.Write(bat)
	require.NoError(t, err)
	_, err = writer.WriteEnd(ctx, objectio.WriteOptions{
		Type: objectio.WriteTS,
		Val:  time.Unix(100, 0),
	})
	require.NoError(t, err)
}
