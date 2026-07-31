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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type releaseTrackingData struct {
	releases *atomic.Int32
	bytes    []byte
}

func (r *releaseTrackingData) Size() int64 {
	return int64(len(r.bytes))
}

func (r *releaseTrackingData) Bytes() []byte {
	return r.bytes
}

func (r *releaseTrackingData) Slice(int) fscache.Data {
	return r
}

func (r *releaseTrackingData) Retain() {
}

func (r *releaseTrackingData) Release() {
	r.releases.Add(1)
}

type partialReadErrorFS struct {
	fileservice.FileService
	data fscache.Data
	err  error
}

func (p *partialReadErrorFS) Name() string {
	return "partial-read-error"
}

func (p *partialReadErrorFS) Read(_ context.Context, vector *fileservice.IOVector) error {
	if len(vector.Entries) > 0 {
		vector.Entries[0].CachedData = p.data
	}
	return p.err
}

func (p *partialReadErrorFS) ReadCache(context.Context, *fileservice.IOVector) error {
	return nil
}

type trackingCacheDataAllocator struct {
	data fscache.Data
}

func (t *trackingCacheDataAllocator) AllocateCacheData(context.Context, int) fscache.Data {
	return t.data
}

func (t *trackingCacheDataAllocator) AllocateCacheDataWithHint(context.Context, int, malloc.Hints) fscache.Data {
	return t.data
}

func (t *trackingCacheDataAllocator) CopyToCacheData(context.Context, []byte) fscache.Data {
	return t.data
}

func TestReadOneBlockWithMetaReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := moerr.NewInternalErrorNoCtx("read canceled after partial cache fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}

	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int8))
	col.setLocation(NewExtent(1, 0, 1, 1))

	ioVec, err := ReadOneBlockWithMeta(
		context.Background(),
		&meta,
		"test-object",
		0,
		[]uint16{0},
		[]types.Type{types.T_int8.ToType()},
		mpool.MustNewZero(),
		fs,
		constructorFactory,
		fileservice.Policy(0),
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
	require.Empty(t, ioVec.Entries)
}

func TestReadOneBlockDoesNotConfuseUserTSWithCommitTS(t *testing.T) {
	readErr := moerr.NewInternalErrorNoCtx("must not read the user timestamp")
	fs := &partialReadErrorFS{err: readErr}

	meta := BuildMetaData(1, 1)
	block := meta.GetBlockMeta(0)
	block.BlockHeader().SetRows(1)
	block.BlockHeader().SetMaxSeqnum(0)
	block.BlockHeader().SetMetaColumnCount(1)
	block.ColumnMeta(0).setDataType(uint8(types.T_TS))
	block.ColumnMeta(0).setLocation(NewExtent(1, 0, 1, 1))

	ioVec, err := ReadOneBlockWithMeta(
		context.Background(),
		&meta,
		"test-object",
		0,
		[]uint16{SEQNUM_COMMITTS},
		[]types.Type{types.T_TS.ToType()},
		mpool.MustNewZero(),
		fs,
		constructorFactory,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	require.Len(t, ioVec.Entries, 1)
	require.NotNil(t, ioVec.Entries[0].CachedData)
	ioVec.ReleaseReadResultOnError()
}

func TestReadOneBlockSynthesizesCommitTSForEmptyMetadata(t *testing.T) {
	readErr := moerr.NewInternalErrorNoCtx("must not issue a storage read")
	fs := &partialReadErrorFS{err: readErr}

	meta := BuildMetaData(1, 1)
	block := meta.GetBlockMeta(0)
	block.BlockHeader().SetRows(1)
	block.BlockHeader().SetMetaColumnCount(0)

	ioVec, err := ReadOneBlockWithMeta(
		context.Background(),
		&meta,
		"empty-metadata",
		0,
		[]uint16{SEQNUM_COMMITTS},
		[]types.Type{types.T_TS.ToType()},
		mpool.MustNewZero(),
		fs,
		constructorFactory,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	require.Len(t, ioVec.Entries, 1)
	require.NotNil(t, ioVec.Entries[0].CachedData)
	ioVec.ReleaseReadResultOnError()
}

func TestReadAllBlocksWithMetaReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := moerr.NewInternalErrorNoCtx("read canceled after partial all-blocks fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}

	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int8))
	col.setLocation(NewExtent(1, 0, 1, 1))

	ioVec, err := ReadAllBlocksWithMeta(
		context.Background(),
		&meta,
		"test-object",
		[]uint16{0},
		fileservice.Policy(0),
		mpool.MustNewZero(),
		fs,
		constructorFactory,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
	require.Empty(t, ioVec.Entries)
}

func TestReadExtentReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := moerr.NewInternalErrorNoCtx("read canceled after partial extent fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}
	extent := NewExtent(1, 0, 1, 1)

	_, err := ReadExtent(
		context.Background(),
		"test-object",
		&extent,
		fileservice.Policy(0),
		fs,
		constructorFactory,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
}

func TestConstructorFactoryReleasesDecompressionDataOnError(t *testing.T) {
	var releases atomic.Int32
	allocator := &trackingCacheDataAllocator{
		data: &releaseTrackingData{
			releases: &releases,
			bytes:    make([]byte, 16),
		},
	}

	cacheData, err := constructorFactory(16, compress.Lz4)(
		context.Background(),
		bytes.NewReader([]byte("not-lz4")),
		[]byte("not-lz4"),
		allocator,
	)
	require.Error(t, err)
	require.Nil(t, cacheData)
	require.Equal(t, int32(1), releases.Load())
}

func TestColumnCacheConstructorValidatesAndMarksV2(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type:    IOET_ColData,
		Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)

	cacheData, err := columnCacheConstructorFactory(int64(len(encoded)), compress.None)(
		context.Background(),
		bytes.NewReader(encoded),
		encoded,
		fileservice.DefaultCacheDataAllocator(),
	)
	require.NoError(t, err)
	require.True(t, isValidatedVectorCacheData(cacheData))
	defer cacheData.Release()

	obj, err := DecodeCached(cacheData)
	require.NoError(t, err)
	require.Equal(t, "value longer than inline storage", obj.(*vector.Vector).GetStringAt(0))

	target := vector.NewVecFromReuse()
	require.NoError(t, MustVectorToCached(target, cacheData))
	require.Equal(t, "value longer than inline storage", target.GetStringAt(0))
}

func TestColumnCacheConstructorRejectsInvalidV2BeforeAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source, []byte("value longer than inline storage"), false, mp))
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type:    IOET_ColData,
		Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)
	varlenOffset := IOEntryHeaderSize + 1 + types.TSize + 4 + 4
	invalidOffset := uint32(len(encoded) + 1)
	copy(encoded[varlenOffset+4:varlenOffset+8], types.EncodeUint32(&invalidOffset))

	var releases atomic.Int32
	allocator := &trackingCacheDataAllocator{
		data: &releaseTrackingData{
			releases: &releases,
			bytes:    encoded,
		},
	}
	cacheData, err := columnCacheConstructorFactory(int64(len(encoded)), compress.None)(
		context.Background(),
		bytes.NewReader(encoded),
		encoded,
		allocator,
	)
	require.Error(t, err)
	require.Nil(t, cacheData)
	require.Equal(t, int32(1), releases.Load())

	unmarked := fileservice.NewBytes(encoded)
	defer unmarked.Release()
	_, err = DecodeCached(unmarked)
	require.Error(t, err)
}

func TestColumnCacheConstructorRejectsNilCacheData(t *testing.T) {
	allocator := &trackingCacheDataAllocator{}
	cacheData, err := columnCacheConstructorFactory(1, compress.None)(
		context.Background(),
		bytes.NewReader([]byte{1}),
		[]byte{1},
		allocator,
	)
	require.Error(t, err)
	require.Nil(t, cacheData)
}

func TestValidatedVectorCacheDataSliceDropsCapability(t *testing.T) {
	data := &validatedVectorCacheData{Data: fileservice.NewBytes([]byte{1, 2, 3})}
	require.True(t, isValidatedVectorCacheData(data))

	same := data.Slice(3)
	require.True(t, isValidatedVectorCacheData(same))

	shorter := same.Slice(2)
	require.False(t, isValidatedVectorCacheData(shorter))
	shorter.Release()
}

func TestValidatedVectorCapabilitySurvivesMemoryCache(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source, []byte("cached value"), false, mp))
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type:    IOET_ColData,
		Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)
	cacheData, err := validateVectorCacheData(fileservice.NewBytes(encoded))
	require.NoError(t, err)

	ctx := context.Background()
	cache := fileservice.NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	write := fileservice.IOVector{
		FilePath: "object",
		Entries: []fileservice.IOEntry{{
			Offset:     0,
			Size:       int64(len(encoded)),
			CachedData: cacheData,
		}},
	}
	require.NoError(t, cache.Update(ctx, &write, false))
	write.Release()

	read := fileservice.IOVector{
		FilePath: "object",
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(encoded)),
		}},
	}
	require.NoError(t, cache.Read(ctx, &read))
	require.True(t, isValidatedVectorCacheData(read.Entries[0].CachedData))
	obj, err := DecodeCached(read.Entries[0].CachedData)
	require.NoError(t, err)
	require.Equal(t, "cached value", obj.(*vector.Vector).GetStringAt(0))
	read.Release()
}

func TestReadOneBlockAllColumnsReleasesPartialReadOnError(t *testing.T) {
	var releases atomic.Int32
	readErr := moerr.NewInternalErrorNoCtx("read canceled after partial all-columns fill")
	fs := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &releases},
		err:  readErr,
	}

	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int8))
	col.setLocation(NewExtent(1, 0, 1, 1))

	_, err := ReadOneBlockAllColumns(
		context.Background(),
		&meta,
		"test-object",
		0,
		[]uint16{0},
		fileservice.Policy(0),
		fs,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), releases.Load())
}
