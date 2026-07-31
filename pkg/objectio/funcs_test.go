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
	"github.com/matrixorigin/matrixone/pkg/pb/gossip"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
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

type objectioRemoteCacheClient struct {
	response *query.Response
	sends    atomic.Int32
	releases atomic.Int32
}

func (*objectioRemoteCacheClient) ServiceID() string { return "objectio-remote-cache-test" }

func (c *objectioRemoteCacheClient) SendMessage(
	context.Context,
	string,
	*query.Request,
) (*query.Response, error) {
	c.sends.Add(1)
	return c.response, nil
}

func (*objectioRemoteCacheClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *objectioRemoteCacheClient) Release(resp *query.Response) {
	c.releases.Add(1)
	for _, data := range resp.GetCacheDataResponse.ResponseCacheData {
		clear(data.Data)
	}
}

func (*objectioRemoteCacheClient) Close() error { return nil }

type objectioRemoteCacheRouter struct{}

func (objectioRemoteCacheRouter) Target(fscache.CacheKey) string { return "remote" }
func (objectioRemoteCacheRouter) AddItem(gossip.CommonItem)      {}

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
	data := &validatedVectorCacheData{data: fileservice.NewBytes([]byte{1, 2, 3})}
	defer data.Release()
	require.True(t, isValidatedVectorCacheData(data))

	same := data.Slice(3)
	require.True(t, isValidatedVectorCacheData(same))

	shorter := same.Slice(2)
	require.False(t, isValidatedVectorCacheData(shorter))
	require.Equal(t, []byte{1, 2, 3}, data.Bytes())
	shorter.Release()
}

func TestValidatedVectorCacheDataDoesNotExposeMutableBackingBytes(t *testing.T) {
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
	cacheData, err := validateVectorCacheData(fileservice.NewBytes(encoded))
	require.NoError(t, err)
	defer cacheData.Release()

	// Bytes is part of the public FileService cache interface. Mutating the
	// returned slice must not mutate bytes covered by the validation capability.
	exposed := cacheData.Bytes()
	varlenOffset := IOEntryHeaderSize + 1 + types.TSize + 4 + 4
	invalidOffset := uint32(len(encoded) + 1)
	copy(exposed[varlenOffset+4:varlenOffset+8], types.EncodeUint32(&invalidOffset))

	var obj any
	require.NotPanics(t, func() {
		obj, err = DecodeCached(cacheData)
		if err == nil {
			require.Equal(t, "value longer than inline storage", obj.(*vector.Vector).GetStringAt(0))
		}
	})
	require.NoError(t, err)
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

func TestRemoteColumnCacheHitIsValidatedBeforeMemoryAdmission(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source, []byte("remote cached value"), false, mp))
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(mp)

	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type:    IOET_ColData,
		Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)
	queryClient := &objectioRemoteCacheClient{
		response: &query.Response{
			GetCacheDataResponse: &query.GetCacheDataResponse{
				ResponseCacheData: []*query.ResponseCacheData{{
					Index: 0,
					Hit:   true,
					Data:  append([]byte(nil), encoded...),
				}},
			},
		},
	}
	capacity := toml.ByteSize(1 << 20)
	ctx := context.Background()
	fs, err := fileservice.NewS3FS(
		ctx,
		fileservice.ObjectStorageArguments{
			Name:     "s3",
			Endpoint: "disk",
			Bucket:   t.TempDir(),
		},
		fileservice.CacheConfig{
			MemoryCapacity:     &capacity,
			RemoteCacheEnabled: true,
			QueryClient:        queryClient,
			KeyRouterFactory: func() client.KeyRouter[query.CacheKey] {
				return objectioRemoteCacheRouter{}
			},
		},
		nil,
		false,
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	// The remote key uses the compressed extent length, while CachedData has
	// the decompressed OriginSize. This is the normal object-column contract.
	ext := NewExtent(compress.Lz4, 0, 1, uint32(len(encoded)))
	var validations atomic.Int32
	newRead := func() *fileservice.IOVector {
		entry := newColumnIOEntry(ext, columnCacheConstructorFactory)
		validate := entry.ValidateCacheData
		entry.ValidateCacheData = func(data fscache.Data) (fscache.Data, error) {
			validations.Add(1)
			return validate(data)
		}
		return &fileservice.IOVector{
			FilePath: "remote-object",
			Entries:  []fileservice.IOEntry{entry},
		}
	}

	first := newRead()
	require.NoError(t, fs.Read(ctx, first))
	require.True(t, first.Entries[0].WasFromCache())
	require.True(t, isValidatedVectorCacheData(first.Entries[0].CachedData))
	obj, err := DecodeCached(first.Entries[0].CachedData)
	require.NoError(t, err)
	require.Equal(t, "remote cached value", obj.(*vector.Vector).GetStringAt(0))
	first.Release()

	second := newRead()
	require.NoError(t, fs.Read(ctx, second))
	require.True(t, second.Entries[0].WasFromCache())
	require.True(t, isValidatedVectorCacheData(second.Entries[0].CachedData))
	obj, err = DecodeCached(second.Entries[0].CachedData)
	require.NoError(t, err)
	require.Equal(t, "remote cached value", obj.(*vector.Vector).GetStringAt(0))
	second.Release()

	require.Equal(t, int32(1), validations.Load(), "memory-cache hit must not revalidate")
	require.Equal(t, int32(1), queryClient.sends.Load(), "second read must not return to remote cache")
	require.Equal(t, int32(1), queryClient.releases.Load())
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
