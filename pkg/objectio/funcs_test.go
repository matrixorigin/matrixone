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

func (r *releaseTrackingData) Capacity() int64 {
	return int64(cap(r.bytes))
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

type validatedVectorBytesProbe struct {
	backing    []byte
	bytesCalls atomic.Int32
}

func (d *validatedVectorBytesProbe) Bytes() []byte {
	d.bytesCalls.Add(1)
	return bytes.Clone(d.backing)
}

func (d *validatedVectorBytesProbe) Size() int64            { return int64(len(d.backing)) }
func (d *validatedVectorBytesProbe) Capacity() int64        { return int64(cap(d.backing)) }
func (d *validatedVectorBytesProbe) Slice(int) fscache.Data { return d }
func (d *validatedVectorBytesProbe) Retain()                {}
func (d *validatedVectorBytesProbe) Release()               {}
func (d *validatedVectorBytesProbe) validatedVectorSnapshot() []byte {
	return bytes.Clone(d.backing)
}
func (d *validatedVectorBytesProbe) validatedVectorBackingForScope() []byte {
	return d.backing
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

func (t *trackingCacheDataAllocator) BackingSize(size int) int {
	return size
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

func TestResolveSpecialColumnLayoutCompatibility(t *testing.T) {
	t.Run("commit-only", func(t *testing.T) {
		block := NewBlock(NewSeqnums([]uint16{0, SEQNUM_COMMITTS}))
		block.ColumnMeta(0).setDataType(uint8(types.T_int64))
		block.ColumnMeta(1).setDataType(uint8(types.T_TS))

		layout := ResolveSpecialColumnLayout(block)
		require.Equal(t, uint16(1), layout.CommitTS)
		require.Equal(t, uint16(InvalidSpecialColumnPosition), layout.Abort)
	})

	t.Run("commit-and-abort-with-sparse-schema", func(t *testing.T) {
		block := NewBlock(NewSeqnums([]uint16{3, SEQNUM_COMMITTS, SEQNUM_ABORT}))
		block.ColumnMeta(3).setDataType(uint8(types.T_int64))
		block.ColumnMeta(4).setDataType(uint8(types.T_TS))
		block.ColumnMeta(5).setDataType(uint8(types.T_bool))

		layout := ResolveSpecialColumnLayout(block)
		require.Equal(t, uint16(4), layout.CommitTS)
		require.Equal(t, uint16(5), layout.Abort)
	})

	t.Run("physical-address-before-mvcc", func(t *testing.T) {
		block := NewBlock(NewSeqnums([]uint16{
			3,
			SEQNUM_ROWID,
			SEQNUM_COMMITTS,
			SEQNUM_ABORT,
		}))
		block.ColumnMeta(3).setDataType(uint8(types.T_int64))
		block.ColumnMeta(4).setDataType(uint8(types.T_Rowid))
		block.ColumnMeta(5).setDataType(uint8(types.T_TS))
		block.ColumnMeta(6).setDataType(uint8(types.T_bool))

		layout := ResolveSpecialColumnLayout(block)
		require.Equal(t, uint16(4), layout.PhysicalAddr)
		require.Equal(t, uint16(5), layout.CommitTS)
		require.Equal(t, uint16(6), layout.Abort)
	})

	t.Run("physical-address-between-commit-and-abort", func(t *testing.T) {
		block := NewBlock(NewSeqnums([]uint16{
			1,
			SEQNUM_COMMITTS,
			SEQNUM_ROWID,
			SEQNUM_ABORT,
		}))
		block.ColumnMeta(1).setDataType(uint8(types.T_int64))
		block.ColumnMeta(2).setDataType(uint8(types.T_TS))
		block.ColumnMeta(3).setDataType(uint8(types.T_Rowid))
		block.ColumnMeta(4).setDataType(uint8(types.T_bool))

		layout := ResolveSpecialColumnLayout(block)
		require.Equal(t, uint16(3), layout.PhysicalAddr)
		require.Equal(t, uint16(2), layout.CommitTS)
		require.Equal(t, uint16(4), layout.Abort)
	})

	t.Run("user-columns-are-not-special", func(t *testing.T) {
		block := NewBlock(NewSeqnums([]uint16{0, 1}))
		block.ColumnMeta(0).setDataType(uint8(types.T_TS))
		block.ColumnMeta(1).setDataType(uint8(types.T_bool))

		layout := ResolveSpecialColumnLayout(block)
		require.Equal(t, uint16(InvalidSpecialColumnPosition), layout.CommitTS)
		require.Equal(t, uint16(InvalidSpecialColumnPosition), layout.Abort)
	})
}

func TestTombstoneAbortSelectionIncludesCommitTS(t *testing.T) {
	hidden := HiddenColumnSelection_Abort
	require.Equal(
		t,
		[]uint16{0, 1, SEQNUM_COMMITTS, SEQNUM_ABORT},
		GetTombstoneSeqnums(hidden),
	)
	require.Equal(
		t,
		[]string{
			TombstoneAttr_Rowid_Attr,
			TombstoneAttr_PK_Attr,
			TombstoneAttr_CommitTs_Attr,
			TombstoneAttr_Abort_Attr,
		},
		GetTombstoneAttrs(hidden),
	)
}

func TestReadOneBlockAbortColumnCompatibility(t *testing.T) {
	t.Run("new-object-reads-abort", func(t *testing.T) {
		readErr := moerr.NewInternalErrorNoCtx("abort column storage read")
		fs := &partialReadErrorFS{err: readErr}
		meta := BuildMetaData(1, 3)
		block := meta.GetBlockMeta(0)
		block.BlockHeader().SetRows(1)
		block.BlockHeader().SetMaxSeqnum(0)
		block.BlockHeader().SetMetaColumnCount(3)
		block.ColumnMeta(0).setDataType(uint8(types.T_int64))
		block.ColumnMeta(1).setDataType(uint8(types.T_TS))
		block.ColumnMeta(2).setDataType(uint8(types.T_bool))
		block.ColumnMeta(2).setLocation(NewExtent(1, 0, 1, 1))

		_, err := ReadOneBlockWithMeta(
			context.Background(), &meta, "new-object", 0,
			[]uint16{SEQNUM_ABORT}, []types.Type{types.T_bool.ToType()},
			mpool.MustNewZero(), fs, constructorFactory, fileservice.Policy(0),
		)
		require.ErrorIs(t, err, readErr)
	})

	t.Run("old-object-synthesizes-missing-abort", func(t *testing.T) {
		readErr := moerr.NewInternalErrorNoCtx("must not read storage")
		fs := &partialReadErrorFS{err: readErr}
		meta := BuildMetaData(1, 2)
		block := meta.GetBlockMeta(0)
		block.BlockHeader().SetRows(1)
		block.BlockHeader().SetMaxSeqnum(0)
		block.BlockHeader().SetMetaColumnCount(2)
		block.ColumnMeta(0).setDataType(uint8(types.T_int64))
		block.ColumnMeta(1).setDataType(uint8(types.T_TS))

		ioVec, err := ReadOneBlockWithMeta(
			context.Background(), &meta, "old-object", 0,
			[]uint16{SEQNUM_ABORT}, []types.Type{types.T_bool.ToType()},
			mpool.MustNewZero(), fs, constructorFactory, fileservice.Policy(0),
		)
		require.NoError(t, err)
		require.Len(t, ioVec.Entries, 1)
		require.NotNil(t, ioVec.Entries[0].CachedData)
		ioVec.ReleaseReadResultOnError()
	})
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

	ownedTarget := vector.NewVecFromReuse()
	require.NoError(t, MustVectorToCachedWithMpool(ownedTarget, cacheData, mp))
	require.False(t, ownedTarget.NeedDup())
	require.Equal(t, "value longer than inline storage", ownedTarget.GetStringAt(0))
	invalidOffset := uint32(len(encoded) + 1)
	copy(ownedTarget.GetData()[4:8], types.EncodeUint32(&invalidOffset))
	ownedTarget.GetArea()[0] = 'X'
	ownedTarget.Free(mp)
	require.Equal(t, int64(0), mp.CurrNB())

	obj, err = DecodeCached(cacheData)
	require.NoError(t, err)
	require.Equal(t, "value longer than inline storage", obj.(*vector.Vector).GetStringAt(0))
}

func TestCopyCachedVectorRowsMaterializesOnlySelectedRows(t *testing.T) {
	const (
		rowCount = 8192
		valueLen = 119
	)

	writerMP := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < rowCount; i++ {
		value := bytes.Repeat([]byte{byte('a' + i%26)}, valueLen)
		require.NoError(t, vector.AppendBytes(source, value, false, writerMP))
	}
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(writerMP)
	require.Zero(t, writerMP.CurrNB())
	mpool.DeleteMPool(writerMP)

	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type:    IOET_ColData,
		Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)
	cacheData, err := validateVectorCacheData(fileservice.NewBytes(encoded))
	require.NoError(t, err)
	require.True(t, isValidatedVectorCacheData(cacheData))
	defer cacheData.Release()

	queryMP, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(queryMP)

	sels := []int64{0, 7, 511, 1024, 2047, 4095, 6000, 7001, 8000, 8191}
	selected := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, CopyCachedVectorRows(selected, cacheData, sels, queryMP))
	require.Equal(t, len(sels), selected.Length())
	for i, sel := range sels {
		require.Equal(
			t,
			bytes.Repeat([]byte{byte('a' + int(sel)%26)}, valueLen),
			selected.GetBytesAt(i),
		)
	}

	// The result owns its payload. Mutating it cannot invalidate the cache's
	// validation marker or affect a later selected-row materialization.
	selected.GetBytesAt(0)[0] = 'X'
	again := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, CopyCachedVectorRows(again, cacheData, sels[:1], queryMP))
	require.Equal(t, bytes.Repeat([]byte{'a'}, valueLen), again.GetBytesAt(0))
	probe := &validatedVectorBytesProbe{
		backing: cacheData.(validatedVectorCacheDataMarker).validatedVectorBackingForScope(),
	}
	probeResult := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, CopyCachedVectorRows(probeResult, probe, sels[:1], queryMP))
	require.Zero(t, probe.bytesCalls.Load(), "sealed backing must not be cloned before scoped materialization")

	invalidRows := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NotPanics(t, func() {
		err = CopyCachedVectorRows(invalidRows, cacheData, []int64{-1, rowCount}, queryMP)
	})
	require.Error(t, err)
	require.Zero(t, invalidRows.Length())
	wrongType := vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.Error(t, CopyCachedVectorRows(wrongType, cacheData, sels[:1], queryMP))
	widerType := vector.NewOffHeapVecWithType(types.New(types.T_varchar, 512, 0))
	require.NoError(t, CopyCachedVectorRows(widerType, cacheData, sels[:1], queryMP))
	require.Equal(t, bytes.Repeat([]byte{'a'}, valueLen), widerType.GetBytesAt(0))
	window, err := MaterializeCachedVectorWindow(cacheData, 7000, 3, queryMP)
	require.NoError(t, err)
	require.Equal(t, 3, window.Length())
	require.Equal(t, bytes.Repeat([]byte{byte('a' + 7000%26)}, valueLen), window.GetBytesAt(0))
	window.Free(queryMP)
	_, err = MaterializeCachedVectorWindow(cacheData, rowCount-1, 2, queryMP)
	require.Error(t, err)
	_, err = MaterializeCachedVectorWindow(cacheData, 0, 1, nil)
	require.Error(t, err)

	needle := bytes.Repeat([]byte{'h'}, valueLen)
	search := NewReadFilterSearch(types.T_varchar, [][]byte{needle})
	needle[0] = 'X'
	probeSearch := &validatedVectorBytesProbe{
		backing: cacheData.(validatedVectorCacheDataMarker).validatedVectorBackingForScope(),
	}
	searched, err := SearchCachedVector(
		fileservice.IOEntry{CachedData: probeSearch},
		search,
		false,
	)
	require.NoError(t, err)
	require.Zero(t, probeSearch.bytesCalls.Load(), "scoped search must not clone the sealed block")
	var expectedSearch []int64
	for row := 7; row < rowCount; row += 26 {
		expectedSearch = append(expectedSearch, int64(row))
	}
	require.Equal(t, expectedSearch, searched)

	require.NotPanics(t, func() {
		_, err = SearchCachedVector(
			fileservice.IOEntry{},
			search,
			false,
		)
	})
	require.Error(t, err)

	wrongOIDSearch := NewReadFilterSearch(types.T_char, [][]byte{[]byte("not present")})
	allRows, err := SearchCachedVector(
		fileservice.IOEntry{CachedData: cacheData},
		wrongOIDSearch,
		false,
	)
	require.NoError(t, err)
	require.Len(t, allRows, rowCount, "physical OID mismatch must fail open")

	// Copying the complete 8192-row payload cannot fit in this pool. The same
	// bounded pool succeeds above because only ten selected values are copied.
	whole := vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.Error(t, CopyCachedVectorAll(whole, cacheData, queryMP))

	selected.Free(queryMP)
	again.Free(queryMP)
	probeResult.Free(queryMP)
	invalidRows.Free(queryMP)
	wrongType.Free(queryMP)
	widerType.Free(queryMP)
	whole.Free(queryMP)
	require.Zero(t, queryMP.CurrNB())
}

func TestReadFilterSearchMatchesLegacyVarlenPredicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	makeVector := func(values []string) *vector.Vector {
		vec := vector.NewVec(types.T_varchar.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
		}
		return vec
	}
	sortedVec := makeVector([]string{"a", "aa", "ab", "aba", "b", "ba", "c"})
	unsortedVec := makeVector([]string{"ba", "a", "c", "aba", "b", "aa", "ab"})
	defer sortedVec.Free(mp)
	defer unsortedVec.Free(mp)

	exactValues := [][]byte{[]byte("aa"), []byte("ba")}
	prefixValues := [][]byte{[]byte("a"), []byte("ba")}
	prefixNeedles := makeVector([]string{"a", "ba"})
	defer prefixNeedles.Free(mp)

	type searchCase struct {
		name     string
		search   *ReadFilterSearch
		expected func(*vector.Vector, bool) []int64
	}
	cases := []searchCase{
		{
			name:   "exact",
			search: NewReadFilterSearch(types.T_varchar, exactValues),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				if sorted {
					return vector.VarlenBinarySearchOffsetByValFactory(exactValues)(vec)
				}
				return vector.VarlenLinearSearchOffsetByValFactory(exactValues)(vec)
			},
		},
		{
			name:   "prefix-equal",
			search: NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("ab")}),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				if sorted {
					return vector.CollectOffsetsByPrefixEqFactory([]byte("ab"))(vec)
				}
				return vector.LinearCollectOffsetsByPrefixEqFactory([]byte("ab"))(vec)
			},
		},
		{
			name:   "prefix-in",
			search: NewReadFilterPrefixSearch(types.T_varchar, prefixValues),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				if sorted {
					return vector.CollectOffsetsByPrefixInFactory(prefixNeedles)(vec)
				}
				return vector.LinearCollectOffsetsByPrefixInFactory(prefixNeedles)(vec)
			},
		},
		{
			name:   "less-open",
			search: NewReadFilterLessSearch(types.T_varchar, []byte("b"), false),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				return vector.VarlenSearchOffsetByLess([]byte("b"), false, sorted)(vec)
			},
		},
		{
			name:   "less-closed",
			search: NewReadFilterLessSearch(types.T_varchar, []byte("b"), true),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				return vector.VarlenSearchOffsetByLess([]byte("b"), true, sorted)(vec)
			},
		},
		{
			name:   "greater-open",
			search: NewReadFilterGreaterSearch(types.T_varchar, []byte("ab"), false),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				return vector.VarlenSearchOffsetByGreat([]byte("ab"), false, sorted)(vec)
			},
		},
		{
			name:   "greater-closed",
			search: NewReadFilterGreaterSearch(types.T_varchar, []byte("ab"), true),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				return vector.VarlenSearchOffsetByGreat([]byte("ab"), true, sorted)(vec)
			},
		},
	}
	for hint := uint8(0); hint <= 3; hint++ {
		cases = append(cases, searchCase{
			name:   fmt.Sprintf("between-hint-%d", hint),
			search: NewReadFilterBetweenSearch(types.T_varchar, []byte("aa"), []byte("ba"), hint),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				if sorted {
					return vector.CollectOffsetsByBetweenString("aa", "ba", hint)(vec)
				}
				return vector.LinearCollectOffsetsByBetweenString("aa", "ba", hint)(vec)
			},
		})
		cases = append(cases, searchCase{
			name:   fmt.Sprintf("prefix-between-hint-%d", hint),
			search: NewReadFilterPrefixBetweenSearch(types.T_varchar, []byte("a"), []byte("b"), hint),
			expected: func(vec *vector.Vector, sorted bool) []int64 {
				if hint == 0 {
					if sorted {
						return vector.CollectOffsetsByPrefixBetweenFactory([]byte("a"), []byte("b"))(vec)
					}
					return vector.LinearCollectOffsetsByPrefixBetweenFactory([]byte("a"), []byte("b"))(vec)
				}
				if sorted {
					return vector.CollectOffsetsByPrefixInRangeFactory([]byte("a"), []byte("b"), hint)(vec)
				}
				return vector.LinearCollectOffsetsByPrefixInRangeFactory([]byte("a"), []byte("b"), hint)(vec)
			},
		})
	}

	for _, test := range cases {
		for _, sorted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sorted=%t", test.name, sorted), func(t *testing.T) {
				vec := unsortedVec
				if sorted {
					vec = sortedVec
				}
				require.Equal(t, test.expected(vec, sorted), test.search.search(vec, sorted))
			})
		}
	}

	combined := CombineReadFilterSearch(
		NewReadFilterSearch(types.T_varchar, [][]byte{[]byte("aa"), []byte("b")}),
		NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("a")}),
	)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, combined.search(sortedVec, true))

	constVec, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("ab"), 4, mp)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3}, NewReadFilterSearch(
		types.T_varchar, [][]byte{[]byte("ab")},
	).search(constVec, true))
	require.Equal(t, []int64{3, 2, 1, 0}, NewReadFilterGreaterSearch(
		types.T_varchar, []byte("aa"), false,
	).search(constVec, true))
	constVec.Free(mp)

	constNull := vector.NewConstNull(types.T_varchar.ToType(), 4, mp)
	require.Equal(t, []int64{0, 1, 2, 3}, NewReadFilterSearch(
		types.T_varchar, [][]byte{[]byte("ab")},
	).search(constNull, true))
	constNull.Free(mp)
	flatNull := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(flatNull, []byte("ab"), false, mp))
	require.NoError(t, vector.AppendBytes(flatNull, nil, true, mp))
	require.Equal(t, []int64{0, 1}, NewReadFilterSearch(
		types.T_varchar, [][]byte{[]byte("missing")},
	).search(flatNull, false))
	flatNull.Free(mp)

	wrongOID := vector.NewVec(types.T_char.ToType())
	require.NoError(t, vector.AppendBytes(wrongOID, []byte("missing"), false, mp))
	require.Equal(t, []int64{0}, NewReadFilterSearch(
		types.T_varchar, [][]byte{[]byte("ab")},
	).search(wrongOID, false))
	wrongOID.Free(mp)
}

func TestCachedCommitTSHelpersBroadcastConstants(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	encode := func(vec *vector.Vector) fscache.Data {
		payload, err := vec.MarshalBinary()
		require.NoError(t, err)
		encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
			Type:    IOET_ColData,
			Version: IOET_ColumnData_V2,
		})...)
		encoded = append(encoded, payload...)
		data, err := validateVectorCacheData(fileservice.NewBytes(encoded))
		require.NoError(t, err)
		return data
	}

	commit := types.BuildTS(20, 0)
	constTS, err := vector.NewConstFixed(types.T_TS.ToType(), commit, 4, mp)
	require.NoError(t, err)
	constData := encode(constTS)
	constTS.Free(mp)
	defer constData.Release()

	matched, usable, err := AnyCachedTSInRange(
		constData,
		[]int64{3},
		types.BuildTS(15, 0),
		types.BuildTS(20, 0),
	)
	require.NoError(t, err)
	require.True(t, usable)
	require.True(t, matched)

	abortVec := vector.NewVec(types.T_bool.ToType())
	for _, aborted := range []bool{false, false, false, true} {
		require.NoError(t, vector.AppendFixed(abortVec, aborted, false, mp))
	}
	abortData := encode(abortVec)
	abortVec.Free(mp)
	defer abortData.Release()
	matched, usable, err = AnyCachedTSInRangeWithAbort(
		constData,
		abortData,
		[]int64{3},
		types.BuildTS(15, 0),
		types.BuildTS(20, 0),
	)
	require.NoError(t, err)
	require.True(t, usable)
	require.False(t, matched, "aborted rows must not count as persisted PK changes")

	matched, usable, err = AnyCachedTSInRange(
		constData,
		[]int64{3},
		types.BuildTS(20, 0),
		types.BuildTS(30, 0),
	)
	require.NoError(t, err)
	require.True(t, usable)
	require.False(t, matched, "the commit range is left-open")

	visible, err := FilterCachedRowsByCommitTS(
		constData,
		[]int64{0, 1, 2, 3},
		types.BuildTS(19, 0),
	)
	require.NoError(t, err)
	require.Empty(t, visible)
	visible, err = FilterCachedRowsByCommitTS(
		constData,
		[]int64{0, 1, 2, 3},
		commit,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3}, visible)

	matched, usable, err = AnyCachedTSInRange(
		constData,
		[]int64{4},
		types.BuildTS(15, 0),
		types.BuildTS(25, 0),
	)
	require.NoError(t, err)
	require.False(t, usable)
	require.False(t, matched)

	constNull := vector.NewConstNull(types.T_TS.ToType(), 4, mp)
	constNullData := encode(constNull)
	constNull.Free(mp)
	defer constNullData.Release()
	_, err = FilterCachedRowsByCommitTS(
		constNullData,
		[]int64{0},
		commit,
	)
	require.Error(t, err)
	matched, usable, err = AnyCachedTSInRange(
		constNullData,
		[]int64{0},
		types.BuildTS(15, 0),
		types.BuildTS(25, 0),
	)
	require.NoError(t, err)
	require.False(t, usable)
	require.False(t, matched)
}

func TestReadFilterPrefixSearchDoesNotAllocatePerBlockRow(t *testing.T) {
	const rowCount = 8192
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	source := vector.NewVec(types.T_varchar.ToType())
	for row := 0; row < rowCount; row++ {
		require.NoError(t, vector.AppendBytes(
			source,
			[]byte(fmt.Sprintf("key-%05d", row)),
			false,
			mp,
		))
	}
	defer source.Free(mp)
	values := make([][]byte, 10)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("key-%05d", i*811))
	}
	search := NewReadFilterPrefixSearch(types.T_varchar, values)

	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows := search.search(source, true)
			if len(rows) != len(values) {
				b.Fatalf("unexpected prefix hits: %v", rows)
			}
		}
	})
	require.Less(
		t,
		result.AllocedBytesPerOp(),
		int64(4<<10),
		"sparse PREFIX_IN must not allocate a block-sized marks array",
	)
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

func TestValidatedVectorCacheDataDecodedVectorDoesNotMutateCache(t *testing.T) {
	mp := mpool.MustNewZero()
	source := vector.NewVec(types.T_varchar.ToType())
	const value = "value longer than inline storage"
	require.NoError(t, vector.AppendBytes(source, []byte(value), false, mp))
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

	firstObject, err := DecodeCached(cacheData)
	require.NoError(t, err)
	first := firstObject.(*vector.Vector)
	defer first.Free(mp)

	// A decoded Vector intentionally exposes mutable data and area slices. The
	// trusted cache marker must remain valid even if that Vector is modified.
	invalidOffset := uint32(len(encoded) + 1)
	copy(first.GetData()[4:8], types.EncodeUint32(&invalidOffset))
	first.GetArea()[0] = 'X'
	firstVarlena, _ := vector.MustVarlenaRawData(first)
	offset, _ := firstVarlena[0].OffsetLen()
	require.Equal(t, invalidOffset, offset)

	var secondObject any
	require.NotPanics(t, func() {
		secondObject, err = DecodeCached(cacheData)
	})
	require.NoError(t, err)
	second := secondObject.(*vector.Vector)
	defer second.Free(mp)
	require.Equal(t, value, second.GetStringAt(0))
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

func TestReadOneBlockAllColumnsWindowMaterializesRequestedRows(t *testing.T) {
	writerMP := mpool.MustNewZero()
	source := vector.NewVec(types.T_int64.ToType())
	for i := int64(0); i < 8; i++ {
		require.NoError(t, vector.AppendFixed(source, i, false, writerMP))
	}
	payload, err := source.MarshalBinary()
	require.NoError(t, err)
	source.Free(writerMP)
	encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
		Type: IOET_ColData, Version: IOET_ColumnData_V2,
	})...)
	encoded = append(encoded, payload...)
	var releases atomic.Int32
	fs := &partialReadErrorFS{data: &releaseTrackingData{releases: &releases, bytes: encoded}}
	meta := BuildMetaData(1, 1)
	col := meta.GetBlockMeta(0).ColumnMeta(0)
	col.setDataType(uint8(types.T_int64))
	col.setLocation(NewExtent(1, 0, uint32(len(encoded)), uint32(len(encoded))))
	queryMP := mpool.MustNewZero()
	bat, err := ReadOneBlockAllColumnsWindow(
		context.Background(), &meta, "test-object", 0, []uint16{0},
		2, 3, fileservice.Policy(0), fs, queryMP, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))
	bat.Clean(queryMP)
	require.Equal(t, int32(1), releases.Load())
	_, err = ReadOneBlockAllColumnsWindow(
		context.Background(), &meta, "test-object", 0, []uint16{0},
		0, 0, fileservice.Policy(0), fs, queryMP, 0, nil,
	)
	require.Error(t, err)

	var errorReleases atomic.Int32
	readErr := moerr.NewInternalErrorNoCtx("window read failed")
	errorFS := &partialReadErrorFS{
		data: &releaseTrackingData{releases: &errorReleases},
		err:  readErr,
	}
	_, err = ReadOneBlockAllColumnsWindow(
		context.Background(), &meta, "test-object", 0, []uint16{0},
		0, 1, fileservice.Policy(0), errorFS, queryMP, 0, nil,
	)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), errorReleases.Load())
	require.Zero(t, queryMP.CurrNB())
	mpool.DeleteMPool(queryMP)
	mpool.DeleteMPool(writerMP)
}
