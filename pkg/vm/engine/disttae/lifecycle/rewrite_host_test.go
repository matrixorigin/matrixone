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

package lifecycle

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestRewriteHostUsesDoMergeAndWriteAsOnlyProducer(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	fs, err := fileservice.NewMemoryFS(
		"lifecycle-rewrite-producer",
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	first := makeRewriteProducerBatch(t, mp, 0, 1, 2)
	second := makeRewriteProducerBatch(t, mp, 3, 4, 5)
	t.Cleanup(func() {
		first.Clean(mp)
		second.Clean(mp)
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	base := &rewriteProducerMergeHost{
		batches: []*batch.Batch{first, second},
		deleted: []*nulls.Nulls{
			nulls.Build(3, 0),
			nulls.Build(3, 1),
		},
		mp: mp,
		fs: fs,
	}
	t.Cleanup(func() {
		if base.transferTable != nil {
			base.transferTable.Release()
			base.transferTable = nil
		}
		mergesort.DrainTransferSlabPool()
	})
	host, err := NewRewriteHost(
		base,
		func(
			_ context.Context,
			physicalBlock *batch.Batch,
			_ *nulls.Nulls,
		) (*nulls.Nulls, error) {
			if physicalBlock == first {
				return nulls.Build(3, 1), nil
			}
			return nulls.Build(3, 2), nil
		},
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, mergesort.DoMergeAndWrite(ctx, "lifecycle-test", 0, host))
	report, err := host.ScanReport()
	require.NoError(t, err)
	require.Equal(t, uint64(2), report.SnapshotDeletedRows)
	require.Equal(t, uint64(2), report.ExpiredRows)
	require.Equal(t, uint64(2), report.LiveRows)
	require.Equal(t, 2, base.releaseCount)

	require.Len(t, base.commitEntry.CreatedObjs, 1)
	created := objectio.ObjectStats(base.commitEntry.CreatedObjs[0])
	require.Equal(t, uint32(2), created.Rows())
	require.Equal(t, uint32(1), created.BlkCnt())
	require.NotNil(t, base.transferTable)
	require.Equal(t, 2, base.transferTable.Len())
	firstMap := base.transferTable.GetBlockMap(0)
	require.Equal(t, api.NoTransfer, firstMap[0].ObjIdx)
	require.Equal(t, api.NoTransfer, firstMap[1].ObjIdx)
	require.Equal(t, api.TransferDestPos{
		ObjIdx: 0,
		BlkIdx: 0,
		RowIdx: 0,
	}, firstMap[2])
	secondMap := base.transferTable.GetBlockMap(1)
	require.Equal(t, api.TransferDestPos{
		ObjIdx: 0,
		BlkIdx: 0,
		RowIdx: 1,
	}, secondMap[0])
	require.Equal(t, api.NoTransfer, secondMap[1].ObjIdx)
	require.Equal(t, api.NoTransfer, secondMap[2].ObjIdx)
}

func makeRewriteProducerBatch(
	t *testing.T,
	mp *mpool.MPool,
	values ...int64,
) *batch.Batch {
	t.Helper()
	value := batch.NewWithSize(1)
	value.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for _, item := range values {
		require.NoError(t, vector.AppendFixed(value.Vecs[0], item, false, mp))
	}
	value.SetRowCount(len(values))
	return value
}

type rewriteProducerMergeHost struct {
	batches       []*batch.Batch
	deleted       []*nulls.Nulls
	next          int
	releaseCount  int
	mp            *mpool.MPool
	fs            fileservice.FileService
	commitEntry   api.MergeCommitEntry
	transferTable *mergesort.TransferTable
}

func (host *rewriteProducerMergeHost) GetVector(
	typ *types.Type,
) (*vector.Vector, func()) {
	value := vector.NewVec(*typ)
	return value, func() { value.Free(host.mp) }
}
func (host *rewriteProducerMergeHost) GetMPool() *mpool.MPool { return host.mp }
func (*rewriteProducerMergeHost) Name() string                { return "lifecycle-producer-test" }
func (*rewriteProducerMergeHost) HostHintName() string        { return "test" }
func (*rewriteProducerMergeHost) TaskSourceNote() string      { return "test" }
func (host *rewriteProducerMergeHost) GetCommitEntry() *api.MergeCommitEntry {
	return &host.commitEntry
}
func (*rewriteProducerMergeHost) HasBigDelEvent() bool { return false }
func (host *rewriteProducerMergeHost) SetTransferTable(table *mergesort.TransferTable) {
	host.transferTable = table
}
func (host *rewriteProducerMergeHost) PrepareNewWriter() *ioutil.BlockWriter {
	return ioutil.ConstructWriterWithSegmentID(
		objectio.NewSegmentid(),
		0,
		0,
		[]uint16{0},
		0,
		true,
		false,
		host.fs,
		nil,
	)
}
func (*rewriteProducerMergeHost) DoTransfer() bool     { return false }
func (*rewriteProducerMergeHost) GetObjectCnt() int    { return 1 }
func (*rewriteProducerMergeHost) GetBlkCnts() []int    { return []int{2} }
func (*rewriteProducerMergeHost) GetAccBlkCnts() []int { return []int{0} }
func (*rewriteProducerMergeHost) GetSortKeyType() types.Type {
	return types.T_int64.ToType()
}
func (host *rewriteProducerMergeHost) LoadNextBatch(
	_ context.Context,
	_ uint32,
	_ *batch.Batch,
) (*batch.Batch, *nulls.Nulls, func(), error) {
	if host.next == len(host.batches) {
		return nil, nil, nil, mergesort.ErrNoMoreBlocks
	}
	index := host.next
	host.next++
	return host.batches[index], host.deleted[index], func() {
		host.releaseCount++
	}, nil
}
func (*rewriteProducerMergeHost) GetTotalSize() uint64       { return 0 }
func (*rewriteProducerMergeHost) GetTotalRowCnt() uint32     { return 6 }
func (*rewriteProducerMergeHost) GetBlockMaxRows() uint32    { return 3 }
func (*rewriteProducerMergeHost) GetObjectMaxBlocks() uint16 { return 2 }
func (*rewriteProducerMergeHost) GetTargetObjSize() uint32   { return 0 }

func TestRewriteHostPreservesPhysicalBatchAndUnionsDAndE(t *testing.T) {
	mp := mpool.MustNewZero()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for value := int64(0); value < 5; value++ {
		require.NoError(t, vector.AppendFixed(source.Vecs[0], value, false, mp))
	}
	source.SetRowCount(5)
	base := &rewriteFakeMergeHost{
		bat:     source,
		deleted: nulls.Build(5, 1),
		mp:      mp,
	}
	var archived *batch.Batch
	host, err := NewRewriteHost(
		base,
		func(
			_ context.Context,
			got *batch.Batch,
			deleted *nulls.Nulls,
		) (*nulls.Nulls, error) {
			require.Same(t, source, got)
			require.True(t, deleted.Contains(1))
			return nulls.Build(5, 3), nil
		},
		func(_ context.Context, got *batch.Batch, expired *nulls.Nulls) error {
			archived = got
			require.True(t, expired.Contains(3))
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, host.DoTransfer())

	got, skipped, release, err := host.LoadNextBatch(context.Background(), 0, nil)
	require.NoError(t, err)
	require.Same(t, source, got)
	require.Same(t, source, archived)
	require.True(t, skipped.Contains(1))
	require.True(t, skipped.Contains(3))
	require.False(t, skipped.Contains(0))
	release()
	require.Equal(t, 1, base.releaseCount)
	report, err := host.ScanReport()
	require.NoError(t, err)
	require.Equal(t, uint64(1), report.SnapshotDeletedRows)
	require.Equal(t, uint64(1), report.ExpiredRows)
	require.Equal(t, uint64(3), report.LiveRows)
	source.Clean(mp)
}

func TestRewriteHostRejectsShortObjectScan(t *testing.T) {
	value := batch.NewWithSize(0)
	value.SetRowCount(2)
	base := &rewriteFakeMergeHost{
		bat:            value,
		expectedBlocks: 2,
		expectedRows:   4,
	}
	host, err := NewRewriteHost(base, func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nulls.Build(2, 0), nil
	}, nil)
	require.NoError(t, err)
	_, _, release, err := host.LoadNextBatch(context.Background(), 0, nil)
	require.NoError(t, err)
	release()
	_, err = host.ScanReport()
	require.ErrorContains(t, err, "scan is incomplete")
}

func TestRewriteHostRejectsInvalidClassAndMultipleSources(t *testing.T) {
	base := &rewriteFakeMergeHost{objectCount: 2}
	_, err := NewRewriteHost(base, func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nil, nil
	}, nil)
	require.Error(t, err)

	base.objectCount = 1
	base.bat = batch.NewWithSize(0)
	base.bat.SetRowCount(2)
	base.deleted = nulls.Build(2, 0)
	host, err := NewRewriteHost(base, func(
		context.Context,
		*batch.Batch,
		*nulls.Nulls,
	) (*nulls.Nulls, error) {
		return nulls.Build(2, 0), nil
	}, nil)
	require.NoError(t, err)
	_, _, _, err = host.LoadNextBatch(context.Background(), 0, nil)
	require.Error(t, err)
	require.Equal(t, 1, base.releaseCount)
}

type rewriteFakeMergeHost struct {
	bat            *batch.Batch
	deleted        *nulls.Nulls
	mp             *mpool.MPool
	releaseCount   int
	objectCount    int
	expectedBlocks int
	expectedRows   uint32
}

func (host *rewriteFakeMergeHost) GetVector(
	typ *types.Type,
) (*vector.Vector, func()) {
	value := vector.NewVec(*typ)
	return value, func() { value.Free(host.mp) }
}
func (host *rewriteFakeMergeHost) GetMPool() *mpool.MPool { return host.mp }
func (*rewriteFakeMergeHost) Name() string                { return "lifecycle-test" }
func (*rewriteFakeMergeHost) HostHintName() string        { return "CN" }
func (*rewriteFakeMergeHost) TaskSourceNote() string      { return "" }
func (*rewriteFakeMergeHost) GetCommitEntry() *api.MergeCommitEntry {
	return &api.MergeCommitEntry{}
}
func (*rewriteFakeMergeHost) HasBigDelEvent() bool { return false }
func (*rewriteFakeMergeHost) SetTransferTable(*mergesort.TransferTable) {
}
func (*rewriteFakeMergeHost) PrepareNewWriter() *ioutil.BlockWriter { return nil }
func (*rewriteFakeMergeHost) DoTransfer() bool                      { return false }
func (host *rewriteFakeMergeHost) GetObjectCnt() int {
	if host.objectCount == 0 {
		return 1
	}
	return host.objectCount
}
func (host *rewriteFakeMergeHost) GetBlkCnts() []int {
	if host.expectedBlocks == 0 {
		return []int{1}
	}
	return []int{host.expectedBlocks}
}
func (*rewriteFakeMergeHost) GetAccBlkCnts() []int { return []int{0} }
func (*rewriteFakeMergeHost) GetSortKeyType() types.Type {
	return types.T_int64.ToType()
}
func (host *rewriteFakeMergeHost) LoadNextBatch(
	context.Context,
	uint32,
	*batch.Batch,
) (*batch.Batch, *nulls.Nulls, func(), error) {
	return host.bat, host.deleted, func() { host.releaseCount++ }, nil
}
func (*rewriteFakeMergeHost) GetTotalSize() uint64 { return 0 }
func (host *rewriteFakeMergeHost) GetTotalRowCnt() uint32 {
	if host.expectedRows != 0 {
		return host.expectedRows
	}
	if host.bat != nil {
		return uint32(host.bat.RowCount())
	}
	return 1
}
func (*rewriteFakeMergeHost) GetBlockMaxRows() uint32    { return 8192 }
func (*rewriteFakeMergeHost) GetObjectMaxBlocks() uint16 { return 256 }
func (*rewriteFakeMergeHost) GetTargetObjSize() uint32   { return 0 }
