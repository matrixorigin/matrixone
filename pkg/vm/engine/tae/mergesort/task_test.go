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

package mergesort

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestGetTransferSlabReturnsAllocationError(t *testing.T) {
	useCapacityLimitedTransferSlabMPool(t)

	slab, err := getTransferSlab(1)

	require.Error(t, err)
	require.Nil(t, slab)
}

func TestGetTransferSlabReturnsFreshAndPooledSlabs(t *testing.T) {
	DrainTransferSlabPool()
	bucket := &transferSlabBuckets[transferSlabSmall]
	oldSize, oldMaxIdle := bucket.size, bucket.maxIdle
	bucket.size, bucket.maxIdle = 1, 1
	t.Cleanup(func() {
		DrainTransferSlabPool()
		bucket.size, bucket.maxIdle = oldSize, oldMaxIdle
	})

	fresh, err := getTransferSlab(1)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.Equal(t, api.NoTransfer, fresh[0].ObjIdx)

	fresh[0].ObjIdx = 0
	putTransferSlab(fresh)

	pooled, err := getTransferSlab(1)
	require.NoError(t, err)
	require.Len(t, pooled, 1)
	require.True(t, &fresh[0] == &pooled[0])
	require.Equal(t, api.NoTransfer, pooled[0].ObjIdx)
	putTransferSlab(pooled)
}

func TestDoMergeAndWriteReturnsTransferSlabAllocationError(t *testing.T) {
	for _, test := range []struct {
		name       string
		sortKeyPos int
	}{
		{name: "sorted merge", sortKeyPos: 0},
		{name: "reshape", sortKeyPos: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			useCapacityLimitedTransferSlabMPool(t)
			host := &transferSlabFailureHost{}

			err := DoMergeAndWrite(context.Background(), "txn", test.sortKeyPos, host)

			require.Error(t, err)
			require.False(t, host.loadCalled)
			require.False(t, host.prepareWriterCalled)
			require.False(t, host.setTransferTableCalled)
		})
	}
}

func useCapacityLimitedTransferSlabMPool(t *testing.T) {
	t.Helper()

	DrainTransferSlabPool()
	old := transferSlabMPool
	limited, err := mpool.NewMPool(t.Name(), mpool.MB, mpool.NoFixed)
	require.NoError(t, err)
	transferSlabMPool = limited
	t.Cleanup(func() {
		DrainTransferSlabPool()
		transferSlabMPool = old
		mpool.DeleteMPool(limited)
	})
}

type transferSlabFailureHost struct {
	commitEntry            api.MergeCommitEntry
	loadCalled             bool
	prepareWriterCalled    bool
	setTransferTableCalled bool
}

func (h *transferSlabFailureHost) GetVector(*types.Type) (*vector.Vector, func()) {
	panic("unexpected vector allocation")
}

func (h *transferSlabFailureHost) GetMPool() *mpool.MPool {
	panic("unexpected mpool access")
}

func (h *transferSlabFailureHost) Name() string {
	return "transfer-slab-failure"
}

func (h *transferSlabFailureHost) HostHintName() string {
	return "test"
}

func (h *transferSlabFailureHost) TaskSourceNote() string {
	return "test"
}

func (h *transferSlabFailureHost) GetCommitEntry() *api.MergeCommitEntry {
	return &h.commitEntry
}

func (h *transferSlabFailureHost) HasBigDelEvent() bool {
	return false
}

func (h *transferSlabFailureHost) SetTransferTable(*TransferTable) {
	h.setTransferTableCalled = true
}

func (h *transferSlabFailureHost) PrepareNewWriter() *ioutil.BlockWriter {
	h.prepareWriterCalled = true
	return nil
}

func (h *transferSlabFailureHost) DoTransfer() bool {
	return true
}

func (h *transferSlabFailureHost) GetObjectCnt() int {
	return 1
}

func (h *transferSlabFailureHost) GetBlkCnts() []int {
	return []int{1}
}

func (h *transferSlabFailureHost) GetAccBlkCnts() []int {
	return []int{0}
}

func (h *transferSlabFailureHost) GetSortKeyType() types.Type {
	return types.T_int64.ToType()
}

func (h *transferSlabFailureHost) LoadNextBatch(
	context.Context,
	uint32,
	*batch.Batch,
) (*batch.Batch, *nulls.Nulls, func(), error) {
	h.loadCalled = true
	return nil, nil, nil, errors.New("unexpected batch load")
}

func (h *transferSlabFailureHost) GetTotalSize() uint64 {
	return 0
}

func (h *transferSlabFailureHost) GetTotalRowCnt() uint32 {
	return 1
}

func (h *transferSlabFailureHost) GetBlockMaxRows() uint32 {
	return 1
}

func (h *transferSlabFailureHost) GetObjectMaxBlocks() uint16 {
	return 1
}

func (h *transferSlabFailureHost) GetTargetObjSize() uint32 {
	return 0
}
