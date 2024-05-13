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

package data

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type CheckpointUnit interface {
	MutationInfo() string
	RunCalibration() (int, error)
	// EstimateScore(time.Duration, bool) int
}

type ObjectAppender interface {
	GetID() *common.ID
	GetMeta() any
	// see more notes in flushtabletail.go
	LockFreeze()
	UnlockFreeze()
	CheckFreeze() bool
	IsSameColumns(otherSchema any /*avoid import cycle*/) bool
	PrepareAppend(rows uint32,
		txn txnif.AsyncTxn) (
		node txnif.AppendNode, created bool, n uint32, err error)
	ApplyAppend(bat *containers.Batch,
		txn txnif.AsyncTxn,
	) (int, error)
	IsAppendable() bool
	ReplayAppend(bat *containers.Batch,
		txn txnif.AsyncTxn) (int, error)
	Close()
}

type ObjectReplayer interface {
	OnReplayDelete(blkID uint16, node txnif.DeleteNode) (err error)
	OnReplayAppend(node txnif.AppendNode) (err error)
	OnReplayAppendPayload(bat *containers.Batch) (err error)
}

type Object interface {
	CheckpointUnit
	ObjectReplayer

	DeletesInfo() string

	GetRowsOnReplay() uint64
	GetID() *common.ID
	IsAppendable() bool
	PrepareCompact() bool
	PrepareCompactInfo() (bool, string)
	GetDeltaPersistedTS() types.TS

	Rows() (int, error)
	CheckFlushTaskRetry(startts types.TS) bool

	GetColumnDataById(
		ctx context.Context, txn txnif.AsyncTxn, readSchema any /*avoid import cycle*/, blkID uint16, colIdx int, mp *mpool.MPool,
	) (*containers.ColumnView, error)
	GetColumnDataByIds(
		ctx context.Context, txn txnif.AsyncTxn, readSchema any, blkID uint16, colIdxes []int, mp *mpool.MPool,
	) (*containers.BlockView, error)
	Prefetch(idxes []uint16, blkID uint16) error
	GetMeta() any

	MakeAppender() (ObjectAppender, error)
	RangeDelete(txn txnif.AsyncTxn, blkID uint16, start, end uint32, pk containers.Vector, dt handle.DeleteType) (txnif.DeleteNode, error)
	TryDeleteByDeltaloc(txn txnif.AsyncTxn, blkID uint16, deltaLoc objectio.Location) (node txnif.TxnEntry, ok bool, err error)

	GetTotalChanges() int
	CollectChangesInRange(ctx context.Context, blkID uint16, startTs, endTs types.TS, mp *mpool.MPool) (*containers.BlockView, error)

	// check wether any delete intents with prepared ts within [from, to]
	HasDeleteIntentsPreparedIn(from, to types.TS) (bool, bool)
	HasDeleteIntentsPreparedInByBlock(blockID uint16, from, to types.TS) (bool, bool)

	// check if all rows are committed before ts
	// NOTE: here we assume that the object is visible to the ts
	// if the object is an appendable object:
	// 1. if the object is not frozen, return false
	// 2. if the object is frozen and in-memory, check with the max ts committed
	// 3. if the object is persisted, return false
	// if the object is not an appendable object:
	// only check with the created ts
	CoarseCheckAllRowsCommittedBefore(ts types.TS) bool

	BatchDedup(ctx context.Context,
		txn txnif.AsyncTxn,
		pks containers.Vector,
		pksZM index.ZM,
		rowmask *roaring.Bitmap,
		precommit bool,
		bf objectio.BloomFilter,
		mp *mpool.MPool,
	) error
	//TODO::
	//BatchDedupByMetaLoc(txn txnif.AsyncTxn, fs *objectio.ObjectFS,
	//	metaLoc objectio.Location, rowmask *roaring.Bitmap, precommit bool) error

	GetByFilter(ctx context.Context, txn txnif.AsyncTxn, filter *handle.Filter, mp *mpool.MPool) (uint16, uint32, error)
	GetValue(ctx context.Context, txn txnif.AsyncTxn, readSchema any, blkID uint16, row, col int, mp *mpool.MPool) (any, bool, error)
	Foreach(
		ctx context.Context,
		readSchema any,
		blkID uint16,
		colIdx int,
		op func(v any, isNull bool, row int) error,
		sels []uint32,
		mp *mpool.MPool,
	) error
	PPString(level common.PPLevel, depth int, prefix string, blkid int) string
	EstimateMemSize() (int, int)
	GetRuntime() *dbutils.Runtime

	Init() error
	TryUpgrade() error
	GCInMemeoryDeletesByTSForTest(types.TS)
	UpgradeAllDeleteChain()
	CollectAppendInRange(start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.BatchWithVersion, error)
	CollectDeleteInRange(ctx context.Context, start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.Batch, *bitmap.Bitmap, error)
	CollectDeleteInRangeByBlock(ctx context.Context, blkID uint16, start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.Batch, error)
	PersistedCollectDeleteInRange(
		ctx context.Context,
		b *containers.Batch,
		blkID uint16,
		start, end types.TS,
		withAborted bool,
		mp *mpool.MPool,
	) (bat *containers.Batch, err error)
	// GetAppendNodeByRow(row uint32) (an txnif.AppendNode)
	// GetDeleteNodeByRow(row uint32) (an txnif.DeleteNode)
	GetFs() *objectio.ObjectFS
	FreezeAppend()
	UpdateDeltaLoc(txn txnif.TxnReader, blkID uint16, deltaLoc objectio.Location) (bool, txnif.TxnEntry, error)

	Close()
}

type Tombstone interface {
	EstimateMemSizeLocked() (dsize int)
	GetChangeIntentionCntLocked() uint32
	GetDeleteCnt() uint32
	GetDeletesListener() func(uint64, types.TS) error
	GetDeltaLocAndCommitTSByTxn(blkID uint16, txn txnif.TxnReader) (objectio.Location, types.TS)
	GetDeltaLocAndCommitTS(blkID uint16) (objectio.Location, types.TS, types.TS)
	GetDeltaPersistedTS() types.TS
	// GetOrCreateDeleteChain(blkID uint16) *updates.MVCCHandle
	HasDeleteIntentsPreparedIn(from types.TS, to types.TS) (found bool, isPersist bool)
	HasInMemoryDeleteIntentsPreparedInByBlock(blockID uint16, from, to types.TS) (bool, bool)
	IsDeletedLocked(row uint32, txn txnif.TxnReader, blkID uint16) (bool, error)
	SetDeletesListener(l func(uint64, types.TS) error)
	StringLocked(level common.PPLevel, depth int, prefix string) string
	// TryGetDeleteChain(blkID uint16) *updates.MVCCHandle
	UpgradeAllDeleteChain()
	UpgradeDeleteChain(blkID uint16)
	UpgradeDeleteChainByTSLocked(ts types.TS)
	ReplayDeltaLoc(any, uint16)
	VisitDeletes(ctx context.Context, start, end types.TS, bat, tnBatch *containers.Batch, skipMemory bool) (*containers.Batch, int, int, error)
	GetObject() any
	InMemoryDeletesExisted() bool
	// for test
	GetLatestDeltaloc(uint16) objectio.Location
}
