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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type CheckpointUnit interface {
	MutationInfo() string
	RunCalibration() int
	// EstimateScore(time.Duration, bool) int
	BuildCompactionTaskFactory() (tasks.TxnTaskFactory, tasks.TaskType, []common.ID, error)
}

type BlockAppender interface {
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

type BlockReplayer interface {
	OnReplayDelete(node txnif.DeleteNode) (err error)
	OnReplayAppend(node txnif.AppendNode) (err error)
	OnReplayAppendPayload(bat *containers.Batch) (err error)
}

type Block interface {
	CheckpointUnit
	BlockReplayer

	DeletesInfo() string

	GetRowsOnReplay() uint64
	GetID() *common.ID
	IsAppendable() bool
	PrepareCompact() bool
	PrepareCompactInfo() (bool, string)

	CheckFlushTaskRetry(startts types.TS) bool

	Rows() int
	GetColumnDataById(
		ctx context.Context, txn txnif.AsyncTxn, readSchema any /*avoid import cycle*/, colIdx int, mp *mpool.MPool,
	) (*containers.ColumnView, error)
	GetColumnDataByIds(
		ctx context.Context, txn txnif.AsyncTxn, readSchema any, colIdxes []int, mp *mpool.MPool,
	) (*containers.BlockView, error)
	Prefetch(idxes []uint16) error
	GetMeta() any

	MakeAppender() (BlockAppender, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32, pk containers.Vector, dt handle.DeleteType) (txnif.DeleteNode, error)
	TryDeleteByDeltaloc(txn txnif.AsyncTxn, deltaLoc objectio.Location) (node txnif.DeleteNode, ok bool, err error)

	GetTotalChanges() int
	CollectChangesInRange(ctx context.Context, startTs, endTs types.TS, mp *mpool.MPool) (*containers.BlockView, error)

	// check wether any delete intents with prepared ts within [from, to]
	HasDeleteIntentsPreparedIn(from, to types.TS) (bool, bool)

	// check if all rows are committed before ts
	// NOTE: here we assume that the block is visible to the ts
	// if the block is an appendable block:
	// 1. if the block is not frozen, return false
	// 2. if the block is frozen and in-memory, check with the max ts committed
	// 3. if the block is persisted, return false
	// if the block is not an appendable block:
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

	GetByFilter(ctx context.Context, txn txnif.AsyncTxn, filter *handle.Filter, mp *mpool.MPool) (uint32, error)
	GetValue(ctx context.Context, txn txnif.AsyncTxn, readSchema any, row, col int, mp *mpool.MPool) (any, bool, error)
	Foreach(
		ctx context.Context,
		readSchema any,
		colIdx int,
		op func(v any, isNull bool, row int) error,
		sels []uint32,
		mp *mpool.MPool,
	) error
	PPString(level common.PPLevel, depth int, prefix string) string
	EstimateMemSize() (int, int)
	GetRuntime() *dbutils.Runtime

	Init() error
	TryUpgrade() error
	GCInMemeoryDeletesByTS(types.TS)
	CollectAppendInRange(start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.BatchWithVersion, error)
	CollectDeleteInRange(ctx context.Context, start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.Batch, error)
	CollectDeleteInRangeAfterDeltalocation(ctx context.Context, start, end types.TS, withAborted bool, mp *mpool.MPool) (*containers.Batch, error)
	// GetAppendNodeByRow(row uint32) (an txnif.AppendNode)
	// GetDeleteNodeByRow(row uint32) (an txnif.DeleteNode)
	GetFs() *objectio.ObjectFS
	FreezeAppend()

	Close()
}
