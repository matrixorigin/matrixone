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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ObjectAppender interface {
	GetID() *common.ID
	GetMeta() any
	// see more notes in flushtabletail.go
	LockFreeze()
	UnlockFreeze()
	CheckFreeze() bool
	IsSameColumns(otherSchema any /*avoid import cycle*/) bool
	PrepareAppend(isMergeCompact bool, rows uint32,
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
	OnReplayAppend(node txnif.AppendNode) (err error)
	OnReplayAppendPayload(bat *containers.Batch) (err error)
}

type Object interface {
	ObjectReplayer

	GetRowsOnReplay() uint64
	GetID() *common.ID
	IsAppendable() bool
	PrepareCompact() bool
	PrepareCompactInfo() (bool, string)

	Rows() (int, error)
	CheckFlushTaskRetry(startts types.TS) bool

	Prefetch(idxes []uint16, blkID uint16) error
	GetMeta() any

	MakeAppender() (ObjectAppender, error)

	GetTotalChanges() int
	TryUpgrade() error

	// check if all rows are committed before ts
	// NOTE: here we assume that the object is visible to the ts
	// if the object is an appendable object:
	// 1. if the object is not frozen, return false
	// 2. if the object is frozen and in-memory, check with the max ts committed
	// 3. if the object is persisted, return false
	// if the object is not an appendable object:
	// only check with the created ts
	CoarseCheckAllRowsCommittedBefore(ts types.TS) bool
	GetDuplicatedRows(
		ctx context.Context,
		txn txnif.TxnReader,
		keys containers.Vector,
		keysZM index.ZM,
		precommit bool,
		checkWWConflict bool,
		skipCommittedBeforeTxnForAblk bool,
		rowIDs containers.Vector,
		mp *mpool.MPool,
	) (err error)
	GetMaxRowByTS(ts types.TS) (uint32, error)
	GetValue(ctx context.Context, txn txnif.AsyncTxn, readSchema any, blkID uint16, row, col int, skipCheckDelete bool, mp *mpool.MPool) (any, bool, error)
	PPString(level common.PPLevel, depth int, prefix string, blkid int) string
	EstimateMemSize() (int, int)
	GetRuntime() *dbutils.Runtime

	Init() error
	GetFs() *objectio.ObjectFS
	FreezeAppend()

	Contains(
		ctx context.Context,
		txn txnif.TxnReader,
		isCommitting bool,
		keys containers.Vector,
		keysZM index.ZM,
		mp *mpool.MPool) (err error)
	Close()
	Scan(
		ctx context.Context,
		bat **containers.Batch,
		txn txnif.TxnReader,
		readSchema any,
		blkID uint16,
		colIdxes []int,
		mp *mpool.MPool,
	) (err error)
	FillBlockTombstones(
		ctx context.Context,
		txn txnif.TxnReader,
		blkID *objectio.Blockid,
		deletes **nulls.Nulls,
		mp *mpool.MPool) error
	CollectObjectTombstoneInRange(
		ctx context.Context,
		start, end types.TS,
		objID *types.Objectid,
		bat **containers.Batch,
		mp *mpool.MPool,
		vpool *containers.VectorPool,
	) (err error)
	ScanInMemory(
		ctx context.Context,
		batches map[uint32]*containers.BatchWithVersion,
		start, end types.TS,
		mp *mpool.MPool,
	) (err error)
	UpdateMeta(any)
}
