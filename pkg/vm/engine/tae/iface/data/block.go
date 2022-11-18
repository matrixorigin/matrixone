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
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type CheckpointUnit interface {
	MutationInfo() string
	RunCalibration() int
	EstimateScore(time.Duration, bool) int
	BuildCompactionTaskFactory() (tasks.TxnTaskFactory, tasks.TaskType, []common.ID, error)
}

type BlockAppender interface {
	GetID() *common.ID
	GetMeta() any
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

	Rows() int
	GetColumnDataByName(txn txnif.AsyncTxn, attr string, buffer *bytes.Buffer) (*model.ColumnView, error)
	GetColumnDataById(txn txnif.AsyncTxn, colIdx int, buffer *bytes.Buffer) (*model.ColumnView, error)
	GetMeta() any
	GetBufMgr() base.INodeManager

	MakeAppender() (BlockAppender, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32, dt handle.DeleteType) (txnif.DeleteNode, error)

	GetTotalChanges() int
	CollectChangesInRange(startTs, endTs types.TS) (*model.BlockView, error)
	CollectAppendLogIndexes(startTs, endTs types.TS) ([]*wal.Index, error)

	// check wether any delete intents with prepared ts within [from, to]
	HasDeleteIntentsPreparedIn(from, to types.TS) bool

	BatchDedup(txn txnif.AsyncTxn, pks containers.Vector, rowmask *roaring.Bitmap, precommit bool) error
	GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (uint32, error)
	GetValue(txn txnif.AsyncTxn, row, col int) (any, error)
	PPString(level common.PPLevel, depth int, prefix string) string

	Init() error
	TryUpgrade() error
	CollectAppendInRange(start, end types.TS, withAborted bool) (*containers.Batch, error)
	CollectDeleteInRange(start, end types.TS, withAborted bool) (*containers.Batch, error)
	// GetAppendNodeByRow(row uint32) (an txnif.AppendNode)
	// GetDeleteNodeByRow(row uint32) (an txnif.DeleteNode)
	GetFs() *objectio.ObjectFS

	Close()
}
