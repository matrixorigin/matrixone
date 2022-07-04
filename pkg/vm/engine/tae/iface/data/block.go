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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type CheckpointUnit interface {
	MutationInfo() string
	RunCalibration()
	EstimateScore() int
	BuildCompactionTaskFactory() (tasks.TxnTaskFactory, tasks.TaskType, []common.ID, error)
}

type BlockAppender interface {
	GetID() *common.ID
	GetMeta() any
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *containers.Batch,
		txn txnif.AsyncTxn,
		anode txnif.AppendNode,
	) (txnif.AppendNode, int, error)
	IsAppendable() bool
	ReplayAppend(bat *containers.Batch) error
}

type BlockReplayer interface {
	OnReplayDelete(node txnif.DeleteNode) (err error)
	OnReplayUpdate(colIdx uint16, node txnif.UpdateNode) (err error)
	OnReplayAppend(node txnif.AppendNode) (err error)
	OnReplayAppendPayload(bat *containers.Batch) (err error)
}

type Block interface {
	CheckpointUnit
	BlockReplayer

	GetRowsOnReplay() uint64
	GetID() *common.ID
	IsAppendable() bool
	Rows(txn txnif.AsyncTxn, coarse bool) int
	GetColumnDataByName(txn txnif.AsyncTxn, attr string, buffer *bytes.Buffer) (*model.ColumnView, error)
	GetColumnDataById(txn txnif.AsyncTxn, colIdx int, buffer *bytes.Buffer) (*model.ColumnView, error)
	GetMeta() any
	GetBufMgr() base.INodeManager

	MakeAppender() (BlockAppender, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32, dt handle.DeleteType) (txnif.DeleteNode, error)
	Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v any) (txnif.UpdateNode, error)

	GetTotalChanges() int
	CollectChangesInRange(startTs, endTs uint64) (*model.BlockView, error)
	CollectAppendLogIndexes(startTs, endTs uint64) ([]*wal.Index, error)

	BatchDedup(txn txnif.AsyncTxn, pks containers.Vector, rowmask *roaring.Bitmap) error
	GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (uint32, error)
	GetValue(txn txnif.AsyncTxn, row, col int) (any, error)
	PPString(level common.PPLevel, depth int, prefix string) string
	GetBlockFile() file.Block

	SetMaxCheckpointTS(ts uint64)
	GetMaxCheckpointTS() uint64
	GetMaxVisibleTS() uint64

	CheckpointWALClosure(endTs uint64) tasks.FuncT
	SyncBlockDataClosure(ts uint64, rows uint32) tasks.FuncT
	FlushColumnDataClosure(ts uint64, colIdx int, colData containers.Vector, sync bool) tasks.FuncT
	ForceCompact() error
	Destroy() error
	ReplayIndex() error
	Flush()
	Close()
}
