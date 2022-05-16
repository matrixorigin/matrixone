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
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	io.Closer
	GetID() *common.ID
	GetMeta() interface{}
	PrepareAppend(rows uint32) (n uint32, err error)
	ApplyAppend(bat *batch.Batch, offset, length uint32, txn txnif.AsyncTxn) (txnif.AppendNode, uint32, error)
	OnReplayInsertNode(bat *batch.Batch, offset, length uint32, txn txnif.AsyncTxn) (node txnif.AppendNode, from uint32, err error)
	IsAppendable() bool
	OnReplayAppendNode(maxrow uint32)
}

type Block interface {
	CheckpointUnit
	
	OnReplayDelete(start, end uint32) (err error)
	OnReplayUpdate(row uint32, colIdx uint16, v interface{})(err error)
	GetID() *common.ID
	IsAppendable() bool
	Rows(txn txnif.AsyncTxn, coarse bool) int
	GetColumnDataByName(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (*model.ColumnView, error)
	GetColumnDataById(txn txnif.AsyncTxn, colIdx int, compressed, decompressed *bytes.Buffer) (*model.ColumnView, error)

	MakeAppender() (BlockAppender, error)
	RangeDelete(txn txnif.AsyncTxn, start, end uint32) (txnif.DeleteNode, error)
	Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (txnif.UpdateNode, error)

	GetTotalChanges() int
	CollectChangesInRange(startTs, endTs uint64) *model.BlockView
	CollectAppendLogIndexes(startTs, endTs uint64) []*wal.Index

	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
	GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (uint32, error)
	GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (interface{}, error)
	PPString(level common.PPLevel, depth int, prefix string) string
	GetBlockFile() file.Block

	SetMaxCheckpointTS(ts uint64)
	GetMaxCheckpointTS() uint64
	GetMaxVisibleTS() uint64

	CheckpointWALClosure(endTs uint64) tasks.FuncT
	SyncBlockDataClosure(ts uint64, rows uint32) tasks.FuncT
	FlushColumnDataClosure(ts uint64, colIdx int, colData *vector.Vector, sync bool) tasks.FuncT
	ForceCompact() error
	Destroy() error
	ReplayData() error
}
