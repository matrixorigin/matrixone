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

package txnentries

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type compactBlockEntry struct {
	sync.RWMutex
	txn       txnif.AsyncTxn
	from      handle.Block
	to        handle.Block
	scheduler tasks.TaskScheduler
	mapping   []uint32
	deletes   *roaring.Bitmap
}

func NewCompactBlockEntry(txn txnif.AsyncTxn, from, to handle.Block, scheduler tasks.TaskScheduler, sortIdx []uint32, deletes *roaring.Bitmap) *compactBlockEntry {
	mapping := make([]uint32, len(sortIdx))
	for i, idx := range sortIdx {
		mapping[idx] = uint32(i)
	}
	return &compactBlockEntry{
		txn:       txn,
		from:      from,
		to:        to,
		scheduler: scheduler,
		mapping:   mapping,
		deletes:   deletes,
	}
}

func (entry *compactBlockEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (entry *compactBlockEntry) ApplyRollback(index *wal.Index) (err error) {
	//TODO:?
	return
}
func (entry *compactBlockEntry) ApplyCommit(index *wal.Index) (err error) {
	if err = entry.scheduler.Checkpoint([]*wal.Index{index}); err != nil {
		// TODO:
		// Right now scheduler may be stopped before ApplyCommit and then it returns schedule error here.
		// We'll ensure the schduler can only be stopped after txn manager being stopped.
		logutil.Warnf("Schedule checkpoint task failed: %v", err)
		err = nil
	}
	return entry.PostCommit()
}
func (entry *compactBlockEntry) PostCommit() (err error) {
	meta := entry.from.GetMeta().(*catalog.BlockEntry)
	if _, err = entry.scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, meta.AsCommonID(), meta.GetBlockData().CheckpointWALClosure(entry.txn.GetCommitTS())); err != nil {
		// TODO:
		// Right now scheduler may be stopped before ApplyCommit and then it returns schedule error here.
		// We'll ensure the schduler can only be stopped after txn manager being stopped.
		logutil.Warnf("Schedule checkpoint task failed: %v", err)
		err = nil
	}
	return
}
func (entry *compactBlockEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	cmd = newCompactBlockCmd((*common.ID)(entry.from.Fingerprint()), (*common.ID)(entry.to.Fingerprint()), entry.txn, csn)
	return
}

func (entry *compactBlockEntry) Set1PC()     {}
func (entry *compactBlockEntry) Is1PC() bool { return false }
func (entry *compactBlockEntry) PrepareCommit() (err error) {
	dataBlock := entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData()
	view, err := dataBlock.CollectChangesInRange(entry.txn.GetStartTS(), entry.txn.GetCommitTS())
	if view == nil || err != nil {
		return
	}
	deletes := entry.deletes
	for _, column := range view.Columns {
		column.UpdateMask, column.UpdateVals, column.DeleteMask = compute.ShuffleByDeletes(
			column.UpdateMask,
			column.UpdateVals,
			column.DeleteMask, deletes)
	}
	_, _, view.DeleteMask = compute.ShuffleByDeletes(nil, nil, view.DeleteMask, deletes)
	if view.DeleteMask != nil {
		it := view.DeleteMask.Iterator()
		for it.HasNext() {
			row := it.Next()
			if entry.mapping != nil && len(entry.mapping) > int(row) {
				row = entry.mapping[row]
			}
			if err = entry.to.RangeDelete(row, row, handle.DT_MergeCompact); err != nil {
				return
			}
		}
	}
	return
}
