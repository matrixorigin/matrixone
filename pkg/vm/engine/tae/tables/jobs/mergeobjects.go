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

package jobs

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type mergeObjectsTask struct {
	*tasks.BaseTask
	txn         txnif.AsyncTxn
	rt          *dbutils.Runtime
	mergedObjs  []*catalog.ObjectEntry
	createdBlks []*catalog.BlockEntry
	compacted   []handle.Block
	commitEntry *mergesort.MergeCommitEntry
	rel         handle.Relation
	did, tid    uint64
}

func NewMergeObjectsTask(
	ctx *tasks.Context, txn txnif.AsyncTxn,
	mergedObjs []*catalog.ObjectEntry,
	rt *dbutils.Runtime,
) (task *mergeObjectsTask, err error) {
	if len(mergedObjs) == 0 {
		panic("empty mergedObjs")
	}
	task = &mergeObjectsTask{
		txn:         txn,
		rt:          rt,
		mergedObjs:  mergedObjs,
		createdBlks: make([]*catalog.BlockEntry, 0),
		compacted:   make([]handle.Block, 0),
	}

	task.did = mergedObjs[0].GetTable().GetDB().ID
	database, err := txn.GetDatabaseByID(task.did)
	if err != nil {
		return
	}
	task.tid = mergedObjs[0].GetTable().ID
	task.rel, err = database.GetRelationByID(task.tid)
	if err != nil {
		return
	}
	for _, meta := range mergedObjs {
		obj, err := task.rel.GetObject(&meta.ID)
		if err != nil {
			return nil, err
		}
		defer obj.Close()
		it := obj.MakeBlockIt()
		for ; it.Valid(); it.Next() {
			blk := it.GetBlock()
			task.compacted = append(task.compacted, blk)
		}
		if err != nil {
			return nil, err
		}
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

// impl DisposableVecPool
func (task *mergeObjectsTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := task.rt.VectorPool.Transient.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (task *mergeObjectsTask) GetMPool() *mpool.MPool {
	return task.rt.VectorPool.Transient.MPool()
}

func (task *mergeObjectsTask) PrepareData() ([]*batch.Batch, []*nulls.Nulls, func(), error) {
	var err error
	views := make([]*containers.BlockView, len(task.compacted))
	releaseF := func() {
		for _, view := range views {
			if view != nil {
				view.Close()
			}
		}
	}
	defer func() {
		if err != nil {
			releaseF()
		}
	}()
	schema := task.rel.Schema().(*catalog.Schema)
	idxs := make([]int, 0, len(schema.ColDefs)-1)
	attrs := make([]string, 0, len(schema.ColDefs)-1)
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		idxs = append(idxs, def.Idx)
		attrs = append(attrs, def.Name)
	}
	for i, block := range task.compacted {
		if views[i], err = block.GetColumnDataByIds(context.Background(), idxs, common.MergeAllocator); err != nil {
			return nil, nil, nil, err
		}
	}

	batches := make([]*batch.Batch, 0, len(task.compacted))
	dels := make([]*nulls.Nulls, 0, len(task.compacted))
	for _, view := range views {
		batch := batch.New(true, attrs)
		if len(attrs) != len(view.Columns) {
			panic(fmt.Sprintf("mismatch %v, %v, %v", attrs, len(attrs), len(view.Columns)))
		}
		for i, col := range view.Columns {
			batch.Vecs[i] = col.GetData().GetDownstreamVector()
		}
		batch.SetRowCount(view.Columns[0].Length())
		batches = append(batches, batch)
		dels = append(dels, view.DeleteMask)
	}

	return batches, dels, releaseF, nil
}

func (task *mergeObjectsTask) PrepareCommitEntry() *mergesort.MergeCommitEntry {
	schema := task.rel.Schema().(*catalog.Schema)
	commitEntry := &mergesort.MergeCommitEntry{}
	commitEntry.DbID = task.did
	commitEntry.TableID = task.tid
	commitEntry.Tablename = schema.Name
	commitEntry.StartTs = task.txn.GetStartTS()
	for _, o := range task.mergedObjs {
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, o.GetObjectStats())
	}
	task.commitEntry = commitEntry
	// leave mapping to ReadMergeAndWrite
	return commitEntry
}

func (task *mergeObjectsTask) PrepareNewWriterFunc() func() *blockio.BlockWriter {
	schema := task.rel.Schema().(*catalog.Schema)
	seqnums := make([]uint16, 0, len(schema.ColDefs)-1)
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		seqnums = append(seqnums, def.SeqNum)
	}
	sortkeyIsPK := false
	sortkeyPos := -1

	if schema.HasPK() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
		sortkeyIsPK = true
	} else if schema.HasSortKey() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
	}
	return mergesort.GetMustNewWriter(task.rt.Fs.Service, schema.Version, seqnums, sortkeyPos, sortkeyIsPK)
}

func (task *mergeObjectsTask) Execute(ctx context.Context) (err error) {
	phaseNumber := 0
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr] Mergeblocks", common.OperationField(task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()

	schema := task.rel.Schema().(*catalog.Schema)
	sortkeyPos := -1
	if schema.HasSortKey() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
	}
	if err = mergesort.DoMergeAndWrite(ctx, sortkeyPos, int(schema.BlockMaxRows), task); err != nil {
		return err
	}

	if task.createdBlks, err = HandleMergeEntryInTxn(task.txn, task.commitEntry, task.rt); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.MergeBlocks.Add(1)
	})
	return nil
}

func (task *mergeObjectsTask) GetCreatedBlocks() []*catalog.BlockEntry {
	return task.createdBlks
}

func HandleMergeEntryInTxn(txn txnif.AsyncTxn, entry *mergesort.MergeCommitEntry, rt *dbutils.Runtime) ([]*catalog.BlockEntry, error) {
	database, err := txn.GetDatabaseByID(entry.DbID)
	if err != nil {
		return nil, err
	}
	rel, err := database.GetRelationByID(entry.TableID)
	if err != nil {
		return nil, err
	}

	mergedObjs := make([]*catalog.ObjectEntry, 0, len(entry.MergedObjs))
	createdObjs := make([]*catalog.ObjectEntry, 0, len(entry.CreatedObjectStats))
	mergedBlks := make([]*catalog.BlockEntry, 0, len(entry.MergedObjs)*2)
	createdBlks := make([]*catalog.BlockEntry, 0, len(entry.CreatedObjectStats)*2)
	ids := make([]*common.ID, 0, len(entry.MergedObjs)*2)

	// drop merged blocks and objects
	for _, drop := range entry.MergedObjs {
		objID := drop.ObjectName().ObjectId()
		obj, err := rel.GetObject(objID)
		if err != nil {
			return nil, err
		}
		mergedObjs = append(mergedObjs, obj.GetMeta().(*catalog.ObjectEntry))
		it := obj.MakeBlockIt()
		for ; it.Valid(); it.Next() {
			blk := it.GetBlock()
			if err = obj.SoftDeleteBlock(blk.ID()); err != nil {
				return nil, err
			}
			mergedBlks = append(mergedBlks, blk.GetMeta().(*catalog.BlockEntry))
			ids = append(ids, blk.Fingerprint())
		}
		if err = rel.SoftDeleteObject(objID); err != nil {
			return nil, err
		}
	}

	// construct new object,
	for _, stats := range entry.CreatedObjectStats {
		objID := stats.ObjectName().ObjectId()
		obj, err := rel.CreateNonAppendableObject(false, new(objectio.CreateObjOpt).WithId(objID))
		if err != nil {
			return nil, err
		}
		createdObjs = append(createdObjs, obj.GetMeta().(*catalog.ObjectEntry))
		// set stats and sorted property
		if err = obj.UpdateStats(stats); err != nil {
			return nil, err
		}
		objEntry := obj.GetMeta().(*catalog.ObjectEntry)
		objEntry.SetSorted()

		num := stats.ObjectName().Num()
		blkCount := stats.BlkCnt()
		totalRow := stats.Rows()
		blkMaxRows := rel.Schema().(*catalog.Schema).BlockMaxRows
		for i := uint16(0); i < uint16(blkCount); i++ {
			var blkRow uint32
			if totalRow > blkMaxRows {
				blkRow = blkMaxRows
			} else {
				blkRow = totalRow
			}
			totalRow -= blkRow
			metaloc := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), blkRow, i)
			blk, err := obj.CreateNonAppendableBlock(
				new(objectio.CreateBlockOpt).
					WithMetaloc(metaloc).
					WithFileIdx(num).
					WithBlkIdx(i))
			if err != nil {
				return nil, err
			}
			blkEntry := blk.GetMeta().(*catalog.BlockEntry)
			createdBlks = append(createdBlks, blkEntry)
			if err = blkEntry.GetBlockData().Init(); err != nil {
				return nil, err
			}
		}
	}

	txnEntry := txnentries.NewMergeObjectsEntry(
		txn,
		rel,
		mergedObjs,
		createdObjs,
		mergedBlks,
		createdBlks,
		entry.Booking,
		rt,
	)

	if err = txn.LogTxnEntry(entry.DbID, entry.TableID, txnEntry, ids); err != nil {
		return nil, err
	}

	return createdBlks, nil
}
