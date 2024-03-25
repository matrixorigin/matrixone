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

package db

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

// objHelper holds some temp statistics and founds deletable objects of a table.
// If a Object has no any non-dropped blocks, it can be deleted. Except the
// Object has the max Object id, appender may creates block in it.
type objHelper struct {
	// Statistics
	objHasNonDropBlk     bool
	objRowCnt, objRowDel int
	objNonAppend         bool
	isCreating           bool

	// Found deletable Objects
	maxObjId    uint64
	objCandids  []*catalog.ObjectEntry // appendable
	nobjCandids []*catalog.ObjectEntry // non-appendable
}

func newObjHelper() *objHelper {
	return &objHelper{
		objCandids:  make([]*catalog.ObjectEntry, 0),
		nobjCandids: make([]*catalog.ObjectEntry, 0),
	}
}

func (d *objHelper) reset() {
	d.resetForNewObj()
	d.maxObjId = 0
	d.objCandids = d.objCandids[:0]
	d.nobjCandids = d.nobjCandids[:0]
}

func (d *objHelper) resetForNewObj() {
	d.objHasNonDropBlk = false
	d.objNonAppend = false
	d.isCreating = false
	d.objRowCnt = 0
	d.objRowDel = 0
}

// call this when a non dropped block was found when iterating blocks of a Object,
// which make the builder skip this Object
func (d *objHelper) hintNonDropBlock() {
	d.objHasNonDropBlk = true
}

func (d *objHelper) push(entry *catalog.ObjectEntry) {
	isAppendable := entry.IsAppendable()
	if isAppendable && d.maxObjId < entry.SortHint {
		d.maxObjId = entry.SortHint
	}
	if d.objHasNonDropBlk {
		return
	}
	// all blocks has been dropped
	if isAppendable {
		d.objCandids = append(d.objCandids, entry)
	} else {
		d.nobjCandids = append(d.nobjCandids, entry)
	}
}

// unused
// copy out Object entries expect the one with max Object id.
// func (d *objHelper) finish() []*catalog.ObjectEntry {
// 	sort.Slice(d.objCandids, func(i, j int) bool { return d.objCandids[i].SortHint < d.objCandids[j].SortHint })
// 	if last := len(d.objCandids) - 1; last >= 0 && d.objCandids[last].SortHint == d.maxObjId {
// 		d.objCandids = d.objCandids[:last]
// 	}
// 	if len(d.objCandids) == 0 && len(d.nobjCandids) == 0 {
// 		return nil
// 	}
// 	ret := make([]*catalog.ObjectEntry, len(d.objCandids)+len(d.nobjCandids))
// 	copy(ret[:len(d.objCandids)], d.objCandids)
// 	copy(ret[len(d.objCandids):], d.nobjCandids)
// 	return ret
// }

type MergeTaskBuilder struct {
	db *DB
	*catalog.LoopProcessor
	tid  uint64
	name string
	tbl  *catalog.TableEntry

	ObjectHelper *objHelper
	objPolicy    merge.Policy
	executor     *merge.MergeExecutor
	tableRowCnt  int
	tableRowDel  int

	// concurrecy control
	suspend    atomic.Bool
	suspendCnt atomic.Int32
}

func newMergeTaskBuiler(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		ObjectHelper:  newObjHelper(),
		objPolicy:     merge.NewBasicPolicy(),
		executor:      merge.NewMergeExecutor(db.Runtime, db.CNMergeSched),
	}

	op.DatabaseFn = op.onDataBase
	op.TableFn = op.onTable
	op.ObjectFn = op.onObject
	op.PostObjectFn = op.onPostObject
	op.PostTableFn = op.onPostTable
	return op
}

func (s *MergeTaskBuilder) ManuallyMerge(entry *catalog.TableEntry, objs []*catalog.ObjectEntry) error {
	// stop new merge task
	s.suspend.Store(true)
	defer s.suspend.Store(false)
	// waiting the runing merge sched task to finish
	for s.suspendCnt.Load() < 2 {
		time.Sleep(50 * time.Millisecond)
	}

	// all status are safe in the TaskBuilder
	for _, obj := range objs {
		// TODO(_), delete this if every object has objectStat in memory
		if err := obj.CheckAndLoad(); err != nil {
			return err
		}
	}
	return s.executor.ManuallyExecute(entry, objs)
}

func (s *MergeTaskBuilder) ConfigPolicy(tbl *catalog.TableEntry, c any) {
	f := func() txnif.AsyncTxn {
		txn, _ := s.db.StartTxn(nil)
		return txn
	}

	s.objPolicy.SetConfig(tbl, f, c)
}

func (s *MergeTaskBuilder) GetPolicy(tbl *catalog.TableEntry) any {
	return s.objPolicy.GetConfig(tbl)
}

func (s *MergeTaskBuilder) trySchedMergeTask() {
	if s.tid == 0 {
		return
	}
	// delObjs := s.ObjectHelper.finish()
	s.executor.ExecuteFor(s.tbl, s.objPolicy)
}

func (s *MergeTaskBuilder) resetForTable(entry *catalog.TableEntry) {
	s.tid = 0
	if entry != nil {
		s.tid = entry.ID
		s.tbl = entry
		s.name = entry.GetLastestSchemaLocked().Name
		s.tableRowCnt = 0
		s.tableRowDel = 0
	}
	s.ObjectHelper.reset()
	s.objPolicy.ResetForTable(entry)
}

func (s *MergeTaskBuilder) PreExecute() error {
	s.executor.RefreshMemInfo()
	return nil
}

func (s *MergeTaskBuilder) PostExecute() error {
	s.executor.PrintStats()
	return nil
}
func (s *MergeTaskBuilder) onDataBase(dbEntry *catalog.DBEntry) (err error) {
	if s.suspend.Load() {
		s.suspendCnt.Add(1)
		return moerr.GetOkStopCurrRecur()
	}
	s.suspendCnt.Store(0)
	if merge.StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if s.executor.MemAvailBytes() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}
	return
}

func (s *MergeTaskBuilder) onTable(tableEntry *catalog.TableEntry) (err error) {
	if merge.StopMerge.Load() || s.suspend.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if !tableEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}
	s.resetForTable(tableEntry)
	return
}

func (s *MergeTaskBuilder) onPostTable(tableEntry *catalog.TableEntry) (err error) {
	// base on the info of tableEntry, we can decide whether to merge or not
	tableEntry.Stats.AddRowStat(s.tableRowCnt, s.tableRowDel)
	s.trySchedMergeTask()
	return
}

func (s *MergeTaskBuilder) onObject(objectEntry *catalog.ObjectEntry) (err error) {
	if !objectEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	objectEntry.RLock()
	defer objectEntry.RUnlock()

	// Skip uncommitted entries
	if !objectEntry.IsCommitted() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry.BaseEntryImpl) {
		return moerr.GetOkStopCurrRecur()
	}

	s.ObjectHelper.resetForNewObj()
	if objectEntry.IsAppendable() {
		return
	}

	// for sorted Objects, we just collect the rows and dels on this Object
	// for non-sorted Objects, flushTableTail will take care of them, here we just check if it is deletable(having no active blocks)

	// it has active blk, this obj can't be deleted
	// active object have active blks
	s.ObjectHelper.hintNonDropBlock()
	if !objectEntry.IsCommitted() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry.BaseEntryImpl) {
		// txn appending metalocs
		s.ObjectHelper.isCreating = true
		return moerr.GetOkStopCurrRecur()
	}
	if !catalog.NonAppendableBlkFilter(objectEntry) {
		panic("append block in sorted Object")
	}
	// nblks in appenable objs or non-sorted non-appendable objs
	// these blks are formed by continuous append
	objectEntry.RUnlock()
	rows, err := objectEntry.GetObjectData().Rows()
	if err != nil {
		return
	}
	dels := objectEntry.GetObjectData().GetTotalChanges()
	objectEntry.RLock()
	s.ObjectHelper.objRowCnt += rows
	s.ObjectHelper.objRowDel += dels
	return
}

func (s *MergeTaskBuilder) onPostObject(obj *catalog.ObjectEntry) (err error) {
	s.ObjectHelper.push(obj)

	if !s.ObjectHelper.objNonAppend || s.ObjectHelper.isCreating {
		return nil
	}
	// for sorted Objects, we have to feed it to policy to see if it is qualified to be merged
	obj.SetRemainingRows(s.ObjectHelper.objRowCnt - s.ObjectHelper.objRowDel)

	obj.CheckAndLoad()
	s.tableRowCnt += s.ObjectHelper.objRowCnt
	s.tableRowDel += s.ObjectHelper.objRowDel

	s.objPolicy.OnObject(obj)
	return nil
}
