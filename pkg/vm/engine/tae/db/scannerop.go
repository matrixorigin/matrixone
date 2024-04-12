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

type MergeTaskBuilder struct {
	db *DB
	*catalog.LoopProcessor
	tid  uint64
	name string
	tbl  *catalog.TableEntry

	objPolicy   merge.Policy
	executor    *merge.MergeExecutor
	tableRowCnt int
	tableRowDel int

	// concurrecy control
	suspend    atomic.Bool
	suspendCnt atomic.Int32
}

func newMergeTaskBuiler(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		objPolicy:     merge.NewBasicPolicy(),
		executor:      merge.NewMergeExecutor(db.Runtime),
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
	tableEntry.RLock()
	// this table is creating or altering
	if !tableEntry.IsCommitted() {
		tableEntry.RUnlock()
		return moerr.GetOkStopCurrRecur()
	}
	tableEntry.RUnlock()
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
	// TODO: consider the case: add metaloc, is it possible to see a constructing object?
	if !objectEntry.IsCommitted() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry.BaseEntryImpl) {
		return moerr.GetOkStopCurrRecur()
	}

	if objectEntry.IsAppendable() {
		return moerr.GetOkStopCurrRecur()
	}

	objectEntry.RUnlock()
	// Rows will check objectStat, and if not loaded, it will load it.
	rows, err := objectEntry.GetObjectData().Rows()
	if err != nil {
		return
	}

	dels := objectEntry.GetObjectData().GetTotalChanges()

	// these operations do not require object lock
	objectEntry.SetRemainingRows(rows - dels)
	s.tableRowCnt += rows
	s.tableRowDel += dels
	s.objPolicy.OnObject(objectEntry)
	objectEntry.RLock()
	return
}

func (s *MergeTaskBuilder) onPostObject(obj *catalog.ObjectEntry) (err error) {
	return nil
}
