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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

	objDeltaLocCnt    map[*catalog.ObjectEntry]int
	objDeltaLocRowCnt map[*catalog.ObjectEntry]uint32
	distinctDeltaLocs map[string]struct{}
	mergingObjs       map[*catalog.ObjectEntry]struct{}

	objPolicy   merge.Policy
	executor    *merge.MergeExecutor
	tableRowCnt int
	tableRowDel int

	// concurrecy control
	suspend    atomic.Bool
	suspendCnt atomic.Int32
}

func newMergeTaskBuilder(db *DB) *MergeTaskBuilder {
	op := &MergeTaskBuilder{
		db:                db,
		LoopProcessor:     new(catalog.LoopProcessor),
		objPolicy:         merge.NewBasicPolicy(),
		executor:          merge.NewMergeExecutor(db.Runtime, db.CNMergeSched),
		objDeltaLocRowCnt: make(map[*catalog.ObjectEntry]uint32),
		distinctDeltaLocs: make(map[string]struct{}),
		objDeltaLocCnt:    make(map[*catalog.ObjectEntry]int),
		mergingObjs:       make(map[*catalog.ObjectEntry]struct{}),
	}

	op.DatabaseFn = op.onDataBase
	op.TableFn = op.onTable
	op.ObjectFn = op.onObject
	op.PostObjectFn = op.onPostObject
	op.PostTableFn = op.onPostTable
	return op
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

func (s *MergeTaskBuilder) resetForTable(entry *catalog.TableEntry) {
	s.tid = 0
	if entry != nil {
		s.tid = entry.ID
		s.name = entry.GetLastestSchemaLocked().Name
		s.tableRowCnt = 0
		s.tableRowDel = 0

		clear(s.objDeltaLocCnt)
		clear(s.objDeltaLocRowCnt)
		clear(s.distinctDeltaLocs)
	}
	s.objPolicy.ResetForTable(entry)
}

func (s *MergeTaskBuilder) PreExecute() error {
	s.executor.RefreshMemInfo()
	for obj := range s.mergingObjs {
		if !objectValid(obj) {
			delete(s.mergingObjs, obj)
		}
	}
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
	if !tableEntry.IsCommittedLocked() {
		tableEntry.RUnlock()
		return moerr.GetOkStopCurrRecur()
	}
	tableEntry.RUnlock()
	s.resetForTable(tableEntry)

	deltaLocRows := uint32(0)
	distinctDeltaLocs := 0
	tblRows := 0
	for objIt := tableEntry.MakeObjectIt(true); objIt.Valid(); objIt.Next() {
		objectEntry := objIt.Get().GetPayload()
		if !objectValid(objectEntry) {
			continue
		}

		var rows int
		rows, err = objectEntry.GetObjectData().Rows()
		if err != nil {
			return
		}
		dels := objectEntry.GetObjectData().GetTotalChanges()
		objectEntry.SetRemainingRows(rows - dels)
		s.tableRowCnt += rows
		s.tableRowDel += dels
		tblRows += rows - dels

		tombstone := tableEntry.TryGetTombstone(objectEntry.ID)
		if tombstone == nil {
			continue
		}
		for j := range objectEntry.BlockCnt() {
			deltaLoc := tombstone.GetLatestDeltaloc(uint16(j))
			if deltaLoc == nil || deltaLoc.IsEmpty() {
				continue
			}
			if _, ok := s.distinctDeltaLocs[util.UnsafeBytesToString(deltaLoc)]; !ok {
				s.distinctDeltaLocs[util.UnsafeBytesToString(deltaLoc)] = struct{}{}
				s.objDeltaLocCnt[objectEntry]++
				s.objDeltaLocRowCnt[objectEntry] += deltaLoc.Rows()
				deltaLocRows += deltaLoc.Rows()
				distinctDeltaLocs++
			}
		}
	}
	return
}

func (s *MergeTaskBuilder) onPostTable(tableEntry *catalog.TableEntry) (err error) {
	// base on the info of tableEntry, we can decide whether to merge or not
	tableEntry.Stats.AddRowStat(s.tableRowCnt, s.tableRowDel)
	if s.tid == 0 {
		return
	}
	// delObjs := s.ObjectHelper.finish()

	mobjs, kind := s.objPolicy.Revise(s.executor.CPUPercent(), int64(s.executor.MemAvailBytes()),
		merge.DisableDeltaLocMerge.Load())
	if len(mobjs) > 1 {
		for _, m := range mobjs {
			s.mergingObjs[m] = struct{}{}
		}
		s.executor.ExecuteFor(tableEntry, mobjs, kind)
	}
	return
}

func (s *MergeTaskBuilder) onObject(objectEntry *catalog.ObjectEntry) (err error) {
	if _, ok := s.mergingObjs[objectEntry]; ok {
		return moerr.GetOkStopCurrRecur()
	}
	if !objectEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	if !objectValid(objectEntry) {
		return moerr.GetOkStopCurrRecur()
	}

	// Rows will check objectStat, and if not loaded, it will load it.
	remainingRows := objectEntry.GetRemainingRows()
	deltaLocRows := s.objDeltaLocRowCnt[objectEntry]
	if !merge.DisableDeltaLocMerge.Load() && deltaLocRows > uint32(remainingRows) {
		deltaLocCnt := s.objDeltaLocCnt[objectEntry]
		rate := float64(deltaLocRows) / float64(remainingRows)
		logutil.Infof(
			"[DeltaLoc Merge] tblId: %s(%d), obj: %s, deltaLoc: %d, rows: %d, deltaLocRows: %d, rate: %f",
			s.name, s.tid, objectEntry.ID.String(), deltaLocCnt, remainingRows, deltaLocRows, rate)
		s.objPolicy.OnObject(objectEntry, true)
	} else {
		s.objPolicy.OnObject(objectEntry, false)
	}
	return
}

func (s *MergeTaskBuilder) onPostObject(obj *catalog.ObjectEntry) (err error) {
	return nil
}

func objectValid(objectEntry *catalog.ObjectEntry) bool {
	objectEntry.RLock()
	defer objectEntry.RUnlock()
	if !objectEntry.IsCommittedLocked() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry.BaseEntryImpl) {
		return false
	}

	if objectEntry.IsAppendable() {
		return false
	}
	return true
}
