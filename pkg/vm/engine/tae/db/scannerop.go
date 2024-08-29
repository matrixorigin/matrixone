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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	dto "github.com/prometheus/client_model/go"
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

	skipForTransPageLimit bool
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
	op.TombstoneFn = op.onTombstone
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
		s.name = entry.GetLastestSchemaLocked(false).Name
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
	s.skipForTransPageLimit = false
	m := &dto.Metric{}
	v2.TaskMergeTransferPageLengthGauge.Write(m)
	pagesize := m.GetGauge().GetValue() * 28 /*int32 + rowid(24b)*/ * 1.3 /*map inflationg factor*/
	if pagesize > float64(s.executor.TransferPageSizeLimit()) {
		logutil.Infof("[mergeblocks] skip merge scanning due to transfer page %s, limit %s",
			common.HumanReadableBytes(int(pagesize)),
			common.HumanReadableBytes(int(s.executor.TransferPageSizeLimit())))
		s.skipForTransPageLimit = true
	}
	return nil
}

func (s *MergeTaskBuilder) PostExecute() error {
	s.executor.PrintStats()
	return nil
}
func (s *MergeTaskBuilder) onDataBase(dbEntry *catalog.DBEntry) (err error) {
	if merge.StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if s.executor.MemAvailBytes() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}

	if s.skipForTransPageLimit {
		return moerr.GetOkStopCurrRecur()
	}

	return
}

func (s *MergeTaskBuilder) onTable(tableEntry *catalog.TableEntry) (err error) {
	if merge.StopMerge.Load() {
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

	return
}

func (s *MergeTaskBuilder) onPostTable(tableEntry *catalog.TableEntry) (err error) {
	// base on the info of tableEntry, we can decide whether to merge or not
	tableEntry.Stats.AddRowStat(s.tableRowCnt, s.tableRowDel)
	if s.tid == 0 {
		return
	}
	// delObjs := s.ObjectHelper.finish()

	mobjs, kind := s.objPolicy.Revise(s.executor.CPUPercent(), int64(s.executor.MemAvailBytes()))
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
	s.objPolicy.OnObject(objectEntry, false)
	return
}
func (s *MergeTaskBuilder) onTombstone(objectEntry *catalog.ObjectEntry) (err error) {
	return s.onObject(objectEntry)
}
func (s *MergeTaskBuilder) onPostObject(obj *catalog.ObjectEntry) (err error) {
	return nil
}

func objectValid(objectEntry *catalog.ObjectEntry) bool {
	if !objectEntry.IsCommitted() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry) {
		return false
	}

	if objectEntry.IsAppendable() {
		return false
	}
	return true
}
