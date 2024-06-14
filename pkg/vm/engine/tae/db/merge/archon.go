// Copyright 2024 Matrix Origin
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

package merge

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type ObjectMergeArchon struct {
	tid      uint64
	executor *MergeExecutor

	run      int
	policies [2]policy

	tableRowCnt int
	tableRowDel int
}

func NewObjectMergeArchon(rt *dbutils.Runtime, scheduler CNMergeScheduler) *ObjectMergeArchon {
	return &ObjectMergeArchon{
		executor: NewMergeExecutor(rt, scheduler),
		policies: [2]policy{
			newSingleObjPolicy(nil),
			newMultiObjPolicy(nil),
		},
	}
}

func (m *ObjectMergeArchon) OnPostDatabase(database *catalog.DBEntry) error {
	return nil
}

func (m *ObjectMergeArchon) OnPostObject(object *catalog.ObjectEntry) error {
	return nil
}

func (m *ObjectMergeArchon) OnTombstone(tombstone data.Tombstone) error {
	return nil
}

func (m *ObjectMergeArchon) OnDatabase(*catalog.DBEntry) error {
	if StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if m.executor.MemAvailBytes() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}
	return nil
}

func (m *ObjectMergeArchon) OnTable(tableEntry *catalog.TableEntry) error {
	if StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if !tableEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}
	tableEntry.RLock()
	defer tableEntry.RUnlock()
	// this table is creating or altering
	if !tableEntry.IsCommittedLocked() {
		return moerr.GetOkStopCurrRecur()
	}
	m.resetForTable(tableEntry)
	return nil
}

func (m *ObjectMergeArchon) resetForTable(entry *catalog.TableEntry) {
	m.tid = 0
	if entry != nil {
		m.tid = entry.ID
		m.tableRowCnt = 0
		m.tableRowDel = 0
	}
	m.policies[m.run].Clear()
}

func (m *ObjectMergeArchon) PreExecute() error {
	logutil.Infof("[Mergeblocks] Start Run %d", m.run)
	m.executor.RefreshMemInfo()
	return nil
}

func (m *ObjectMergeArchon) PostExecute() error {
	m.run = (m.run + 1) % len(m.policies)
	m.executor.PrintStats()
	return nil
}

func (m *ObjectMergeArchon) OnPostTable(tableEntry *catalog.TableEntry) (err error) {
	if m.tid == 0 {
		return
	}

	tableEntry.Stats.AddRowStat(m.tableRowCnt, m.tableRowDel)
	// for multi-object run. determine which objects to merge based on all objects.
	mobjs, kind := m.policies[m.run].Revise(m.executor.CPUPercent(), int64(m.executor.MemAvailBytes()))
	if len(mobjs) == 0 {
		return
	}
	if m.run > 0 && len(mobjs) < 2 {
		return
	}

	if m.run == 0 {
		m.executor.ExecuteSingleObjMerge(tableEntry, mobjs, kind)
	} else {
		m.executor.ExecuteMultiObjMerge(tableEntry, mobjs, kind)
	}
	return
}

func (m *ObjectMergeArchon) OnObject(objectEntry *catalog.ObjectEntry) error {
	if !objectEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	objectEntry.RLock()
	defer objectEntry.RUnlock()

	// Skip uncommitted entries
	// TODO: consider the case: add metaloc, is it possible to see a constructing object?
	if !objectEntry.IsCommittedLocked() || !catalog.ActiveObjectWithNoTxnFilter(objectEntry.BaseEntryImpl) {
		return moerr.GetOkStopCurrRecur()
	}

	if objectEntry.IsAppendable() {
		return moerr.GetOkStopCurrRecur()
	}

	// Rows will check objectStat, and if not loaded, it will load it.
	rows, err := objectEntry.GetObjectData().Rows()
	if err != nil {
		return err
	}
	dels := objectEntry.GetObjectData().GetTotalChanges()

	// these operations do not require object lock
	objectEntry.SetRemainingRows(rows - dels)
	m.tableRowCnt += rows
	m.tableRowDel += dels
	m.policies[m.run].OnObject(objectEntry)
	return nil
}
