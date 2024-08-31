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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Scheduler struct {
	tid      uint64
	executor *Executor

	policyGroup *policyGroup

	tableRowCnt int
	tableRowDel int
}

func NewScheduler(rt *dbutils.Runtime, scheduler CNMergeScheduler) *Scheduler {
	return &Scheduler{
		executor:    NewMergeExecutor(rt, scheduler),
		policyGroup: newPolicyGroup(),
	}
}

func (m *Scheduler) OnPostDatabase(*catalog.DBEntry) error {
	return nil
}

func (m *Scheduler) OnPostObject(*catalog.ObjectEntry) error {
	return nil
}

func (m *Scheduler) OnTombstone(obj *catalog.ObjectEntry) error {
	return m.OnObject(obj)
}

func (m *Scheduler) OnDatabase(*catalog.DBEntry) error {
	if StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if m.executor.MemAvailBytes() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}
	return nil
}

func (m *Scheduler) OnTable(tableEntry *catalog.TableEntry) error {
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

func (m *Scheduler) resetForTable(entry *catalog.TableEntry) {
	m.tid = 0
	if entry != nil {
		m.tid = entry.ID
		m.tableRowCnt = 0
		m.tableRowDel = 0
	}
	m.policyGroup.resetForTable(entry)
	m.executor.RefreshMemInfo()
}

func (m *Scheduler) PreExecute() error {
	m.executor.RefreshMemInfo()
	return nil
}

func (m *Scheduler) PostExecute() error {
	m.executor.PrintStats()
	return nil
}

func (m *Scheduler) OnPostTable(tableEntry *catalog.TableEntry) (err error) {
	if m.tid == 0 {
		return
	}

	tableEntry.Stats.AddRowStat(m.tableRowCnt, m.tableRowDel)
	// for multi-object run. determine which objects to merge based on all objects.
	targets, kind := m.policyGroup.revise(m.executor.CPUPercent(), int64(m.executor.MemAvailBytes()))
	for _, target := range targets {
		if len(target) > 1 {
			m.executor.ExecuteObjMerge(tableEntry, target, kind)
		}
	}
	return
}

func (m *Scheduler) OnObject(objectEntry *catalog.ObjectEntry) error {
	if !objectEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	if !objectValid(objectEntry) {
		return moerr.GetOkStopCurrRecur()
	}

	// Rows will check objectStat, and if not loaded, it will load it.
	rows, err := objectEntry.GetObjectData().Rows()
	if err != nil {
		return err
	}
	dels := objectEntry.GetObjectData().GetTotalChanges()

	m.tableRowCnt += rows
	m.tableRowDel += dels
	m.policyGroup.onObject(objectEntry)
	return nil
}

func objectValid(objectEntry *catalog.ObjectEntry) bool {
	// Skip uncommitted entries
	// TODO: is it possible to see a constructing object?
	if !objectEntry.IsCommitted() || objectEntry.HasDropCommitted() {
		return false
	}

	return !objectEntry.IsAppendable()
}

func (m *Scheduler) ConfigPolicy(tbl *catalog.TableEntry, txn txnif.AsyncTxn, c *BasicPolicyConfig) {
	m.policyGroup.setConfig(tbl, txn, c)
}

func (m *Scheduler) GetPolicy(tbl *catalog.TableEntry) *BasicPolicyConfig {
	return m.policyGroup.getConfig(tbl)
}
