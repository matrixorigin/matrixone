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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	dto "github.com/prometheus/client_model/go"
)

type Scheduler struct {
	tid uint64

	catalog *catalog.Catalog

	policies *policyGroup
	executor *executor

	skipForTransPageLimit bool

	rc *resourceController

	stopMerge atomic.Bool
}

func NewScheduler(rt *dbutils.Runtime, sched *CNMergeScheduler) *Scheduler {
	op := &Scheduler{
		policies: newPolicyGroup(
			newObjOverlapPolicy(),
			newObjCompactPolicy(rt.Fs.Service),
			newTombstonePolicy(),
		),
		executor: newMergeExecutor(rt, sched),
		rc:       new(resourceController),
	}
	return op
}

func (s *Scheduler) Schedule() {
	if s.stopMerge.Load() {
		return
	}
	dbutils.PrintMemStats()
	err := s.PreExecute()
	if err != nil {
		panic(err)
	}
	if err = s.catalog.RecurLoop(s); err != nil {
		logutil.Errorf("DBScanner Execute: %v", err)
	}
	err = s.PostExecute()
	if err != nil {
		panic(err)
	}
}

func (s *Scheduler) ConfigPolicy(tbl *catalog.TableEntry, txn txnif.AsyncTxn, c *BasicPolicyConfig) error {
	return s.policies.setConfig(tbl, txn, c)
}

func (s *Scheduler) GetPolicy(tbl *catalog.TableEntry) *BasicPolicyConfig {
	return s.policies.getConfig(tbl)
}

func (s *Scheduler) resetForTable(entry *catalog.TableEntry) {
	s.tid = 0
	if entry != nil {
		s.tid = entry.ID
	}
	s.policies.resetForTable(entry)
}

func (s *Scheduler) PreExecute() error {
	s.rc.refresh()
	s.skipForTransPageLimit = false
	m := &dto.Metric{}
	if err := v2.TaskMergeTransferPageLengthGauge.Write(m); err != nil {
		return err
	}
	pagesize := m.GetGauge().GetValue() * 28 /*int32 + rowid(24b)*/
	if pagesize > float64(s.rc.transferPageLimit) {
		logutil.Infof("[mergeblocks] skip merge scanning due to transfer page %s, limit %s",
			common.HumanReadableBytes(int(pagesize)),
			common.HumanReadableBytes(int(s.rc.transferPageLimit)))
		s.skipForTransPageLimit = true
	}
	return nil
}

func (s *Scheduler) PostExecute() error {
	s.rc.printStats()
	return nil
}

func (s *Scheduler) OnDatabase(*catalog.DBEntry) (err error) {
	if s.stopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if s.rc.availableMem() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}

	if s.skipForTransPageLimit {
		return moerr.GetOkStopCurrRecur()
	}

	return
}

func (s *Scheduler) OnTable(tableEntry *catalog.TableEntry) (err error) {
	if s.stopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}

	if s.executor.rt.LockMergeService.IsLockedByUser(tableEntry.ID, tableEntry.GetLastestSchema(false).Name) {
		logutil.Infof("LockMerge skip table scan due to user lock %d", tableEntry.ID)
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

func (s *Scheduler) OnPostTable(tableEntry *catalog.TableEntry) (err error) {
	if s.stopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	// base on the info of tableEntry, we can decide whether to merge or not
	if s.tid == 0 {
		return
	}
	// delObjs := s.ObjectHelper.finish()

	results := s.policies.revise(s.rc)
	for _, r := range results {
		if len(r.objs) > 0 {
			s.executor.executeFor(tableEntry, r.objs, r.kind)
		}
	}
	return
}

func (s *Scheduler) OnObject(objectEntry *catalog.ObjectEntry) (err error) {
	if s.stopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if !objectValid(objectEntry) {
		return moerr.GetOkStopCurrRecur()
	}

	s.policies.onObject(objectEntry)
	return
}

func (s *Scheduler) OnTombstone(tombstone *catalog.ObjectEntry) error {
	return s.OnObject(tombstone)
}

func (s *Scheduler) OnPostDatabase(database *catalog.DBEntry) error {
	return nil
}

func (s *Scheduler) OnPostObject(object *catalog.ObjectEntry) error {
	return nil
}

func (s *Scheduler) StopMerge(tblEntry *catalog.TableEntry, reentrant bool) error {
	c := new(engine.ConstraintDef)
	binary := tblEntry.GetLastestSchema(false).Constraint
	err := c.UnmarshalBinary(binary)
	if err != nil {
		return err
	}
	indexTableNames := make([]string, 0, len(c.Cts))
	for _, constraint := range c.Cts {
		if indices, ok := constraint.(*engine.IndexDef); ok {
			for _, index := range indices.Indexes {
				indexTableNames = append(indexTableNames, index.IndexTableName)
			}
		}
	}

	tblName := tblEntry.GetLastestSchema(false).Name
	return s.executor.rt.LockMergeService.LockFromUser(tblEntry.GetID(), tblName, reentrant, indexTableNames...)
}

func (s *Scheduler) StartMerge(tblID uint64, reentrant bool) error {
	return s.executor.rt.LockMergeService.UnlockFromUser(tblID, reentrant)
}

func (s *Scheduler) StopMergeService() {
	s.stopMerge.Store(true)
}

func (s *Scheduler) StartMergeService() {
	s.stopMerge.Store(false)
}

func (s *Scheduler) CNActiveObjectsString() string {
	return s.executor.cnSched.activeObjsString()
}

func (s *Scheduler) RemoveCNActiveObjects(ids []objectio.ObjectId) {
	s.executor.cnSched.removeActiveObject(ids)
}

func (s *Scheduler) PruneCNActiveObjects(id uint64, ago time.Duration) {
	s.executor.cnSched.prune(id, ago)
}
