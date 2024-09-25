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
	"strings"
	"sync"
	"time"

	fcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	dto "github.com/prometheus/client_model/go"
)

type Scheduler struct {
	*catalog.LoopProcessor
	tid uint64

	policies *policyGroup
	executor *executor

	skipForTransPageLimit bool

	stoppedTables struct {
		sync.RWMutex
		m map[uint64]struct {
			time.Time
			indexes []string
		}
		indexes map[string]struct{}
	}
}

func NewScheduler(rt *dbutils.Runtime, sched CNMergeScheduler) *Scheduler {
	op := &Scheduler{
		LoopProcessor: new(catalog.LoopProcessor),
		policies:      newPolicyGroup(newBasicPolicy(), newObjCompactPolicy(rt.Fs.Service), newObjOverlapPolicy(), newTombstonePolicy()),
		executor:      newMergeExecutor(rt, sched),
		stoppedTables: struct {
			sync.RWMutex
			m map[uint64]struct {
				time.Time
				indexes []string
			}
			indexes map[string]struct{}
		}{m: make(map[uint64]struct {
			time.Time
			indexes []string
		}), indexes: make(map[string]struct{})},
	}

	op.DatabaseFn = op.onDataBase
	op.TableFn = op.onTable
	op.ObjectFn = op.onObject
	op.TombstoneFn = op.onTombstone
	op.PostObjectFn = op.onPostObject
	op.PostTableFn = op.onPostTable
	return op
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
	s.executor.refreshMemInfo()
	s.skipForTransPageLimit = false
	m := &dto.Metric{}
	v2.TaskMergeTransferPageLengthGauge.Write(m)
	pagesize := m.GetGauge().GetValue() * 28 /*int32 + rowid(24b)*/
	if pagesize > float64(s.executor.transferPageSizeLimit()) {
		logutil.Infof("[mergeblocks] skip merge scanning due to transfer page %s, limit %s",
			common.HumanReadableBytes(int(pagesize)),
			common.HumanReadableBytes(int(s.executor.transferPageSizeLimit())))
		s.skipForTransPageLimit = true
	}
	return nil
}

func (s *Scheduler) PostExecute() error {
	s.executor.printStats()
	return nil
}

func (s *Scheduler) onDataBase(*catalog.DBEntry) (err error) {
	if StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}
	if s.executor.memAvailBytes() < 100*common.Const1MBytes {
		return moerr.GetOkStopCurrRecur()
	}

	if s.skipForTransPageLimit {
		return moerr.GetOkStopCurrRecur()
	}

	return
}

func (s *Scheduler) onTable(tableEntry *catalog.TableEntry) (err error) {
	if StopMerge.Load() {
		return moerr.GetOkStopCurrRecur()
	}

	if s.executor.rt.LockMergeService.IsLockedByUser(tableEntry.ID) {
		logutil.Infof("LockMerge skip table scan due to user lock %d", tableEntry.ID)
		return moerr.GetOkStopCurrRecur()
	}

	if !tableEntry.IsActive() {
		return moerr.GetOkStopCurrRecur()
	}

	s.stoppedTables.RLock()
	if t, ok := s.stoppedTables.m[tableEntry.GetID()]; ok {
		if time.Now().After(t.Add(10 * time.Minute)) {
			logutil.Warnf("table %s has stopped merging for over 10 minutes", tableEntry.GetFullName())
		}
		s.stoppedTables.RUnlock()
		return moerr.GetOkStopCurrRecur()
	}
	tableName := tableEntry.GetLastestSchema(false).Name
	if _, ok := s.stoppedTables.indexes[tableName]; ok {
		s.stoppedTables.RUnlock()
		return moerr.GetOkStopCurrRecur()
	}
	s.stoppedTables.RUnlock()

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

func (s *Scheduler) onPostTable(tableEntry *catalog.TableEntry) (err error) {
	// base on the info of tableEntry, we can decide whether to merge or not
	if s.tid == 0 {
		return
	}
	// delObjs := s.ObjectHelper.finish()

	results := s.policies.revise(s.executor.CPUPercent(), int64(s.executor.memAvailBytes()))
	for _, r := range results {
		if len(r.objs) > 0 {
			s.executor.executeFor(tableEntry, r.objs, r.kind)
		}
	}
	return
}

func (s *Scheduler) onObject(objectEntry *catalog.ObjectEntry) (err error) {
	if !objectValid(objectEntry) {
		return moerr.GetOkStopCurrRecur()
	}

	s.policies.onObject(objectEntry)
	return
}
func (s *Scheduler) onTombstone(objectEntry *catalog.ObjectEntry) (err error) {
	return s.onObject(objectEntry)
}
func (s *Scheduler) onPostObject(*catalog.ObjectEntry) (err error) {
	return nil
}

func (s *Scheduler) StopMerge(tblEntry *catalog.TableEntry) error {
	s.stoppedTables.Lock()
	defer s.stoppedTables.Unlock()

	tblName := tblEntry.GetLastestSchema(false).Name
	if strings.HasPrefix(tblName, fcatalog.PrefixIndexTableName) {
		return moerr.NewInternalErrorNoCtx("cannot stop merging index table manually")
	}

	if _, ok := s.stoppedTables.m[tblEntry.GetID()]; ok {
		return moerr.NewInternalErrorNoCtxf("%s is already stopped", tblEntry.GetFullName())
	}

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
				s.stoppedTables.indexes[index.IndexTableName] = struct{}{}
			}
		}
	}

	s.stoppedTables.m[tblEntry.GetID()] = struct {
		time.Time
		indexes []string
	}{Time: time.Now(), indexes: indexTableNames}
	return nil
}

func (s *Scheduler) StartMerge(tblID uint64) {
	s.stoppedTables.Lock()
	defer s.stoppedTables.Unlock()
	for _, index := range s.stoppedTables.m[tblID].indexes {
		delete(s.stoppedTables.indexes, index)
	}
	delete(s.stoppedTables.m, tblID)
}

func objectValid(objectEntry *catalog.ObjectEntry) bool {
	if objectEntry.IsAppendable() {
		return false
	}
	if !objectEntry.IsActive() {
		return false
	}
	if !objectEntry.IsCommitted() {
		return false
	}
	if objectEntry.IsCreatingOrAborted() {
		return false
	}
	return true
}
