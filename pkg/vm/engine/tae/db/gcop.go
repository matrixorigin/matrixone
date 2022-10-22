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
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

// type GCType int16

// const (
// 	GCType_Block GCType = iota
// 	GCType_Segment
// 	GCType_Table
// 	GCType_DB
// )

const (
	GCState_Active int32 = iota
	GCState_Noop
	GCState_Scheduled
	GCState_ScheduledDone
)

/*

// Destroy is not thread-safe
func gcBlockClosure(entry *catalog.BlockEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		logutil.Debugf("[GCBLK] | %s | Started", entry.Repr())
		defer func() {
			if err == nil {
				logutil.Debugf("[GCBLK] | %s | Removed", entry.Repr())
			} else {
				logutil.Warnf("Cannot remove block %s, maybe removed before", entry.String())
			}
		}()
		segment := entry.GetSegment()

		if err = entry.DestroyData(); err != nil {
			return
		}
		// For appendable segment, keep all soft-deleted blocks until the segment is soft-deleted
		if gct == GCType_Block && entry.IsAppendable() {
			return
		}
		err = segment.RemoveEntry(entry)
		return
	}
}

// Destroy is not thread-safe
func gcSegmentClosure(entry *catalog.SegmentEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCSEG] | %s | Started", entry.Repr())
		defer func() {
			if err != nil {
				logutil.Warnf("Cannot remove segment %s, maybe removed before: %v", entry.String(), err)
			} else {
				logutil.Debugf("[GCSEG] | %s | BLKS=%s | Removed", entry.Repr(), common.IDArraryString(scopes))
			}
		}()
		table := entry.GetTable()
		it := entry.MakeBlockIt(false)
		for it.Valid() {
			blk := it.Get().GetPayload()
			scopes = append(scopes, *blk.AsCommonID())
			err = gcBlockClosure(blk, gct)()
			if err != nil {
				return
			}
			it.Next()
		}
		if err = entry.DestroyData(); err != nil {
			return
		}
		err = table.RemoveEntry(entry)
		return
	}
}

// TODO
func gcTableClosure(entry *catalog.TableEntry, gct GCType) tasks.FuncT {
	return func() (err error) {
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCTABLE] | %s | Started", entry.String())
		defer func() {
			logutil.Debugf("[GCTABLE] | %s | Ended: %v | SEGS=%s", entry.String(), err, common.IDArraryString(scopes))
		}()
		dbEntry := entry.GetDB()
		it := entry.MakeSegmentIt(false)
		for it.Valid() {
			seg := it.Get().GetPayload()
			scopes = append(scopes, *seg.AsCommonID())
			if err = gcSegmentClosure(seg, gct)(); err != nil {
				return
			}
			it.Next()
		}
		err = dbEntry.RemoveEntry(entry)
		return
	}
}

// TODO
func gcDatabaseClosure(entry *catalog.DBEntry) tasks.FuncT {
	return func() (err error) {
		return
		scopes := make([]common.ID, 0)
		logutil.Debugf("[GCDB] | %s | Started", entry.String())
		defer func() {
			logutil.Debugf("[GCDB] | %s | Ended: %v | TABLES=%s", entry.String(), err, common.IDArraryString(scopes))
		}()
		it := entry.MakeTableIt(false)
		for it.Valid() {
			table := it.Get().GetPayload()
			scopes = append(scopes, *table.AsCommonID())
			if err = gcTableClosure(table, GCType_DB)(); err != nil {
				return
			}
			it.Next()
		}
		err = entry.GetCatalog().RemoveEntry(entry)
		return
	}
}
*/

type gcCandidates struct {
	blocks   *common.Tree
	segments *common.Tree
	tables   *common.Tree
	dbs      map[uint64]bool
}

func newGCCandidates() *gcCandidates {
	return &gcCandidates{
		blocks:   common.NewTree(),
		segments: common.NewTree(),
		tables:   common.NewTree(),
		dbs:      make(map[uint64]bool),
	}
}

func (candidates *gcCandidates) Reset() {
	candidates.blocks.Reset()
	candidates.segments.Reset()
	candidates.tables.Reset()
	candidates.dbs = make(map[uint64]bool)
}

func (candidates *gcCandidates) IsEmpty() bool {
	if !candidates.blocks.IsEmpty() {
		return false
	}
	if !candidates.segments.IsEmpty() {
		return false
	}
	if !candidates.tables.IsEmpty() {
		return false
	}
	return len(candidates.dbs) == 0
}

func (candidates *gcCandidates) AddBlock(dbId, tableId, segmentId, blockId uint64) {
	candidates.blocks.AddBlock(dbId, tableId, segmentId, blockId)
}

func (candidates *gcCandidates) AddSegment(dbId, tableId, segmentId uint64) {
	candidates.segments.AddSegment(dbId, tableId, segmentId)
}

func (candidates *gcCandidates) AddTable(dbId, tableId uint64) {
	candidates.tables.AddTable(dbId, tableId)
}

func (candidates *gcCandidates) AddDB(dbId uint64) {
	candidates.dbs[dbId] = true
}

func (candidates *gcCandidates) String() string {
	if candidates.IsEmpty() {
		return ""
	}
	var w bytes.Buffer
	if len(candidates.dbs) != 0 {
		_, _ = w.WriteString("DB TO GC:[")
		for id := range candidates.dbs {
			_, _ = w.WriteString(fmt.Sprintf(" %d", id))
		}
		_, _ = w.WriteString("]\n")
	}
	if !candidates.tables.IsEmpty() {
		_, _ = w.WriteString("TABLE TO GC:[")
		_, _ = w.WriteString(candidates.tables.String())
		_, _ = w.WriteString("]\n")
	}
	if !candidates.segments.IsEmpty() {
		_, _ = w.WriteString("SEMENT TO GC:[")
		_, _ = w.WriteString(candidates.segments.String())
		_, _ = w.WriteString("]\n")
	}
	if !candidates.blocks.IsEmpty() {
		_, _ = w.WriteString("BLOCK TO GC:[")
		_, _ = w.WriteString(candidates.blocks.String())
		_, _ = w.WriteString("]\n")
	}
	return w.String()
}

type garbageCollector struct {
	*catalog.LoopProcessor
	db              *DB
	state           atomic.Int32
	loopState       int32
	epoch           types.TS
	checkpointedLsn uint64
	clock           *types.TsAlloctor
	candidates      *gcCandidates
	minInterval     time.Duration
	lastRunTime     time.Time
}

func newGarbageCollector(
	db *DB,
	minInterval time.Duration) *garbageCollector {
	ckp := &garbageCollector{
		LoopProcessor: new(catalog.LoopProcessor),
		db:            db,
		minInterval:   minInterval,
		clock:         types.NewTsAlloctor(db.Opts.Clock),
		candidates:    newGCCandidates(),
	}
	ckp.BlockFn = ckp.onBlock
	ckp.SegmentFn = ckp.onSegment
	ckp.TableFn = ckp.onTable
	ckp.DatabaseFn = ckp.onDatabase
	ckp.refreshEpoch()
	ckp.ResetState()
	return ckp
}

func (ckp *garbageCollector) ResetState() {
	ckp.state.Store(GCState_Active)
}

func (ckp *garbageCollector) StopSchedule() {
	ckp.state.Store(GCState_ScheduledDone)
	// logutil.Infof("Stop Schedule GCJOB")
}

func (ckp *garbageCollector) StartSchedule() {
	ckp.state.Store(GCState_Scheduled)
	// logutil.Infof("Start Schedule GCJOB")
}

func (ckp *garbageCollector) refreshEpoch() {
	ckp.epoch = types.BuildTS(time.Now().UTC().UnixNano()-ckp.minInterval.Nanoseconds(), 0)
}

func (ckp *garbageCollector) PreExecute() (err error) {
	// logutil.Infof("GCJOB INV=%d, ACT INV=%s", ckp.minInterval.Milliseconds()/2, time.Since(ckp.lastRunTime))
	ckp.checkpointedLsn = ckp.db.Scheduler.GetCheckpointedLSN()
	ckp.loopState = ckp.state.Load()
	// If scheduled done, we need to refresh a new gc epoch
	if ckp.loopState == GCState_ScheduledDone {
		ckp.refreshRunTime()
		ckp.refreshEpoch()
		ckp.ResetState()
		ckp.loopState = GCState_Active
	}
	// If state is active and the interval since last run time is below a limit. Skip this loop
	if ckp.canRun() && time.Since(ckp.lastRunTime) < ckp.minInterval/2 {
		ckp.loopState = GCState_Noop
		// logutil.Infof("Start Noop GCJOB")
	}
	if ckp.canRun() {
		ckp.refreshRunTime()
		// logutil.Infof("Start Run GCJOB")
	}
	return
}

func (ckp *garbageCollector) refreshRunTime() {
	ckp.lastRunTime = time.Now()
}

func (ckp *garbageCollector) PostExecute() (err error) {
	if !ckp.canRun() {
		return
	}
	if ckp.candidates.IsEmpty() {
		ckp.refreshEpoch()
	} else {
		// logutil.Infof("Epoch: %s", ckp.epoch.ToString())
		ckp.db.PrintStats()
		ckp.StartSchedule()
		_, err = ckp.db.Scheduler.ScheduleFn(
			nil,
			tasks.GCTask,
			func() error {
				defer ckp.StopSchedule()
				// logutil.Info(ckp.candidates.String())
				// TODO: GC all candidates
				ckp.candidates.Reset()
				return nil
			})
	}
	return
}

func (ckp *garbageCollector) canRun() bool {
	return ckp.loopState == GCState_Active
}

func (ckp *garbageCollector) isEntryCheckpointed(entry catalog.BaseEntry) bool {
	node := entry.GetLatestNodeLocked()
	index := node.GetLogIndex()
	if index == nil {
		return false
	}
	if index.LSN <= ckp.checkpointedLsn {
		return true
	}
	return false
}

func (ckp *garbageCollector) isCandidate(
	entry catalog.BaseEntry,
	terminated bool,
	rwlocker *sync.RWMutex) (ok bool) {
	entry.RLock()
	defer entry.RUnlock()
	ok = false
	if terminated {
		ok = ckp.isEntryCheckpointed(entry)
		return
	}
	if !entry.HasDropCommittedLocked() {
		return
	}
	if visible, _ := entry.IsVisible(ckp.epoch, rwlocker); visible {
		return
	}
	ok = ckp.isEntryCheckpointed(entry)
	return
}

func (ckp *garbageCollector) onBlock(entry *catalog.BlockEntry) (err error) {
	if !ckp.canRun() {
		return
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(ckp.epoch) {
			terminated = false
		}
	}
	if ckp.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
		id := entry.AsCommonID()
		ckp.candidates.AddBlock(entry.GetSegment().GetTable().GetDB().ID,
			id.TableID,
			id.SegmentID,
			id.BlockID)
	}
	return
}

func (ckp *garbageCollector) onSegment(entry *catalog.SegmentEntry) (err error) {
	if !ckp.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(ckp.epoch) {
			terminated = false
		}
	}
	if ckp.isCandidate(entry.MetaBaseEntry, terminated, entry.RWMutex) {
		id := entry.AsCommonID()
		ckp.candidates.AddSegment(entry.GetTable().GetDB().ID,
			id.TableID,
			id.SegmentID)
	}
	return
}

func (ckp *garbageCollector) onTable(entry *catalog.TableEntry) (err error) {
	if !ckp.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	var ts types.TS
	var terminated bool
	if ts, terminated = entry.GetTerminationTS(); terminated {
		if ts.Greater(ckp.epoch) {
			terminated = false
		}
	}
	if ckp.isCandidate(entry.TableBaseEntry, terminated, entry.RWMutex) {
		ckp.candidates.AddTable(entry.GetDB().ID, entry.ID)
	}
	return
}

func (ckp *garbageCollector) onDatabase(entry *catalog.DBEntry) (err error) {
	if !ckp.canRun() {
		return moerr.GetOkStopCurrRecur()
	}
	if ckp.isCandidate(entry.DBBaseEntry, false, entry.RWMutex) {
		ckp.candidates.AddDB(entry.ID)
	}
	return
}
