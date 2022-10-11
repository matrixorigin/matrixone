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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

type calibrationOp struct {
	*catalog.LoopProcessor
	db              *DB
	blkCntOfSegment int
}

func newCalibrationOp(db *DB) *calibrationOp {
	processor := &calibrationOp{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
	}
	processor.TableFn = processor.onTable
	processor.BlockFn = processor.onBlock
	processor.SegmentFn = processor.onSegment
	processor.PostSegmentFn = processor.onPostSegment
	return processor
}

func (processor *calibrationOp) PreExecute() error  { return nil }
func (processor *calibrationOp) PostExecute() error { return nil }

func (processor *calibrationOp) onTable(tableEntry *catalog.TableEntry) (err error) {
	if !tableEntry.IsActive() {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}

func (processor *calibrationOp) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	if !segmentEntry.IsActive() {
		err = moerr.GetOkStopCurrRecur()
	}
	processor.blkCntOfSegment = 0
	return
}

func (processor *calibrationOp) onPostSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	if processor.blkCntOfSegment >= int(segmentEntry.GetTable().GetSchema().SegmentMaxBlocks) {
		segmentData := segmentEntry.GetSegmentData()
		taskFactory, taskType, scopes, err := segmentData.BuildCompactionTaskFactory()
		if err != nil || taskFactory == nil {
			logutil.Warnf("%s: %v", segmentData.MutationInfo(), err)
		} else {
			_, err = processor.db.Scheduler.ScheduleMultiScopedTxnTask(nil, taskType, scopes, taskFactory)
			logutil.Debugf("[Mergeblocks] | %s | Scheduled | State=%v | Scopes=%s", segmentEntry.String(), err, common.IDArraryString(scopes))
		}
	}
	processor.blkCntOfSegment = 0
	return
}

func (processor *calibrationOp) onBlock(blockEntry *catalog.BlockEntry) (err error) {
	if !blockEntry.IsActive() {
		// logutil.Debugf("Noop for block %s: table or db was dropped", blockEntry.Repr())
		processor.blkCntOfSegment = 0
		return
	}

	blockEntry.RLock()
	// 1. Skip uncommitted entries
	if !blockEntry.IsCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	if blockEntry.GetSegment().IsAppendable() && catalog.ActiveWithNoTxnFilter(blockEntry.MetaBaseEntry) && catalog.NonAppendableBlkFilter(blockEntry) {
		processor.blkCntOfSegment++
	}
	blockEntry.RUnlock()

	data := blockEntry.GetBlockData()

	// 3. Run calibration and estimate score for checkpoint
	if data.RunCalibration() > 0 {
		processor.db.CKPDriver.EnqueueCheckpointUnit(data)
	}
	return
}

type catalogStatsMonitor struct {
	*catalog.LoopProcessor
	db                 *DB
	unCheckpointedCnt  int64
	minTs              types.TS
	maxTs              types.TS
	gcTs               types.TS
	lastScheduleTime   time.Time
	cntLimit           int64
	intervalLimit      time.Duration
	lastStatsPrintTime time.Time
}

func newCatalogStatsMonitor(db *DB, cntLimit int64, intervalLimit time.Duration) *catalogStatsMonitor {
	if cntLimit <= 0 {
		logutil.Warnf("Catalog uncheckpoint cnt limit %d is too small and is changed to %d", cntLimit, options.DefaultCatalogUnCkpLimit)
		cntLimit = options.DefaultCatalogUnCkpLimit
	}
	if intervalLimit <= time.Duration(0) || intervalLimit >= time.Second*180 {
		logutil.Warnf("Catalog checkpoint schedule interval limit %d is too small|big and is changed to %d", intervalLimit, options.DefaultCatalogCkpInterval)
		intervalLimit = time.Millisecond * time.Duration(options.DefaultCatalogCkpInterval)
	}
	monitor := &catalogStatsMonitor{
		LoopProcessor:      new(catalog.LoopProcessor),
		db:                 db,
		intervalLimit:      intervalLimit,
		cntLimit:           cntLimit,
		lastStatsPrintTime: time.Now(),
	}
	monitor.BlockFn = monitor.onBlock
	monitor.SegmentFn = monitor.onSegment
	monitor.TableFn = monitor.onTable
	monitor.DatabaseFn = monitor.onDatabase
	return monitor
}

func (monitor *catalogStatsMonitor) PreExecute() error {
	monitor.unCheckpointedCnt = 0
	monitor.minTs = monitor.db.Catalog.GetCheckpointed().MaxTS.Next()
	monitor.maxTs = monitor.db.Scheduler.GetCheckpointTS()
	monitor.gcTs = monitor.db.Scheduler.GetGCTS()
	return nil
}

func (monitor *catalogStatsMonitor) PostExecute() error {
	if time.Since(monitor.lastStatsPrintTime) > time.Second {
		monitor.db.PrintStats()
		monitor.lastStatsPrintTime = time.Now()
	}
	if monitor.unCheckpointedCnt == 0 {
		monitor.lastScheduleTime = time.Now()
		return nil
	}
	if monitor.unCheckpointedCnt >= monitor.cntLimit || time.Since(monitor.lastScheduleTime) >= monitor.intervalLimit {
		logutil.Debugf("[Monotor] Catalog Total Uncheckpointed Cnt [%d, %d]: %d", monitor.minTs, monitor.maxTs, monitor.unCheckpointedCnt)
		// logutil.Info("Catalog Checkpoint Scheduled")
		_, err := monitor.db.Scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, nil, monitor.db.Catalog.CheckpointClosure(monitor.maxTs))
		if err != nil {
			return err
		}
		monitor.lastScheduleTime = time.Now()
	}
	return nil
}

func (monitor *catalogStatsMonitor) onBlock(entry *catalog.BlockEntry) (err error) {
	if monitor.minTs.LessEq(monitor.maxTs) && catalog.CheckpointSelectOp(entry.MetaBaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.HasDropCommittedLocked() && !entry.DeleteAfter(monitor.gcTs) {
		logIndex := entry.GetLogIndex()
		if logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if gcNeeded {
		scopes := MakeBlockScopes(entry)
		// _, err = monitor.db.Scheduler.ScheduleMultiScopedFn(nil, tasks.GCTask, scopes, gcBlockClosure(entry, GCType_Block))
		_, err = monitor.db.Scheduler.ScheduleFn(nil, tasks.GCTask, gcBlockClosure(entry, GCType_Block))
		logutil.Debugf("[GCBLK] | %s | Scheduled | Err=%v | Scopes=%s", entry.Repr(), err, common.IDArraryString(scopes))
		if err != nil {
			// if err == tasks.ErrScheduleScopeConflict {
			// 	logutil.Debugf("Schedule | [GC BLK] | %s | Err=%s | Scopes=%s", entry.String(), err, scopes)
			// }
			err = nil
		}
	} else {
		blkData := entry.GetBlockData()
		ts, terminated := entry.GetTerminationTS()
		if terminated && blkData.GetMaxCheckpointTS().Less(ts) {
			_, err = monitor.db.Scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, entry.AsCommonID(), blkData.CheckpointWALClosure(ts))
			if err != nil {
				logutil.Warnf("CheckpointWALClosure %s: %v", entry.Repr(), err)
				err = nil
			}
		}
	}
	return
}

func (monitor *catalogStatsMonitor) onSegment(entry *catalog.SegmentEntry) (err error) {
	if monitor.minTs.LessEq(monitor.maxTs) && catalog.CheckpointSelectOp(entry.MetaBaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.HasDropCommittedLocked() && !entry.DeleteAfter(monitor.gcTs) {
		logIndex := entry.GetLogIndex()
		if logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if gcNeeded {
		scopes := MakeSegmentScopes(entry)
		_, err = monitor.db.Scheduler.ScheduleFn(nil, tasks.GCTask, gcSegmentClosure(entry, GCType_Segment))
		logutil.Debugf("[GCSEG] | %s | Scheduled | Err=%v | Scopes=%s", entry.Repr(), err, common.IDArraryString(scopes))
		if err != nil {
			// if err != tasks.ErrScheduleScopeConflict {
			// logutil.Warnf("Schedule | [GC] | %s | Err=%s", entry.String(), err)
			// }
			err = nil
		} else {
			err = moerr.GetOkStopCurrRecur()
		}
	}
	return
}

func (monitor *catalogStatsMonitor) onTable(entry *catalog.TableEntry) (err error) {
	if monitor.minTs.LessEq(monitor.maxTs) && catalog.CheckpointSelectOp(entry.TableBaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.HasDropCommittedLocked() && !entry.DeleteAfter(monitor.gcTs) {
		if logIndex := entry.GetLogIndex(); logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if !gcNeeded {
		return
	}

	scopes := MakeTableScopes(entry)
	// _, err = monitor.db.Scheduler.ScheduleMultiScopedFn(nil, tasks.GCTask, scopes, gcTableClosure(entry, GCType_Table))
	_, err = monitor.db.Scheduler.ScheduleFn(nil, tasks.GCTask, gcTableClosure(entry, GCType_Table))
	logutil.Debugf("[GCTABLE] | %s | Scheduled | Err=%v | Scopes=%s", entry.String(), err, common.IDArraryString(scopes))
	if err != nil {
		err = nil
	} else {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}

func (monitor *catalogStatsMonitor) onDatabase(entry *catalog.DBEntry) (err error) {
	if monitor.minTs.LessEq(monitor.maxTs) && catalog.CheckpointSelectOp(entry.DBBaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.HasDropCommittedLocked() && !entry.DeleteAfter(monitor.gcTs) {
		if logIndex := entry.GetLogIndex(); logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if !gcNeeded {
		return
	}

	scopes := MakeDBScopes(entry)
	_, err = monitor.db.Scheduler.ScheduleFn(nil, tasks.GCTask, gcDatabaseClosure(entry))
	// _, err = monitor.db.Scheduler.ScheduleMultiScopedFn(nil, tasks.GCTask, scopes, gcDatabaseClosure(entry))
	logutil.Debugf("[GCDB] | %s | Scheduled | Err=%v | Scopes=%s", entry.String(), err, common.IDArraryString(scopes))
	if err != nil {
		err = nil
	} else {
		err = moerr.GetOkStopCurrRecur()
	}
	return
}
