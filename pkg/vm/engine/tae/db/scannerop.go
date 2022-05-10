package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	safeTs          uint64
}

func newCalibrationOp(db *DB) *calibrationOp {
	processor := &calibrationOp{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
	}
	processor.BlockFn = processor.onBlock
	processor.SegmentFn = processor.onSegment
	processor.PostSegmentFn = processor.onPostSegment
	return processor
}

func (processor *calibrationOp) PreExecute() error  { return nil }
func (processor *calibrationOp) PostExecute() error { return nil }

func (processor *calibrationOp) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	processor.blkCntOfSegment = 0
	return
}

func (processor *calibrationOp) onPostSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	if processor.blkCntOfSegment >= int(segmentEntry.GetTable().GetSchema().SegmentMaxBlocks) {
		segmentData := segmentEntry.GetSegmentData()
		taskFactory, taskType, scopes, err := segmentData.BuildCompactionTaskFactory()
		if err != nil || taskFactory == nil {
			logutil.Warnf("%s: %v", segmentData.MutationInfo(), err)
		}
		processor.db.Scheduler.ScheduleMultiScopedTxnTask(nil, taskType, scopes, taskFactory)
		logutil.Infof("Mergeblocks %s was scheduled", segmentEntry.String())
	}
	processor.blkCntOfSegment = 0
	return
}

func (processor *calibrationOp) onBlock(blockEntry *catalog.BlockEntry) (err error) {
	blockEntry.RLock()
	// 1. Skip uncommitted entries
	if !blockEntry.IsCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	// 2. Skip committed dropped entries
	if blockEntry.IsDroppedCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	if blockEntry.GetSegment().IsAppendable() && catalog.ActiveWithNoTxnFilter(blockEntry.BaseEntry) && catalog.NonAppendableBlkFilter(blockEntry) {
		processor.blkCntOfSegment++
	}
	blockEntry.RUnlock()

	data := blockEntry.GetBlockData()

	// 3. Run calibration and estimate score for checkpoint
	data.RunCalibration()
	score := data.EstimateScore()
	if score > 0 {
		processor.db.CKPDriver.EnqueueCheckpointUnit(data)
	}
	return
}

type catalogStatsMonitor struct {
	*catalog.LoopProcessor
	db                *DB
	unCheckpointedCnt int64
	minTs             uint64
	maxTs             uint64
	lastScheduleTime  time.Time
	cntLimit          int64
	intervalLimit     time.Duration
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
		LoopProcessor: new(catalog.LoopProcessor),
		db:            db,
		intervalLimit: intervalLimit,
		cntLimit:      cntLimit,
	}
	monitor.BlockFn = monitor.onBlock
	monitor.SegmentFn = monitor.onSegment
	monitor.TableFn = monitor.onTable
	monitor.DatabaseFn = monitor.onDatabase
	return monitor
}

func (monitor *catalogStatsMonitor) PreExecute() error {
	monitor.unCheckpointedCnt = 0
	monitor.minTs = monitor.db.Catalog.GetCheckpointed().MaxTS + 1
	monitor.maxTs = monitor.db.Scheduler.GetSafeTS()
	return nil
}

func (monitor *catalogStatsMonitor) PostExecute() error {
	if monitor.unCheckpointedCnt == 0 {
		monitor.lastScheduleTime = time.Now()
		return nil
	}
	if monitor.unCheckpointedCnt >= monitor.cntLimit || time.Since(monitor.lastScheduleTime) >= monitor.intervalLimit {
		logutil.Infof("[Monotor] Catalog Total Uncheckpointed Cnt [%d, %d]: %d", monitor.minTs, monitor.maxTs, monitor.unCheckpointedCnt)
		logutil.Info("Catalog Checkpoint Scheduled")
		monitor.db.Scheduler.ScheduleScopedFn(nil, tasks.CheckpointTask, nil, monitor.db.Catalog.CheckpointClosure(monitor.maxTs))
		monitor.lastScheduleTime = time.Now()
	}
	return nil
}

func (monitor *catalogStatsMonitor) onBlock(entry *catalog.BlockEntry) (err error) {
	if monitor.minTs <= monitor.maxTs && catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.IsDroppedCommitted() && !entry.DeleteAfter(monitor.maxTs) {
		logIndex := entry.GetLogIndex()
		if logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if gcNeeded {
		scopes := MakeBlockScopes(entry)
		_, err = monitor.db.Scheduler.ScheduleMultiScopedFn(nil, tasks.GCTask, scopes, gcBlockClosure(entry))
		if err != nil {
			if err != tasks.ErrScheduleScopeConflict {
				logutil.Warnf("Schedule | [GC] | %s | Err=%s", entry.String(), err)
			}
			err = nil
		}
	}
	return
}

func (monitor *catalogStatsMonitor) onSegment(entry *catalog.SegmentEntry) (err error) {
	if monitor.minTs <= monitor.maxTs && catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
		return
	}
	checkpointed := monitor.db.Scheduler.GetCheckpointedLSN()
	gcNeeded := false
	entry.RLock()
	if entry.IsDroppedCommitted() {
		logIndex := entry.GetLogIndex()
		if logIndex != nil {
			gcNeeded = checkpointed >= logIndex.LSN
		}
	}
	entry.RUnlock()
	if gcNeeded {
		scopes := MakeSegmentScopes(entry)
		_, err = monitor.db.Scheduler.ScheduleMultiScopedFn(nil, tasks.GCTask, scopes, gcSegmentClosure(entry))
		if err != nil {
			if err != tasks.ErrScheduleScopeConflict {
				logutil.Warnf("Schedule | [GC] | %s | Err=%s", entry.String(), err)
			}
			err = nil
		}
		err = catalog.ErrStopCurrRecur
	}
	return
}

func (monitor *catalogStatsMonitor) onTable(entry *catalog.TableEntry) (err error) {
	if monitor.minTs <= monitor.maxTs && catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
	}
	return
}

func (monitor *catalogStatsMonitor) onDatabase(entry *catalog.DBEntry) (err error) {
	if catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
	}
	return
}
