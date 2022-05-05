package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
	db *DB
}

func newCalibrationOp(db *DB) *calibrationOp {
	processor := &calibrationOp{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
	}
	processor.BlockFn = processor.onBlock
	return processor
}

func (processor *calibrationOp) PreExecute() error  { return nil }
func (processor *calibrationOp) PostExecute() error { return nil }

func (processor *calibrationOp) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	segmentEntry.RLock()
	maxTs := segmentEntry.MaxCommittedTS()
	segmentEntry.RUnlock()
	if maxTs == txnif.UncommitTS {
		panic(maxTs)
	}
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
		logutil.Warnf("Catalog checkpoint schedule interval limit %s is too small|big and is changed to %s", intervalLimit, options.DefaultCatalogCkpInterval)
		intervalLimit = time.Duration(options.DefaultCatalogCkpInterval)
	}
	monitor := &catalogStatsMonitor{
		LoopProcessor: new(catalog.LoopProcessor),
		db:            db,
	}
	monitor.BlockFn = monitor.onBlock
	monitor.SegmentFn = monitor.onSegment
	monitor.TableFn = monitor.onTable
	monitor.DatabaseFn = monitor.onDatabase
	return monitor
}

func (monitor *catalogStatsMonitor) PreExecute() error {
	monitor.unCheckpointedCnt = 0
	monitor.minTs = monitor.db.Catalog.GetCheckpointed() + 1
	monitor.maxTs = monitor.db.TxnMgr.StatSafeTS()
	return nil
}

func (monitor *catalogStatsMonitor) PostExecute() error {
	logutil.Infof("[Monotor] Catalog Total Uncheckpointed Cnt [%d, %d]: %d", monitor.minTs, monitor.maxTs, monitor.unCheckpointedCnt)
	if monitor.unCheckpointedCnt >= monitor.cntLimit || time.Since(monitor.lastScheduleTime) >= monitor.intervalLimit {
		monitor.db.Scheduler.ScheduleScopedFn(nil, tasks.CheckpointCatalogTask, nil, monitor.db.Catalog.CheckpointClosure(monitor.maxTs))
		monitor.lastScheduleTime = time.Now()
	}
	return nil
}

func (monitor *catalogStatsMonitor) onBlock(entry *catalog.BlockEntry) (err error) {
	if monitor.minTs <= monitor.maxTs && catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
	}
	return
}

func (monitor *catalogStatsMonitor) onSegment(entry *catalog.SegmentEntry) (err error) {
	if monitor.minTs <= monitor.maxTs && catalog.CheckpointSelectOp(entry.BaseEntry, monitor.minTs, monitor.maxTs) {
		monitor.unCheckpointedCnt++
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
