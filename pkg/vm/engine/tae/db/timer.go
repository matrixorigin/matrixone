package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type calibrationProcessor struct {
	*catalog.LoopProcessor
	db           *DB
	uncommitted  int
	lastCkpTS    uint64
	lastExecTime time.Time
}

func newCalibrationProcessor(db *DB) *calibrationProcessor {
	processor := &calibrationProcessor{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
		lastExecTime:  time.Now(),
	}
	processor.BlockFn = processor.onBlock
	return processor
}

func (processor *calibrationProcessor) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	segmentEntry.RLock()
	maxTs := segmentEntry.MaxCommittedTS()
	segmentEntry.RUnlock()
	if maxTs == txnif.UncommitTS {
		panic(maxTs)
	}
	// if maxTs > processor.maxCommittedTS {
	// 	processor.maxCommittedTS = maxTs
	// }
	return

}

func (processor *calibrationProcessor) onBlock(blockEntry *catalog.BlockEntry) (err error) {
	blockEntry.RLock()
	if !blockEntry.IsCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	if blockEntry.IsDroppedCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	blockEntry.RUnlock()
	data := blockEntry.GetBlockData()
	data.RunCalibration()
	score := data.EstimateScore()
	if score > 0 {
		processor.db.CKPDriver.EnqueueCheckpointUnit(data)
	}
	// blockEntry.RLock()
	// if blockEntry.IsDroppedCommitted() {
	// 	blockEntry.RUnlock()
	// 	return
	// }
	// blockEntry.RUnlock()
	// if data.EstimateScore() > 20 {
	// 	factory := tables.CompactBlockTaskFactory(blockEntry)
	// 	ctx := tasks.Context{Waitable: true}
	// 	task, _ := processor.db.TaskScheduler.ScheduleTxnTask(&ctx, factory)
	// 	err = task.WaitDone()
	// }
	return
}

type timedLooper struct {
	db        *DB
	processor *calibrationProcessor
}

func newTimedLooper(db *DB, processor *calibrationProcessor) *timedLooper {
	c := &timedLooper{
		processor: processor,
		db:        db,
	}
	return c
}

func (collector *timedLooper) OnStopped() {
	logutil.Infof("TimedLooper Stopped")
}

func (collector *timedLooper) OnExec() {
	collector.processor.lastCkpTS = collector.db.Catalog.GetCheckpointed()
	collector.processor.uncommitted = 0
	collector.db.Opts.Catalog.RecurLoop(collector.processor)
	if collector.processor.uncommitted == 0 {
		collector.processor.lastExecTime = time.Now()
	} else if collector.processor.uncommitted > 1 || time.Since(collector.processor.lastExecTime) > time.Duration(30)*time.Second {
		safeTs := collector.db.TxnMgr.StatSafeTS()
		closure := func(ts uint64) func() error {
			return func() error {
				return collector.db.Catalog.Checkpoint(ts)
			}
		}
		collector.db.Scheduler.ScheduleFn(nil, tasks.CheckpointWalTask, closure(safeTs))
		collector.processor.lastExecTime = time.Now()
	}
}
