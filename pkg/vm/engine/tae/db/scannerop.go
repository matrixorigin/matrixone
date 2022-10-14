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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
