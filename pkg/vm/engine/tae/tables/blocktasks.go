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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func (blk *dataBlock) CheckpointWALClosure(currTs types.TS) tasks.FuncT {
	return func() error {
		return blk.CheckpointWAL(currTs)
	}
}

func (blk *dataBlock) CheckpointWAL(currTs types.TS) (err error) {
	if blk.meta.IsAppendable() {
		return blk.ABlkCheckpointWAL(currTs)
	}
	return blk.BlkCheckpointWAL(currTs)
}

func (blk *dataBlock) BlkCheckpointWAL(currTs types.TS) (err error) {
	defer func() {
		logutil.Info("[Done]", common.ReprerField("blk", blk.meta),
			common.OperationField("ckp-wal"),
			common.ErrorField(err),
			common.AnyField("curr", currTs))
	}()
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Info("[Start]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.AnyField("ckped", ckpTs),
		common.AnyField("curr", currTs))
	if currTs.LessEq(ckpTs) {
		return
	}
	view, err := blk.CollectChangesInRange(ckpTs.Next(), currTs)
	if err != nil {
		return
	}
	cnt := 0
	for _, column := range view.Columns {
		idxes := column.LogIndexes
		cnt += len(idxes)
		if err = blk.scheduler.Checkpoint(idxes); err != nil {
			return
		}
		// for _, index := range idxes {
		// 	logutil.Infof("Ckp2Index  %s", index.String())
		// }
	}
	if err = blk.scheduler.Checkpoint(view.DeleteLogIndexes); err != nil {
		return
	}
	blk.SetMaxCheckpointTS(currTs)
	return
}

func (blk *dataBlock) ABlkCheckpointWAL(currTs types.TS) (err error) {
	defer func() {
		if err != nil {
			logutil.Warn("[Done]", common.ReprerField("blk", blk.meta),
				common.OperationField("ckp-wal"),
				common.ErrorField(err))
		}
	}()
	ckpTs := blk.GetMaxCheckpointTS()
	logutil.Info("[Start]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.AnyField("ckpTs", ckpTs),
		common.AnyField("curr", currTs))
	if currTs.LessEq(ckpTs) {
		return
	}
	indexes, err := blk.CollectAppendLogIndexes(ckpTs.Next(), currTs)
	if err != nil {
		return
	}
	//view, err := blk.CollectChangesInRange(ckpTs+1, currTs+1)
	view, err := blk.CollectChangesInRange(ckpTs.Next(), currTs.Next())
	if err != nil {
		return
	}
	for _, column := range view.Columns {
		idxes := column.LogIndexes
		if err = blk.scheduler.Checkpoint(idxes); err != nil {
			return
		}
		// for _, index := range idxes {
		// 	logutil.Infof("Ckp1Index  %s", index.String())
		// }
	}
	if err = blk.scheduler.Checkpoint(indexes); err != nil {
		return
	}
	if err = blk.scheduler.Checkpoint(view.DeleteLogIndexes); err != nil {
		return
	}
	logutil.Info("[Done]", common.ReprerField("blk", blk.meta),
		common.OperationField("ckp-wal"),
		common.CountField(len(indexes)))
	// for _, index := range indexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	// for _, index := range view.DeleteLogIndexes {
	// 	logutil.Infof("Ckp1Index  %s", index.String())
	// }
	blk.SetMaxCheckpointTS(currTs)
	return
}
