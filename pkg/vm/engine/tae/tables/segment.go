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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type dataSegment struct {
	common.ClosedState
	meta       *catalog.SegmentEntry
	scheduler  tasks.TaskScheduler
	indexCache model.LRUCache
}

func newSegment(
	meta *catalog.SegmentEntry,
	indexCache model.LRUCache,
	dir string) *dataSegment {
	seg := &dataSegment{
		meta:       meta,
		indexCache: indexCache,
		scheduler:  meta.GetScheduler(),
	}
	return seg
}

func (segment *dataSegment) Destroy() (err error) {
	if !segment.TryClose() {
		return
	}
	return
}

func (segment *dataSegment) GetID() uint64 { panic("not support") }

func (segment *dataSegment) BatchDedup(txn txnif.AsyncTxn, pks containers.Vector) (err error) {
	// TODO: segment level index
	return moerr.GetOkExpectedPossibleDup()
	// blkIt := segment.meta.MakeBlockIt(false)
	// for blkIt.Valid() {
	// 	block := blkIt.Get().GetPayload().(*catalog.BlockEntry)
	// 	if err = block.GetBlockData().BatchDedup(txn, pks); err != nil {
	// 		return
	// 	}
	// 	blkIt.Next()
	// }
	// return nil
}

func (segment *dataSegment) MutationInfo() string { return "" }

func (segment *dataSegment) RunCalibration() int                                  { return 0 }
func (segment *dataSegment) EstimateScore(interval time.Duration, force bool) int { return 0 }

func (segment *dataSegment) BuildCompactionTaskFactory() (factory tasks.TxnTaskFactory, taskType tasks.TaskType, scopes []common.ID, err error) {
	if segment.meta.IsAppendable() {
		segment.meta.RLock()
		dropped := segment.meta.HasDropCommittedLocked()
		inTxn := segment.meta.IsCreatingOrAborted()
		segment.meta.RUnlock()
		if dropped || inTxn {
			return
		}
		filter := catalog.NewComposedFilter()
		filter.AddBlockFilter(catalog.NonAppendableBlkFilter)
		filter.AddCommitFilter(catalog.ActiveWithNoTxnFilter)
		blks := segment.meta.CollectBlockEntries(filter.FilteCommit, filter.FilteBlock)
		if len(blks) < int(segment.meta.GetTable().GetLastestSchema().SegmentMaxBlocks) {
			return
		}
		for _, blk := range blks {
			scopes = append(scopes, *blk.AsCommonID())
		}
		factory = jobs.CompactSegmentTaskFactory(blks, segment.scheduler)
		taskType = tasks.DataCompactionTask
		return
	}
	return
}
