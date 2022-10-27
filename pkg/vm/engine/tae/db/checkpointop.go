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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type catalogCheckpointer struct {
	*catalog.LoopProcessor
	db               *DB
	cnt              int64
	minTs            types.TS
	maxTs            types.TS
	cntLimit         int64
	intervalLimit    time.Duration
	lastScheduleTime time.Time
}

func newCatalogCheckpointer(
	db *DB,
	cntLimit int64,
	intervalLimit time.Duration) *catalogCheckpointer {
	ckp := &catalogCheckpointer{
		LoopProcessor:    new(catalog.LoopProcessor),
		db:               db,
		cntLimit:         cntLimit,
		intervalLimit:    intervalLimit,
		lastScheduleTime: time.Now(),
	}
	ckp.BlockFn = ckp.onBlock
	ckp.SegmentFn = ckp.onSegment
	ckp.TableFn = ckp.onTable
	ckp.DatabaseFn = ckp.onDatabase
	return ckp
}

func (ckp *catalogCheckpointer) PreExecute() (err error) {
	ckp.cnt = 0
	ckp.minTs = ckp.db.Catalog.GetCheckpointed().MaxTS.Next()
	ckp.maxTs = ckp.db.Scheduler.GetCheckpointTS()
	return
}

func (ckp *catalogCheckpointer) PostExecute() (err error) {
	// If no committed in between, refresh schedule time and quit quickly
	if ckp.cnt == 0 {
		ckp.lastScheduleTime = time.Now()
		return
	}

	// If the committed cnt is less than the limit and the last schedule
	// time is more than interval limit, quit quickly
	if ckp.cnt < ckp.cntLimit &&
		time.Since(ckp.lastScheduleTime) >= ckp.intervalLimit {
		return
	}

	logutil.Debugf("[CatalogCheckpointer] Catalog Total Uncheckpointed Cnt [%d, %d]: %d",
		ckp.minTs,
		ckp.maxTs,
		ckp.cnt)
	if _, err = ckp.db.Scheduler.ScheduleScopedFn(
		nil,
		tasks.CheckpointTask,
		nil,
		ckp.db.Catalog.CheckpointClosure(ckp.maxTs)); err != nil {
		return
	}
	ckp.lastScheduleTime = time.Now()

	return
}

func (ckp *catalogCheckpointer) onBlock(entry *catalog.BlockEntry) (err error) {
	if catalog.CheckpointSelectOp(entry.MetaBaseEntry, ckp.minTs, ckp.maxTs) {
		ckp.cnt++
	}
	return
}

func (ckp *catalogCheckpointer) onSegment(entry *catalog.SegmentEntry) (err error) {
	if catalog.CheckpointSelectOp(entry.MetaBaseEntry, ckp.minTs, ckp.maxTs) {
		ckp.cnt++
	}
	return
}

func (ckp *catalogCheckpointer) onTable(entry *catalog.TableEntry) (err error) {
	if catalog.CheckpointSelectOp(entry.TableBaseEntry, ckp.minTs, ckp.maxTs) {
		ckp.cnt++
	}
	return
}

func (ckp *catalogCheckpointer) onDatabase(entry *catalog.DBEntry) (err error) {
	if catalog.CheckpointSelectOp(entry.DBBaseEntry, ckp.minTs, ckp.maxTs) {
		ckp.cnt++
	}
	return
}
