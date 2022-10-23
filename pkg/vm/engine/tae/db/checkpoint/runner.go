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

package checkpoint

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type blockVisitor struct {
	common.NoopTreeVisitor
	blockFn func(uint64, uint64, uint64, uint64) error
}

func (visitor *blockVisitor) VisitBlock(dbID, tableID, segmentID, id uint64) (err error) {
	if visitor.blockFn != nil {
		err = visitor.blockFn(dbID, tableID, segmentID, id)
	}
	return
}

type runner struct {
	options struct {
		collectInterval     time.Duration
		maxFlushInterval    time.Duration
		dirtyEntryQueueSize int
		waitQueueSize       int
	}

	source    logtail.Collector
	catalog   *catalog.Catalog
	scheduler tasks.TaskScheduler

	stopper *stopper.Stopper

	dirtyEntryQueue sm.Queue
	waitQueue       sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewRunner(
	catalog *catalog.Catalog,
	scheduler tasks.TaskScheduler,
	source logtail.Collector,
	opts ...Option) *runner {
	r := &runner{
		catalog:   catalog,
		scheduler: scheduler,
		source:    source,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()
	r.stopper = stopper.NewStopper("CheckpointRunner")
	r.dirtyEntryQueue = sm.NewSafeQueue(r.options.dirtyEntryQueueSize, 100, r.onDirtyEntries)
	r.waitQueue = sm.NewSafeQueue(r.options.waitQueueSize, 100, r.onWaitWaitableItems)
	return r
}

func (r *runner) fillDefaults() {
	if r.options.collectInterval <= 0 {
		// TODO: define default value
		r.options.collectInterval = time.Second * 5
	}
	if r.options.dirtyEntryQueueSize <= 0 {
		r.options.dirtyEntryQueueSize = 10000
	}
	if r.options.waitQueueSize <= 1000 {
		r.options.waitQueueSize = 1000
	}
}

func (r *runner) tryCompactBlock(dbID, tableID, segmentID, id uint64) (err error) {
	db, err := r.catalog.GetDatabaseByID(dbID)
	if err != nil {
		panic(err)
	}
	table, err := db.GetTableEntryByID(tableID)
	if err != nil {
		panic(err)
	}
	segment, err := table.GetSegmentByID(segmentID)
	if err != nil {
		panic(err)
	}
	blk, err := segment.GetBlockEntryByID(id)
	if err != nil {
		panic(err)
	}
	blkData := blk.GetBlockData()
	score := blkData.EstimateScore(r.options.maxFlushInterval)
	logutil.Infof("%s [SCORE=%d]", blk.String(), score)
	if score < 100 {
		return
	}

	factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
	if err != nil || factory == nil {
		logutil.Warnf("%s: %v", blkData.MutationInfo(), err)
		return nil
	}

	if _, err = r.scheduler.ScheduleMultiScopedTxnTask(nil, taskType, scopes, factory); err != nil {
		logutil.Warnf("%s: %v", blkData.MutationInfo(), err)
	}

	// always return nil
	return nil
}

func (r *runner) onWaitWaitableItems(items ...any) {
	// TODO: change for more waitable items
	start := time.Now()
	for _, item := range items {
		ckpEntry := item.(wal.LogEntry)
		err := ckpEntry.WaitDone()
		if err != nil {
			panic(err)
		}
		ckpEntry.Free()
	}
	logutil.Debugf("Total [%d] WAL Checkpointed | [%s]", len(items), time.Since(start))
}

func (r *runner) onDirtyEntries(entries ...any) {
	merged := logtail.NewEmptyDirtyTreeEntry()
	for _, entry := range entries {
		e := entry.(*logtail.DirtyTreeEntry)
		merged.Merge(e)
	}
	if merged.IsEmpty() {
		return
	}
	logutil.Infof(merged.String())
	visitor := new(blockVisitor)
	visitor.blockFn = r.tryCompactBlock

	if err := merged.GetTree().Visit(visitor); err != nil {
		panic(err)
	}
}

func (r *runner) cronCollect(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(r.options.collectInterval, func() {
		r.source.Run()
		entry := r.source.GetAndRefreshMerged()
		if entry.IsEmpty() {
			logutil.Info("No dirty block found")
			return
		}
		r.dirtyEntryQueue.Enqueue(entry)
	}, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}

func (r *runner) EnqueueWait(item any) (err error) {
	_, err = r.waitQueue.Enqueue(item)
	return
}

func (r *runner) Start() {
	r.onceStart.Do(func() {
		r.waitQueue.Start()
		r.dirtyEntryQueue.Start()
		if err := r.stopper.RunNamedTask("dirty-collector-job", r.cronCollect); err != nil {
			panic(err)
		}
	})
}

func (r *runner) Stop() {
	r.onceStop.Do(func() {
		r.stopper.Stop()
		r.dirtyEntryQueue.Stop()
		r.waitQueue.Stop()
	})
}
