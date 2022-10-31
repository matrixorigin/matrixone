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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"github.com/tidwall/btree"
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

type timeBasedPolicy struct {
	interval time.Duration
}

func (p *timeBasedPolicy) Check(last types.TS) bool {
	physical := last.Physical()
	return physical <= time.Now().UTC().UnixNano()-p.interval.Nanoseconds()
}

type runner struct {
	options struct {
		collectInterval        time.Duration
		maxFlushInterval       time.Duration
		minIncrementalInterval time.Duration
		minGlobalInterval      time.Duration

		dirtyEntryQueueSize int
		waitQueueSize       int
		checkpointQueueSize int
		minCount            int
	}

	source    logtail.Collector
	catalog   *catalog.Catalog
	scheduler tasks.TaskScheduler
	fs        *objectio.ObjectFS
	observers *observers

	stopper *stopper.Stopper

	storage struct {
		sync.RWMutex
		entries    *btree.BTreeG[*CheckpointEntry]
		prevGlobal *CheckpointEntry
	}

	incrementalPolicy *timeBasedPolicy
	globalPolicy      *timeBasedPolicy

	dirtyEntryQueue     sm.Queue
	waitQueue           sm.Queue
	checkpointQueue     sm.Queue
	postCheckpointQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func MockRunner(fs *objectio.ObjectFS, c *catalog.Catalog) *runner {
	r := &runner{
		fs:      fs,
		catalog: c,
	}
	r.storage.entries = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.Less(b.end)
	}, btree.Options{
		NoLocks: true,
	})
	return r
}

func NewRunner(
	fs *objectio.ObjectFS,
	catalog *catalog.Catalog,
	scheduler tasks.TaskScheduler,
	source logtail.Collector,
	opts ...Option) *runner {
	r := &runner{
		catalog:   catalog,
		scheduler: scheduler,
		source:    source,
		fs:        fs,
		observers: new(observers),
	}
	r.storage.entries = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.Less(b.end)
	}, btree.Options{
		NoLocks: true,
	})
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()

	r.incrementalPolicy = &timeBasedPolicy{interval: r.options.minIncrementalInterval}
	r.globalPolicy = &timeBasedPolicy{interval: r.options.minGlobalInterval}
	r.stopper = stopper.NewStopper("CheckpointRunner")
	r.dirtyEntryQueue = sm.NewSafeQueue(r.options.dirtyEntryQueueSize, 100, r.onDirtyEntries)
	r.waitQueue = sm.NewSafeQueue(r.options.waitQueueSize, 100, r.onWaitWaitableItems)
	r.checkpointQueue = sm.NewSafeQueue(r.options.checkpointQueueSize, 100, r.onCheckpointEntries)
	r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
	return r
}

func (r *runner) onCheckpointEntries(items ...any) {
	entry := r.MaxCheckpoint()
	if entry.IsFinished() {
		return
	}

	check := func() (done bool) {
		tree := r.source.ScanInRangePruned(entry.GetStart(), entry.GetEnd())
		tree.GetTree().Compact()
		if tree.IsEmpty() {
			done = true
		}
		return
	}

	if !check() {
		logutil.Debugf("%s is waiting", entry.String())
		return
	}

	now := time.Now()
	if entry.IsIncremental() {
		r.doIncrementalCheckpoint(entry)
	} else {
		r.doGlobalCheckpoint(entry)
	}
	r.syncCheckpointMetadata(entry.start, entry.end)

	entry.SetState(ST_Finished)
	logutil.Debugf("%s is done, takes %s", entry.String(), time.Since(now))

	r.postCheckpointQueue.Enqueue(entry)
}
func (r *runner) collectCheckpointMetadata() *containers.Batch {
	bat := makeRespBatchFromSchema(CheckpointSchema)
	r.storage.RLock()
	entries := r.storage.entries.Items()
	r.storage.RUnlock()
	for _, entry := range entries {
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()))
	}
	return bat
}
func (r *runner) TestCheckpoint(entry *CheckpointEntry) {
	r.doIncrementalCheckpoint(entry)
	r.storage.entries.Set(entry)
	r.syncCheckpointMetadata(entry.start, entry.end)
}
func (r *runner) syncCheckpointMetadata(start, end types.TS) {
	bat := r.collectCheckpointMetadata()
	name := blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, start, end)
	writer := blockio.NewWriter(context.Background(), r.fs, name)
	writer.WriteBlock(bat)
	writer.Sync()
}

func (r *runner) doIncrementalCheckpoint(entry *CheckpointEntry) {
	builder, err := logtail.CollectSnapshot(r.catalog, entry.start, entry.end)
	if err != nil {
		panic(err)
	}
	writer := entry.NewCheckpointWriter(r.fs)
	blks := builder.WriteToFS(writer)
	entry.EncodeAndSetLocation(blks)
}

func (r *runner) doGlobalCheckpoint(entry *CheckpointEntry) {
	// TODO
	builder, err := logtail.CollectSnapshot(r.catalog, entry.start, entry.end)
	if err != nil {
		panic(err)
	}
	writer := entry.NewCheckpointWriter(r.fs)
	blks := builder.WriteToFS(writer)
	entry.EncodeAndSetLocation(blks)
}

func (r *runner) onPostCheckpointEntries(entries ...any) {
	for _, e := range entries {
		entry := e.(*CheckpointEntry)

		// 1. broadcast event
		r.observers.OnNewCheckpoint(entry.GetEnd())

		// TODO:
		// 2. remove previous checkpoint

		logutil.Debugf("Post %s", entry.String())
	}
}

func (r *runner) MaxGlobalCheckpoint() *CheckpointEntry {
	r.storage.RLock()
	defer r.storage.RUnlock()
	return r.storage.prevGlobal
}

func (r *runner) MaxCheckpoint() *CheckpointEntry {
	r.storage.RLock()
	defer r.storage.RUnlock()
	entry, _ := r.storage.entries.Max()
	return entry
}

func (r *runner) tryAddNewCheckpointEntry(entry *CheckpointEntry) (success bool) {
	r.storage.Lock()
	defer r.storage.Unlock()
	maxEntry, _ := r.storage.entries.Max()
	if maxEntry != nil && entry.IsIncremental() {
		if !maxEntry.GetEnd().Next().Equal(entry.GetStart()) {
			success = false
		} else if !maxEntry.IsFinished() {
			success = false
		} else {
			r.storage.entries.Set(entry)
			success = true
		}
		return
	}
	r.storage.entries.Set(entry)
	r.storage.prevGlobal = entry
	success = true
	return
}

func (r *runner) tryScheduleFirstCheckpoint() {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	_, count := r.source.ScanInRange(types.TS{}, ts)
	if count < r.options.minCount {
		return
	}

	entry := NewCheckpointEntry(types.TS{}, ts)
	r.tryAddNewCheckpointEntry(entry)
	r.checkpointQueue.Enqueue(struct{}{})
}

func (r *runner) tryScheduleIncrementalCheckpoint(prev types.TS) {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	_, count := r.source.ScanInRange(prev.Next(), ts)
	if count < r.options.minCount {
		return
	}
	entry := NewCheckpointEntry(prev.Next(), ts)
	r.tryAddNewCheckpointEntry(entry)
	r.checkpointQueue.Enqueue(struct{}{})
}

func (r *runner) tryScheduleGlobalCheckpoint(ts types.TS) {
	entry := NewCheckpointEntry(types.TS{}, ts)
	r.tryAddNewCheckpointEntry(entry)
	r.checkpointQueue.Enqueue(struct{}{})
}

func (r *runner) tryScheduleCheckpoint() {
	entry := r.MaxCheckpoint()

	// no prev checkpoint found. try schedule the first
	// checkpoint
	if entry == nil {
		r.tryScheduleFirstCheckpoint()
		return
	}

	if entry.IsRunning() {
		return
	}

	if entry.IsPendding() {
		r.checkpointQueue.Enqueue(struct{}{})
		return
	}

	// if the prev checkpoint is a global checkpoint, try
	// schedule an incremental checkpoint this time
	if !entry.IsIncremental() {
		if r.incrementalPolicy.Check(entry.GetEnd()) {
			r.tryScheduleIncrementalCheckpoint(entry.GetEnd())
		}
		return
	}

	prevGlobal := r.MaxGlobalCheckpoint()

	if r.globalPolicy.Check(prevGlobal.GetEnd()) {
		r.tryScheduleGlobalCheckpoint(entry.GetEnd())
		return
	}

	if r.incrementalPolicy.Check(entry.GetEnd()) {
		r.tryScheduleIncrementalCheckpoint(entry.GetEnd())
	}
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
	if r.options.checkpointQueueSize <= 1000 {
		r.options.checkpointQueueSize = 1000
	}
	if r.options.minIncrementalInterval <= 0 {
		r.options.minIncrementalInterval = time.Minute
	}
	if r.options.minGlobalInterval < 10*r.options.minIncrementalInterval {
		r.options.minGlobalInterval = 10 * r.options.minIncrementalInterval
	}
	if r.options.minCount <= 0 {
		r.options.minCount = 10000
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
	logutil.Debugf("%s [SCORE=%d]", blk.String(), score)
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
	logutil.Debugf(merged.String())
	visitor := new(blockVisitor)
	visitor.blockFn = r.tryCompactBlock

	if err := merged.GetTree().Visit(visitor); err != nil {
		panic(err)
	}
}

func (r *runner) crontask(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(r.options.collectInterval, func() {
		r.source.Run()
		entry := r.source.GetAndRefreshMerged()
		if entry.IsEmpty() {
			logutil.Debugf("No dirty block found")
		} else {
			r.dirtyEntryQueue.Enqueue(entry)
		}
		_ = r.tryScheduleCheckpoint
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
		r.postCheckpointQueue.Start()
		r.checkpointQueue.Start()
		r.dirtyEntryQueue.Start()
		r.waitQueue.Start()
		if err := r.stopper.RunNamedTask("dirty-collector-job", r.crontask); err != nil {
			panic(err)
		}
	})
}

func (r *runner) Stop() {
	r.onceStop.Do(func() {
		r.stopper.Stop()
		r.dirtyEntryQueue.Stop()
		r.checkpointQueue.Stop()
		r.postCheckpointQueue.Stop()
		r.waitQueue.Stop()
	})
}
