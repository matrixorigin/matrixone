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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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

type timeBasedPolicy struct {
	interval time.Duration
}

func (p *timeBasedPolicy) Check(last types.TS) bool {
	physical := last.Physical()
	return physical <= time.Now().UTC().UnixNano()-p.interval.Nanoseconds()
}

// Q: What does runner do?
// A: A checkpoint runner organizes and manages	all checkpoint-related behaviors. It roughly
//    does the following things:
//    - Manage the life cycle of all checkpoints and provide some query interfaces.
//    - A cron job periodically collects and analyzes dirty blocks, and flushes eligibl dirty
//      blocks to the remote storage
//    - The cron job peridically test whether a new checkpoint can be created. If it is not
//      satisfied, it will wait for next trigger. Otherwise, it will start the process of
//      creating a checkpoint.

// Q: How to collect dirty blocks?
// A: There is a logtail manager maintains all transaction information that occurred over a
//    period of time. When a checkpoint is generated, we clean up the data before the
//    checkpoint timestamp in logtail.
//
//         |----to prune----|                                           Time
//    -----+----------------+-------------------------------------+----------->
//         t1         checkpoint-t10                             t100
//
//    For each transaction, it maintains a dirty block list.
//
//    [t1]: TB1-[1]
//    [t2]: TB2-[2]
//    [t3]: TB1-[1],TB2-[3]
//    [t4]: []
//    [t5]: TB2[3,4]
//    .....
//    .....
//    When collecting the dirty blocks in [t1, t5], it will get 2 block list, which is represented
//    with `common.Tree`
//                  [t1,t5] - - - - - - - - - <DirtyTreeEntry>
//                  /     \
//               [TB1]   [TB2]
//                 |       |
//                [1]   [2,3,4] - - - - - - - leaf nodes are all dirty blocks
//    We store the dirty tree entries into the internal storage. Over time, we'll see something like
//    this inside the storage:
//    - Entry[t1,  t5]
//    - Entry[t6, t12]
//    - Entry[t13,t29]
//    - Entry[t30,t47]
//    .....
//    .....
//    When collecting the dirty blocks in [t1, t20], it will get 3 dirty trees from [t1,t5],[t6,t12],
//    [t13,t29] and merge the three trees into a tree with all the leaf nodes being dirty blocks.
//
//    In order to reduce the workload of scan, we have always been incremental scan. And also we will
//    continue to clean up the entries in the storage.

// Q: How to test whether a block need to be flushed?
// A: It is an open question. There are a lot of options, just chose a simple strategy for now.
//    Must:
//    - The born transaction of the block was committed
//    - No uncommitted transaction on the block
//    Factors:
//    - Max rows reached
//    - Delete ratio
//    - Max flush timeout

// Q: How to do incremental checkpoint?
// A: 1. Decide a checkpoint timestamp
//    2. Wait all transactions before timestamp were committed
//    3. Wait all dirty blocks before the timestamp were flushed
//    4. Prepare checkpoint data
//    5. Persist the checkpoint data
//    6. Persist the checkpoint meta data
//    7. Notify checkpoint events to all the observers
//    8. Schedule to remove stale checkpoint meta objects

// Q: How to boot from the checkpoints?
// A: When a meta version is created, it contains all information of the previouse version. So we always
//
//	delete the stale versions when a new version is created. Over time, the number of objects under
//	`ckp/` is small.
//	1. List all meta objects under `ckp/`. Get the latest meta object and read all checkpoint informations
//	   from the meta object.
//	2. Apply the latest global checkpoint
//	3. Apply the incremental checkpoint start from the version right after the global checkpoint to the
//	   latest version.
type runner struct {
	options struct {
		// checkpoint scanner interval duration
		collectInterval time.Duration

		// maximum dirty block flush interval duration
		maxFlushInterval time.Duration

		// minimum incremental checkpoint interval duration
		minIncrementalInterval time.Duration

		// minimum global checkpoint interval duration
		minGlobalInterval time.Duration

		// minimum count of uncheckpointed transactions allowed before the next checkpoint
		minCount int

		forceFlushTimeout       time.Duration
		forceFlushCheckInterval time.Duration

		dirtyEntryQueueSize int
		waitQueueSize       int
		checkpointQueueSize int
	}

	// logtail sourcer
	source    logtail.Collector
	catalog   *catalog.Catalog
	scheduler tasks.TaskScheduler
	fs        *objectio.ObjectFS
	observers *observers
	wal       wal.Driver

	stopper *stopper.Stopper

	// memory storage of the checkpoint entries
	storage struct {
		sync.RWMutex
		entries    *btree.BTreeG[*CheckpointEntry]
		prevGlobal *CheckpointEntry
	}

	// checkpoint policy
	incrementalPolicy *timeBasedPolicy
	globalPolicy      *timeBasedPolicy

	dirtyEntryQueue     sm.Queue
	waitQueue           sm.Queue
	checkpointQueue     sm.Queue
	postCheckpointQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewRunner(
	fs *objectio.ObjectFS,
	catalog *catalog.Catalog,
	scheduler tasks.TaskScheduler,
	source logtail.Collector,
	wal wal.Driver,
	opts ...Option) *runner {
	r := &runner{
		catalog:   catalog,
		scheduler: scheduler,
		source:    source,
		fs:        fs,
		observers: new(observers),
		wal:       wal,
	}
	r.storage.entries = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.start.Less(b.start)
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

// Only used in UT
func (r *runner) DebugUpdateOptions(opts ...Option) {
	for _, opt := range opts {
		opt(r)
	}
}

func (r *runner) onCheckpointEntries(items ...any) {
	var err error
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
		err = r.doIncrementalCheckpoint(entry)
	} else {
		err = r.doGlobalCheckpoint(entry)
	}
	if err != nil {
		logutil.Errorf("Do checkpoint %s: %v", entry.String(), err)
		return
	}
	if err = r.saveCheckpoint(entry.start, entry.end); err != nil {
		logutil.Errorf("Save checkpoint %s: %v", entry.String(), err)
		// TODO:
		// 1. Retry
		// 2. Clean garbage
		return
	}

	lsn := r.source.GetMaxLSN(entry.start, entry.end)
	e, err := r.wal.RangeCheckpoint(1, lsn)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}

	entry.SetState(ST_Finished)
	logutil.Debugf("%s is done, takes %s", entry.String(), time.Since(now))

	r.postCheckpointQueue.Enqueue(entry)
}
func (r *runner) collectCheckpointMetadata() *containers.Batch {
	bat := makeRespBatchFromSchema(CheckpointSchema)
	entries := r.GetAllCheckpoints()
	for _, entry := range entries {
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()))
	}
	return bat
}
func (r *runner) GetAllCheckpoints() []*CheckpointEntry {
	r.storage.Lock()
	snapshot := r.storage.entries.Copy()
	r.storage.Unlock()
	return snapshot.Items()
}
func (r *runner) MaxLSN() uint64 {
	endTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return r.source.GetMaxLSN(types.TS{}, endTs)
}
func (r *runner) MockCheckpoint(end types.TS) {
	var err error
	entry := NewCheckpointEntry(types.TS{}, end)
	if err = r.doIncrementalCheckpoint(entry); err != nil {
		panic(err)
	}
	if err = r.saveCheckpoint(entry.start, entry.end); err != nil {
		panic(err)
	}
	r.storage.Lock()
	r.storage.entries.Set(entry)
	r.storage.Unlock()
	entry.SetState(ST_Finished)
	r.storage.prevGlobal = entry
	lsn := r.source.GetMaxLSN(entry.start, entry.end)
	e, err := r.wal.RangeCheckpoint(1, lsn)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}
}
func (r *runner) FlushTable(dbID, tableID uint64, ts types.TS) (err error) {
	makeCtx := func() *DirtyCtx {
		tree := r.source.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		tableTree := tree.GetTree().GetTable(tableID)
		if tableTree == nil {
			return nil
		}
		nTree := common.NewTree()
		nTree.Tables[tableID] = tableTree
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, nTree)
		dirtyCtx := new(DirtyCtx)
		dirtyCtx.tree = entry
		dirtyCtx.force = true
		return dirtyCtx
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.options.forceFlushTimeout)
	defer cancel()

	ticker := time.NewTicker(r.options.forceFlushCheckInterval)
	defer ticker.Stop()

	dirtyCtx := makeCtx()
	if dirtyCtx == nil {
		return
	}
	if _, err = r.dirtyEntryQueue.Enqueue(dirtyCtx); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			logutil.Warnf("Flush %d-%d timeout", dbID, tableID)
			return
		case <-ticker.C:
			if dirtyCtx = makeCtx(); dirtyCtx == nil {
				return
			}
			if _, err = r.dirtyEntryQueue.Enqueue(dirtyCtx); err != nil {
				return
			}
		}
	}
}

func (r *runner) TestCheckpoint(entry *CheckpointEntry) {
	r.doIncrementalCheckpoint(entry)
	r.storage.entries.Set(entry)
	r.storage.Unlock()
	entry.SetState(ST_Finished)
	r.storage.prevGlobal = entry
	lsn := r.source.GetMaxLSN(entry.start, entry.end)
	e, err := r.wal.RangeCheckpoint(1, lsn)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}
}

func (r *runner) saveCheckpoint(start, end types.TS) (err error) {
	bat := r.collectCheckpointMetadata()
	name := blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, start, end)
	writer := blockio.NewWriter(context.Background(), r.fs, name)
	if _, err = writer.WriteBlock(bat); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.Sync()
	return
}

func (r *runner) doIncrementalCheckpoint(entry *CheckpointEntry) (err error) {
	factory := logtail.IncrementalCheckpointDataFactory(entry.start, entry.end)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()

	filename := uuid.NewString()
	writer := blockio.NewWriter(context.Background(), r.fs, filename)
	blks, err := data.WriteTo(writer)
	if err != nil {
		return
	}
	location := blockio.EncodeMetalocFromMetas(filename, blks)
	entry.SetLocation(location)
	return
}

func (r *runner) doGlobalCheckpoint(entry *CheckpointEntry) (err error) {
	// TODO: do global checkpoint
	return r.doIncrementalCheckpoint(entry)
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

func (r *runner) ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry {
	r.storage.RLock()
	tree := r.storage.entries.Copy()
	r.storage.RUnlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(ts, ts))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			incrementals = append(incrementals, e)
			if !it.Next() {
				break
			}
		}
	}
	return incrementals
}

func (r *runner) tryAddNewCheckpointEntry(entry *CheckpointEntry) (success bool) {
	r.storage.Lock()
	defer r.storage.Unlock()
	maxEntry, _ := r.storage.entries.Max()

	// if it's the first entry, add it
	if maxEntry == nil {
		r.storage.entries.Set(entry)
		success = true
		return
	}

	// if it is not the right candidate, skip this request
	// [startTs, endTs] --> [endTs+1, ?]
	if !maxEntry.GetEnd().Next().Equal(entry.GetStart()) {
		success = false
		return
	}

	// if the max entry is not finished, skip this request
	if !maxEntry.IsFinished() {
		success = false
		return
	}

	r.storage.entries.Set(entry)
	if !maxEntry.IsIncremental() {
		r.storage.prevGlobal = maxEntry
	}

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

	if prevGlobal != nil && r.globalPolicy.Check(prevGlobal.GetEnd()) {
		// FIXME
		r.tryScheduleGlobalCheckpoint(entry.GetEnd())
		return
	}

	if r.incrementalPolicy.Check(entry.GetEnd()) {
		r.tryScheduleIncrementalCheckpoint(entry.GetEnd())
	}
}

func (r *runner) fillDefaults() {
	if r.options.forceFlushTimeout <= 0 {
		r.options.forceFlushTimeout = time.Second * 10
	}
	if r.options.forceFlushCheckInterval <= 0 {
		r.options.forceFlushCheckInterval = time.Millisecond * 400
	}
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

func (r *runner) tryCompactBlock(dbID, tableID, segmentID, id uint64, force bool) (err error) {
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
	score := blkData.EstimateScore(r.options.maxFlushInterval, force)
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

func (r *runner) tryCompactTree(entry *logtail.DirtyTreeEntry, force bool) {
	if entry.IsEmpty() {
		return
	}
	logutil.Debugf(entry.String())
	visitor := new(common.BaseTreeVisitor)
	visitor.BlockFn = func(force bool) func(uint64, uint64, uint64, uint64) error {
		return func(dbID, tableID, segmentID, id uint64) (err error) {
			return r.tryCompactBlock(dbID, tableID, segmentID, id, force)
		}
	}(force)

	if err := entry.GetTree().Visit(visitor); err != nil {
		panic(err)
	}
}

func (r *runner) onDirtyEntries(entries ...any) {
	normal := logtail.NewEmptyDirtyTreeEntry()
	force := logtail.NewEmptyDirtyTreeEntry()
	for _, entry := range entries {
		e := entry.(*DirtyCtx)
		if e.force {
			force.Merge(e.tree)
		} else {
			normal.Merge(e.tree)
		}
	}
	if !force.IsEmpty() {
		r.tryCompactTree(force, true)
	}

	if !normal.IsEmpty() {
		r.tryCompactTree(normal, false)
	}
}

func (r *runner) crontask(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(r.options.collectInterval, func() {
		r.source.Run()
		entry := r.source.GetAndRefreshMerged()
		if entry.IsEmpty() {
			logutil.Debugf("No dirty block found")
		} else {
			e := new(DirtyCtx)
			e.tree = entry
			r.dirtyEntryQueue.Enqueue(e)
		}
		r.tryScheduleCheckpoint()
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

func (r *runner) CollectCheckpointsInRange(start, end types.TS) (locations string, checkpointed types.TS) {
	r.storage.Lock()
	tree := r.storage.entries.Copy()
	r.storage.Unlock()
	locs := make([]string, 0)
	pivot := NewCheckpointEntry(start, end)

	// For debug
	// checkpoints := make([]*CheckpointEntry, 0)
	// defer func() {
	// 	items := tree.Items()
	// 	logutil.Infof("CollectCheckpointsInRange: Pivot: %s", pivot.String())
	// 	for i, item := range items {
	// 		logutil.Infof("CollectCheckpointsInRange: Source[%d]: %s", i, item.String())
	// 	}
	// 	for i, ckp := range checkpoints {
	// 		logutil.Infof("CollectCheckpointsInRange: Found[%d]:%s", i, ckp.String())
	// 	}
	// 	logutil.Infof("CollectCheckpointsInRange: Checkpointed=%s", checkpointed.ToString())
	// }()

	iter := tree.Iter()
	defer iter.Release()

	if ok := iter.Seek(pivot); ok {
		if ok = iter.Prev(); ok {
			e := iter.Item()
			if !e.IsCommitted() {
				return
			}
			if e.HasOverlap(start, end) {
				locs = append(locs, e.GetLocation())
				checkpointed = e.GetEnd()
				// checkpoints = append(checkpoints, e)
			}
			iter.Next()
		}
		for {
			e := iter.Item()
			if !e.IsCommitted() || !e.HasOverlap(start, end) {
				break
			}
			locs = append(locs, e.GetLocation())
			checkpointed = e.GetEnd()
			// checkpoints = append(checkpoints, e)
			if ok = iter.Next(); !ok {
				break
			}
		}
	} else {
		// if it is empty, quick quit
		if ok = iter.Last(); !ok {
			return
		}
		// get last entry
		e := iter.Item()
		// if it is committed and visible, quick quit
		if !e.IsCommitted() || !e.HasOverlap(start, end) {
			return
		}
		locs = append(locs, e.GetLocation())
		checkpointed = e.GetEnd()
		// checkpoints = append(checkpoints, e)
	}

	if len(locs) == 0 {
		return
	}
	locations = strings.Join(locs, ";")
	return
}
