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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
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

type countBasedPolicy struct {
	minCount int
	current  int
}

func (p *countBasedPolicy) Check() bool {
	return p.current >= p.minCount
}

func (p *countBasedPolicy) Add(cnt int) {
	p.current++
}

func (p *countBasedPolicy) Reset() {
	p.current = 0
}

type globalCheckpointContext struct {
	force    bool
	end      types.TS
	interval time.Duration
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
		globalMinCount            int
		forceUpdateGlobalInterval bool
		globalVersionInterval     time.Duration

		// minimum count of uncheckpointed transactions allowed before the next checkpoint
		minCount int

		forceFlushTimeout       time.Duration
		forceFlushCheckInterval time.Duration

		dirtyEntryQueueSize int
		waitQueueSize       int
		checkpointQueueSize int
	}

	ctx context.Context

	// logtail sourcer
	source    logtail.Collector
	catalog   *catalog.Catalog
	rt        *dbutils.Runtime
	observers *observers
	wal       wal.Driver
	disabled  atomic.Bool

	stopper *stopper.Stopper

	// memory storage of the checkpoint entries
	storage struct {
		sync.RWMutex
		entries *btree.BTreeG[*CheckpointEntry]
		globals *btree.BTreeG[*CheckpointEntry]
	}

	gcTS atomic.Value

	// checkpoint policy
	incrementalPolicy *timeBasedPolicy
	globalPolicy      *countBasedPolicy

	dirtyEntryQueue            sm.Queue
	waitQueue                  sm.Queue
	incrementalCheckpointQueue sm.Queue
	globalCheckpointQueue      sm.Queue
	postCheckpointQueue        sm.Queue
	gcCheckpointQueue          sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewRunner(
	ctx context.Context,
	rt *dbutils.Runtime,
	catalog *catalog.Catalog,
	source logtail.Collector,
	wal wal.Driver,
	opts ...Option) *runner {
	r := &runner{
		ctx:       ctx,
		rt:        rt,
		catalog:   catalog,
		source:    source,
		observers: new(observers),
		wal:       wal,
	}
	r.storage.entries = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.Less(b.end)
	}, btree.Options{
		NoLocks: true,
	})
	r.storage.globals = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.Less(b.end)
	}, btree.Options{
		NoLocks: true,
	})
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()

	r.incrementalPolicy = &timeBasedPolicy{interval: r.options.minIncrementalInterval}
	r.globalPolicy = &countBasedPolicy{minCount: r.options.globalMinCount}
	r.stopper = stopper.NewStopper("CheckpointRunner")
	r.dirtyEntryQueue = sm.NewSafeQueue(r.options.dirtyEntryQueueSize, 100, r.onDirtyEntries)
	r.waitQueue = sm.NewSafeQueue(r.options.waitQueueSize, 100, r.onWaitWaitableItems)
	r.incrementalCheckpointQueue = sm.NewSafeQueue(r.options.checkpointQueueSize, 100, r.onIncrementalCheckpointEntries)
	r.globalCheckpointQueue = sm.NewSafeQueue(r.options.checkpointQueueSize, 100, r.onGlobalCheckpointEntries)
	r.gcCheckpointQueue = sm.NewSafeQueue(100, 100, r.onGCCheckpointEntries)
	r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
	return r
}

// Only used in UT
func (r *runner) DebugUpdateOptions(opts ...Option) {
	for _, opt := range opts {
		opt(r)
	}
}

func (r *runner) onGlobalCheckpointEntries(items ...any) {
	for _, item := range items {
		ctx := item.(*globalCheckpointContext)
		doCheckpoint := false
		if ctx.force {
			doCheckpoint = true
		} else {
			r.globalPolicy.Add(1)
			if r.globalPolicy.Check() {
				doCheckpoint = true
			}
		}
		if doCheckpoint {
			now := time.Now()
			entry, err := r.doGlobalCheckpoint(ctx.end, ctx.interval)
			if err != nil {
				logutil.Errorf("Global checkpoint %v failed: %v", entry, err)
				continue
			}
			if err := r.saveCheckpoint(entry.start, entry.end); err != nil {
				logutil.Errorf("Global checkpoint %v failed: %v", entry, err)
				continue
			}
			logutil.Infof("%s is done, takes %s", entry.String(), time.Since(now))
			r.globalPolicy.Reset()
		}
	}
}

func (r *runner) onGCCheckpointEntries(items ...any) {
	gcTS, needGC := r.getTSTOGC()
	if !needGC {
		return
	}
	r.gcCheckpointEntries(gcTS)
}

func (r *runner) getTSTOGC() (ts types.TS, needGC bool) {
	ts = r.getGCTS()
	if ts.IsEmpty() {
		return
	}
	tsTOGC := r.getTSToGC()
	if tsTOGC.Less(ts) {
		ts = tsTOGC
	}
	gcedTS := r.getGCedTS()
	if gcedTS.GreaterEq(ts) {
		return
	}
	needGC = true
	return
}

func (r *runner) gcCheckpointEntries(ts types.TS) {
	if ts.IsEmpty() {
		return
	}
	incrementals := r.GetAllIncrementalCheckpoints()
	for _, incremental := range incrementals {
		if incremental.LessEq(ts) {
			err := incremental.GCEntry(r.rt.Fs)
			if err != nil {
				logutil.Warnf("gc %v failed: %v", incremental.String(), err)
				panic(err)
			}
			err = incremental.GCMetadata(r.rt.Fs)
			if err != nil {
				panic(err)
			}
			r.DeleteIncrementalEntry(incremental)
		}
	}
	globals := r.GetAllGlobalCheckpoints()
	for _, global := range globals {
		if global.LessEq(ts) {
			err := global.GCEntry(r.rt.Fs)
			if err != nil {
				panic(err)
			}
			err = global.GCMetadata(r.rt.Fs)
			if err != nil {
				panic(err)
			}
			r.DeleteGlobalEntry(global)
		}
	}
}

func (r *runner) onIncrementalCheckpointEntries(items ...any) {
	now := time.Now()
	entry := r.MaxCheckpoint()
	// In some unit tests, ckp is managed manually, and ckp deletiton (CleanPenddingCheckpoint)
	// can be called when the queue still has unexecuted task.
	// Add `entry == nil` here as protective codes
	if entry == nil || entry.GetState() != ST_Running {
		return
	}
	err := r.doIncrementalCheckpoint(entry)
	if err != nil {
		logutil.Errorf("Do checkpoint %s: %v", entry.String(), err)
		return
	}
	entry.SetState(ST_Finished)
	if err = r.saveCheckpoint(entry.start, entry.end); err != nil {
		logutil.Errorf("Save checkpoint %s: %v", entry.String(), err)
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

	logutil.Infof("%s is done, takes %s, truncate %d", entry.String(), time.Since(now), lsn)

	r.postCheckpointQueue.Enqueue(entry)
	r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{end: entry.end, interval: r.options.globalVersionInterval})
}

func (r *runner) DeleteIncrementalEntry(entry *CheckpointEntry) {
	r.storage.Lock()
	defer r.storage.Unlock()
	r.storage.entries.Delete(entry)
	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DeleteIncrementalEntry.Add(1)
	})
}
func (r *runner) DeleteGlobalEntry(entry *CheckpointEntry) {
	r.storage.Lock()
	defer r.storage.Unlock()
	r.storage.globals.Delete(entry)
	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DeleteGlobalEntry.Add(1)
	})
}
func (r *runner) FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) (err error) {
	makeCtx := func() *DirtyCtx {
		tree := r.source.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		tableTree := tree.GetTree().GetTable(tableID)
		if tableTree == nil {
			return nil
		}
		nTree := model.NewTree()
		nTree.Tables[tableID] = tableTree
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, nTree)
		dirtyCtx := new(DirtyCtx)
		dirtyCtx.tree = entry
		dirtyCtx.force = true
		return dirtyCtx
	}

	op := func() (ok bool, err error) {
		dirtyCtx := makeCtx()
		if dirtyCtx == nil {
			return true, nil
		}
		if _, err = r.dirtyEntryQueue.Enqueue(dirtyCtx); err != nil {
			return true, nil
		}
		return false, nil
	}

	err = common.RetryWithIntervalAndTimeout(
		op,
		r.options.forceFlushTimeout,
		r.options.forceFlushCheckInterval, true)
	if moerr.IsMoErrCode(err, moerr.ErrInternal) || moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		logutil.Warnf("Flush %d-%d :%v", dbID, tableID, err)
		return nil
	}
	return
}

func (r *runner) saveCheckpoint(start, end types.TS) (err error) {
	bat := r.collectCheckpointMetadata(start, end)
	name := blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, r.rt.Fs.Service)
	if err != nil {
		return err
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(r.ctx)
	return
}

func (r *runner) doIncrementalCheckpoint(entry *CheckpointEntry) (err error) {
	factory := logtail.IncrementalCheckpointDataFactory(entry.start, entry.end)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()

	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(r.rt.Fs.Service, name, 0, nil)
	if err != nil {
		return err
	}
	blks, err := data.WriteTo(writer)
	if err != nil {
		return
	}
	location := objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	entry.SetLocation(location)

	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}

func (r *runner) doGlobalCheckpoint(end types.TS, interval time.Duration) (entry *CheckpointEntry, err error) {
	entry = NewCheckpointEntry(types.TS{}, end.Next(), ET_Global)
	factory := logtail.GlobalCheckpointDataFactory(entry.end, interval)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()

	segmentid := objectio.NewSegmentid()
	name := objectio.BuildObjectName(segmentid, 0)
	writer, err := blockio.NewBlockWriterNew(r.rt.Fs.Service, name, 0, nil)
	if err != nil {
		return
	}
	blks, err := data.WriteTo(writer)
	if err != nil {
		return
	}
	location := objectio.BuildLocation(name, blks[0].GetExtent(), 0, blks[0].GetID())
	entry.SetLocation(location)
	r.tryAddNewGlobalCheckpointEntry(entry)
	entry.SetState(ST_Finished)

	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoGlobalCheckPoint.Add(1)
	})
	return
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

func (r *runner) tryAddNewGlobalCheckpointEntry(entry *CheckpointEntry) (success bool) {
	r.storage.Lock()
	defer r.storage.Unlock()
	r.storage.globals.Set(entry)
	return true
}

func (r *runner) tryAddNewIncrementalCheckpointEntry(entry *CheckpointEntry) (success bool) {
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

	success = true
	return
}

func (r *runner) tryScheduleIncrementalCheckpoint(start types.TS) {
	ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	_, count := r.source.ScanInRange(start, ts)
	if count < r.options.minCount {
		return
	}
	entry := NewCheckpointEntry(start, ts, ET_Incremental)
	r.tryAddNewIncrementalCheckpointEntry(entry)
}

func (r *runner) tryScheduleCheckpoint() {
	if r.disabled.Load() {
		return
	}
	entry := r.MaxCheckpoint()
	global := r.MaxGlobalCheckpoint()

	// no prev checkpoint found. try schedule the first
	// checkpoint
	if entry == nil {
		if global == nil {
			r.tryScheduleIncrementalCheckpoint(types.TS{})
			return
		} else {
			maxTS := global.end.Prev()
			if r.incrementalPolicy.Check(maxTS) {
				r.tryScheduleIncrementalCheckpoint(maxTS.Next())
			}
			return
		}
	}

	if entry.IsPendding() {
		check := func() (done bool) {
			if !r.source.IsCommitted(entry.GetStart(), entry.GetEnd()) {
				return false
			}
			tree := r.source.ScanInRangePruned(entry.GetStart(), entry.GetEnd())
			tree.GetTree().Compact()
			return tree.IsEmpty()
		}

		if !check() {
			logutil.Debugf("%s is waiting", entry.String())
			return
		}
		entry.SetState(ST_Running)
		r.incrementalCheckpointQueue.Enqueue(struct{}{})
		return
	}

	if entry.IsRunning() {
		r.incrementalCheckpointQueue.Enqueue(struct{}{})
		return
	}

	if r.incrementalPolicy.Check(entry.end) {
		r.tryScheduleIncrementalCheckpoint(entry.end.Next())
	}
}

func (r *runner) fillDefaults() {
	if r.options.forceFlushTimeout <= 0 {
		r.options.forceFlushTimeout = time.Second * 90
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
	if r.options.globalMinCount <= 0 {
		r.options.globalMinCount = 10
	}
	if r.options.minCount <= 0 {
		r.options.minCount = 10000
	}
}

func (r *runner) tryCompactBlock(dbID, tableID uint64, id *objectio.Blockid, force bool) (err error) {
	db, err := r.catalog.GetDatabaseByID(dbID)
	if err != nil {
		panic(err)
	}
	table, err := db.GetTableEntryByID(tableID)
	if err != nil {
		panic(err)
	}
	sid := objectio.ToSegmentId(id)
	segment, err := table.GetSegmentByID(sid)
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

	if _, err = r.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, taskType, scopes, factory); err != nil {
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
	visitor := new(model.BaseTreeVisitor)
	visitor.BlockFn = func(force bool) func(uint64, uint64, *objectio.Segmentid, uint16, uint16) error {
		return func(dbID, tableID uint64, segmentID *objectio.Segmentid, num, seq uint16) (err error) {
			id := objectio.NewBlockid(segmentID, num, seq)
			return r.tryCompactBlock(dbID, tableID, id, force)
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
		r.incrementalCheckpointQueue.Start()
		r.globalCheckpointQueue.Start()
		r.gcCheckpointQueue.Start()
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
		r.incrementalCheckpointQueue.Stop()
		r.globalCheckpointQueue.Stop()
		r.gcCheckpointQueue.Stop()
		r.postCheckpointQueue.Stop()
		r.waitQueue.Stop()
	})
}

func (r *runner) GetDirtyCollector() logtail.Collector {
	return r.source
}

func (r *runner) CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (locations string, checkpointed types.TS, err error) {
	if r.IsTSStale(end) {
		return "", types.TS{}, moerr.NewInternalError(ctx, "ts %v is staled", end.ToString())
	}
	r.storage.Lock()
	tree := r.storage.entries.Copy()
	global, _ := r.storage.globals.Max()
	r.storage.Unlock()
	locs := make([]string, 0)
	newStart := start
	if global != nil && global.HasOverlap(start, end) {
		locs = append(locs, global.GetLocation().String())
		newStart = global.end.Next()
		checkpointed = global.GetEnd()
	}
	pivot := NewCheckpointEntry(newStart, newStart, ET_Incremental)

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
				if len(locs) == 0 {
					return
				}
				locations = strings.Join(locs, ";")
				return
			}
			if e.HasOverlap(newStart, end) {
				locs = append(locs, e.GetLocation().String())
				checkpointed = e.GetEnd()
				// checkpoints = append(checkpoints, e)
			}
			iter.Next()
		}
		for {
			e := iter.Item()
			if !e.IsCommitted() || !e.HasOverlap(newStart, end) {
				break
			}
			locs = append(locs, e.GetLocation().String())
			checkpointed = e.GetEnd()
			// checkpoints = append(checkpoints, e)
			if ok = iter.Next(); !ok {
				break
			}
		}
	} else {
		// if it is empty, quick quit
		if ok = iter.Last(); !ok {
			if len(locs) == 0 {
				return
			}
			locations = strings.Join(locs, ";")
			return
		}
		// get last entry
		e := iter.Item()
		// if it is committed and visible, quick quit
		if !e.IsCommitted() || !e.HasOverlap(newStart, end) {
			if len(locs) == 0 {
				return
			}
			locations = strings.Join(locs, ";")
			return
		}
		locs = append(locs, e.GetLocation().String())
		checkpointed = e.GetEnd()
		// checkpoints = append(checkpoints, e)
	}

	if len(locs) == 0 {
		return
	}
	locations = strings.Join(locs, ";")
	return
}
