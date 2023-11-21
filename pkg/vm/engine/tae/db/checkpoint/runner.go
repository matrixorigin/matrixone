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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/util/fault"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

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
}

func (p *countBasedPolicy) Check(current int) bool {
	return current >= p.minCount
}

type globalCheckpointContext struct {
	force       bool
	end         types.TS
	interval    time.Duration
	truncateLSN uint64
	ckpLSN      uint64
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

		checkpointBlockRows int
		checkpointSize      int

		reservedWALEntryCount uint64
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

func (r *runner) String() string {
	var buf bytes.Buffer
	_, _ = fmt.Fprintf(&buf, "CheckpointRunner<")
	_, _ = fmt.Fprintf(&buf, "collectInterval=%v, ", r.options.collectInterval)
	_, _ = fmt.Fprintf(&buf, "maxFlushInterval=%v, ", r.options.maxFlushInterval)
	_, _ = fmt.Fprintf(&buf, "minIncrementalInterval=%v, ", r.options.minIncrementalInterval)
	_, _ = fmt.Fprintf(&buf, "globalMinCount=%v, ", r.options.globalMinCount)
	_, _ = fmt.Fprintf(&buf, "globalVersionInterval=%v, ", r.options.globalVersionInterval)
	_, _ = fmt.Fprintf(&buf, "minCount=%v, ", r.options.minCount)
	_, _ = fmt.Fprintf(&buf, "forceFlushTimeout=%v, ", r.options.forceFlushTimeout)
	_, _ = fmt.Fprintf(&buf, "forceFlushCheckInterval=%v, ", r.options.forceFlushCheckInterval)
	_, _ = fmt.Fprintf(&buf, "dirtyEntryQueueSize=%v, ", r.options.dirtyEntryQueueSize)
	_, _ = fmt.Fprintf(&buf, "waitQueueSize=%v, ", r.options.waitQueueSize)
	_, _ = fmt.Fprintf(&buf, "checkpointQueueSize=%v, ", r.options.checkpointQueueSize)
	_, _ = fmt.Fprintf(&buf, "checkpointBlockRows=%v, ", r.options.checkpointBlockRows)
	_, _ = fmt.Fprintf(&buf, "checkpointSize=%v, ", r.options.checkpointSize)
	_, _ = fmt.Fprintf(&buf, ">")
	return buf.String()
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
			entriesCount := r.GetPenddingIncrementalCount()
			if r.globalPolicy.Check(entriesCount) {
				doCheckpoint = true
			}
		}
		if doCheckpoint {
			now := time.Now()
			entry, err := r.doGlobalCheckpoint(ctx.end, ctx.ckpLSN, ctx.truncateLSN, ctx.interval)
			if err != nil {
				logutil.Errorf("Global checkpoint %v failed: %v", entry, err)
				continue
			}
			if err := r.saveCheckpoint(entry.start, entry.end, 0, 0); err != nil {
				logutil.Errorf("Global checkpoint %v failed: %v", entry, err)
				continue
			}
			logutil.Infof("%s is done, takes %s", entry.String(), time.Since(now))
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
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				logutil.Warnf("gc %v failed: %v", incremental.String(), err)
				panic(err)
			}
			err = incremental.GCMetadata(r.rt.Fs)
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				panic(err)
			}
			r.DeleteIncrementalEntry(incremental)
		}
	}
	globals := r.GetAllGlobalCheckpoints()
	for _, global := range globals {
		if global.LessEq(ts) {
			err := global.GCEntry(r.rt.Fs)
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				panic(err)
			}
			err = global.GCMetadata(r.rt.Fs)
			if err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
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
	lsn := r.source.GetMaxLSN(entry.start, entry.end)
	lsnToTruncate := uint64(0)
	if lsn > r.options.reservedWALEntryCount {
		lsnToTruncate = lsn - r.options.reservedWALEntryCount
	}
	entry.SetLSN(lsn, lsnToTruncate)
	entry.SetState(ST_Finished)
	if err = r.saveCheckpoint(entry.start, entry.end, lsn, lsnToTruncate); err != nil {
		logutil.Errorf("Save checkpoint %s: %v", entry.String(), err)
		return
	}

	e, err := r.wal.RangeCheckpoint(1, lsnToTruncate)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}

	logutil.Infof("%s is done, takes %s, truncate %d, checkpoint %d, reserve %d",
		entry.String(), time.Since(now), lsnToTruncate, lsn, r.options.reservedWALEntryCount)

	r.postCheckpointQueue.Enqueue(entry)
	r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
		end:         entry.end,
		interval:    r.options.globalVersionInterval,
		ckpLSN:      lsn,
		truncateLSN: lsnToTruncate,
	})
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
	iarg, sarg, flush := fault.TriggerFault("flush_table_error")
	if flush && (iarg == 0 || rand.Int63n(iarg) == 0) {
		return moerr.NewInternalError(ctx, sarg)
	}
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

func (r *runner) saveCheckpoint(start, end types.TS, ckpLSN, truncateLSN uint64) (err error) {
	bat := r.collectCheckpointMetadata(start, end, ckpLSN, truncateLSN)
	defer bat.Close()
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
	factory := logtail.IncrementalCheckpointDataFactory(entry.start, entry.end, r.rt.Fs.Service, true)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()
	cnLocation, tnLocation, _, err := data.WriteTo(r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize)
	if err != nil {
		return
	}
	entry.SetLocation(cnLocation, tnLocation)

	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}

func checkpointMetaInfoFactory(entries []*CheckpointEntry) []*logtail.CkpLocVers {
	ret := make([]*logtail.CkpLocVers, 0)
	for idx := range entries {
		ret = append(ret, &logtail.CkpLocVers{
			Location: entries[idx].GetLocation(),
			Version:  entries[idx].GetVersion(),
		})
	}
	return ret
}

func (r *runner) doCheckpointForBackup(entry *CheckpointEntry) (location string, err error) {
	factory := logtail.BackupCheckpointDataFactory(entry.start, entry.end)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()
	cnLocation, tnLocation, _, err := data.WriteTo(r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize)
	if err != nil {
		return
	}
	entry.SetLocation(cnLocation, tnLocation)
	location = fmt.Sprintf("%s:%d:%s:%s:%s", cnLocation.String(), entry.GetVersion(), entry.end.ToString(), tnLocation.String(), entry.start.ToString())
	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}

func (r *runner) doGlobalCheckpoint(end types.TS, ckpLSN, truncateLSN uint64, interval time.Duration) (entry *CheckpointEntry, err error) {
	entry = NewCheckpointEntry(types.TS{}, end.Next(), ET_Global)
	entry.ckpLSN = ckpLSN
	entry.truncateLSN = truncateLSN
	factory := logtail.GlobalCheckpointDataFactory(entry.end, interval,
		r.rt.Fs.Service, checkpointMetaInfoFactory(r.GetAllCheckpoints()))
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	defer data.Close()

	cnLocation, tnLocation, _, err := data.WriteTo(r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize)
	if err != nil {
		return
	}
	entry.SetLocation(cnLocation, tnLocation)
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

func (r *runner) tryScheduleIncrementalCheckpoint(start, end types.TS) {
	// ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	_, count := r.source.ScanInRange(start, end)
	if count < r.options.minCount {
		return
	}
	entry := NewCheckpointEntry(start, end, ET_Incremental)
	r.tryAddNewIncrementalCheckpointEntry(entry)
}

func (r *runner) tryScheduleCheckpoint(endts types.TS) {
	if r.disabled.Load() {
		return
	}
	entry := r.MaxCheckpoint()
	global := r.MaxGlobalCheckpoint()

	// no prev checkpoint found. try schedule the first
	// checkpoint
	if entry == nil {
		if global == nil {
			r.tryScheduleIncrementalCheckpoint(types.TS{}, endts)
			return
		} else {
			maxTS := global.end.Prev()
			if r.incrementalPolicy.Check(maxTS) {
				r.tryScheduleIncrementalCheckpoint(maxTS.Next(), endts)
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
			if !tree.IsEmpty() && entry.CheckPrintTime() {
				logutil.Infof("waiting for dirty tree %s", tree.String())
				entry.IncrWaterLine()
			}
			return tree.IsEmpty()
		}

		if !check() {
			logutil.Debugf("%s is waiting", entry.String())
			return
		}
		entry.SetState(ST_Running)
		v2.TaskCkpEntryPendingDurationHistogram.Observe(time.Since(entry.lastPrint).Seconds())
		r.incrementalCheckpointQueue.Enqueue(struct{}{})
		return
	}

	if entry.IsRunning() {
		r.incrementalCheckpointQueue.Enqueue(struct{}{})
		return
	}

	if r.incrementalPolicy.Check(entry.end) {
		r.tryScheduleIncrementalCheckpoint(entry.end.Next(), endts)
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
	if r.options.checkpointBlockRows <= 0 {
		r.options.checkpointBlockRows = logtail.DefaultCheckpointBlockRows
	}
	if r.options.checkpointSize <= 0 {
		r.options.checkpointSize = logtail.DefaultCheckpointSize
	}
}

func (r *runner) tryCompactBlock(dbID, tableID uint64, id *objectio.Blockid, force bool) (err error) {
	if !r.rt.Throttle.CanCompact() {
		return
	}
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

func (r *runner) fireFlushTabletail(table *catalog.TableEntry, tree *model.TableTree, endTs types.TS) error {
	metas := make([]*catalog.BlockEntry, 0, 10)
	for _, seg := range tree.Segs {
		segment, err := table.GetSegmentByID(seg.ID)
		if err != nil {
			panic(err)
		}
		for blk := range seg.Blks {
			bid := objectio.NewBlockid(seg.ID, blk.Num, blk.Seq)
			block, err := segment.GetBlockEntryByID(bid)
			if err != nil {
				panic(err)
			}
			metas = append(metas, block)
		}
	}

	// freeze all append
	scopes := make([]common.ID, 0, len(metas))
	for _, meta := range metas {
		if !meta.GetBlockData().PrepareCompact() {
			logutil.Infof("[FlushTabletail] %d-%s / %s false prepareCompact ", table.ID, table.GetLastestSchema().Name, meta.ID.String())
			return moerr.GetOkExpectedEOB()
		}
		scopes = append(scopes, *meta.AsCommonID())
	}

	factory := jobs.FlushTableTailTaskFactory(metas, r.rt, endTs)
	if _, err := r.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory); err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Infof("[FlushTabletail] %d-%s %v", table.ID, table.GetLastestSchema().Name, err)
		}
		return moerr.GetOkExpectedEOB()
	}
	return nil
}

func (r *runner) EstimateTableMemSize(table *catalog.TableEntry, tree *model.TableTree) int {
	size := 0
	for _, seg := range tree.Segs {
		segment, err := table.GetSegmentByID(seg.ID)
		if err != nil {
			panic(err)
		}
		for blk := range seg.Blks {
			bid := objectio.NewBlockid(seg.ID, blk.Num, blk.Seq)
			block, err := segment.GetBlockEntryByID(bid)
			if err != nil {
				panic(err)
			}
			size += block.GetBlockData().EstimateMemSize()
		}
	}
	return size
}

func (r *runner) tryCompactTree(entry *logtail.DirtyTreeEntry, force bool) {
	if entry.IsEmpty() {
		return
	}
	logutil.Debugf(entry.String())
	visitor := new(model.BaseTreeVisitor)

	visitor.TableFn = func(dbID, tableID uint64) error {
		db, err := r.catalog.GetDatabaseByID(dbID)
		if err != nil {
			panic(err)
		}
		table, err := db.GetTableEntryByID(tableID)
		if err != nil {
			panic(err)
		}

		if !table.Stats.Inited {
			table.Stats.Lock()
			table.Stats.InitWithLock(r.options.maxFlushInterval)
			table.Stats.Unlock()
		}

		dirtyTree := entry.GetTree().GetTable(tableID)
		_, endTs := entry.GetTimeRange()

		size := r.EstimateTableMemSize(table, dirtyTree)

		stats := &table.Stats
		stats.Lock()
		defer stats.Unlock()

		// debug log, delete later
		if !stats.LastFlush.IsEmpty() && size > 2*1000*1024 {
			logutil.Infof("[flushtabletail] %s(%s)  FlushCountDown %v",
				table.GetLastestSchema().Name,
				common.HumanReadableBytes(size),
				time.Until(stats.FlushDeadline))
		}

		if force {
			logutil.Infof("[flushtabletail] force flush %s", table.GetLastestSchema().Name)
			if err := r.fireFlushTabletail(table, dirtyTree, endTs); err == nil {
				stats.ResetDeadlineWithLock()
			}
			return moerr.GetOkStopCurrRecur()
		}

		if stats.LastFlush.IsEmpty() {
			// first boot, just bail out, and never enter this branch again
			stats.LastFlush = stats.LastFlush.Next()
			stats.ResetDeadlineWithLock()
			return moerr.GetOkStopCurrRecur()
		}

		if stats.FlushDeadline.Before(time.Now()) || size > stats.FlushMemCapacity {
			if err := r.fireFlushTabletail(table, dirtyTree, endTs); err == nil {
				stats.ResetDeadlineWithLock()
			}
		}

		return moerr.GetOkStopCurrRecur()
	}
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
		_, endts := entry.GetTimeRange()
		if entry.IsEmpty() {
			logutil.Debugf("[flushtabletail]No dirty block found")
		} else {
			e := new(DirtyCtx)
			e.tree = entry
			r.dirtyEntryQueue.Enqueue(e)
		}
		r.tryScheduleCheckpoint(endts)
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
		locs = append(locs, strconv.Itoa(int(global.version)))
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
				locs = append(locs, strconv.Itoa(int(e.version)))
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
			locs = append(locs, strconv.Itoa(int(e.version)))
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
		locs = append(locs, strconv.Itoa(int(e.version)))
		checkpointed = e.GetEnd()
		// checkpoints = append(checkpoints, e)
	}

	if len(locs) == 0 {
		return
	}
	locations = strings.Join(locs, ";")
	return
}
