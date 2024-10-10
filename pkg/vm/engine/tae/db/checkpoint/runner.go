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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"

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

type tableAndSize struct {
	tbl   *catalog.TableEntry
	asize int
	dsize int
}

// Q: What does runner do?
// A: A checkpoint runner organizes and manages	all checkpoint-related behaviors. It roughly
//    does the following things:
//    - Manage the life cycle of all checkpoints and provide some query interfaces.
//    - A cron job periodically collects and analyzes dirty blocks, and flushes eligible dirty
//      blocks to the remote storage
//    - The cron job periodically test whether a new checkpoint can be created. If it is not
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
// A: When a meta version is created, it contains all information of the previous version. So we always
//
//	delete the stale versions when a new version is created. Over time, the number of objects under
//	`ckp/` is small.
//	1. List all meta objects under `ckp/`. Get the latest meta object and read all checkpoint information
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

	// logtail source
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
		incrementals *btree.BTreeG[*CheckpointEntry]
		globals      *btree.BTreeG[*CheckpointEntry]
		compacted    atomic.Pointer[CheckpointEntry]
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

	objMemSizeList []tableAndSize

	checkpointMetaFiles struct {
		sync.RWMutex
		files map[string]struct{}
	}

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
	r.storage.incrementals = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.LT(&b.end)
	}, btree.Options{
		NoLocks: true,
	})
	r.storage.globals = btree.NewBTreeGOptions(func(a, b *CheckpointEntry) bool {
		return a.end.LT(&b.end)
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
	r.checkpointMetaFiles.files = make(map[string]struct{})
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

func (r *runner) AddCheckpointMetaFile(name string) {
	r.checkpointMetaFiles.Lock()
	defer r.checkpointMetaFiles.Unlock()
	r.checkpointMetaFiles.files[name] = struct{}{}
}

func (r *runner) RemoveCheckpointMetaFile(name string) {
	r.checkpointMetaFiles.Lock()
	defer r.checkpointMetaFiles.Unlock()
	delete(r.checkpointMetaFiles.files, name)
}

func (r *runner) GetCheckpointMetaFiles() map[string]struct{} {
	r.checkpointMetaFiles.RLock()
	defer r.checkpointMetaFiles.RUnlock()
	files := make(map[string]struct{})
	for k, v := range r.checkpointMetaFiles.files {
		files[k] = v
	}
	return files
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
			if _, err := r.doGlobalCheckpoint(
				ctx.end, ctx.ckpLSN, ctx.truncateLSN, ctx.interval,
			); err != nil {
				continue
			}
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
	if tsTOGC.LT(&ts) {
		ts = tsTOGC
	}
	gcedTS := r.getGCedTS()
	if gcedTS.GE(&ts) {
		return
	}
	needGC = true
	return
}

// PXU TODO: delete in the loop
func (r *runner) gcCheckpointEntries(ts types.TS) {
	if ts.IsEmpty() {
		return
	}
	incrementals := r.GetAllIncrementalCheckpoints()
	for _, incremental := range incrementals {
		if incremental.LessEq(ts) {
			r.DeleteIncrementalEntry(incremental)
		}
	}
	globals := r.GetAllGlobalCheckpoints()
	for _, global := range globals {
		if global.LessEq(ts) {
			r.DeleteGlobalEntry(global)
		}
	}
}

func (r *runner) onIncrementalCheckpointEntries(items ...any) {
	now := time.Now()
	entry := r.MaxIncrementalCheckpoint()
	// In some unit tests, ckp is managed manually, and ckp deletion (CleanPendingCheckpoint)
	// can be called when the queue still has unexecuted task.
	// Add `entry == nil` here as protective codes
	if entry == nil || entry.GetState() != ST_Running {
		return
	}
	var (
		err           error
		errPhase      string
		lsnToTruncate uint64
		lsn           uint64
		fatal         bool
		fields        []zap.Field
	)
	now = time.Now()

	logutil.Info(
		"Checkpoint-Start",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			var logger func(msg string, fields ...zap.Field)
			if fatal {
				logger = logutil.Fatal
			} else {
				logger = logutil.Error
			}
			logger(
				"Checkpoint-Error",
				zap.String("entry", entry.String()),
				zap.Error(err),
				zap.String("phase", errPhase),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.Uint64("truncate", lsnToTruncate))
			fields = append(fields, zap.Uint64("lsn", lsn))
			fields = append(fields, zap.Uint64("reserve", r.options.reservedWALEntryCount))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"Checkpoint-End",
				fields...,
			)
		}
	}()

	var files []string
	var file string
	if fields, files, err = r.doIncrementalCheckpoint(entry); err != nil {
		errPhase = "do-ckp"
		return
	}

	lsn = r.source.GetMaxLSN(entry.start, entry.end)
	if lsn > r.options.reservedWALEntryCount {
		lsnToTruncate = lsn - r.options.reservedWALEntryCount
	}
	entry.SetLSN(lsn, lsnToTruncate)
	entry.SetState(ST_Finished)

	if file, err = r.saveCheckpoint(
		entry.start, entry.end, lsn, lsnToTruncate,
	); err != nil {
		errPhase = "save-ckp"
		return
	}
	files = append(files, file)

	var logEntry wal.LogEntry
	if logEntry, err = r.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
		errPhase = "wal-ckp"
		fatal = true
		return
	}
	if err = logEntry.WaitDone(); err != nil {
		errPhase = "wait-wal-ckp-done"
		fatal = true
		return
	}

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
	r.storage.incrementals.Delete(entry)
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

func (r *runner) DeleteCompactedEntry(entry *CheckpointEntry) {
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

func (r *runner) saveCheckpoint(start, end types.TS, ckpLSN, truncateLSN uint64) (name string, err error) {
	bat := r.collectCheckpointMetadata(start, end, ckpLSN, truncateLSN)
	defer bat.Close()
	name = blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, start, end)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, r.rt.Fs.Service)
	if err != nil {
		return
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(r.ctx)
	if err != nil {
		return
	}
	fileName := blockio.EncodeCheckpointMetadataFileNameWithoutDir(PrefixMetadata, start, end)
	r.AddCheckpointMetaFile(fileName)
	return
}

func (r *runner) doIncrementalCheckpoint(entry *CheckpointEntry) (fields []zap.Field, files []string, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(r.rt.SID(), entry.start, entry.end, true)
	data, err := factory(r.catalog)
	if err != nil {
		return
	}
	fields = data.ExportStats("")
	defer data.Close()
	var cnLocation, tnLocation objectio.Location
	cnLocation, tnLocation, files, err = data.WriteTo(r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize)
	if err != nil {
		return
	}
	files = append(files, cnLocation.Name().String())
	entry.SetLocation(cnLocation, tnLocation)

	perfcounter.Update(r.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}

func (r *runner) doCheckpointForBackup(entry *CheckpointEntry) (location string, err error) {
	factory := logtail.BackupCheckpointDataFactory(r.rt.SID(), entry.start, entry.end)
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

func (r *runner) doGlobalCheckpoint(
	end types.TS, ckpLSN, truncateLSN uint64, interval time.Duration,
) (entry *CheckpointEntry, err error) {
	var (
		errPhase string
		fields   []zap.Field
	)
	now := time.Now()

	entry = NewCheckpointEntry(r.rt.SID(), types.TS{}, end.Next(), ET_Global)
	entry.ckpLSN = ckpLSN
	entry.truncateLSN = truncateLSN

	logutil.Info(
		"Checkpoint-Start",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			logutil.Error(
				"Checkpoint-Error",
				zap.String("entry", entry.String()),
				zap.String("phase", errPhase),
				zap.Error(err),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"Checkpoint-End",
				fields...,
			)
		}
	}()

	factory := logtail.GlobalCheckpointDataFactory(r.rt.SID(), entry.end, interval)
	data, err := factory(r.catalog)
	if err != nil {
		errPhase = "collect"
		return
	}
	fields = data.ExportStats("")
	defer data.Close()

	cnLocation, tnLocation, files, err := data.WriteTo(
		r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize,
	)
	if err != nil {
		errPhase = "flush"
		return
	}

	entry.SetLocation(cnLocation, tnLocation)
	r.tryAddNewGlobalCheckpointEntry(entry)
	entry.SetState(ST_Finished)
	var name string
	if name, err = r.saveCheckpoint(entry.start, entry.end, 0, 0); err != nil {
		errPhase = "save"
		return
	}
	files = append(files, name)

	fileEntry, err := store.BuildFilesEntry(files)
	if err != nil {
		return
	}
	_, err = r.wal.AppendEntry(store.GroupFiles, fileEntry)
	if err != nil {
		return
	}
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

func (r *runner) tryAddNewCompactedCheckpointEntry(entry *CheckpointEntry) (success bool) {
	if entry.entryType != ET_Compacted {
		panic("tryAddNewCompactedCheckpointEntry entry type is error")
	}
	r.storage.Lock()
	defer r.storage.Unlock()
	old := r.storage.compacted.Load()
	if old != nil {
		end := old.end
		if entry.end.LT(&end) {
			return true
		}
	}
	r.storage.compacted.Store(entry)
	return true
}

func (r *runner) tryAddNewIncrementalCheckpointEntry(entry *CheckpointEntry) (success bool) {
	r.storage.Lock()
	defer r.storage.Unlock()
	maxEntry, _ := r.storage.incrementals.Max()

	// if it's the first entry, add it
	if maxEntry == nil {
		r.storage.incrementals.Set(entry)
		success = true
		return
	}

	// if it is not the right candidate, skip this request
	// [startTs, endTs] --> [endTs+1, ?]
	endTS := maxEntry.GetEnd()
	startTS := entry.GetStart()
	nextTS := endTS.Next()
	if !nextTS.Equal(&startTS) {
		success = false
		return
	}

	// if the max entry is not finished, skip this request
	if !maxEntry.IsFinished() {
		success = false
		return
	}

	r.storage.incrementals.Set(entry)

	success = true
	return
}

// Since there is no wal after recovery, the checkpoint lsn before backup must be set to 0.
func (r *runner) tryAddNewBackupCheckpointEntry(entry *CheckpointEntry) (success bool) {
	entry.entryType = ET_Incremental
	success = r.tryAddNewIncrementalCheckpointEntry(entry)
	if !success {
		return
	}
	r.storage.Lock()
	defer r.storage.Unlock()
	it := r.storage.incrementals.Iter()
	for it.Next() {
		e := it.Item()
		e.ckpLSN = 0
		e.truncateLSN = 0
	}
	return
}

func (r *runner) tryScheduleIncrementalCheckpoint(start, end types.TS) {
	// ts := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	_, count := r.source.ScanInRange(start, end)
	if count < r.options.minCount {
		return
	}
	entry := NewCheckpointEntry(r.rt.SID(), start, end, ET_Incremental)
	r.tryAddNewIncrementalCheckpointEntry(entry)
}

func (r *runner) tryScheduleCheckpoint(endts types.TS) {
	if r.disabled.Load() {
		return
	}
	entry := r.MaxIncrementalCheckpoint()
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
			if !tree.IsEmpty() && entry.TooOld() {
				logutil.Infof("waiting for dirty tree %s", tree.String())
				entry.DeferRetirement()
			}
			return tree.IsEmpty()
		}

		if !check() {
			logutil.Debugf("%s is waiting", entry.String())
			return
		}
		entry.SetState(ST_Running)
		v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())
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

func (r *runner) fireFlushTabletail(table *catalog.TableEntry, tree *model.TableTree) error {
	tableDesc := fmt.Sprintf("%d-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
	metas := make([]*catalog.ObjectEntry, 0, 10)
	for _, obj := range tree.Objs {
		object, err := table.GetObjectByID(obj.ID, false)
		if err != nil {
			panic(err)
		}
		metas = append(metas, object)
	}
	tombstoneMetas := make([]*catalog.ObjectEntry, 0, 10)
	for _, obj := range tree.Tombstones {
		object, err := table.GetObjectByID(obj.ID, true)
		if err != nil {
			panic(err)
		}
		tombstoneMetas = append(tombstoneMetas, object)
	}

	// freeze all append
	scopes := make([]common.ID, 0, len(metas))
	for _, meta := range metas {
		if !meta.GetObjectData().PrepareCompact() {
			logutil.Info("[FlushTabletail] data prepareCompact false", zap.String("table", tableDesc), zap.String("obj", meta.ID().String()))
			return moerr.GetOkExpectedEOB()
		}
		scopes = append(scopes, *meta.AsCommonID())
	}
	for _, meta := range tombstoneMetas {
		if !meta.GetObjectData().PrepareCompact() {
			logutil.Info("[FlushTabletail] tomb prepareCompact false", zap.String("table", tableDesc), zap.String("obj", meta.ID().String()))
			return moerr.GetOkExpectedEOB()
		}
		scopes = append(scopes, *meta.AsCommonID())
	}

	factory := jobs.FlushTableTailTaskFactory(metas, tombstoneMetas, r.rt)
	if _, err := r.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory); err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Error("[FlushTabletail] Sched Failure", zap.String("table", tableDesc), zap.Error(err))
		}
		return moerr.GetOkExpectedEOB()
	}
	return nil
}

func (r *runner) EstimateTableMemSize(table *catalog.TableEntry, tree *model.TableTree) (asize int, dsize int) {
	for _, obj := range tree.Objs {
		object, err := table.GetObjectByID(obj.ID, false)
		if err != nil {
			panic(err)
		}
		a, _ := object.GetObjectData().EstimateMemSize()
		asize += a
	}
	for _, obj := range tree.Tombstones {
		object, err := table.GetObjectByID(obj.ID, true)
		if err != nil {
			panic(err)
		}
		a, _ := object.GetObjectData().EstimateMemSize()
		dsize += a
	}
	return
}

func (r *runner) collectTableMemUsage(entry *logtail.DirtyTreeEntry) (memPressureRate float64) {
	// reuse the list
	r.objMemSizeList = r.objMemSizeList[:0]
	sizevisitor := new(model.BaseTreeVisitor)
	var totalSize int
	sizevisitor.TableFn = func(did, tid uint64) error {
		db, err := r.catalog.GetDatabaseByID(did)
		if err != nil {
			panic(err)
		}
		table, err := db.GetTableEntryByID(tid)
		if err != nil {
			panic(err)
		}
		table.Stats.Init(r.options.maxFlushInterval)
		dirtyTree := entry.GetTree().GetTable(tid)
		asize, dsize := r.EstimateTableMemSize(table, dirtyTree)
		totalSize += asize + dsize
		r.objMemSizeList = append(r.objMemSizeList, tableAndSize{table, asize, dsize})
		return moerr.GetOkStopCurrRecur()
	}
	if err := entry.GetTree().Visit(sizevisitor); err != nil {
		panic(err)
	}

	slices.SortFunc(r.objMemSizeList, func(a, b tableAndSize) int {
		return b.asize - a.asize // sort by asize desc
	})

	pressure := float64(totalSize) / float64(common.RuntimeOverallFlushMemCap.Load())
	if pressure > 1.0 {
		pressure = 1.0
	}

	logutil.Info(
		"[flushtabletail] mem scan result",
		zap.Float64("pressure", pressure),
		zap.String("totalSize", common.HumanReadableBytes(totalSize)),
	)
	return pressure
}

func (r *runner) checkFlushConditionAndFire(entry *logtail.DirtyTreeEntry, force bool, pressure float64) {
	count := 0
	for _, ticket := range r.objMemSizeList {
		table, asize, dsize := ticket.tbl, ticket.asize, ticket.dsize
		dirtyTree := entry.GetTree().GetTable(table.ID)

		if force {
			logutil.Infof("[flushtabletail] force flush %v-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
			if err := r.fireFlushTabletail(table, dirtyTree); err == nil {
				table.Stats.ResetDeadline(r.options.maxFlushInterval)
			}
			continue
		}

		flushReady := func() bool {
			if !table.IsActive() {
				count++
				if pressure < 0.5 || count < 200 {
					// if the table has been dropped, flush it immediately if
					// resources are available.
					// count is used to avoid too many flushes in one round
					return true
				}
				return false
			}
			// time to flush
			if table.Stats.GetFlushDeadline().Before(time.Now()) {
				return true
			}
			// this table is too large, flush it
			if asize+dsize > int(common.FlushMemCapacity.Load()) {
				return true
			}
			// unflushed data is too large, flush it
			if asize > common.Const1MBytes && rand.Float64() < pressure {
				return true
			}
			return false
		}

		ready := flushReady()

		if asize+dsize > 2*1000*1024 {
			logutil.Infof("[flushtabletail] %v(%v) %v dels  FlushCountDown %v, flushReady %v",
				table.GetLastestSchemaLocked(false).Name,
				common.HumanReadableBytes(asize+dsize),
				common.HumanReadableBytes(dsize),
				time.Until(table.Stats.GetFlushDeadline()),
				ready,
			)
		}

		if ready {
			if err := r.fireFlushTabletail(table, dirtyTree); err == nil {
				table.Stats.ResetDeadline(r.options.maxFlushInterval)
			}
		}
	}
}

// for a non-forced dirty tree, it contains all unflushed data in the db at the latest moment
func (r *runner) scheduleFlush(entry *logtail.DirtyTreeEntry, force bool) {
	if entry.IsEmpty() {
		return
	}
	logutil.Debug(entry.String())

	pressure := r.collectTableMemUsage(entry)
	r.checkFlushConditionAndFire(entry, force, pressure)
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
	r.scheduleFlush(force, true)
	r.scheduleFlush(normal, false)
}

func (r *runner) crontask(ctx context.Context) {
	// friendly for freezing objects, avoiding fierece refer cnt compectition
	lag := 3 * time.Second
	if r.options.maxFlushInterval < time.Second {
		// test env, no need to lag
		lag = 0 * time.Second
	}
	hb := w.NewHeartBeaterWithFunc(r.options.collectInterval, func() {
		r.source.Run(lag)
		entry := r.source.GetAndRefreshMerged()
		if !entry.IsEmpty() {
			e := new(DirtyCtx)
			e.tree = entry
			r.dirtyEntryQueue.Enqueue(e)
		}
		_, endts := entry.GetTimeRange()
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

func (r *runner) CollectCheckpointsInRange(
	ctx context.Context, start, end types.TS,
) (locations string, checkpointed types.TS, err error) {
	if r.IsTSStale(end) {
		return "", types.TS{}, moerr.NewInternalErrorf(ctx, "ts %v is staled", end.ToString())
	}
	r.storage.Lock()
	tree := r.storage.incrementals.Copy()
	global, _ := r.storage.globals.Max()
	r.storage.Unlock()
	locs := make([]string, 0)
	ckpStart := types.MaxTs()
	newStart := start
	if global != nil && global.HasOverlap(start, end) {
		locs = append(locs, global.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(global.version)))
		newStart = global.end.Next()
		ckpStart = global.GetEnd()
		checkpointed = global.GetEnd()
	}
	pivot := NewCheckpointEntry(r.rt.SID(), newStart, newStart, ET_Incremental)

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
				duration := fmt.Sprintf("[%s_%s]",
					ckpStart.ToString(),
					ckpStart.ToString())
				locs = append(locs, duration)
				locations = strings.Join(locs, ";")
				return
			}
			if e.HasOverlap(newStart, end) {
				locs = append(locs, e.GetLocation().String())
				locs = append(locs, strconv.Itoa(int(e.version)))
				start := e.GetStart()
				if start.LT(&ckpStart) {
					ckpStart = start
				}
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
			start := e.GetStart()
			if start.LT(&ckpStart) {
				ckpStart = start
			}
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
			duration := fmt.Sprintf("[%s_%s]",
				ckpStart.ToString(),
				ckpStart.ToString())
			locs = append(locs, duration)
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
			duration := fmt.Sprintf("[%s_%s]",
				ckpStart.ToString(),
				ckpStart.ToString())
			locs = append(locs, duration)
			locations = strings.Join(locs, ";")
			return
		}
		locs = append(locs, e.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(e.version)))
		start := e.GetStart()
		if start.LT(&ckpStart) {
			ckpStart = start
		}
		checkpointed = e.GetEnd()
		// checkpoints = append(checkpoints, e)
	}

	if len(locs) == 0 {
		return
	}
	duration := fmt.Sprintf("[%s_%s]",
		ckpStart.ToString(),
		checkpointed.ToString())
	locs = append(locs, duration)
	locations = strings.Join(locs, ";")
	return
}
