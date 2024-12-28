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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
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

func (g globalCheckpointContext) String() string {
	return fmt.Sprintf(
		"GCTX[%v][%s][%d,%d][%s]",
		g.force,
		g.end.ToString(),
		g.truncateLSN,
		g.ckpLSN,
		g.interval.String(),
	)
}

func (g *globalCheckpointContext) Merge(other *globalCheckpointContext) {
	if other == nil {
		return
	}
	if other.force {
		g.force = true
	}
	if other.end.LE(&g.end) {
		return
	}
	g.end = other.end
	g.interval = other.interval
	g.truncateLSN = other.truncateLSN
	g.ckpLSN = other.ckpLSN
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

	// memory storage of the checkpoint entries
	store *runnerStore

	// checkpoint policy
	incrementalPolicy *timeBasedPolicy
	globalPolicy      *countBasedPolicy

	executor atomic.Pointer[checkpointExecutor]

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
	opts ...Option,
) *runner {
	r := &runner{
		ctx:       ctx,
		rt:        rt,
		catalog:   catalog,
		source:    source,
		observers: new(observers),
		wal:       wal,
	}
	for _, opt := range opts {
		opt(r)
	}
	r.fillDefaults()

	r.store = newRunnerStore(r.rt.SID(), r.options.globalVersionInterval, time.Minute*2)

	r.incrementalPolicy = &timeBasedPolicy{interval: r.options.minIncrementalInterval}
	r.globalPolicy = &countBasedPolicy{minCount: r.options.globalMinCount}
	r.incrementalCheckpointQueue = sm.NewSafeQueue(r.options.checkpointQueueSize, 100, r.onIncrementalCheckpointEntries)
	r.globalCheckpointQueue = sm.NewSafeQueue(r.options.checkpointQueueSize, 100, r.onGlobalCheckpointEntries)
	r.gcCheckpointQueue = sm.NewSafeQueue(100, 100, r.onGCCheckpointEntries)
	r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
	r.StartExecutor()

	return r
}

func (r *runner) StopExecutor(err error) {
	executor := r.executor.Load()
	if executor == nil {
		return
	}
	executor.StopWithCause(err)
	r.executor.CompareAndSwap(executor, nil)
}

func (r *runner) StartExecutor() {
	for {
		executor := r.executor.Load()
		if executor != nil {
			executor.StopWithCause(ErrExecutorRestarted)
		}
		newExecutor := newCheckpointExecutor(r)
		if r.executor.CompareAndSwap(executor, newExecutor) {
			break
		}
	}
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
	_, _ = fmt.Fprintf(&buf, "checkpointQueueSize=%v, ", r.options.checkpointQueueSize)
	_, _ = fmt.Fprintf(&buf, "checkpointBlockRows=%v, ", r.options.checkpointBlockRows)
	_, _ = fmt.Fprintf(&buf, "checkpointSize=%v, ", r.options.checkpointSize)
	_, _ = fmt.Fprintf(&buf, ">")
	return buf.String()
}

func (r *runner) AddCheckpointMetaFile(name string) {
	r.store.AddMetaFile(name)
}

func (r *runner) RemoveCheckpointMetaFile(name string) {
	r.store.RemoveMetaFile(name)
}

func (r *runner) GetCheckpointMetaFiles() map[string]struct{} {
	return r.store.GetMetaFiles()
}

func (r *runner) onGlobalCheckpointEntries(items ...any) {
	var (
		err              error
		mergedCtx        *globalCheckpointContext
		fromCheckpointed types.TS
		toCheckpointed   types.TS
		now              = time.Now()
	)
	executor := r.executor.Load()
	if executor == nil {
		err = ErrCheckpointDisabled
	}
	defer func() {
		var createdEntry string
		logger := logutil.Debug
		if err != nil {
			logger = logutil.Error
		} else {
			toEntry := r.store.MaxGlobalCheckpoint()
			if toEntry != nil {
				toCheckpointed = toEntry.GetEnd()
				createdEntry = toEntry.String()
			}
		}

		if err != nil || time.Since(now) > time.Second*10 || toCheckpointed.GT(&fromCheckpointed) {
			logger(
				"GCKP-Execute-End",
				zap.Duration("cost", time.Since(now)),
				zap.String("ctx", mergedCtx.String()),
				zap.String("created", createdEntry),
				zap.Error(err),
			)
		}
	}()

	for _, item := range items {
		oneCtx := item.(*globalCheckpointContext)
		if mergedCtx == nil {
			mergedCtx = oneCtx
		} else {
			mergedCtx.Merge(oneCtx)
		}
	}
	if mergedCtx == nil {
		return
	}

	fromEntry := r.store.MaxGlobalCheckpoint()
	if fromEntry != nil {
		fromCheckpointed = fromEntry.GetEnd()
	}

	if mergedCtx.end.LE(&fromCheckpointed) {
		logutil.Info(
			"GCKP-Execute-Skip",
			zap.String("have", fromCheckpointed.ToString()),
			zap.String("want", mergedCtx.end.ToString()),
		)
		return
	}

	// [force==false and ickpCount < count policy]
	if !mergedCtx.force {
		ickpCount := r.store.GetPenddingIncrementalCount()
		if !r.globalPolicy.Check(ickpCount) {
			logutil.Debug(
				"GCKP-Execute-Skip",
				zap.Int("pending-ickp", ickpCount),
				zap.String("want", mergedCtx.end.ToString()),
			)
			return
		}
	}

	err = executor.RunGCKP(mergedCtx)
}

func (r *runner) onGCCheckpointEntries(items ...any) {
	r.store.TryGC()
}

func (r *runner) onIncrementalCheckpointEntries(items ...any) {
	var (
		err error
		now = time.Now()
	)
	executor := r.executor.Load()
	if executor == nil {
		err = ErrCheckpointDisabled
	}
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		if err != nil || time.Since(now) > time.Second*10 {
			logger(
				"ICKP-Execute-Runner-End",
				zap.Duration("cost", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	err = executor.RunICKP()
}

func (r *runner) saveCheckpoint(
	start, end types.TS,
) (name string, err error) {
	if injectErrMsg, injected := objectio.CheckpointSaveInjected(); injected {
		return "", moerr.NewInternalErrorNoCtx(injectErrMsg)
	}
	bat := r.collectCheckpointMetadata(start, end)
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

// TODO: using ctx
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

	entry = NewCheckpointEntry(
		r.rt.SID(),
		types.TS{},
		end.Next(),
		ET_Global,
	)
	entry.ckpLSN = ckpLSN
	entry.truncateLSN = truncateLSN

	logutil.Info(
		"GCKP-Execute-Start",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			logutil.Error(
				"GCKP-Execute-Error",
				zap.String("entry", entry.String()),
				zap.String("phase", errPhase),
				zap.Error(err),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"GCKP-Execute-End",
				fields...,
			)
		}
	}()

	if ok := r.store.AddGCKPIntent(entry); !ok {
		err = ErrBadIntent
		return
	}

	var data *logtail.CheckpointData
	factory := logtail.GlobalCheckpointDataFactory(r.rt.SID(), entry.end, interval)

	if data, err = factory(r.catalog); err != nil {
		r.store.RemoveGCKPIntent()
		errPhase = "collect"
		return
	}
	defer data.Close()

	fields = data.ExportStats("")

	cnLocation, tnLocation, files, err := data.WriteTo(
		r.rt.Fs.Service, r.options.checkpointBlockRows, r.options.checkpointSize,
	)
	if err != nil {
		r.store.RemoveGCKPIntent()
		errPhase = "flush"
		return
	}

	entry.SetLocation(cnLocation, tnLocation)

	files = append(files, cnLocation.Name().String())
	var name string
	if name, err = r.saveCheckpoint(entry.start, entry.end); err != nil {
		r.store.RemoveGCKPIntent()
		errPhase = "save"
		return
	}
	defer func() {
		entry.SetState(ST_Finished)
	}()

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

func (r *runner) softScheduleCheckpoint(ts *types.TS) (ret *CheckpointEntry, err error) {
	var (
		updated bool
	)
	intent := r.store.GetICKPIntent()

	check := func() (done bool) {
		if !r.source.IsCommitted(intent.GetStart(), intent.GetEnd()) {
			return false
		}
		tree := r.source.ScanInRangePruned(intent.GetStart(), intent.GetEnd())
		tree.GetTree().Compact()
		if !tree.IsEmpty() && intent.TooOld() {
			logutil.Warn(
				"CheckPoint-Wait-TooOld",
				zap.String("entry", intent.String()),
				zap.Duration("age", intent.Age()),
			)
			intent.DeferRetirement()
		}
		return tree.IsEmpty()
	}

	now := time.Now()

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		} else {
			ret = intent
		}
		intentInfo := "nil"
		ageStr := ""
		if intent != nil {
			intentInfo = intent.String()
			ageStr = intent.Age().String()
		}
		if (err != nil && err != ErrPendingCheckpoint) || (intent != nil && intent.TooOld()) {
			logger(
				"ICKP-Schedule-Soft",
				zap.String("intent", intentInfo),
				zap.String("ts", ts.ToString()),
				zap.Duration("cost", time.Since(now)),
				zap.String("age", ageStr),
				zap.Error(err),
			)
		}
	}()

	if intent == nil {
		start := r.store.GetCheckpointed()
		if ts.LT(&start) {
			return
		}
		if !r.incrementalPolicy.Check(start) {
			return
		}
		_, count := r.source.ScanInRange(start, *ts)
		if count < r.options.minCount {
			return
		}
		intent, updated = r.store.UpdateICKPIntent(ts, true, false)
		if updated {
			logutil.Info(
				"ICKP-Schedule-Soft-Updated",
				zap.String("intent", intent.String()),
				zap.String("ts", ts.ToString()),
			)
		}
	}

	// [intent == nil]
	// if intent is nil, it means no need to do checkpoint
	if intent == nil {
		return
	}

	// [intent != nil]

	var (
		policyChecked  bool
		flushedChecked bool
	)
	policyChecked = intent.IsPolicyChecked()
	if !policyChecked {
		if !r.incrementalPolicy.Check(intent.GetStart()) {
			return
		}
		_, count := r.source.ScanInRange(intent.GetStart(), intent.GetEnd())
		if count < r.options.minCount {
			return
		}
		policyChecked = true
	}

	flushedChecked = intent.IsFlushChecked()
	if !flushedChecked && check() {
		flushedChecked = true
	}

	if policyChecked != intent.IsPolicyChecked() || flushedChecked != intent.IsFlushChecked() {
		endTS := intent.GetEnd()
		intent, updated = r.store.UpdateICKPIntent(&endTS, policyChecked, flushedChecked)

		if updated {
			logutil.Info(
				"ICKP-Schedule-Soft-Updated",
				zap.String("intent", intent.String()),
				zap.String("endTS", ts.ToString()),
			)
		}
	}

	// no need to do checkpoint
	if intent == nil {
		return
	}

	if intent.end.LT(ts) {
		err = ErrPendingCheckpoint
		r.TryTriggerExecuteICKP()
		return
	}

	if intent.AllChecked() {
		r.TryTriggerExecuteICKP()
	}
	return
}

func (r *runner) TryTriggerExecuteGCKP(ctx *globalCheckpointContext) (err error) {
	if r.disabled.Load() {
		return
	}
	_, err = r.globalCheckpointQueue.Enqueue(ctx)
	return
}

func (r *runner) TryTriggerExecuteICKP() (err error) {
	if r.disabled.Load() {
		return
	}
	_, err = r.incrementalCheckpointQueue.Enqueue(struct{}{})
	return
}

// NOTE:
// when `force` is true, it must be called after force flush till the given ts
// force: if true, not to check the validness of the checkpoint
func (r *runner) TryScheduleCheckpoint(
	ts types.TS, force bool,
) (ret Intent, err error) {
	if r.disabled.Load() {
		return
	}
	if !force {
		return r.softScheduleCheckpoint(&ts)
	}

	intent, updated := r.store.UpdateICKPIntent(&ts, true, true)
	if intent == nil {
		return
	}

	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}

		logger(
			"ICKP-Schedule-Force",
			zap.String("intent", intent.String()),
			zap.String("ts", ts.ToString()),
			zap.Bool("updated", updated),
			zap.Duration("cost", time.Since(now)),
			zap.Error(err),
		)
	}()

	r.TryTriggerExecuteICKP()

	if intent.end.LT(&ts) {
		err = ErrPendingCheckpoint
		return
	}

	return intent, nil
}

func (r *runner) getRunningCKPJob(gckp bool) (job *checkpointJob, err error) {
	executor := r.executor.Load()
	if executor == nil {
		err = ErrExecutorClosed
		return
	}
	job = executor.RunningCKPJob(gckp)
	return
}

func (r *runner) fillDefaults() {
	if r.options.collectInterval <= 0 {
		// TODO: define default value
		r.options.collectInterval = time.Second * 5
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

func (r *runner) Start() {
	r.onceStart.Do(func() {
		r.postCheckpointQueue.Start()
		r.incrementalCheckpointQueue.Start()
		r.globalCheckpointQueue.Start()
		r.gcCheckpointQueue.Start()
	})
}

func (r *runner) Stop() {
	r.onceStop.Do(func() {
		r.incrementalCheckpointQueue.Stop()
		r.globalCheckpointQueue.Stop()
		r.gcCheckpointQueue.Stop()
		r.postCheckpointQueue.Stop()
	})
}

func (r *runner) GetDirtyCollector() logtail.Collector {
	return r.source
}

func (r *runner) CollectCheckpointsInRange(
	ctx context.Context, start, end types.TS,
) (locations string, checkpointed types.TS, err error) {
	return r.store.CollectCheckpointsInRange(ctx, start, end)
}
