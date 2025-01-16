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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
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

type gckpContext struct {
	force            bool
	end              types.TS
	histroyRetention time.Duration
	truncateLSN      uint64
	ckpLSN           uint64
}

func (g gckpContext) String() string {
	return fmt.Sprintf(
		"GCTX[%v][%s][%d,%d][%s]",
		g.force,
		g.end.ToString(),
		g.truncateLSN,
		g.ckpLSN,
		g.histroyRetention.String(),
	)
}

func (g *gckpContext) Merge(other *gckpContext) {
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
	g.histroyRetention = other.histroyRetention
	g.truncateLSN = other.truncateLSN
	g.ckpLSN = other.ckpLSN
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
	ctx context.Context

	// logtail source
	source    logtail.Collector
	catalog   *catalog.Catalog
	rt        *dbutils.Runtime
	observers *observers
	wal       wal.Driver

	// memory storage of the checkpoint entries
	store *runnerStore

	executor atomic.Pointer[checkpointExecutor]

	postCheckpointQueue sm.Queue
	gcCheckpointQueue   sm.Queue
	replayQueue         sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewRunner(
	ctx context.Context,
	rt *dbutils.Runtime,
	catalog *catalog.Catalog,
	source logtail.Collector,
	wal wal.Driver,
	cfg *CheckpointCfg,
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
	r.StartExecutor(cfg)
	cfg = r.GetCfg()

	// Note: we can't change the global history interval in runtime
	r.store = newRunnerStore(r.rt.SID(), cfg.GlobalHistoryDuration, time.Minute*2)

	r.gcCheckpointQueue = sm.NewSafeQueue(100, 100, r.onGCCheckpointEntries)
	r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
	r.replayQueue = sm.NewSafeQueue(100, 10, r.onReplayCheckpoint)

	return r
}

func (r *runner) GetCfg() *CheckpointCfg {
	executor := r.executor.Load()
	if executor == nil {
		return nil
	}
	return executor.GetCfg()
}

func (r *runner) StopExecutor(err error) (cfg *CheckpointCfg) {
	for {
		running := r.executor.Load()
		if running == nil {
			return
		}
		running.StopWithCause(err)
		if stopped := r.executor.CompareAndSwap(running, nil); stopped {
			cfg = running.GetCfg()
			break
		}
	}
	return
}

func (r *runner) StartExecutor(cfg *CheckpointCfg) {
	for {
		executor := r.executor.Load()
		if executor != nil {
			executor.StopWithCause(ErrExecutorRestarted)
		}
		newExecutor := newCheckpointExecutor(r, cfg)
		if r.executor.CompareAndSwap(executor, newExecutor) {
			break
		}
	}
}

func (r *runner) String() string {
	cfg := r.GetCfg()
	if cfg == nil {
		return "<RO-CKPRunner>"
	}
	return fmt.Sprintf("<RW-CKPRunner[%s]>", cfg.String())
}

func (r *runner) ModeString() string {
	cfg := r.GetCfg()
	if cfg == nil {
		return "<RO>"
	}
	return "<RW>"
}

func (r *runner) ReplayCKPEntry(entry *CheckpointEntry) (err error) {
	_, err = r.replayQueue.Enqueue(entry)
	return
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

func (r *runner) TryTriggerExecuteGCKP(ctx *gckpContext) (err error) {
	executor := r.executor.Load()
	if executor == nil {
		err = ErrExecutorClosed
		return
	}
	return executor.TriggerExecutingGCKP(ctx)
}

func (r *runner) TryTriggerExecuteICKP() (err error) {
	executor := r.executor.Load()
	if executor == nil {
		err = ErrExecutorClosed
		return
	}

	return executor.TriggerExecutingICKP()
}

// NOTE:
// when `force` is true, it must be called after force flush till the given ts
// force: if true, not to check the validness of the checkpoint
func (r *runner) TryScheduleCheckpoint(
	ts types.TS, force bool,
) (ret Intent, err error) {
	executor := r.executor.Load()
	if executor == nil {
		err = ErrExecutorClosed
		return
	}
	return executor.TryScheduleCheckpoint(ts, force)
}

func (r *runner) Start() {
	r.onceStart.Do(func() {
		r.postCheckpointQueue.Start()
		r.gcCheckpointQueue.Start()
		r.replayQueue.Start()
		logutil.Info(
			"CKPRunner-Started",
			zap.String("mode", r.ModeString()),
		)
	})
}

func (r *runner) Stop() {
	r.onceStop.Do(func() {
		mode := r.ModeString()
		r.replayQueue.Stop()
		r.StopExecutor(ErrStopRunner)
		r.gcCheckpointQueue.Stop()
		r.postCheckpointQueue.Stop()
		logutil.Info(
			"CKPRunner-Stopped",
			zap.String("mode", mode),
		)
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

//=============================================================================
// internal implmementations
//=============================================================================

func (r *runner) fillDefaults() {}

func (r *runner) getRunningCKPJob(gckp bool) (job *checkpointJob, err error) {
	executor := r.executor.Load()
	if executor == nil {
		err = ErrExecutorClosed
		return
	}
	job = executor.RunningCKPJob(gckp)
	return
}

func (r *runner) replayOneEntry(entry *CheckpointEntry) {
	defer entry.Done()
	if !entry.IsFinished() {
		logutil.Warn(
			"Replay-CKP-NoFinished",
			zap.String("entry", entry.String()),
		)
		return
	}

	if entry.IsGlobal() {
		if ok := r.store.AddGCKPFinishedEntry(entry); !ok {
			logutil.Warn(
				"Replay-GCKP-AddFailed",
				zap.String("entry", entry.String()),
			)
		}
	} else if entry.IsIncremental() {
		if ok := r.store.AddICKPFinishedEntry(entry); !ok {
			// ickp entry is not the youngest neighbor of
			// the max ickp entry
			logutil.Warn(
				"Replay-ICKP-AddFailed",
				zap.String("entry", entry.String()),
			)
		}
	} else if entry.IsBackup() {
		if ok := r.store.AddBackupCKPEntry(entry); !ok {
			logutil.Warn(
				"Replay-Backup-AddFailed",
				zap.String("entry", entry.String()),
			)
		}
	} else if entry.IsCompact() {
		if ok := r.store.UpdateCompacted(entry); !ok {
			logutil.Warn(
				"Replay-Compact-AddFailed",
				zap.String("entry", entry.String()),
			)
		}
	}
}

func (r *runner) onReplayCheckpoint(entries ...any) {
	for _, e := range entries {
		entry := e.(*CheckpointEntry)
		r.replayOneEntry(entry)
	}
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

func (r *runner) onGCCheckpointEntries(items ...any) {
	r.store.TryGC()
}

func (r *runner) saveCheckpoint(
	start, end types.TS,
) (name string, err error) {
	if injectErrMsg, injected := objectio.CheckpointSaveInjected(); injected {
		return "", moerr.NewInternalErrorNoCtx(injectErrMsg)
	}
	bat := r.collectCheckpointMetadata(start, end)
	defer bat.Close()
	name = ioutil.EncodeCKPMetadataFullName(start, end)
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
	fileName := ioutil.EncodeCKPMetadataName(start, end)
	r.store.AddMetaFile(fileName)
	return
}

func IsAllDirtyFlushed(source logtail.Collector, cata *catalog.Catalog, start, end types.TS, doPrint bool) bool {
	tree, _ := source.ScanInRange(start, end)
	ready := true
	var notFlushed *catalog.ObjectEntry
	for _, table := range tree.GetTree().Tables {
		db, err := cata.GetDatabaseByID(table.DbID)
		if err != nil {
			continue
		}
		table, err := db.GetTableEntryByID(table.ID)
		if err != nil {
			continue
		}
		ready, notFlushed = IsTableTailFlushed(table, start, end, false)
		if !ready {
			break
		}
		ready, notFlushed = IsTableTailFlushed(table, start, end, true)
		if !ready {
			break
		}
	}
	if !ready && doPrint {
		table := notFlushed.GetTable()
		tableDesc := fmt.Sprintf("%d-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
		logutil.Info("waiting for dirty tree %s", zap.String("table", tableDesc), zap.String("obj", notFlushed.StringWithLevel(2)))
	}
	return ready
}

func IsTableTailFlushed(table *catalog.TableEntry, start, end types.TS, isTombstone bool) (bool, *catalog.ObjectEntry) {
	var it btree.IterG[*catalog.ObjectEntry]
	if isTombstone {
		table.WaitTombstoneObjectCommitted(end)
		it = table.MakeTombstoneObjectIt()
	} else {
		table.WaitDataObjectCommitted(end)
		it = table.MakeDataObjectIt()
	}
	earlybreak := false
	// some entries shared the same timestamp with end, so we need to seek to the next one
	key := &catalog.ObjectEntry{EntryMVCCNode: catalog.EntryMVCCNode{DeletedAt: end.Next()}}
	var ok bool
	if ok = it.Seek(key); !ok {
		ok = it.Last()
	}
	for ; ok; ok = it.Prev() {
		if earlybreak {
			break
		}
		obj := it.Item()
		if !obj.IsAppendable() || obj.IsDEntry() || obj.CreatedAt.GT(&end) {
			continue
		}
		// check only appendable C entries
		if obj.CreatedAt.LT(&start) {
			earlybreak = true
		}
		// the C entry has no counterpart D entry or the D entry is not committed yet
		if next := obj.GetNextVersion(); next == nil || !next.IsCommitted() {
			return false, obj
		}
	}

	return true, nil
}
