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
	"errors"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

type checkpointJob struct {
	doneCh   chan struct{}
	executor *checkpointExecutor

	runICKPFunc func(context.Context, *runner) error

	gckpCtx     *gckpContext
	runGCKPFunc func(context.Context, *gckpContext, *runner) error

	err error
}

func (job *checkpointJob) RunGCKP(ctx context.Context) (err error) {
	if job.runGCKPFunc != nil {
		return job.runGCKPFunc(ctx, job.gckpCtx, job.executor.runner)
	}

	// two for chaos test
	// one to ensure it has entered `RunGCKP`
	objectio.WaitInjected(objectio.FJ_GCKPWait1)
	// two to block the execution till being notified
	objectio.WaitInjected(objectio.FJ_GCKPWait1)

	_, err = job.doGlobalCheckpoint(
		job.gckpCtx.end,
		job.gckpCtx.ckpLSN,
		job.gckpCtx.truncateLSN,
		job.gckpCtx.histroyRetention,
		job.gckpCtx.predecessor,
	)

	return
}

func (job *checkpointJob) doGlobalCheckpoint(
	end types.TS,
	ckpLSN, truncateLSN uint64,
	interval time.Duration,
	predecessor *CheckpointEntry,
) (entry *CheckpointEntry, err error) {
	var (
		errPhase           string
		fields             []zap.Field
		now                = time.Now()
		runner             = job.executor.runner
		files              []string
		tableIDLocation    objectio.LocationSlice
		metadataPublished  bool
		rollbackFileCount  int
		rollbackCleanupErr error
	)

	entry = NewCheckpointEntry(
		runner.rt.SID(),
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
			errorFields := []zap.Field{
				zap.String("entry", entry.String()),
				zap.String("phase", errPhase),
				zap.Error(err),
				zap.Duration("cost", time.Since(now)),
			}
			if rollbackFileCount > 0 {
				errorFields = append(errorFields,
					zap.Int("rollback-object-count", rollbackFileCount))
			}
			if rollbackCleanupErr != nil {
				errorFields = append(errorFields,
					zap.NamedError("rollback-error", rollbackCleanupErr))
			}
			logutil.Error("GCKP-Execute-Error", errorFields...)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"GCKP-Execute-End",
				fields...,
			)
		}
	}()
	defer func() {
		// A panic may occur after saveCheckpoint has made metadata durable but
		// before it returns. Only an explicit pre-publication error is safe to
		// roll back; an uncertain publication state must never lose referenced
		// data.
		if metadataPublished || err == nil {
			return
		}
		unpublishedFiles := append([]string(nil), files...)
		for i := 0; i < tableIDLocation.Len(); i++ {
			unpublishedFiles = append(
				unpublishedFiles, tableIDLocation.Get(i).Name().String())
		}
		if len(unpublishedFiles) == 0 {
			return
		}
		rollbackFileCount, rollbackCleanupErr =
			ioutil.DeleteUnpublishedObjects(
				job.executor.ctx, runner.rt.Fs, unpublishedFiles...)
		if rollbackCleanupErr != nil {
			err = errors.Join(err, rollbackCleanupErr)
		}
	}()
	if predecessor == nil || !predecessor.IsFinished() ||
		!predecessor.IsIncremental() || !predecessor.end.EQ(&end) {
		errPhase = "resolve-predecessor"
		err = moerr.NewInternalErrorNoCtxf(
			"global checkpoint %s has no finished incremental predecessor", end.ToString())
		return
	}

	predecessorTableIDLocation := predecessor.GetTableIDLocation()

	if ok := runner.store.AddGCKPIntent(entry); !ok {
		errPhase = "add-intent"
		err = ErrBadIntent
		return
	}
	// The intent is deliberately visible to status and rollback paths before
	// its durable metadata has been written. Consumers must ignore that pending
	// state; this barrier makes the publication boundary deterministic in tests.
	objectio.WaitInjectedCtx(job.executor.ctx, objectio.FJ_GCKPWaitAfterIntent)

	var emptyLocation objectio.Location
	var historyStart, historyEnd types.TS
	var historyKnown bool
	var syncErr error
	tableIDLocation, historyStart, historyEnd, historyKnown, syncErr =
		logtail.SyncTableIDBatchWithHistory(
			job.executor.ctx,
			entry.start,
			entry.end,
			job.executor.cfg.TableIDHistoryDuration,
			job.executor.cfg.TableIDSinkerThreshold,
			emptyLocation,
			predecessor.GetVersion(),
			predecessorTableIDLocation,
			entry.end.Prev(),
			common.CheckpointAllocator,
			runner.rt.Fs,
		)
	if syncErr != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "sync-table-id"
		err = syncErr
		return
	}
	requiredHistoryStart := types.BuildTS(
		entry.end.Physical()-job.executor.cfg.TableIDHistoryDuration.Nanoseconds(),
		0,
	)
	requiredHistoryEnd := entry.end.Prev()
	if !historyKnown || historyStart.GT(&requiredHistoryStart) ||
		historyEnd.LT(&requiredHistoryEnd) {
		runner.store.RemoveGCKPIntent()
		errPhase = "validate-table-id-history"
		err = moerr.NewInternalErrorNoCtxf(
			"global checkpoint table-ID history is incomplete: covered %s-%s, required %s-%s",
			historyStart.ToString(), historyEnd.ToString(),
			requiredHistoryStart.ToString(), requiredHistoryEnd.ToString(),
		)
		return
	}
	entry.SetTableIDLocation(tableIDLocation)

	var data *logtail.CheckpointData_V2
	factory := logtail.GlobalCheckpointDataFactory(entry.end, interval, runner.rt.Fs)

	if data, err = factory(runner.catalog); err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "collect"
		return
	}
	defer data.Close()

	var location objectio.Location
	location, files, err = data.Sync(
		job.executor.ctx, runner.rt.Fs,
	)
	fields = data.ExportStats("")
	if err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "flush"
		return
	}

	entry.SetLocation(location, location)

	files = append(files, location.Name().String())
	var name string
	if name, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "save"
		return
	}
	metadataPublished = true
	defer func() {
		entry.SetState(ST_Finished)
	}()

	files = append(files, name)

	if err = appendCheckpointFilesToWAL(runner, files); err != nil {
		errPhase = "wal-files"
		return
	}
	return
}

// resolveICKPTableIDPredecessor pins the newest authoritative, finished
// checkpoint that immediately precedes entry. A GCKP intent is published before
// its table-ID index is durable, so it must never participate in this handoff.
//
// The underlying ICKP is preferred while checkpoint GC still retains it. If it
// has already been collected, a current-format GCKP is used and its coverage is
// validated by SyncTableIDBatchWithHistory while the rows are streamed. Legacy
// checkpoints can legitimately have no table-ID index; returning an empty
// location starts a new, explicitly partial range. doGlobalCheckpoint keeps
// publication fail-closed until that range spans the configured history window.
func (job *checkpointJob) resolveICKPTableIDPredecessor(
	entry *CheckpointEntry,
) (location objectio.LocationSlice, requiredEnd types.TS, err error) {
	runner := job.executor.runner
	global := runner.store.MaxGlobalCheckpoint()
	var globalEnd types.TS
	var globalLocation objectio.LocationSlice
	if global != nil {
		globalEnd = global.GetEnd()
		globalLocation = global.GetTableIDLocation()
	}
	globalMatches := global != nil && globalEnd.EQ(&entry.start)

	if !entry.start.IsEmpty() {
		incremental := runner.store.MaxIncrementalCheckpoint()
		predecessorEnd := entry.start.Prev()
		if incremental != nil {
			incrementalEnd := incremental.GetEnd()
			if incrementalEnd.EQ(&predecessorEnd) {
				location = incremental.GetTableIDLocation()
				if location.Len() > 0 {
					requiredEnd = predecessorEnd
				}
				return
			}
		}
	}

	if globalMatches {
		// Empty table-ID metadata identifies a legacy checkpoint. Recover by
		// beginning a new partial range; never convert unreadable or malformed
		// non-empty metadata into an apparently valid range.
		if globalLocation.Len() == 0 {
			return nil, types.TS{}, nil
		}
		return globalLocation, entry.start.Prev(), nil
	}
	if entry.start.IsEmpty() {
		return nil, types.TS{}, nil
	}
	return nil, types.TS{}, moerr.NewInternalErrorNoCtxf(
		"incremental checkpoint %s has no finished checkpoint predecessor",
		entry.String(),
	)
}

func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
	if job.runICKPFunc != nil {
		return job.runICKPFunc(ctx, job.executor.runner)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	runner := job.executor.runner

	entry, rollback := runner.store.TakeICKPIntent()
	if entry == nil {
		return
	}

	var (
		errPhase           string
		lsnToTruncate      uint64
		lsn                uint64
		fatal              bool
		fields             []zap.Field
		now                = time.Now()
		files              []string
		tableIDLocation    objectio.LocationSlice
		metadataPublished  bool
		rollbackFileCount  int
		rollbackCleanupErr error
	)

	logutil.Info(
		"ICKP-Execute-Start",
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
			errorFields := []zap.Field{
				zap.String("entry", entry.String()),
				zap.Error(err),
				zap.String("phase", errPhase),
				zap.Duration("cost", time.Since(now)),
			}
			if rollbackFileCount > 0 {
				errorFields = append(errorFields,
					zap.Int("rollback-object-count", rollbackFileCount))
			}
			if rollbackCleanupErr != nil {
				errorFields = append(errorFields,
					zap.NamedError("rollback-error", rollbackCleanupErr))
			}
			logger("ICKP-Execute-Error", errorFields...)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.Uint64("truncate", lsnToTruncate))
			fields = append(fields, zap.Uint64("lsn", lsn))
			fields = append(fields, zap.Uint64("reserve", job.executor.cfg.IncrementalReservedWALCount))
			fields = append(fields, zap.String("entry", entry.String()))
			fields = append(fields, zap.Duration("age", entry.Age()))
			logutil.Info(
				"ICKP-Execute-End",
				fields...,
			)
		}
	}()
	defer func() {
		// See doGlobalCheckpoint: a panic leaves publication uncertain, so only
		// an explicit error before the durable metadata boundary may delete data.
		if metadataPublished || err == nil {
			return
		}
		unpublishedFiles := append([]string(nil), files...)
		for i := 0; i < tableIDLocation.Len(); i++ {
			unpublishedFiles = append(
				unpublishedFiles, tableIDLocation.Get(i).Name().String())
		}
		if len(unpublishedFiles) == 0 {
			return
		}
		rollbackFileCount, rollbackCleanupErr =
			ioutil.DeleteUnpublishedObjects(
				job.executor.ctx, runner.rt.Fs, unpublishedFiles...)
		if rollbackCleanupErr != nil {
			err = errors.Join(err, rollbackCleanupErr)
		}
	}()

	preTableIDLocation, requiredHistoryEnd, err :=
		job.resolveICKPTableIDPredecessor(entry)
	if err != nil {
		errPhase = "resolve-table-id-predecessor"
		rollback()
		return err
	}

	var file string
	if fields, files, err = job.executor.doIncrementalCheckpoint(entry); err != nil {
		errPhase = "do-ckp"
		rollback()
		return
	}

	tableIDLocation, _, _, _, err = logtail.SyncTableIDBatchWithHistory(
		job.executor.ctx,
		entry.start,
		entry.end,
		job.executor.cfg.TableIDHistoryDuration,
		job.executor.cfg.TableIDSinkerThreshold,
		entry.GetLocation(),
		entry.GetVersion(),
		preTableIDLocation,
		requiredHistoryEnd,
		common.CheckpointAllocator,
		runner.rt.Fs,
	)
	if err != nil {
		errPhase = "sync-table-id"
		rollback()
		return
	}
	entry.SetTableIDLocation(tableIDLocation)

	lsn = resolveCheckpointLSN(
		runner.store,
		runner.source.GetMaxLSN(entry.start, entry.end),
	)
	if lsn > job.executor.cfg.IncrementalReservedWALCount {
		lsnToTruncate = lsn - job.executor.cfg.IncrementalReservedWALCount
	}
	entry.SetLSN(lsn, lsnToTruncate)

	if prepared := runner.store.PrepareCommitICKPIntent(entry); !prepared {
		errPhase = "prepare"
		rollback()
		err = moerr.NewInternalErrorNoCtxf("cannot prepare ickp")
		return
	}

	if file, err = runner.saveCheckpoint(
		entry.start, entry.end,
	); err != nil {
		errPhase = "save-ckp"
		runner.store.RollbackICKPIntent(entry)
		rollback()
		return
	}
	metadataPublished = true

	defer func() {
		runner.store.CommitICKPIntent(entry)
		runner.postCheckpointQueue.Enqueue(entry)
		if runner.forceGCKPRequests.Load() == 0 {
			runner.TryTriggerExecuteGCKP(&gckpContext{
				end:              entry.end,
				histroyRetention: job.executor.cfg.GlobalHistoryDuration,
				ckpLSN:           lsn,
				truncateLSN:      lsnToTruncate,
				predecessor:      entry,
			})
		}
	}()

	v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())

	files = append(files, file)
	if lsnToTruncate == 0 {
		// There is no valid user-WAL range [1, 0]. Publish the checkpoint files
		// through GroupFiles, as global checkpoints do, without advancing the
		// user-WAL checkpoint/truncation watermark. This covers both an initial
		// checkpoint with no user WAL and a reservation that covers every LSN.
		if err = appendCheckpointFilesToWAL(runner, files); err != nil {
			errPhase = "wal-files"
			return
		}
		return nil
	}

	// PXU TODO: if crash here, the checkpoint log entry will be lost
	var logEntry wal.LogEntry
	if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
		errPhase = "wal-ckp"
		fatal = true
		return
	}
	if err = logEntry.WaitDone(); err != nil {
		errPhase = "wait-wal-ckp-done"
		fatal = true
		return
	}

	return nil
}

func appendCheckpointFilesToWAL(runner *runner, files []string) error {
	fileEntry, err := wal.BuildFilesEntry(files)
	if err != nil {
		return err
	}
	if _, err = runner.wal.AppendEntry(wal.GroupFiles, fileEntry); err != nil {
		return err
	}
	return nil
}

func resolveCheckpointLSN(store *runnerStore, lsn uint64) uint64 {
	if lsn != 0 {
		return lsn
	}
	// Checkpoint GC may remove every incremental checkpoint after a global
	// checkpoint has durably replaced them. An empty forced checkpoint must
	// inherit its LSN from that durable global boundary instead of emitting a
	// zero-LSN WAL range, which the WAL correctly rejects as invalid.
	if previous := store.MaxCheckpoint(); previous != nil {
		return previous.LSN()
	}
	return 0
}

func (job *checkpointJob) WaitC() <-chan struct{} {
	return job.doneCh
}

// should be called after WaitC
func (job *checkpointJob) Err() error {
	return job.err
}

func (job *checkpointJob) Done(err error) {
	job.err = err
	close(job.doneCh)
}

type checkpointExecutor struct {
	cfg CheckpointCfg

	// checkpoint policy
	incrementalPolicy *timeBasedPolicy
	globalPolicy      *countBasedPolicy

	ctx         context.Context
	cancel      context.CancelCauseFunc
	active      atomic.Bool
	runningICKP atomic.Pointer[checkpointJob]
	runningGCKP atomic.Pointer[checkpointJob]

	runner      *runner
	runICKPFunc func(context.Context, *runner) error
	runGCKPFunc func(context.Context, *gckpContext, *runner) error

	ickpQueue sm.Queue
	gckpQueue sm.Queue
}

func newCheckpointExecutor(
	runner *runner,
	cfg *CheckpointCfg,
) *checkpointExecutor {
	ctx := context.Background()
	if runner != nil {
		ctx = runner.ctx
	}
	if cfg == nil {
		cfg = new(CheckpointCfg)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	e := &checkpointExecutor{
		runner: runner,
		ctx:    ctx,
		cancel: cancel,
		cfg:    *cfg,
	}
	e.fillDefaults()

	e.incrementalPolicy = &timeBasedPolicy{interval: e.cfg.IncrementalInterval}
	e.globalPolicy = &countBasedPolicy{minCount: int(e.cfg.GlobalMinCount)}

	e.ickpQueue = sm.NewSafeQueue(1000, 100, e.onICKPEntries)
	e.gckpQueue = sm.NewSafeQueue(1000, 100, e.onGCKPEntries)
	e.ickpQueue.Start()
	e.gckpQueue.Start()

	e.active.Store(true)
	logutil.Info(
		"CKP-Executor-Started",
		zap.String("cfg", e.cfg.String()),
	)
	return e
}

func (executor *checkpointExecutor) GetCfg() *CheckpointCfg {
	return &executor.cfg
}

func (executor *checkpointExecutor) fillDefaults() {
	executor.cfg.FillDefaults()
}

func (executor *checkpointExecutor) RunningCKPJob(gckp bool) *checkpointJob {
	if gckp {
		return executor.runningGCKP.Load()
	}
	return executor.runningICKP.Load()
}

func (executor *checkpointExecutor) StopWithCause(cause error) {
	if updated := executor.active.CompareAndSwap(true, false); !updated {
		return
	}
	if cause == nil {
		cause = ErrCheckpointDisabled
	}
	executor.cancel(cause)
	job := executor.runningGCKP.Load()
	if job != nil {
		<-job.WaitC()
	}
	executor.runningGCKP.Store(nil)
	job = executor.runningICKP.Load()
	if job != nil {
		<-job.WaitC()
	}
	executor.runningICKP.Store(nil)
	executor.ickpQueue.Stop()
	executor.gckpQueue.Stop()
	logutil.Info(
		"CKP-Executor-Stopped",
		zap.Error(cause),
		zap.String("cfg", executor.cfg.String()),
	)
}
