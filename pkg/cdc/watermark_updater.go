// Copyright 2024 Matrix Origin
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

// Package cdc implements watermark management for Change Data Capture with eventual consistency.
//
// # Watermark Consistency Design
//
// The watermark system follows a "lag-acceptable, advance-forbidden" consistency model:
//   - Watermarks MAY lag behind actual data progress (causes duplicate processing, which is acceptable)
//   - Watermarks MUST NEVER advance ahead of persisted data (would cause data loss, which is forbidden)
//
// This design choice enables:
//  1. Async batching for better performance (updates buffered and persisted in batches every 3s)
//  2. Simplified error handling (UpdateWatermarkOnly never fails, always returns nil)
//  3. Crash resilience (watermark lag on crash is acceptable, prevents data loss)
//
// # Three-Tier Cache Architecture
//
// Watermarks flow through three cache levels before reaching the database:
//
//	cacheUncommitted -> cacheCommitting -> cacheCommitted <-> Database
//
//	- cacheUncommitted: Immediate write buffer, updated synchronously on UpdateWatermarkOnly()
//	- cacheCommitting: Transition state during async batch persistence to database
//	- cacheCommitted: Synchronized with database, represents durable watermark state
//
// Reads prioritize newer caches (uncommitted > committing > committed) to get latest watermark.
//
// # Failure Scenarios and Guarantees
//
// 1. System Crash Before CronJob Persists:
//   - Watermarks in cacheUncommitted are lost
//   - Next run reads old watermark from database
//   - Result: Duplicate data processing (acceptable, handled by idempotency)
//
// 2. CronJob SQL Execution Fails:
//   - Failed keys return to cacheUncommitted and retry independently
//   - Successfully persisted protocol batches still advance cacheCommitted
//   - Result: Duplicate data processing is possible; watermark over-advance is not
//
// 3. Race Between Update and Read:
//   - Reads may get stale watermark if CronJob hasn't persisted yet
//   - Result: Duplicate processing (acceptable, never causes data loss)
//
// The key guarantee: Watermarks never advance beyond successfully persisted data,
// ensuring no data loss even in failure scenarios.
package cdc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

var ErrSetAlreadyPersisted = moerr.NewInternalErrorNoCtx("set already persisted")
var ErrNoWatermarkFound = moerr.NewInternalErrorNoCtx("no watermark found")

const (
	WatermarkUpdateInterval          = time.Second * 3
	ReadWatermarkProjectionList      = "account_id, task_id, db_name, table_name, watermark"
	UpdateWatermarkCronJobNamePrefix = "CDCWatermarkUpdater-CronJob"
)

const (
	watermarkCommitMaxRetries   = 3
	watermarkCircuitBreakPeriod = 30 * time.Second
	fallbackLogThrottleWindow   = time.Second
	// Guarded writes are parsed and planned as INSERT ... SELECT statements.
	// Bound both cardinality and statement size so a many-table CDC task cannot
	// turn one three-second flush into a large allocation or planner spike.
	watermarkWriteMaxRows     = 200
	watermarkWriteMaxSQLBytes = 256 << 10
	watermarkWriteSQLOverhead = 512
)

var cdcWatermarkUpdater atomic.Pointer[CDCWatermarkUpdater]

const (
	JT_CDC_GetOrAddCommittedWM tasks.JobType = 400 + iota
	JT_CDC_CommittingWM
	JT_CDC_UpdateWMErrMsg
	JT_CDC_RemoveCachedWM
)

func init() {
	tasks.RegisterJobType(JT_CDC_GetOrAddCommittedWM, "CDC_GetOrAddCommittedWM")
	tasks.RegisterJobType(JT_CDC_CommittingWM, "CDC_CommittingWM")
	tasks.RegisterJobType(JT_CDC_UpdateWMErrMsg, "CDC_UpdateWMErrMsg")
	tasks.RegisterJobType(JT_CDC_RemoveCachedWM, "CDC_RemoveCachedWM")
}

func GetCDCWatermarkUpdater(
	cnUUID string,
	executor ie.InternalExecutor,
) *CDCWatermarkUpdater {
	updater := cdcWatermarkUpdater.Load()
	for updater == nil {
		newUpdater := NewCDCWatermarkUpdater(
			fmt.Sprintf("cdc_watermark_updater_%s", cnUUID),
			executor,
		)
		newUpdater.Start()
		if cdcWatermarkUpdater.CompareAndSwap(nil, newUpdater) {
			updater = newUpdater
		} else {
			newUpdater.Stop()
			updater = cdcWatermarkUpdater.Load()
		}
	}
	return updater
}

type WatermarkKey struct {
	AccountId uint64
	TaskId    string
	DBName    string
	TableName string
}

func (k *WatermarkKey) String() string {
	return fmt.Sprintf("%d.%s.%s.%s", k.AccountId, k.TaskId, k.DBName, k.TableName)
}

type WatermarkResult struct {
	Watermark types.TS
	Ok        bool
}

type UpdaterJob struct {
	tasks.Job
	Key         *WatermarkKey
	Watermark   types.TS
	ErrMsg      string
	OwnerFence  *OwnerFence
	CleanupMode WatermarkCleanupMode
	done        chan struct{}
}

func (job *UpdaterJob) Init(ctx context.Context, id string, typ tasks.JobType, exec tasks.JobExecutor) {
	job.Job.Init(ctx, id, typ, exec)
	job.done = make(chan struct{})
}

func (job *UpdaterJob) WaitDoneContext(ctx context.Context) *tasks.JobResult {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-job.done:
		return job.GetResult()
	case <-ctx.Done():
		return &tasks.JobResult{Err: context.Cause(ctx)}
	}
}

func (job *UpdaterJob) DoneWithErr(err error) {
	job.Job.DoneWithErr(err)
	close(job.done)
}

func (job *UpdaterJob) DoneWithResult(res any) {
	job.Job.DoneWithResult(res)
	close(job.done)
}

type UpdateOption func(*CDCWatermarkUpdater)

func WithExportStatsInterval(interval time.Duration) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.exportStatsInterval = interval
	}
}

func WithCronJobErrorSupressTimes(times uint64) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.cronJobErrorSupressTimes = times
	}
}

func WithCronJobInterval(interval time.Duration) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.opts.cronJobInterval = interval
	}
}

func WithCustomizedCronJob(fn func(ctx context.Context)) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.customized.cronJob = fn
	}
}

func WithCustomizedScheduleJob(fn func(job *UpdaterJob) (err error)) UpdateOption {
	return func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = fn
	}
}

func NewGetOrAddCommittedWMJob(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_GetOrAddCommittedWM,
		nil,
	)
	job.Key = key
	job.Watermark = *watermark
	return job
}

func NewCommittingWMJob(
	ctx context.Context,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_CommittingWM,
		nil,
	)
	return job
}

func NewUpdateWMErrMsgJob(
	ctx context.Context,
	key *WatermarkKey,
	errMsg string,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_UpdateWMErrMsg,
		nil,
	)
	job.Key = key
	job.ErrMsg = errMsg
	job.OwnerFence, _ = ctx.Value(watermarkOwnerFenceContextKey{}).(*OwnerFence)
	return job
}

func NewRemoveCachedWMJob(
	ctx context.Context,
	key *WatermarkKey,
	mode WatermarkCleanupMode,
) *UpdaterJob {
	job := new(UpdaterJob)
	job.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_CDC_RemoveCachedWM,
		nil,
	)
	job.Key = key
	job.CleanupMode = mode
	return job
}

// CDCWatermarkUpdater manages watermarks for CDC tasks with eventual consistency.
//
// Consistency Model:
// - Watermarks are allowed to LAG behind actual data progress (acceptable: causes duplicate processing)
// - Watermarks MUST NEVER ADVANCE ahead of persisted data (forbidden: would cause data loss)
// - Updates are buffered in memory and persisted asynchronously via batch operations
//
// Three-Tier Cache Architecture:
// 1. cacheUncommitted: In-memory write buffer, updated immediately on UpdateWatermarkOnly()
// 2. cacheCommitting: Transition state during database persistence
// 3. cacheCommitted: Synchronized with database, represents durable watermarks
//
// Update Flow:
//
//	UpdateWatermarkOnly() -> cacheUncommitted (instant, always succeeds)
//	                      -> cacheCommitting (moved by CronJob every 3s)
//	                      -> cacheCommitted + DB (after batch UPDATE succeeds)
//
// Crash Recovery:
// - If system crashes before CronJob persists, watermarks in cacheUncommitted are lost
// - Next run will read old watermark from DB and re-process data (duplicate processing is acceptable)
// - This ensures watermarks never advance beyond persisted data
type CDCWatermarkUpdater struct {
	sync.RWMutex

	opts struct {
		exportStatsInterval      time.Duration
		cronJobInterval          time.Duration
		cronJobErrorSupressTimes uint64
	}

	// sql executor
	ie ie.InternalExecutor

	// Three-tier cache for watermark consistency
	// Read priority: cacheUncommitted -> cacheCommitting -> cacheCommitted
	cacheUncommitted map[WatermarkKey]types.TS // Write buffer, not yet persisted
	cacheCommitting  map[WatermarkKey]types.TS // Being persisted to database
	cacheCommitted   map[WatermarkKey]types.TS // Synchronized with database
	// Stable watermarks are ordered first by source table generation and then
	// by timestamp. Missing entries are legacy generation zero.
	cacheUncommittedGeneration map[WatermarkKey]uint64
	cacheCommittingGeneration  map[WatermarkKey]uint64
	cacheCommittedGeneration   map[WatermarkKey]uint64
	// Stable-epoch updates carry the exact owner claim all the way to the
	// asynchronous durable writer. This prevents a value buffered by a stale CN
	// from being persisted after another owner takes over.
	cacheUncommittedFence map[WatermarkKey]*OwnerFence
	cacheCommittingFence  map[WatermarkKey]*OwnerFence
	activeWatermarkFence  map[WatermarkKey]*OwnerFence

	// Error metadata cache (similar to watermark cache)
	// Cached in memory to avoid synchronous SQL queries in RecordError()
	errorMetadataCache map[WatermarkKey]*ErrorMetadata

	commitFailureCount map[WatermarkKey]uint32    // consecutive persistence failures per key
	commitCircuitOpen  map[WatermarkKey]time.Time // keys in circuit-breaker cool-down

	// Tracks table labels that had non-retryable error metrics set in the previous scan
	// Used for diff-based cleanup: (previous - current) labels get their metrics deleted
	previousErrorLabels map[string]bool

	// Pause registry: tracks which tasks are currently paused
	// Used to prevent watermark updates during pause operations
	// Key: taskId (string), Value: pause timestamp (time.Time)
	pausedTasks sync.Map
	// deletedTasks is a terminal tombstone for dropped task IDs.
	deletedTasks sync.Map
	persistMu    sync.Mutex

	queue        sm.Queue
	cronExecutor *tasks.CancelableJob

	customized struct {
		cronJob                func(ctx context.Context)
		scheduleJob            func(job *UpdaterJob) (err error)
		scheduleJobWithContext func(ctx context.Context, job *UpdaterJob) (err error)
	}

	getOrAddCommittedBuffer []*UpdaterJob
	addCommittedBuffer      []*UpdaterJob
	committingBuffer        []*UpdaterJob
	committingErrMsgBuffer  []*UpdaterJob
	readKeysBuffer          map[WatermarkKey]WatermarkResult

	stats struct {
		runTimes       atomic.Uint64
		skipTimes      atomic.Uint64
		errorTimes     atomic.Uint64
		lastExportTime time.Time
	}

	fallbackLog sync.Map
}

func NewCDCWatermarkUpdater(
	name string,
	ie ie.InternalExecutor,
	opts ...UpdateOption,
) *CDCWatermarkUpdater {
	u := &CDCWatermarkUpdater{
		ie:                         ie,
		cacheUncommitted:           make(map[WatermarkKey]types.TS),
		cacheCommitting:            make(map[WatermarkKey]types.TS),
		cacheCommitted:             make(map[WatermarkKey]types.TS),
		cacheUncommittedGeneration: make(map[WatermarkKey]uint64),
		cacheCommittingGeneration:  make(map[WatermarkKey]uint64),
		cacheCommittedGeneration:   make(map[WatermarkKey]uint64),
		cacheUncommittedFence:      make(map[WatermarkKey]*OwnerFence),
		cacheCommittingFence:       make(map[WatermarkKey]*OwnerFence),
		activeWatermarkFence:       make(map[WatermarkKey]*OwnerFence),
		errorMetadataCache:         make(map[WatermarkKey]*ErrorMetadata), // Initialize error cache
		commitFailureCount:         make(map[WatermarkKey]uint32),
		commitCircuitOpen:          make(map[WatermarkKey]time.Time),
		previousErrorLabels:        make(map[string]bool),

		getOrAddCommittedBuffer: make([]*UpdaterJob, 0, 100),
		addCommittedBuffer:      make([]*UpdaterJob, 0, 100),
		committingBuffer:        make([]*UpdaterJob, 0, 100),
		committingErrMsgBuffer:  make([]*UpdaterJob, 0, 100),
		readKeysBuffer:          make(map[WatermarkKey]WatermarkResult, 100),
	}
	for _, opt := range opts {
		opt(u)
	}
	u.fillDefaults()
	u.queue = sm.NewSafeQueue(5000, 200, u.onJobs)
	u.cronExecutor = tasks.NewCancelableCronJob(
		fmt.Sprintf("%s-%s", UpdateWatermarkCronJobNamePrefix, name),
		u.opts.cronJobInterval,
		u.wrapCronJob(u.customized.cronJob),
		true,
		1,
	)
	return u
}

func (u *CDCWatermarkUpdater) fillDefaults() {
	if u.opts.exportStatsInterval == 0 {
		u.opts.exportStatsInterval = time.Minute // Reduced from 10 minutes to 1 minute for better observability
	}
	if u.opts.cronJobInterval == 0 {
		u.opts.cronJobInterval = WatermarkUpdateInterval
	}
	if u.customized.cronJob == nil {
		u.customized.cronJob = u.cronRun
	}
	if u.customized.scheduleJob == nil {
		u.customized.scheduleJob = u.scheduleJob
		u.customized.scheduleJobWithContext = u.scheduleJobWithContext
	}
	if u.opts.cronJobErrorSupressTimes == 0 {
		u.opts.cronJobErrorSupressTimes = 50 // Reduced from 500 to 50 for more frequent error reporting
	}
}

func (u *CDCWatermarkUpdater) resetJobs(err error) {
	for i := range u.addCommittedBuffer {
		if err != nil && u.addCommittedBuffer[i] != nil {
			u.addCommittedBuffer[i].DoneWithErr(err)
		}
		u.addCommittedBuffer[i] = nil
	}
	u.addCommittedBuffer = u.addCommittedBuffer[:0]
	for i := range u.getOrAddCommittedBuffer {
		if err != nil && u.getOrAddCommittedBuffer[i] != nil {
			u.getOrAddCommittedBuffer[i].DoneWithErr(err)
		}
		u.getOrAddCommittedBuffer[i] = nil
	}
	u.getOrAddCommittedBuffer = u.getOrAddCommittedBuffer[:0]
	for i := range u.committingBuffer {
		if err != nil && u.committingBuffer[i] != nil {
			u.committingBuffer[i].DoneWithErr(err)
		}
		u.committingBuffer[i] = nil
	}
	u.committingBuffer = u.committingBuffer[:0]
	for i := range u.committingErrMsgBuffer {
		if err != nil && u.committingErrMsgBuffer[i] != nil {
			u.committingErrMsgBuffer[i].DoneWithErr(err)
		}
		u.committingErrMsgBuffer[i] = nil
	}
	u.committingErrMsgBuffer = u.committingErrMsgBuffer[:0]
	for key := range u.readKeysBuffer {
		delete(u.readKeysBuffer, key)
	}
}

func (u *CDCWatermarkUpdater) onJobs(jobs ...any) {
	var (
		err    error
		errMsg string
	)
	defer func() {
		u.resetJobs(err)
		if err != nil {
			logutil.Error(
				"cdc.watermark.read_error",
				zap.Error(err),
				zap.String("err-msg", errMsg),
			)
		}
	}()

	for _, j := range jobs {
		job := j.(*UpdaterJob)
		switch job.Type() {
		case JT_CDC_GetOrAddCommittedWM:
			if _, deleted := u.deletedTasks.Load(job.Key.TaskId); deleted {
				job.DoneWithErr(nil)
				continue
			}
			u.getOrAddCommittedBuffer = append(u.getOrAddCommittedBuffer, job)
			u.readKeysBuffer[*job.Key] = WatermarkResult{
				Watermark: types.TS{},
				Ok:        false,
			}
		case JT_CDC_CommittingWM:
			u.committingBuffer = append(u.committingBuffer, job)
		case JT_CDC_UpdateWMErrMsg:
			if _, deleted := u.deletedTasks.Load(job.Key.TaskId); deleted {
				job.DoneWithErr(nil)
				continue
			}
			// Stable rows are created exclusively by startup and diagnostics are
			// update-only. Do not read/recreate progress here: cleanup may already
			// have retired the local cache, and reading it back would leak stale state.
			if job.OwnerFence == nil {
				if _, err := u.GetFromCache(context.Background(), job.Key); err != nil {
					if !errors.Is(err, ErrNoWatermarkFound) {
						job.DoneWithErr(err)
						continue
					}
					if _, exists := u.readKeysBuffer[*job.Key]; !exists {
						u.readKeysBuffer[*job.Key] = WatermarkResult{}
					}
				}
			}
			u.committingErrMsgBuffer = append(u.committingErrMsgBuffer, job)
		case JT_CDC_RemoveCachedWM:
			u.Lock()
			keepDiagnostic := job.CleanupMode == WatermarkCleanupKeepDiagnostic
			var inCommitted bool
			if keepDiagnostic {
				inCommitted = u.retireWatermarkProgressLocked(*job.Key)
			} else {
				inCommitted = u.removeWatermarkStateLocked(*job.Key)
			}
			u.Unlock()

			if keepDiagnostic {
				u.removeWatermarkProgressMetrics(*job.Key)
			} else {
				u.removeWatermarkMetrics(*job.Key)
			}

			job.DoneWithErr(nil)

			fields := []zap.Field{
				zap.String("key", job.Key.String()),
			}
			if inCommitted {
				logutil.Info("cdc.watermark.remove_cached_success", fields...)
			} else {
				logutil.Info("cdc.watermark.remove_cached_skip", fields...)
			}
		default:
			logutil.Fatal("unknown job type", zap.Int("job-type", int(job.Type())))
		}
	}

	// read watermarks from the `mo_cdc_watermark` table
	// it collect all keys in the `getOrAddCommittedBuffer` and
	// read the watermarks from the `mo_cdc_watermark` table. if
	// the watermark is found, notify the job with the watermark, otherwise,
	// add the job to the `addCommittedBuffer`.
	if errMsg, err = u.execReadWM(); err != nil {
		return
	}

	// it collect all keys in the `addCommittedBuffer` and
	// add the watermarks records to the `mo_cdc_watermark` table.
	if errMsg, err = u.execAddWM(); err != nil {
		return
	}

	// batch update watermarks records in the `mo_cdc_watermark` table
	if errMsg, err = u.persistBatchUpdateWM(); err != nil {
		return
	}
	if errMsg, err = u.execBatchUpdateWMErrMsg(); err != nil {
		return
	}

	// A committing job is also the queue barrier used by terminal task
	// deletion. safeQueue may deliver later job types in the same callback
	// batch, so completing it in execBatchUpdateWM would let DELETE race ahead
	// of an already-admitted error-watermark UPSERT. Publish barrier completion
	// only after every persistence phase in this callback has finished.
	u.completeCommittingJobs(nil)
}

func (u *CDCWatermarkUpdater) execReadWM() (errMsg string, err error) {
	if len(u.readKeysBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Second, moerr.CauseWatermarkRead)
	defer cancel()

	readSql := u.constructReadWMSQL(u.readKeysBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	res := u.ie.Query(ctx, readSql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		err = res.Error()
		errMsg = fmt.Sprintf("read sql \"%s\" failed", readSql)
		return
	}

	var (
		key          WatermarkKey
		watermarkStr string
		watermark    types.TS
	)
	for i, rows := uint64(0), res.RowCount(); i < rows; i++ {
		if key.AccountId, err = res.GetUint64(ctx, i, 0); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad account_id", readSql)
			return
		}
		if key.TaskId, err = res.GetString(ctx, i, 1); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad task_id", readSql)
			return
		}
		if key.DBName, err = res.GetString(ctx, i, 2); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad db_name", readSql)
			return
		}
		if key.TableName, err = res.GetString(ctx, i, 3); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad tbl_name", readSql)
			return
		}
		if watermarkStr, err = res.GetString(ctx, i, 4); err != nil {
			errMsg = fmt.Sprintf("read sql \"%s\" bad watermark", readSql)
			return
		}
		watermark, err = parseWatermarkTS(watermarkStr)
		if err != nil {
			errMsg = fmt.Sprintf("read sql %q returned invalid watermark %q", readSql, watermarkStr)
			err = moerr.NewInternalErrorf(
				ctx, "invalid CDC watermark %q for %s: %v", watermarkStr, key.String(), err)
			return
		}

		// update the readKeysBuffer
		u.readKeysBuffer[key] = WatermarkResult{
			Watermark: watermark,
			Ok:        true,
		}
	}

	// for each job in the getOrAddCommittedBuffer, if the watermark is found,
	// notify the job with the watermark, otherwise, add the job to the addCommittedBuffer
	// and clear the getOrAddCommittedBuffer
	// the jobs in the addCommittedBuffer will be processed in the `execAddWM`
	u.Lock()
	defer u.Unlock()
	for key, result := range u.readKeysBuffer {
		if result.Ok {
			u.cacheCommitted[key] = result.Watermark
			// The legacy projection does not carry source_table_id. Stable
			// admission immediately follows with GetWatermarkProgress, which
			// atomically installs both fields.
			delete(u.cacheCommittedGeneration, key)
		}
	}
	for i, job := range u.getOrAddCommittedBuffer {
		if _, deleted := u.deletedTasks.Load(job.Key.TaskId); deleted {
			job.DoneWithErr(nil)
			u.getOrAddCommittedBuffer[i] = nil
			continue
		}
		if u.readKeysBuffer[*job.Key].Ok {
			u.cacheCommitted[*job.Key] = u.readKeysBuffer[*job.Key].Watermark
			delete(u.cacheCommittedGeneration, *job.Key)
			job.DoneWithResult(u.readKeysBuffer[*job.Key].Watermark)
		} else {
			u.addCommittedBuffer = append(u.addCommittedBuffer, job)
		}
		u.getOrAddCommittedBuffer[i] = nil
	}
	u.getOrAddCommittedBuffer = u.getOrAddCommittedBuffer[:0]
	return
}

// execBatchUpdateWM persists buffered watermarks to database in bounded batch operations.
//
// Process Flow:
// 1. Move watermarks: cacheUncommitted -> cacheCommitting
// 2. Clear cacheUncommitted (make room for new updates)
// 3. Execute batch UPDATE SQL to persist cacheCommitting to database
// 4. On success: Move cacheCommitting -> cacheCommitted
// 5. On failure: Return watermarks to cacheUncommitted for retry (with circuit breaker)
func (u *CDCWatermarkUpdater) execBatchUpdateWM() (errMsg string, err error) {
	errMsg, err = u.persistBatchUpdateWM()
	u.completeCommittingJobs(err)
	return
}

// persistBatchUpdateWM performs the watermark persistence phase without
// publishing completion for committing jobs. onJobs uses this split so its
// committing jobs remain full-callback queue barriers; focused helpers and
// tests use execBatchUpdateWM to retain the historical completion contract.
func (u *CDCWatermarkUpdater) persistBatchUpdateWM() (errMsg string, err error) {
	if len(u.committingBuffer) == 0 {
		return "", nil
	}
	u.Lock()
	// no committing jobs and no uncommitted watermarks, skip
	if len(u.committingBuffer)+len(u.cacheUncommitted) == 0 {
		u.Unlock()
		return "", nil
	}
	skippedDueToCircuit := false
	// move uncommitted watermarks to committing
	for key, watermark := range u.cacheUncommitted {
		if openedAt, ok := u.commitCircuitOpen[key]; ok {
			if time.Since(openedAt) < watermarkCircuitBreakPeriod {
				logutil.Debug(
					"cdc.watermark.commit.circuit_skip",
					zap.String("key", key.String()),
					zap.Time("opened-at", openedAt),
				)
				v2.CdcWatermarkCircuitEventCounter.WithLabelValues("skip").Inc()
				skippedDueToCircuit = true
				continue
			}
			u.resetWatermarkCircuitLocked(key)
			logutil.Info(
				"cdc.watermark.commit.circuit_reset",
				zap.String("key", key.String()),
			)
		}
		u.cacheCommitting[key] = watermark
		if generation, ok := u.cacheUncommittedGeneration[key]; ok {
			u.cacheCommittingGeneration[key] = generation
		} else {
			delete(u.cacheCommittingGeneration, key)
		}
		if fence, ok := u.cacheUncommittedFence[key]; ok {
			u.cacheCommittingFence[key] = fence
		} else {
			delete(u.cacheCommittingFence, key)
		}
		delete(u.cacheUncommitted, key)
		delete(u.cacheUncommittedGeneration, key)
		delete(u.cacheUncommittedFence, key)
	}
	// Pipelines from one daemon generation share one immutable OwnerFence.
	// Validate it once per generation rather than issuing one taskservice write
	// per table on every watermark flush.
	fencedKeys := make(map[*OwnerFence][]WatermarkKey, len(u.cacheCommittingFence))
	for key, fence := range u.cacheCommittingFence {
		fencedKeys[fence] = append(fencedKeys[fence], key)
	}
	u.Unlock()

	staleKeys := make([]WatermarkKey, 0)
	retryKeys := make(map[WatermarkKey]struct{})
	var fenceCheckErr error
	for fence, keys := range fencedKeys {
		if fenceErr := fence.Check(context.Background()); fenceErr != nil {
			if IsOwnerFenceLostError(fenceErr) {
				staleKeys = append(staleKeys, keys...)
				logutil.Warn(
					"cdc.watermark.commit.stale_owner_dropped",
					zap.String("task-id", keys[0].TaskId),
					zap.Int("table-count", len(keys)),
					zap.Error(fenceErr),
				)
			} else {
				for _, key := range keys {
					retryKeys[key] = struct{}{}
				}
				fenceCheckErr = joinErrorsPreservingSingle(fenceCheckErr, fenceErr)
				logutil.Warn(
					"cdc.watermark.commit.owner_check_retry",
					zap.String("task-id", keys[0].TaskId),
					zap.Int("table-count", len(keys)),
					zap.Error(fenceErr),
				)
			}
		}
	}
	u.Lock()
	for _, key := range staleKeys {
		delete(u.cacheCommitting, key)
		delete(u.cacheCommittingGeneration, key)
		delete(u.cacheCommittingFence, key)
		u.resetWatermarkCircuitLocked(key)
	}
	committingCount := len(u.cacheCommitting)
	legacyWatermarks := make(map[WatermarkKey]types.TS)
	stableWatermarks := make(map[WatermarkKey]types.TS)
	for key, watermark := range u.cacheCommitting {
		if _, retry := retryKeys[key]; retry {
			continue
		}
		if _, fenced := u.cacheCommittingFence[key]; fenced {
			stableWatermarks[key] = watermark
		} else {
			legacyWatermarks[key] = watermark
		}
	}
	type commitGroup struct {
		sqls []string
		keys map[WatermarkKey]types.TS
	}
	commitGroups := make([]commitGroup, 0, 2)
	if sqls := u.constructBatchUpdateWMSQLs(legacyWatermarks); len(sqls) > 0 {
		commitGroups = append(commitGroups, commitGroup{sqls: sqls, keys: legacyWatermarks})
	}
	if sqls := u.constructBatchUpdateMonotonicWMSQLs(
		stableWatermarks, u.cacheCommittingGeneration, u.cacheCommittingFence); len(sqls) > 0 {
		commitGroups = append(commitGroups, commitGroup{sqls: sqls, keys: stableWatermarks})
	}
	u.Unlock()

	failedKeys := retryKeys
	var sqlExecErr error
	failedBatch := -1
	batchCount := 0
	for _, group := range commitGroups {
		batchCount += len(group.sqls)
	}
	err = fenceCheckErr
	if committingCount == 0 {
		if skippedDueToCircuit {
			err = moerr.NewInternalErrorNoCtx("watermark commit skipped due to circuit breaker")
		}
	} else if batchCount > 0 {
		ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Second, moerr.CauseWatermarkUpdate)
		defer cancel()
		ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		startTime := time.Now()
		u.persistMu.Lock()
		batchIndex := 0
		for _, group := range commitGroups {
			groupFailed := false
			for _, sql := range group.sqls {
				batchErr := u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
				if batchErr != nil {
					if failedBatch < 0 {
						failedBatch = batchIndex
					}
					groupFailed = true
					sqlExecErr = joinErrorsPreservingSingle(sqlExecErr, batchErr)
				}
				batchIndex++
			}
			if groupFailed {
				for key := range group.keys {
					failedKeys[key] = struct{}{}
				}
			}
		}
		u.persistMu.Unlock()
		err = joinErrorsPreservingSingle(err, sqlExecErr)
		duration := time.Since(startTime)
		v2.CdcWatermarkCommitDuration.Observe(duration.Seconds())
		if sqlExecErr != nil {
			placeholderSQLs := make([]string, batchCount)
			errMsg = watermarkBatchError("commit", failedBatch, placeholderSQLs)
		}
	}

	u.Lock()
	defer u.Unlock()

	if err != nil {
		reason := "owner_fence"
		if sqlExecErr != nil {
			reason = "sql"
		} else if fenceCheckErr != nil {
			errMsg = fenceCheckErr.Error()
		} else {
			errMsg = err.Error()
			reason = "circuit_skip"
		}
		v2.CdcWatermarkCommitErrorCounter.WithLabelValues(reason).Inc()
	}

	committedCount := 0
	now := time.Now()
	for key, watermark := range u.cacheCommitting {
		if fence := u.cacheCommittingFence[key]; fence != nil {
			if active, ok := u.activeWatermarkFence[key]; ok && active != fence {
				// A replacement in this process has already published a newer
				// replay generation. The durable SQL was also fenced by that row;
				// never let the obsolete completion poison the shared local cache.
				u.resetWatermarkCircuitLocked(key)
				continue
			}
		}
		if _, failed := failedKeys[key]; failed {
			generation := u.cacheCommittingGeneration[key]
			if existing, ok := u.cacheUncommitted[key]; ok && shouldRetainBufferedWatermark(
				u.cacheUncommittedGeneration[key],
				existing,
				u.cacheUncommittedFence[key],
				generation,
				watermark,
				u.cacheCommittingFence[key],
			) {
				// keep newer watermark
			} else {
				u.cacheUncommitted[key] = watermark
				if generation > 0 {
					u.cacheUncommittedGeneration[key] = generation
				} else {
					delete(u.cacheUncommittedGeneration, key)
				}
				if fence, ok := u.cacheCommittingFence[key]; ok {
					u.cacheUncommittedFence[key] = fence
				} else {
					delete(u.cacheUncommittedFence, key)
				}
			}
			retry := u.commitFailureCount[key] + 1
			u.commitFailureCount[key] = retry
			if retry >= watermarkCommitMaxRetries {
				if _, opened := u.commitCircuitOpen[key]; !opened {
					u.commitCircuitOpen[key] = now
					v2.CdcWatermarkCircuitEventCounter.WithLabelValues("opened").Inc()
					v2.CdcWatermarkCircuitOpenGauge.Inc()
					logutil.Error(
						"cdc.watermark.commit.circuit_open",
						zap.String("key", key.String()),
						zap.Uint32("retry-count", retry),
					)
				}
			}
		} else {
			u.cacheCommitted[key] = watermark
			if generation := u.cacheCommittingGeneration[key]; generation > 0 {
				u.cacheCommittedGeneration[key] = generation
			} else {
				delete(u.cacheCommittedGeneration, key)
			}
			u.resetWatermarkCircuitLocked(key)
			committedCount++
		}
	}
	if committedCount > 0 {
		v2.CdcWatermarkCommitBatchCounter.Inc()
	}

	// clear the committing cache
	for key := range u.cacheCommitting {
		delete(u.cacheCommitting, key)
		delete(u.cacheCommittingGeneration, key)
		delete(u.cacheCommittingFence, key)
	}
	return
}

func (u *CDCWatermarkUpdater) completeCommittingJobs(err error) {
	for i, job := range u.committingBuffer {
		job.DoneWithErr(err)
		u.committingBuffer[i] = nil
	}
	u.committingBuffer = u.committingBuffer[:0]
}

func (u *CDCWatermarkUpdater) execBatchUpdateWMErrMsg() (errMsg string, err error) {
	if len(u.committingErrMsgBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Second, moerr.CauseWatermarkUpdateErrMsg)
	defer cancel()
	errMsgSQLs := u.constructBatchUpdateWMErrMsgSQLs(u.committingErrMsgBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	failedBatch, err := u.execWatermarkSQLBatches(ctx, errMsgSQLs)
	if err != nil {
		errMsg = watermarkBatchError("update err_msg", failedBatch, errMsgSQLs)
	}
	u.Lock()
	defer u.Unlock()
	for i, job := range u.committingErrMsgBuffer {
		job.DoneWithErr(err)
		u.committingErrMsgBuffer[i] = nil
	}
	u.committingErrMsgBuffer = u.committingErrMsgBuffer[:0]
	return
}

type guardedWatermarkRow struct {
	accountID       uint64
	taskID          string
	dbName          string
	tableName       string
	value           string
	generation      uint64
	ownerGeneration uint64
}

type watermarkTaskKey struct {
	accountID uint64
	taskID    string
}

func sortGuardedWatermarkRows(rows []guardedWatermarkRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].accountID != rows[j].accountID {
			return rows[i].accountID < rows[j].accountID
		}
		if rows[i].taskID != rows[j].taskID {
			return rows[i].taskID < rows[j].taskID
		}
		if rows[i].dbName != rows[j].dbName {
			return rows[i].dbName < rows[j].dbName
		}
		return rows[i].tableName < rows[j].tableName
	})
}

func writeQuotedSQLString(builder *strings.Builder, value string) {
	builder.WriteByte('\'')
	builder.WriteString(escapeSQLString(value))
	builder.WriteByte('\'')
}

func writeWatermarkRowPrefix(builder *strings.Builder, row guardedWatermarkRow, withAliases bool) {
	builder.WriteString("SELECT ")
	builder.WriteString(strconv.FormatUint(row.accountID, 10))
	if withAliases {
		builder.WriteString(" AS account_id, ")
		writeQuotedSQLString(builder, row.taskID)
		builder.WriteString(" AS task_id, ")
		writeQuotedSQLString(builder, row.dbName)
		builder.WriteString(" AS db_name, ")
		writeQuotedSQLString(builder, row.tableName)
		builder.WriteString(" AS table_name, ")
		return
	}
	builder.WriteString(", ")
	writeQuotedSQLString(builder, row.taskID)
	builder.WriteString(", ")
	writeQuotedSQLString(builder, row.dbName)
	builder.WriteString(", ")
	writeQuotedSQLString(builder, row.tableName)
	builder.WriteString(", ")
}

func watermarkUpdateRowSQL(row guardedWatermarkRow, withAliases bool) string {
	var builder strings.Builder
	overhead := 32
	if withAliases {
		overhead = 128
	}
	builder.Grow(overhead + len(row.taskID) + len(row.dbName) + len(row.tableName) + len(row.value))
	writeWatermarkRowPrefix(&builder, row, withAliases)
	writeQuotedSQLString(&builder, row.value)
	if withAliases {
		builder.WriteString(" AS watermark")
	}
	return builder.String()
}

func watermarkMonotonicUpdateRowSQL(row guardedWatermarkRow, withAliases bool) string {
	var builder strings.Builder
	builder.Grow(160 + len(row.taskID) + len(row.dbName) + len(row.tableName) + len(row.value))
	writeWatermarkRowPrefix(&builder, row, withAliases)
	writeQuotedSQLString(&builder, row.value)
	if withAliases {
		builder.WriteString(" AS watermark, ")
	} else {
		builder.WriteString(", ")
	}
	builder.WriteString(strconv.FormatUint(row.generation, 10))
	if withAliases {
		builder.WriteString(" AS source_table_id, ")
	} else {
		builder.WriteString(", ")
	}
	builder.WriteString(strconv.FormatUint(row.ownerGeneration, 10))
	if withAliases {
		builder.WriteString(" AS owner_generation")
	}
	return builder.String()
}

func watermarkErrorRowSQL(row guardedWatermarkRow, withAliases bool) string {
	var builder strings.Builder
	overhead := 32
	if withAliases {
		overhead = 128
	}
	builder.Grow(overhead + len(row.taskID) + len(row.dbName) + len(row.tableName) + len(row.value))
	writeWatermarkRowPrefix(&builder, row, withAliases)
	writeQuotedSQLString(&builder, row.value)
	if withAliases {
		builder.WriteString(" AS err_msg")
	}
	return builder.String()
}

func watermarkOwnedErrorRowSQL(row guardedWatermarkRow, withAliases bool) string {
	var builder strings.Builder
	builder.Grow(160 + len(row.taskID) + len(row.dbName) + len(row.tableName) + len(row.value))
	writeWatermarkRowPrefix(&builder, row, withAliases)
	writeQuotedSQLString(&builder, row.value)
	if withAliases {
		builder.WriteString(" AS err_msg, ")
	} else {
		builder.WriteString(", ")
	}
	builder.WriteString(strconv.FormatUint(row.ownerGeneration, 10))
	if withAliases {
		builder.WriteString(" AS owner_generation")
	}
	return builder.String()
}

func watermarkInsertRowSQL(row guardedWatermarkRow, withAliases bool) string {
	var builder strings.Builder
	overhead := 40
	if withAliases {
		overhead = 144
	}
	builder.Grow(overhead + len(row.taskID) + len(row.dbName) + len(row.tableName) + len(row.value))
	writeWatermarkRowPrefix(&builder, row, withAliases)
	writeQuotedSQLString(&builder, row.value)
	if withAliases {
		builder.WriteString(" AS watermark, '' AS err_msg")
	} else {
		builder.WriteString(", ''")
	}
	return builder.String()
}

func watermarkTaskPredicate(row guardedWatermarkRow) string {
	var builder strings.Builder
	builder.Grow(48 + len(row.taskID))
	builder.WriteString("(account_id = ")
	builder.WriteString(strconv.FormatUint(row.accountID, 10))
	builder.WriteString(" AND task_id = ")
	writeQuotedSQLString(&builder, row.taskID)
	builder.WriteByte(')')
	return builder.String()
}

func buildGuardedWatermarkSQLBatches(
	rows []guardedWatermarkRow,
	rowSQL func(guardedWatermarkRow, bool) string,
	wrap func(string, string) string,
) []string {
	if len(rows) == 0 {
		return nil
	}
	sortGuardedWatermarkRows(rows)
	batches := make([]string, 0, (len(rows)+watermarkWriteMaxRows-1)/watermarkWriteMaxRows)
	seenTasks := make(map[watermarkTaskKey]struct{})
	var values, predicates strings.Builder
	rowCount := 0

	flush := func() {
		if rowCount == 0 {
			return
		}
		batches = append(batches, wrap(values.String(), predicates.String()))
		values.Reset()
		predicates.Reset()
		clear(seenTasks)
		rowCount = 0
	}

	for _, row := range rows {
		value := rowSQL(row, rowCount == 0)
		task := watermarkTaskKey{accountID: row.accountID, taskID: row.taskID}
		predicate := ""
		if _, ok := seenTasks[task]; !ok {
			predicate = watermarkTaskPredicate(row)
		}
		projectedBytes := watermarkWriteSQLOverhead + values.Len() + predicates.Len() + len(value)
		if rowCount > 0 {
			projectedBytes += len(" UNION ALL ")
		}
		if predicate != "" {
			projectedBytes += len(predicate)
			if predicates.Len() > 0 {
				projectedBytes += len(" OR ")
			}
		}
		if rowCount > 0 && (rowCount >= watermarkWriteMaxRows || projectedBytes > watermarkWriteMaxSQLBytes) {
			flush()
			value = rowSQL(row, true)
			predicate = watermarkTaskPredicate(row)
		}
		if rowCount > 0 {
			values.WriteString(" UNION ALL ")
		}
		values.WriteString(value)
		if predicate != "" {
			if predicates.Len() > 0 {
				predicates.WriteString(" OR ")
			}
			predicates.WriteString(predicate)
			seenTasks[task] = struct{}{}
		}
		rowCount++
	}
	flush()
	return batches
}

func (u *CDCWatermarkUpdater) execWatermarkSQLBatches(
	ctx context.Context,
	sqls []string,
) (failedBatch int, err error) {
	u.persistMu.Lock()
	defer u.persistMu.Unlock()
	for i, sql := range sqls {
		if err = u.ie.Exec(ctx, sql, ie.SessionOverrideOptions{}); err != nil {
			return i, err
		}
	}
	return -1, nil
}

func watermarkBatchError(operation string, failedBatch int, sqls []string) string {
	if failedBatch < 0 || failedBatch >= len(sqls) {
		return fmt.Sprintf("%s watermark batch failed", operation)
	}
	return fmt.Sprintf(
		"%s watermark batch %d/%d (%d bytes) failed",
		operation,
		failedBatch+1,
		len(sqls),
		len(sqls[failedBatch]),
	)
}

func truncateUTF8Runes(value string, maxRunes int) string {
	if maxRunes < 0 || len(value) <= maxRunes || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes])
}

func (u *CDCWatermarkUpdater) constructBatchUpdateWMSQLs(
	keys map[WatermarkKey]types.TS,
) []string {
	rows := make([]guardedWatermarkRow, 0, len(keys))
	for key, wm := range keys {
		rows = append(rows, guardedWatermarkRow{
			accountID: key.AccountId,
			taskID:    key.TaskId,
			dbName:    key.DBName,
			tableName: key.TableName,
			value:     wm.ToString(),
		})
	}
	return buildGuardedWatermarkSQLBatches(rows, watermarkUpdateRowSQL,
		CDCSQLBuilder.GuardedWatermarkUpdateSQL)
}

func (u *CDCWatermarkUpdater) constructBatchUpdateMonotonicWMSQLs(
	keys map[WatermarkKey]types.TS,
	generations map[WatermarkKey]uint64,
	fences map[WatermarkKey]*OwnerFence,
) []string {
	rows := make([]guardedWatermarkRow, 0, len(keys))
	for key, wm := range keys {
		rows = append(rows, guardedWatermarkRow{
			accountID:       key.AccountId,
			taskID:          key.TaskId,
			dbName:          key.DBName,
			tableName:       key.TableName,
			value:           wm.ToString(),
			generation:      generations[key],
			ownerGeneration: fences[key].GenerationToken(),
		})
	}
	return buildGuardedWatermarkSQLBatches(rows, watermarkMonotonicUpdateRowSQL,
		CDCSQLBuilder.GuardedMonotonicWatermarkUpdateSQL)
}

func (u *CDCWatermarkUpdater) constructBatchUpdateWMErrMsgSQLs(
	jobs []*UpdaterJob,
) []string {
	legacyRows := make([]guardedWatermarkRow, 0, len(jobs))
	ownedRows := make([]guardedWatermarkRow, 0, len(jobs))
	for _, job := range jobs {
		row := guardedWatermarkRow{
			accountID: job.Key.AccountId,
			taskID:    job.Key.TaskId,
			dbName:    job.Key.DBName,
			tableName: job.Key.TableName,
			value:     job.ErrMsg,
		}
		if job.OwnerFence != nil {
			row.ownerGeneration = job.OwnerFence.GenerationToken()
			ownedRows = append(ownedRows, row)
		} else {
			legacyRows = append(legacyRows, row)
		}
	}
	result := buildGuardedWatermarkSQLBatches(legacyRows, watermarkErrorRowSQL,
		CDCSQLBuilder.GuardedWatermarkErrorUpdateSQL)
	return append(result, buildGuardedWatermarkSQLBatches(
		ownedRows, watermarkOwnedErrorRowSQL,
		CDCSQLBuilder.GuardedOwnedWatermarkErrorUpdateSQL)...)
}

func (u *CDCWatermarkUpdater) execAddWM() (errMsg string, err error) {
	if len(u.addCommittedBuffer) == 0 {
		return "", nil
	}
	active := u.addCommittedBuffer[:0]
	for _, job := range u.addCommittedBuffer {
		if _, deleted := u.deletedTasks.Load(job.Key.TaskId); deleted {
			job.DoneWithErr(nil)
			continue
		}
		active = append(active, job)
	}
	u.addCommittedBuffer = active
	if len(u.addCommittedBuffer) == 0 {
		return "", nil
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Second, moerr.CauseWatermarkAdd)
	defer cancel()
	addSQLs := u.constructAddWMSQLs(u.addCommittedBuffer)
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	failedBatch, err := u.execWatermarkSQLBatches(ctx, addSQLs)
	if err != nil {
		errMsg = watermarkBatchError("add", failedBatch, addSQLs)
		return
	}
	u.Lock()
	defer u.Unlock()
	for i, job := range u.addCommittedBuffer {
		if _, deleted := u.deletedTasks.Load(job.Key.TaskId); deleted {
			job.DoneWithErr(nil)
			u.addCommittedBuffer[i] = nil
			continue
		}
		// add the watermark to the cacheCommitted
		u.cacheCommitted[*job.Key] = job.Watermark
		delete(u.cacheCommittedGeneration, *job.Key)
		// notify the job with the watermark
		job.DoneWithResult(job.Watermark)
		// clear the addCommittedBuffer
		u.addCommittedBuffer[i] = nil
	}
	// clear the addCommittedBuffer
	u.addCommittedBuffer = u.addCommittedBuffer[:0]
	return
}

func (u *CDCWatermarkUpdater) constructAddWMSQLs(
	jobs []*UpdaterJob,
) []string {
	rows := make([]guardedWatermarkRow, 0, len(jobs))
	for _, job := range jobs {
		rows = append(rows, guardedWatermarkRow{
			accountID: job.Key.AccountId,
			taskID:    job.Key.TaskId,
			dbName:    job.Key.DBName,
			tableName: job.Key.TableName,
			value:     job.Watermark.ToString(),
		})
	}
	return buildGuardedWatermarkSQLBatches(rows, watermarkInsertRowSQL,
		CDCSQLBuilder.GuardedWatermarkInsertSQL)
}

func (u *CDCWatermarkUpdater) constructReadWMSQL(
	keys map[WatermarkKey]WatermarkResult,
) (readSql string) {
	var (
		idx       int
		filterStr string
	)
	// "(xxx AND yyy) OR (xxx AND yyy)"
	for key := range keys {
		if idx > 0 {
			filterStr += " OR "
		}
		filterStr += fmt.Sprintf(
			"(account_id = %d AND task_id = '%s' AND db_name = '%s' AND table_name = '%s')",
			key.AccountId,
			escapeSQLString(key.TaskId),
			escapeSQLString(key.DBName),
			escapeSQLString(key.TableName),
		)
		idx++
	}
	readSql = CDCSQLBuilder.GetWatermarkWhereSQL(ReadWatermarkProjectionList, filterStr)
	return
}

func (u *CDCWatermarkUpdater) Start() {
	u.queue.Start()
	u.cronExecutor.Start()
}

func (u *CDCWatermarkUpdater) Stop() {
	u.cronExecutor.Stop()
	u.queue.Stop()
}

func (u *CDCWatermarkUpdater) getFromCache(
	key *WatermarkKey,
) (watermark types.TS, ok bool) {
	watermark, _, ok = u.getFromCacheWithGeneration(key)
	return
}

func (u *CDCWatermarkUpdater) getFromCacheWithGeneration(
	key *WatermarkKey,
) (watermark types.TS, generation uint64, ok bool) {
	u.RLock()
	defer u.RUnlock()
	if watermark, ok = u.cacheUncommitted[*key]; ok {
		generation = u.cacheUncommittedGeneration[*key]
		return
	}
	if watermark, ok = u.cacheCommitting[*key]; ok {
		generation = u.cacheCommittingGeneration[*key]
		return
	}
	watermark, ok = u.cacheCommitted[*key]
	generation = u.cacheCommittedGeneration[*key]
	return
}

// GetFromCache retrieves the latest watermark from the three-tier cache.
//
// Lookup Priority (from newest to oldest):
// 1. cacheUncommitted - most recent updates, not yet persisted
// 2. cacheCommitting  - updates being persisted to database
// 3. cacheCommitted   - synchronized with database
//
// Returns ErrNoWatermarkFound if the key doesn't exist in any cache tier.
// This can happen when:
// - A new CDC task is starting for the first time
// - CronJob failed and caches were cleared (watermarks lost, acceptable by design)
func (u *CDCWatermarkUpdater) GetFromCache(
	ctx context.Context,
	key *WatermarkKey,
) (watermark types.TS, err error) {
	var ok bool
	if watermark, ok = u.getFromCache(key); ok {
		return
	}
	err = ErrNoWatermarkFound
	return
}

// GetFromCacheWithGeneration returns the effective cached progress tuple.
// Generation zero denotes a legacy watermark.
func (u *CDCWatermarkUpdater) GetFromCacheWithGeneration(
	ctx context.Context,
	key *WatermarkKey,
) (watermark types.TS, generation uint64, err error) {
	var ok bool
	if watermark, generation, ok = u.getFromCacheWithGeneration(key); ok {
		return
	}
	err = ErrNoWatermarkFound
	return
}

// GetWatermarkProgress atomically loads the durable watermark and its source
// table generation. Reading the fields in separate statements could combine an
// old timestamp with a new generation if the async writer committed between
// them, which could incorrectly classify an incomplete snapshot as complete.
// Stable snapshot admission calls this immediately after GetOrAddCommitted so
// the row is guaranteed to exist. Legacy rows use generation zero.
func (u *CDCWatermarkUpdater) GetWatermarkProgress(
	ctx context.Context,
	key *WatermarkKey,
) (types.TS, uint64, error) {
	readCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkRead)
	defer cancel()
	readCtx = defines.AttachAccountId(readCtx, catalog.System_Account)
	res := u.ie.Query(
		readCtx,
		CDCSQLBuilder.GetWatermarkProgressSQL(key),
		ie.SessionOverrideOptions{},
	)
	if err := res.Error(); err != nil {
		return types.TS{}, 0, classifySnapshotEpochBackendError(err)
	}
	if res.RowCount() != 1 {
		return types.TS{}, 0, &RetryableSnapshotEpochError{err: moerr.NewInternalErrorf(
			ctx, "CDC watermark generation is missing for %s", key.String())}
	}
	watermarkString, err := res.GetString(readCtx, 0, 0)
	if err != nil {
		return types.TS{}, 0, err
	}
	watermark, err := parseWatermarkTS(watermarkString)
	if err != nil {
		return types.TS{}, 0, moerr.NewInternalErrorf(
			ctx, "invalid CDC watermark %q for %s: %v", watermarkString, key.String(), err)
	}
	generation, err := res.GetUint64(readCtx, 0, 1)
	if err != nil {
		return types.TS{}, 0, err
	}
	u.Lock()
	u.cacheCommitted[*key] = watermark
	u.cacheCommittedGeneration[*key] = generation
	u.Unlock()
	return watermark, generation, nil
}

// ClaimWatermarkOwner durably publishes the daemon generation that is allowed
// to advance one table's stable watermark, then returns progress ordered after
// that claim. Both takeover and checkpoint update the same row, so either the
// old checkpoint commits first and is observed here, or it loses the owner
// equality check in the guarded upsert.
func (u *CDCWatermarkUpdater) ClaimWatermarkOwner(
	ctx context.Context,
	key *WatermarkKey,
	fence *OwnerFence,
) (types.TS, uint64, error) {
	ownerGeneration := fence.GenerationToken()
	if ownerGeneration == 0 {
		return types.TS{}, 0, moerr.NewInternalErrorf(
			ctx, "invalid CDC watermark owner generation for %s", key.String())
	}
	if err := fence.Check(ctx); err != nil {
		return types.TS{}, 0, err
	}

	claimCtx, cancel := context.WithTimeoutCause(
		ctx, snapshotEpochPersistenceTimeout, moerr.CauseWatermarkUpdate)
	defer cancel()
	claimCtx = defines.AttachAccountId(claimCtx, catalog.System_Account)
	if err := u.ie.Exec(
		claimCtx,
		CDCSQLBuilder.ClaimWatermarkOwnerSQL(key, ownerGeneration),
		ie.SessionOverrideOptions{},
	); err != nil {
		return types.TS{}, 0, classifySnapshotEpochBackendError(err)
	}

	res := u.ie.Query(
		claimCtx,
		CDCSQLBuilder.GetWatermarkOwnerProgressSQL(key),
		ie.SessionOverrideOptions{},
	)
	if err := res.Error(); err != nil {
		return types.TS{}, 0, classifySnapshotEpochBackendError(err)
	}
	if res.RowCount() != 1 {
		return types.TS{}, 0, NewRetryableSnapshotEpochError(moerr.NewInternalErrorf(
			ctx, "CDC watermark owner row is missing for %s", key.String()))
	}
	currentOwner, err := res.GetUint64(claimCtx, 0, 0)
	if err != nil {
		return types.TS{}, 0, err
	}
	if currentOwner > ownerGeneration {
		return types.TS{}, 0, &OwnerFenceLostError{err: moerr.NewInvalidTask(
			ctx, "CDC watermark owner was superseded", ownerGeneration)}
	}
	if currentOwner < ownerGeneration {
		return types.TS{}, 0, NewRetryableSnapshotEpochError(moerr.NewInternalErrorf(
			ctx, "CDC watermark owner claim %d was not durable for %s (found %d)",
			ownerGeneration, key.String(), currentOwner))
	}
	watermarkText, err := res.GetString(claimCtx, 0, 1)
	if err != nil {
		return types.TS{}, 0, err
	}
	watermark, err := parseWatermarkTS(watermarkText)
	if err != nil {
		return types.TS{}, 0, moerr.NewInternalErrorf(
			ctx, "invalid CDC watermark %q for %s: %v", watermarkText, key.String(), err)
	}
	generation, err := res.GetUint64(claimCtx, 0, 2)
	if err != nil {
		return types.TS{}, 0, err
	}
	if err = fence.Check(ctx); err != nil {
		return types.TS{}, 0, err
	}

	u.Lock()
	if !u.activateWatermarkFenceLocked(*key, fence) {
		u.Unlock()
		return types.TS{}, 0, &OwnerFenceLostError{err: moerr.NewInvalidTask(
			ctx, "CDC watermark owner was superseded locally", ownerGeneration)}
	}
	u.cacheCommitted[*key] = watermark
	u.cacheCommittedGeneration[*key] = generation
	u.Unlock()
	return watermark, generation, nil
}

func parseWatermarkTS(value string) (types.TS, error) {
	physicalString, logicalString, ok := strings.Cut(value, "-")
	if !ok || physicalString == "" || logicalString == "" || strings.Contains(logicalString, "-") {
		return types.TS{}, moerr.NewInternalErrorNoCtx("expected physical-logical")
	}
	physical, err := strconv.ParseInt(physicalString, 10, 64)
	if err != nil {
		return types.TS{}, moerr.NewInternalErrorNoCtxf("invalid physical component: %v", err)
	}
	if physical < 0 {
		return types.TS{}, moerr.NewInternalErrorNoCtx("physical component must be non-negative")
	}
	logical, err := strconv.ParseUint(logicalString, 10, 32)
	if err != nil {
		return types.TS{}, moerr.NewInternalErrorNoCtxf("invalid logical component: %v", err)
	}
	return types.BuildTS(physical, uint32(logical)), nil
}

// UpdateWatermarkErrMsg updates error message with automatic intelligent handling:
// - Control signal filtering (pause/cancel)
// - Retry count tracking and auto-increment
// - Auto-conversion to non-retryable after MaxRetryCount
// - Timestamp recording
// - Error expiration support
//
// Parameters:
//   - ctx: Context
//   - key: Watermark key
//   - errMsg: Error message (empty string to clear error)
//   - errorCtx: Error context (can be nil for backward compatibility)
//
// Call examples:
//   - Retryable: UpdateWatermarkErrMsg(ctx, key, "table not found", &ErrorContext{IsRetryable: true})
//   - Non-retryable: UpdateWatermarkErrMsg(ctx, key, "type mismatch", &ErrorContext{IsRetryable: false})
//   - Clear: UpdateWatermarkErrMsg(ctx, key, "", nil)
//   - Legacy: UpdateWatermarkErrMsg(ctx, key, "retryable error:xxx", nil) // Auto-parsed
//
// Design: Uses in-memory cache to avoid synchronous SQL queries, preserving
// the lazy batch processing design of WatermarkUpdater
func (u *CDCWatermarkUpdater) UpdateWatermarkErrMsg(
	ctx context.Context,
	key *WatermarkKey,
	errMsg string,
	errorCtx *ErrorContext,
) (err error) {
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		return nil
	}
	incomingFence, fenced := ctx.Value(watermarkOwnerFenceContextKey{}).(*OwnerFence)
	if fenced && incomingFence != nil && incomingFence.GenerationToken() == 0 {
		return moerr.NewInternalErrorNoCtx(
			"owner-fenced CDC watermark error update requires a durable owner generation")
	}
	if fenced && incomingFence != nil {
		// The durable owner column closes takeover after this check, while this
		// exact-claim validation also observes a Pause/Restart request before its
		// replacement has reached watermark admission. Neither fence is sufficient
		// alone across that two-system handoff.
		if fenceErr := incomingFence.Check(ctx); fenceErr != nil {
			if IsOwnerFenceLostError(fenceErr) {
				return nil
			}
			// Caller cancellation is lifecycle cleanup. A backend timeout while
			// the caller remains live is instead retryable and must stay visible.
			if ctx.Err() != nil && errors.Is(fenceErr, ctx.Err()) {
				return nil
			}
			return fenceErr
		}
	}
	// 1. Clear error: remove cache and persist empty string
	if errMsg == "" {
		u.Lock()
		if fenced && incomingFence != nil {
			if active := u.activeWatermarkFence[*key]; active != nil && active != incomingFence {
				u.Unlock()
				return nil
			}
		}
		delete(u.errorMetadataCache, *key)
		clearWatermarkErrorGauge(*key)
		u.Unlock()

		job := NewUpdateWMErrMsgJob(ctx, key, "")
		if _, err = u.queue.Enqueue(job); err != nil {
			if errors.Is(err, sm.ErrClose) {
				if u.shouldLogFallback(key) {
					logutil.Info(
						"cdc.watermark.update_errmsg_fallback",
						zap.String("key", key.String()),
						zap.Bool("clear", true),
						zap.Bool("retryable", false),
					)
				}
				return nil
			}
			return
		}
		job.WaitDone()
		err = job.GetResult().Err
		return
	}

	// 2. Parse error context (with backward compatibility)
	isRetryable := false
	isPauseOrCancel := false
	message := errMsg

	if errorCtx != nil {
		// New API: use structured context
		isRetryable = errorCtx.IsRetryable
		isPauseOrCancel = errorCtx.IsPauseOrCancel || IsPauseOrCancelError(errMsg)
	} else {
		// Old API: parse from string prefix (backward compatible)
		if strings.HasPrefix(errMsg, RetryableErrorPrefix) {
			isRetryable = true
			message = strings.TrimPrefix(errMsg, RetryableErrorPrefix)
		} else if strings.HasPrefix(errMsg, "retryable:") {
			isRetryable = true
			message = strings.TrimPrefix(errMsg, "retryable:")
		}

		// Auto-detect control signals
		isPauseOrCancel = IsPauseOrCancelError(errMsg)
	}

	// 3. Filter control signals (pause/cancel) - don't persist
	if isPauseOrCancel {
		logutil.Info(
			"cdc.watermark.update_errmsg_skip_control_signal",
			zap.String("key", key.String()),
			zap.String("signal", errMsg),
		)
		return nil
	}

	// 4. Read from memory cache (NO SQL query - preserves batch processing design)
	u.RLock()
	if fenced && incomingFence != nil {
		if active := u.activeWatermarkFence[*key]; active != nil && active != incomingFence {
			u.RUnlock()
			return nil
		}
	}
	oldMetadata, exists := u.errorMetadataCache[*key]
	u.RUnlock()

	// Make a copy to avoid race conditions
	var oldMetadataCopy *ErrorMetadata
	if exists {
		copy := *oldMetadata
		oldMetadataCopy = &copy
	}

	// 5. Build new metadata (auto-increment retry count)
	record := &ErrorRecord{
		Error:       moerr.NewInternalErrorNoCtx(message),
		IsRetryable: isRetryable,
		Timestamp:   time.Now(),
	}
	newMetadata := BuildErrorMetadata(oldMetadataCopy, record)
	// Keep the in-memory diagnostic bounded by the same catalog contract as
	// mo_cdc_watermark.err_msg. This prevents an upstream error containing a
	// large payload from defeating the guarded statement-size bound.
	newMetadata.Message = truncateUTF8Runes(newMetadata.Message, CDCWatermarkErrMsgMaxLen)

	// 6. Check if exceeded max retry count
	if newMetadata.IsRetryable && newMetadata.RetryCount > MaxRetryCount {
		logutil.Warn(
			"cdc.watermark.update_errmsg_exceeded_retry",
			zap.String("key", key.String()),
			zap.Int("retry-count", newMetadata.RetryCount),
			zap.String("error", newMetadata.Message),
		)
		// Convert to non-retryable
		newMetadata.IsRetryable = false
		newMetadata.Message = fmt.Sprintf("max retry exceeded (%d): %s",
			newMetadata.RetryCount, newMetadata.Message)
	}
	newMetadata.Message = truncateUTF8Runes(newMetadata.Message, CDCWatermarkErrMsgMaxLen)

	// 7. Update memory cache (like UpdateWatermarkOnly - no SQL)
	u.Lock()
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		u.Unlock()
		return nil
	}
	if fenced && incomingFence != nil {
		if active := u.activeWatermarkFence[*key]; active != nil && active != incomingFence {
			u.Unlock()
			return nil
		}
	}
	u.errorMetadataCache[*key] = newMetadata
	// Keep diagnostics and their observable state in the same owner-ordered
	// critical section. Otherwise a retired owner can publish a gauge after its
	// replacement has already cleared the old generation's cache.
	clearWatermarkErrorGauge(*key)
	if !newMetadata.IsRetryable {
		v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
			key.String(), extractErrorType(newMetadata.Message)).Set(1)
	}
	u.Unlock()

	// 8. Format and persist (async via job queue)
	formattedMsg := truncateUTF8Runes(FormatErrorMetadata(newMetadata), CDCWatermarkErrMsgMaxLen)
	logutil.Info(
		"cdc.watermark.update_errmsg_persist",
		zap.String("key", key.String()),
		zap.Bool("retryable", newMetadata.IsRetryable),
		zap.Int("retry-count", newMetadata.RetryCount),
		zap.String("formatted-msg", formattedMsg),
	)

	job := NewUpdateWMErrMsgJob(ctx, key, formattedMsg)
	if _, err = u.queue.Enqueue(job); err != nil {
		if errors.Is(err, sm.ErrClose) {
			if u.shouldLogFallback(key) {
				logutil.Info(
					"cdc.watermark.update_errmsg_fallback",
					zap.String("key", key.String()),
					zap.Bool("clear", false),
					zap.Bool("retryable", newMetadata.IsRetryable),
					zap.Int("retry-count", newMetadata.RetryCount),
				)
			}
			return nil
		}
		return
	}
	job.WaitDone()
	err = job.GetResult().Err
	if err == nil {
		u.Lock()
		u.resetWatermarkCircuitLocked(*key)
		u.Unlock()
	}
	return
}

// UpdateWatermarkOnly buffers a watermark update in memory without immediate persistence.
//
// Consistency Guarantee:
// - This method is called ONLY AFTER data has been successfully committed to the database
// - It buffers the watermark in cacheUncommitted for later batch persistence
// - Always returns nil (never fails) to maintain the consistency model
//
// Persistence Timing:
// - Watermark is persisted asynchronously by CronJob (default: every 3 seconds)
// - If system crashes before CronJob runs, the watermark update is lost
// - This is acceptable: next run will re-read from old watermark (duplicate processing is idempotent)
//
// Why normal buffering returns nil:
//   - By design, watermark lag is acceptable but advance is forbidden
//   - Caller ensures data is committed BEFORE calling this method
//   - Even if this buffer operation "fails" (system crash), watermark stays behind (safe)
//   - An invalid owner-fenced call with no source generation is rejected because
//     persisting that tuple would violate the stable protocol
type watermarkOwnerFenceContextKey struct{}
type watermarkSourceTableIDContextKey struct{}

// WithWatermarkOwnerFence binds the exact daemon claim to an asynchronous
// watermark progress or diagnostic update. Stable diagnostic callers must use
// this context so both set and clear remain generation-owned.
func WithWatermarkOwnerFence(
	ctx context.Context,
	fence *OwnerFence,
	sourceTableID uint64,
) context.Context {
	if fence == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, watermarkOwnerFenceContextKey{}, fence)
	if sourceTableID > 0 {
		ctx = context.WithValue(ctx, watermarkSourceTableIDContextKey{}, sourceTableID)
	}
	return ctx
}

func (u *CDCWatermarkUpdater) UpdateWatermarkOnly(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) (err error) {
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		return nil
	}
	// FIX: Check if this task is paused
	// If paused, reject watermark updates to prevent data loss on resume
	if pauseTime, paused := u.pausedTasks.Load(key.TaskId); paused {
		logutil.Debug(
			"cdc.watermark.update_blocked_by_pause",
			zap.String("task-id", key.TaskId),
			zap.String("key", key.String()),
			zap.String("watermark", watermark.ToString()),
			zap.Time("pause-time", pauseTime.(time.Time)),
		)
		// Return nil to maintain eventual consistency contract
		// But don't actually update the watermark
		return nil
	}

	u.Lock()
	defer u.Unlock()
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		return nil
	}
	incomingFence, fenced := ctx.Value(watermarkOwnerFenceContextKey{}).(*OwnerFence)
	incomingGeneration, _ := ctx.Value(watermarkSourceTableIDContextKey{}).(uint64)
	if fenced && incomingFence != nil && incomingGeneration == 0 {
		return moerr.NewInternalErrorNoCtx(
			"owner-fenced CDC watermark update requires a source table generation")
	}
	if fenced && incomingFence != nil && incomingFence.GenerationToken() == 0 {
		return moerr.NewInternalErrorNoCtx(
			"owner-fenced CDC watermark update requires a durable owner generation")
	}
	// A same-process restart can publish a replacement fence while the retired
	// pipeline is finishing a target commit. Do not let that delayed completion
	// repopulate local progress after ClaimWatermarkOwner cleared it. Cross-CN
	// stale writes are independently rejected by the durable SQL owner fence.
	if fenced && incomingFence != nil {
		if active := u.activeWatermarkFence[*key]; active != nil && active != incomingFence {
			return nil
		}
	}
	if fenced && incomingFence != nil && !u.shouldBufferStableWatermarkLocked(
		*key, *watermark, incomingGeneration, incomingFence) {
		return nil
	}

	oldWatermark, hasOld := u.cacheUncommitted[*key]
	u.cacheUncommitted[*key] = *watermark
	if fenced && incomingFence != nil {
		u.cacheUncommittedFence[*key] = incomingFence
		if incomingGeneration > 0 {
			u.cacheUncommittedGeneration[*key] = incomingGeneration
		} else {
			delete(u.cacheUncommittedGeneration, *key)
		}
	} else {
		delete(u.cacheUncommittedFence, *key)
		delete(u.cacheUncommittedGeneration, *key)
	}

	// Record metrics: watermark update counter
	tableLabel := key.String()
	v2.CdcWatermarkUpdateCounter.WithLabelValues(tableLabel, "commit").Inc()

	// Log watermark updates for better observability
	logutil.Debug(
		"cdc.watermark.buffer_update",
		zap.String("task-id", key.TaskId),
		zap.String("key", key.String()),
		zap.String("old-watermark", oldWatermark.ToString()),
		zap.String("new-watermark", watermark.ToString()),
		zap.Bool("has-old", hasOld),
		zap.Int("uncommitted-count", len(u.cacheUncommitted)),
		zap.Int("committing-count", len(u.cacheCommitting)),
		zap.Int("committed-count", len(u.cacheCommitted)),
	)

	return nil
}

func (u *CDCWatermarkUpdater) shouldBufferStableWatermarkLocked(
	key WatermarkKey,
	watermark types.TS,
	generation uint64,
	fence *OwnerFence,
) bool {
	if current, ok := u.cacheCommitted[key]; ok {
		currentGeneration := u.cacheCommittedGeneration[key]
		if currentGeneration > generation ||
			(currentGeneration == generation && current.GE(&watermark)) {
			return false
		}
	}
	if current, ok := u.cacheCommitting[key]; ok {
		currentGeneration := u.cacheCommittingGeneration[key]
		if currentGeneration > generation ||
			(currentGeneration == generation && current.GT(&watermark)) {
			return false
		}
		if currentGeneration == generation && current.Equal(&watermark) &&
			!fence.supersedes(u.cacheCommittingFence[key]) {
			return false
		}
	}
	if current, ok := u.cacheUncommitted[key]; ok {
		currentGeneration := u.cacheUncommittedGeneration[key]
		if currentGeneration > generation ||
			(currentGeneration == generation && current.GT(&watermark)) {
			return false
		}
		if currentGeneration == generation && current.Equal(&watermark) &&
			!fence.supersedes(u.cacheUncommittedFence[key]) {
			return false
		}
	}
	return true
}

// activateWatermarkFenceLocked publishes a replacement fence and removes local
// non-durable progress that would otherwise outrank the replacement's durable
// reread. A delayed old admission cannot move the local generation backward.
// The caller must hold u.Lock.
func (u *CDCWatermarkUpdater) activateWatermarkFenceLocked(key WatermarkKey, fence *OwnerFence) bool {
	if active := u.activeWatermarkFence[key]; active != nil && active != fence &&
		active.GenerationToken() >= fence.GenerationToken() {
		return false
	}
	if cached := u.cacheUncommittedFence[key]; cached != nil && cached != fence {
		delete(u.cacheUncommitted, key)
		delete(u.cacheUncommittedGeneration, key)
		delete(u.cacheUncommittedFence, key)
	}
	if cached := u.cacheCommittingFence[key]; cached != nil && cached != fence {
		delete(u.cacheCommitting, key)
		delete(u.cacheCommittingGeneration, key)
		delete(u.cacheCommittingFence, key)
	}
	if active := u.activeWatermarkFence[key]; active != nil && active != fence {
		// Retry counts belong to one executor generation. Carrying an old owner's
		// cache into its replacement can prematurely turn a fresh transient error
		// into a permanent failure.
		delete(u.errorMetadataCache, key)
		clearWatermarkErrorGauge(key)
	}
	u.resetWatermarkCircuitLocked(key)
	u.activeWatermarkFence[key] = fence
	return true
}

var watermarkErrorMetricTypes = [...]string{
	"network", "commit", "table_relation", "sinker", "max_retry_exceeded", "unknown",
}

// clearWatermarkErrorGauge resets every possible classification for one table.
// Callers that coordinate this with owner state hold u.Lock.
func clearWatermarkErrorGauge(key WatermarkKey) {
	tableLabel := key.String()
	for _, errorType := range watermarkErrorMetricTypes {
		v2.CdcTableNonRetryableErrorGauge.WithLabelValues(tableLabel, errorType).Set(0)
	}
}

func watermarkProgressIsNewer(
	candidateGeneration uint64,
	candidate types.TS,
	currentGeneration uint64,
	current types.TS,
) bool {
	if candidateGeneration != currentGeneration {
		return candidateGeneration > currentGeneration
	}
	return candidate.GT(&current)
}

func shouldRetainBufferedWatermark(
	existingGeneration uint64,
	existing types.TS,
	existingFence *OwnerFence,
	retryGeneration uint64,
	retry types.TS,
	retryFence *OwnerFence,
) bool {
	if watermarkProgressIsNewer(existingGeneration, existing, retryGeneration, retry) {
		return true
	}
	if watermarkProgressIsNewer(retryGeneration, retry, existingGeneration, existing) {
		return false
	}
	// Equal progress is still generation-sensitive: never let a failed write
	// from an older in-process owner replace the replacement owner's fence.
	return retryFence == nil || !retryFence.supersedes(existingFence)
}

// resetWatermarkCircuitLocked releases both sides of the circuit-breaker
// accounting contract. The caller must hold u.Lock.
func (u *CDCWatermarkUpdater) resetWatermarkCircuitLocked(key WatermarkKey) {
	if _, opened := u.commitCircuitOpen[key]; opened {
		delete(u.commitCircuitOpen, key)
		v2.CdcWatermarkCircuitEventCounter.WithLabelValues("reset").Inc()
		v2.CdcWatermarkCircuitOpenGauge.Dec()
	}
	delete(u.commitFailureCount, key)
}

// retireWatermarkProgressLocked removes every progress cache entry derived
// from a watermark row. It intentionally leaves diagnostic state and
// activeWatermarkFence alone: those have distinct lifecycle owners. The caller
// must hold u.Lock.
func (u *CDCWatermarkUpdater) retireWatermarkProgressLocked(key WatermarkKey) bool {
	_, inCommitted := u.cacheCommitted[key]
	delete(u.cacheUncommitted, key)
	delete(u.cacheCommitting, key)
	delete(u.cacheCommitted, key)
	delete(u.cacheUncommittedGeneration, key)
	delete(u.cacheCommittingGeneration, key)
	delete(u.cacheCommittedGeneration, key)
	delete(u.cacheUncommittedFence, key)
	delete(u.cacheCommittingFence, key)
	u.resetWatermarkCircuitLocked(key)
	return inCommitted
}

// removeWatermarkProgressLocked removes progress and diagnostic state. Orphan
// and task cleanup use this full form because no replacement stream owns the
// diagnostic. Failed-stream retirement uses retireWatermarkProgressLocked so
// retry state survives admission of the replacement pipeline.
func (u *CDCWatermarkUpdater) removeWatermarkProgressLocked(key WatermarkKey) bool {
	inCommitted := u.retireWatermarkProgressLocked(key)
	delete(u.errorMetadataCache, key)
	return inCommitted
}

// removeWatermarkStateLocked additionally retires the active generation. Use
// it only after the stream or task lifecycle has fenced new work. The caller
// must hold u.Lock.
func (u *CDCWatermarkUpdater) removeWatermarkStateLocked(key WatermarkKey) bool {
	inCommitted := u.removeWatermarkProgressLocked(key)
	delete(u.activeWatermarkFence, key)
	return inCommitted
}

func (u *CDCWatermarkUpdater) removeWatermarkProgressMetrics(key WatermarkKey) {
	tableLabel := key.String()
	v2.CdcWatermarkLagSeconds.DeleteLabelValues(tableLabel)
	v2.CdcWatermarkLagRatio.DeleteLabelValues(tableLabel)
	v2.CdcTableLastActivityTimestamp.DeleteLabelValues(tableLabel)
	v2.CdcTableStuckGauge.DeleteLabelValues(tableLabel)
	v2.CdcWatermarkUpdateCounter.DeleteLabelValues(tableLabel, "commit")
	v2.CdcWatermarkUpdateCounter.DeleteLabelValues(tableLabel, "heartbeat")
	v2.CdcHeartbeatCounter.DeleteLabelValues(tableLabel)
	v2.CdcTableNoProgressCounter.DeleteLabelValues(tableLabel)
}

func (u *CDCWatermarkUpdater) removeWatermarkErrorMetrics(key WatermarkKey) {
	tableLabel := key.String()
	for _, errorType := range []string{"network", "commit", "table_relation", "sinker", "max_retry_exceeded", "unknown"} {
		v2.CdcTableNonRetryableErrorGauge.DeleteLabelValues(tableLabel, errorType)
	}
}

func (u *CDCWatermarkUpdater) removeWatermarkMetrics(key WatermarkKey) {
	u.removeWatermarkProgressMetrics(key)
	u.removeWatermarkErrorMetrics(key)
}

func (u *CDCWatermarkUpdater) RemoveCachedWM(
	ctx context.Context,
	key *WatermarkKey,
	mode WatermarkCleanupMode,
) (err error) {
	if err = u.ForceFlush(ctx); err != nil {
		logutil.Warn(
			"cdc.watermark.remove.force_flush_failed",
			zap.String("key", key.String()),
			zap.Error(err),
		)
		// Continue even if flush fails
	}
	job := NewRemoveCachedWMJob(ctx, key, mode)
	if _, err = u.queue.Enqueue(job); err != nil {
		if errors.Is(err, sm.ErrClose) {
			u.removeCachedWMSynchronously(key, mode, true)
			return nil
		}
		job.DoneWithErr(err)
		return err
	}
	job.WaitDone()
	err = job.GetResult().Err
	return
}

func (u *CDCWatermarkUpdater) removeCachedWMSynchronously(
	key *WatermarkKey,
	mode WatermarkCleanupMode,
	logSkip bool,
) {
	u.Lock()
	keepDiagnostic := mode == WatermarkCleanupKeepDiagnostic
	var inCommitted bool
	if keepDiagnostic {
		inCommitted = u.retireWatermarkProgressLocked(*key)
	} else {
		inCommitted = u.removeWatermarkStateLocked(*key)
	}
	u.Unlock()

	if keepDiagnostic {
		u.removeWatermarkProgressMetrics(*key)
	} else {
		u.removeWatermarkMetrics(*key)
	}

	if !u.shouldLogFallback(key) {
		return
	}

	fields := []zap.Field{
		zap.String("key", key.String()),
		zap.Bool("fallback", true),
	}
	if inCommitted {
		logutil.Info("cdc.watermark.remove_cached_success", fields...)
	} else if logSkip {
		logutil.Info("cdc.watermark.remove_cached_skip", fields...)
	}
}

func collectTaskWatermarkKeys[V any](
	dst map[WatermarkKey]struct{},
	src map[WatermarkKey]V,
	accountID uint64,
	taskID string,
) {
	for key := range src {
		if key.AccountId == accountID && key.TaskId == taskID {
			dst[key] = struct{}{}
		}
	}
}

func (u *CDCWatermarkUpdater) shouldLogFallback(key *WatermarkKey) bool {
	if key == nil {
		return true
	}
	now := time.Now()
	ks := key.String()
	if prev, ok := u.fallbackLog.Load(ks); ok {
		if elapsed := now.Sub(prev.(time.Time)); elapsed < fallbackLogThrottleWindow {
			return false
		}
	}
	u.fallbackLog.Store(ks, now)
	return true
}

func (u *CDCWatermarkUpdater) ForceFlush(ctx context.Context) (err error) {
	job := NewCommittingWMJob(ctx)
	if u.customized.scheduleJobWithContext != nil {
		err = u.customized.scheduleJobWithContext(ctx, job)
	} else {
		// Keep test and embedding overrides of the legacy scheduler working.
		err = u.customized.scheduleJob(job)
	}
	if err != nil {
		// The scheduler owns completion when it admits a job. On admission
		// failure no caller waits on this unadmitted job, and legacy custom
		// schedulers may already have completed it before returning the error.
		return
	}
	err = job.WaitDoneContext(ctx).Err
	return
}

// DeleteTaskWatermarks drains watermark writes queued before task cancellation,
// removes the task from every cache tier, and finally deletes its durable rows.
// The caller must fence new updates and stop all task readers before calling
// this method. ForceFlush acts as a queue barrier, so the final DELETE cannot be
// followed by an older buffered write that recreates an orphan watermark.
func (u *CDCWatermarkUpdater) DeleteTaskWatermarks(
	ctx context.Context,
	accountID uint64,
	taskID string,
) error {
	// This is the linearization point for terminal cleanup. The lifecycle owner
	// retains the tombstone until every callback and reader has exited; failures
	// deliberately keep it installed so a late writer cannot recreate the row.
	u.MarkTaskDeleted(taskID)
	flushErr := u.ForceFlush(ctx)
	if flushErr != nil {
		// A failed flush is not a completed queue barrier. An older batch may
		// already have admitted a durable writer and still be blocked in an
		// earlier persistence phase. Keep the tombstone and leave cleanup to the
		// lifecycle owner's retry; deleting now could let that writer recreate
		// the task watermark after the DELETE.
		logutil.Warn(
			"cdc.watermark.delete_task.flush_failed",
			zap.Uint64("account-id", accountID),
			zap.String("task-id", taskID),
			zap.Error(flushErr),
		)
		return flushErr
	}

	keysToClean := make(map[WatermarkKey]struct{})
	u.Lock()
	collectTaskWatermarkKeys(keysToClean, u.cacheUncommitted, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheCommitting, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheCommitted, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheUncommittedGeneration, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheCommittingGeneration, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheCommittedGeneration, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheUncommittedFence, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.cacheCommittingFence, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.activeWatermarkFence, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.errorMetadataCache, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.commitFailureCount, accountID, taskID)
	collectTaskWatermarkKeys(keysToClean, u.commitCircuitOpen, accountID, taskID)
	for key := range keysToClean {
		u.removeWatermarkStateLocked(key)
	}
	u.Unlock()

	for key := range keysToClean {
		u.removeWatermarkMetrics(key)
		u.fallbackLog.Delete(key.String())
	}

	u.persistMu.Lock()
	err := u.ie.Exec(
		defines.AttachAccountId(ctx, catalog.System_Account),
		CDCSQLBuilder.DeleteWatermarkSQL(accountID, taskID),
		ie.SessionOverrideOptions{},
	)
	u.persistMu.Unlock()
	if err != nil {
		logutil.Error(
			"cdc.watermark.delete_task.failed",
			zap.Uint64("account-id", accountID),
			zap.String("task-id", taskID),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (u *CDCWatermarkUpdater) MarkTaskDeleted(taskID string) {
	u.Lock()
	u.deletedTasks.Store(taskID, struct{}{})
	u.pausedTasks.Delete(taskID)
	u.Unlock()
}

// ForgetTaskDeleted releases a terminal tombstone only after the caller has
// proved that all producers for the task have exited and durable deletion has
// succeeded. Keeping the proof at the caller prevents late readers from
// recreating a deleted row.
func (u *CDCWatermarkUpdater) ForgetTaskDeleted(taskID string) {
	u.Lock()
	defer u.Unlock()
	u.deletedTasks.Delete(taskID)
}

// MarkTaskPaused marks a task as paused to block watermark updates
// This is called when a task is being paused to prevent race conditions
func (u *CDCWatermarkUpdater) MarkTaskPaused(taskId string) {
	u.Lock()
	defer u.Unlock()
	if _, deleted := u.deletedTasks.Load(taskId); deleted {
		return
	}
	u.pausedTasks.Store(taskId, time.Now())
	logutil.Info(
		"cdc.watermark.task_marked_paused",
		zap.String("task-id", taskId),
	)
}

// UnmarkTaskPaused removes the pause mark from a task
// This is called when a task resumes or is cancelled
func (u *CDCWatermarkUpdater) UnmarkTaskPaused(taskId string) {
	u.pausedTasks.Delete(taskId)
	logutil.Info(
		"cdc.watermark.task_unmarked_paused",
		zap.String("task-id", taskId),
	)
}

// IsCircuitBreakerOpen returns true if the circuit breaker is currently open for the given key.
func (u *CDCWatermarkUpdater) IsCircuitBreakerOpen(key *WatermarkKey) bool {
	if key == nil {
		return false
	}
	u.RLock()
	defer u.RUnlock()
	if openedAt, ok := u.commitCircuitOpen[*key]; ok {
		return time.Since(openedAt) < watermarkCircuitBreakPeriod
	}
	return false
}

// GetCommitFailureCount returns the number of consecutive commit failures for the given key.
func (u *CDCWatermarkUpdater) GetCommitFailureCount(key *WatermarkKey) uint32 {
	if key == nil {
		return 0
	}
	u.RLock()
	defer u.RUnlock()
	if count, ok := u.commitFailureCount[*key]; ok {
		return count
	}
	return 0
}

// GetOrAddCommitted retrieves the persisted watermark from database, or adds it if not exists.
//
// Used for CDC task initialization to determine the starting watermark:
// - If watermark exists in database: Return the persisted value (resume from last position)
// - If watermark doesn't exist: Add the provided watermark to database (new task starting)
//
// Fast Path:
// - Checks cacheCommitted first to avoid database query if watermark is already in memory
// - Returns immediately if cached watermark >= requested watermark
//
// Slow Path (Cache Miss):
// - Enqueues a job to read watermark from database
// - If found: Updates cacheCommitted and returns persisted value
// - If not found: Inserts new watermark record and returns it
//
// Concurrency: Assumes no concurrent writes to the same key (single reader per table)
func (u *CDCWatermarkUpdater) GetOrAddCommitted(
	ctx context.Context,
	key *WatermarkKey,
	watermark *types.TS,
) (ret types.TS, err error) {
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		return types.TS{}, nil
	}
	u.RLock()
	persisted, ok := u.cacheCommitted[*key]
	u.RUnlock()
	if ok {
		if persisted.GE(watermark) {
			ret = persisted
			return
		}
	}

	job := NewGetOrAddCommittedWMJob(ctx, key, watermark)
	if _, err = u.queue.Enqueue(job); err != nil {
		if errors.Is(err, sm.ErrClose) {
			if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
				return types.TS{}, nil
			}
			if watermark != nil {
				u.Lock()
				u.cacheCommitted[*key] = *watermark
				delete(u.cacheCommittedGeneration, *key)
				u.Unlock()
				ret = *watermark
			}
			if u.shouldLogFallback(key) {
				fields := []zap.Field{
					zap.String("key", key.String()),
					zap.Bool("fallback", true),
				}
				if watermark != nil {
					fields = append(fields, zap.String("watermark", watermark.ToString()))
				}
				logutil.Info("cdc.watermark.get_or_add_fallback", fields...)
			}
			return ret, nil
		}
		return
	}
	job.WaitDone()
	res := job.GetResult()
	if _, deleted := u.deletedTasks.Load(key.TaskId); deleted {
		return types.TS{}, nil
	}
	if res.Err != nil {
		err = res.Err
	} else {
		ret = res.Res.(types.TS)
	}
	return
}

// cron job to move the watermark from uncommitted to
// committing
func (u *CDCWatermarkUpdater) wrapCronJob(job func(ctx context.Context)) func(ctx context.Context) {
	return func(ctx context.Context) {
		if time.Since(u.stats.lastExportTime) > u.opts.exportStatsInterval {
			u.stats.lastExportTime = time.Now()

			// Export detailed statistics
			u.RLock()
			uncommittedCount := len(u.cacheUncommitted)
			committingCount := len(u.cacheCommitting)
			committedCount := len(u.cacheCommitted)

			// Metrics: watermark cache sizes
			v2.CdcWatermarkCacheGauge.WithLabelValues("uncommitted").Set(float64(uncommittedCount))
			v2.CdcWatermarkCacheGauge.WithLabelValues("committing").Set(float64(committingCount))
			v2.CdcWatermarkCacheGauge.WithLabelValues("committed").Set(float64(committedCount))

			// Metrics: watermark lag for each table
			// Default expected frequency: 200ms (typical CDC polling interval)
			// This is a simplified calculation - actual frequency-aware ratio would require task config
			// Baseline for lag ratio: 3 seconds (allows for batch processing delays and network latency)
			// This is more realistic than 0.4s (200ms * 2) which was too small for practical scenarios
			defaultExpectedLagSeconds := 3.0

			// Collect keys to check BEFORE releasing lock (to avoid iterating during query)
			type cachedWatermarkProgress struct {
				watermark  types.TS
				generation uint64
			}
			cachedKeys := make([]WatermarkKey, 0, len(u.cacheCommitted))
			cachedProgress := make(map[WatermarkKey]cachedWatermarkProgress, len(u.cacheCommitted))
			for key, watermark := range u.cacheCommitted {
				if !watermark.IsEmpty() {
					cachedKeys = append(cachedKeys, key)
					cachedProgress[key] = cachedWatermarkProgress{
						watermark:  watermark,
						generation: u.cacheCommittedGeneration[key],
					}
				}
			}
			u.RUnlock()

			// Query database OUTSIDE of lock to avoid holding lock during slow query
			// This fixes the potential deadlock issue where RLock is held during DB query
			queryCtx := defines.AttachAccountId(ctx, catalog.System_Account)
			// Query watermarks that have valid tasks (JOIN with mo_cdc_task)
			sql := "SELECT w.account_id, w.task_id, w.db_name, w.table_name " +
				"FROM `mo_catalog`.`mo_cdc_watermark` AS w " +
				"INNER JOIN `mo_catalog`.`mo_cdc_task` AS t " +
				"ON t.account_id = w.account_id AND t.task_id = w.task_id"

			validWatermarks := make(map[string]bool) // key: "accountId.taskId.dbName.tableName"
			queryFailed := false
			res := u.ie.Query(queryCtx, sql, ie.SessionOverrideOptions{})
			if res.Error() == nil {
				for i := uint64(0); i < res.RowCount(); i++ {
					accountId, _ := res.GetUint64(queryCtx, i, 0)
					taskId, _ := res.GetString(queryCtx, i, 1)
					dbName, _ := res.GetString(queryCtx, i, 2)
					tableName, _ := res.GetString(queryCtx, i, 3)
					key := fmt.Sprintf("%d.%s.%s.%s", accountId, taskId, dbName, tableName)
					validWatermarks[key] = true
				}
			} else {
				logutil.Warn(
					"cdc.watermark.query_valid_watermarks_failed",
					zap.Error(res.Error()),
				)
				// On query failure, skip cleanup to avoid false removal
				queryFailed = true
			}

			// Process cached keys - update metrics for valid ones, cleanup orphans
			keysToRemove := make([]WatermarkKey, 0)
			for _, key := range cachedKeys {
				tableLabel := key.String()
				watermark := cachedProgress[key].watermark

				if !queryFailed && !validWatermarks[tableLabel] {
					// Watermark not in database (orphan)
					keysToRemove = append(keysToRemove, key)
					continue
				}

				// Update metrics for valid watermarks
				wmTime := watermark.ToTimestamp().ToStdTime()
				lagSeconds := time.Since(wmTime).Seconds()
				v2.CdcWatermarkLagSeconds.WithLabelValues(tableLabel).Set(lagSeconds)

				// Calculate lag ratio: actual lag / expected lag
				// Expected lag = 3 seconds (realistic baseline for batch processing)
				// Ratio < 2: normal, 2-5: warning, > 5: critical
				if defaultExpectedLagSeconds > 0 {
					lagRatio := lagSeconds / defaultExpectedLagSeconds
					v2.CdcWatermarkLagRatio.WithLabelValues(tableLabel).Set(lagRatio)
				}
			}

			// Remove orphan keys from cache with double-check to handle race condition
			if len(keysToRemove) > 0 {
				removedKeys := make([]WatermarkKey, 0, len(keysToRemove))
				u.Lock()
				for _, key := range keysToRemove {
					// A restart can replace the generation while the catalog query is
					// in flight. Only retire the exact progress snapshot classified as
					// orphan; the task lifecycle remains the sole owner of the active
					// fence.
					current, stillExists := u.cacheCommitted[key]
					cached := cachedProgress[key]
					if !stillExists || !current.Equal(&cached.watermark) ||
						u.cacheCommittedGeneration[key] != cached.generation {
						continue
					}
					if u.activeWatermarkFence[key] != nil {
						// A catalog snapshot cannot prove that a live local
						// generation has stopped. Its stream/task lifecycle owner
						// will perform the full cleanup after fencing producers.
						continue
					}
					u.removeWatermarkProgressLocked(key)
					removedKeys = append(removedKeys, key)
				}
				u.Unlock()
				for _, key := range removedKeys {
					u.removeWatermarkMetrics(key)
					u.fallbackLog.Delete(key.String())
				}
				logutil.Debug(
					"cdc.watermark.cleanup_orphan_cache",
					zap.Int("removed-count", len(removedKeys)),
				)
			}

			logutil.Info(
				"cdc.watermark.stats",
				zap.Uint64("run-times", u.stats.runTimes.Load()),
				zap.Uint64("skip-times", u.stats.skipTimes.Load()),
				zap.Uint64("error-times", u.stats.errorTimes.Load()),
				zap.Int("uncommitted-watermarks", uncommittedCount),
				zap.Int("committing-watermarks", committingCount),
				zap.Int("committed-watermarks", committedCount),
				zap.Float64("skip-ratio", float64(u.stats.skipTimes.Load())/float64(u.stats.runTimes.Load())),
			)

			// Scan all tables with errors and update non-retryable error metrics
			// Pass validWatermarks to enable diff-based cleanup of stale metrics
			u.scanAndUpdateNonRetryableErrorMetrics(ctx, validWatermarks, queryFailed)
		}
		u.stats.runTimes.Add(1)
		job(ctx)
	}
}

func (u *CDCWatermarkUpdater) scheduleJob(job *UpdaterJob) (err error) {
	_, err = u.queue.Enqueue(job)
	return
}

func (u *CDCWatermarkUpdater) scheduleJobWithContext(ctx context.Context, job *UpdaterJob) (err error) {
	queue, ok := u.queue.(sm.ContextQueue)
	if !ok {
		return u.scheduleJob(job)
	}
	_, err = queue.EnqueueWithContext(ctx, job)
	return
}

// extractErrorType extracts error type from error message for metric labels
// This is a simplified version that extracts common error patterns
func extractErrorType(errMsg string) string {
	if errMsg == "" {
		return "none"
	}

	// Network/system errors
	if strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "network") ||
		strings.Contains(errMsg, "unavailable") ||
		strings.Contains(errMsg, "rpc") ||
		strings.Contains(errMsg, "backend") {
		return "network"
	}

	// Commit errors
	if strings.Contains(errMsg, "commit") {
		return "commit"
	}

	// Table relation errors
	if strings.Contains(errMsg, "relation") ||
		strings.Contains(errMsg, "truncated") ||
		strings.Contains(errMsg, "table not found") ||
		strings.Contains(errMsg, "table") {
		return "table_relation"
	}

	// Sinker errors
	if strings.Contains(errMsg, "sinker") {
		return "sinker"
	}

	// Max retry exceeded
	if strings.Contains(errMsg, "max retry exceeded") {
		return "max_retry_exceeded"
	}

	// Default category
	return "unknown"
}

// scanAndUpdateNonRetryableErrorMetrics scans all tables with errors from database
// and updates non-retryable error metrics.
// Uses diff-based cleanup: compares current labels with previous run to delete stale metrics.
// Parameters:
//   - validWatermarks: map of valid table labels (from JOIN query), used to filter orphans
//   - queryFailed: if true, skip cleanup to avoid false removal
func (u *CDCWatermarkUpdater) scanAndUpdateNonRetryableErrorMetrics(
	ctx context.Context,
	validWatermarks map[string]bool,
	queryFailed bool,
) {
	// Query all watermarks with err_msg != ''
	// Use System_Account context for querying
	queryCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	projection := "account_id, task_id, db_name, table_name, err_msg"
	whereClause := "err_msg != ''"
	sql := CDCSQLBuilder.GetWatermarkWhereSQL(projection, whereClause)

	res := u.ie.Query(queryCtx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		logutil.Warn(
			"cdc.watermark.scan_errors_failed",
			zap.Error(res.Error()),
		)
		return
	}

	// Track tables with non-retryable errors in THIS scan
	// Used for diff-based cleanup: (previous - current) labels get deleted
	currentErrorLabels := make(map[string]bool)
	nonRetryableCount := 0

	// Common error types for metric cleanup
	errorTypes := []string{"network", "commit", "table_relation", "sinker", "max_retry_exceeded", "unknown"}

	// Process each row
	for i := uint64(0); i < res.RowCount(); i++ {
		accountId, err := res.GetUint64(queryCtx, i, 0)
		if err != nil {
			logutil.Warn(
				"cdc.watermark.scan_errors_get_account_id_failed",
				zap.Error(err),
				zap.Uint64("row", i),
			)
			continue
		}
		taskId, _ := res.GetString(queryCtx, i, 1)
		dbName, _ := res.GetString(queryCtx, i, 2)
		tableName, _ := res.GetString(queryCtx, i, 3)
		errMsg, _ := res.GetString(queryCtx, i, 4)

		if errMsg == "" {
			continue
		}

		// Build table label: account_id.task_id.db_name.table_name
		tableLabel := fmt.Sprintf("%d.%s.%s.%s", accountId, taskId, dbName, tableName)

		// Skip orphan tables (not in validWatermarks)
		// These will be cleaned up by the orphan cleanup in wrapCronJob
		if !queryFailed && !validWatermarks[tableLabel] {
			continue
		}

		// Parse error metadata
		metadata := ParseErrorMetadata(errMsg)
		if metadata == nil {
			continue
		}

		// Check if non-retryable (ShouldRetry returns false)
		if !ShouldRetry(metadata) {
			// Track this label as having non-retryable error
			currentErrorLabels[tableLabel] = true
			// Extract error type from message
			errorType := extractErrorType(metadata.Message)
			// Set metric to 1 (has non-retryable error)
			v2.CdcTableNonRetryableErrorGauge.WithLabelValues(tableLabel, errorType).Set(1)
			nonRetryableCount++
		}
		// Note: For retryable errors, we don't set metrics here.
		// The cleanup below handles labels that were previously non-retryable but now aren't.
	}

	// Diff-based cleanup: Delete metrics for labels that were in previous but not in current
	// This handles:
	// 1. Error cleared (err_msg became empty) - label not in query results
	// 2. Error became retryable - label not in currentErrorLabels
	// 3. Task deleted (orphan) - label not in validWatermarks
	if !queryFailed {
		cleanedCount := 0
		for label := range u.previousErrorLabels {
			if !currentErrorLabels[label] {
				// This label no longer has non-retryable error
				// Delete all error type metrics for this label
				for _, et := range errorTypes {
					v2.CdcTableNonRetryableErrorGauge.DeleteLabelValues(label, et)
				}
				cleanedCount++
			}
		}
		if cleanedCount > 0 {
			logutil.Debug(
				"cdc.watermark.cleanup_stale_error_metrics",
				zap.Int("cleaned-count", cleanedCount),
			)
		}
		// Update previous for next run
		u.previousErrorLabels = currentErrorLabels
	}

	// Update total count
	v2.CdcTableNonRetryableErrorTotalGauge.Set(float64(nonRetryableCount))

	logutil.Debug(
		"cdc.watermark.scan_errors_complete",
		zap.Int("total-tables-with-errors", int(res.RowCount())),
		zap.Int("non-retryable-count", nonRetryableCount),
		zap.Int("current-error-labels", len(currentErrorLabels)),
		zap.Int("previous-error-labels", len(u.previousErrorLabels)),
	)
}

// cronRun is the periodic job that moves watermarks from cacheUncommitted to database.
//
// Execution Interval: Every 3 seconds (configurable via cronJobInterval)
//
// Process:
// 1. Check if previous commit is still in progress (cacheCommitting not empty)
//   - If yes: Skip this run to avoid concurrent commits
//
// 2. Move all watermarks: cacheUncommitted -> cacheCommitting
// 3. Call ForceFlush to persist cacheCommitting to database
//
// Concurrency Control:
// - Only one CronJob execution at a time (skips if cacheCommitting is not empty)
// - This prevents concurrent database updates for the same watermarks
//
// Error Handling:
// - Errors are logged but suppressed (only log every N times to avoid spam)
// - Failed keys remain buffered for retry with a per-key circuit breaker
func (u *CDCWatermarkUpdater) cronRun(ctx context.Context) {
	u.Lock()
	// if there is any watermark in committing, skip the current run
	if len(u.cacheCommitting) > 0 || len(u.cacheUncommitted) == 0 {
		u.stats.skipTimes.Add(1)
		u.Unlock()
		return
	}
	// move all watermarks from uncommitted to committing
	for key, watermark := range u.cacheUncommitted {
		u.cacheCommitting[key] = watermark
		if generation, ok := u.cacheUncommittedGeneration[key]; ok {
			u.cacheCommittingGeneration[key] = generation
		} else {
			delete(u.cacheCommittingGeneration, key)
		}
		if fence, ok := u.cacheUncommittedFence[key]; ok {
			u.cacheCommittingFence[key] = fence
		} else {
			delete(u.cacheCommittingFence, key)
		}
		delete(u.cacheUncommitted, key)
		delete(u.cacheUncommittedGeneration, key)
		delete(u.cacheUncommittedFence, key)
	}
	u.Unlock()

	var err error
	defer func() {
		if err != nil {
			u.stats.errorTimes.Add(1)
			times := u.stats.errorTimes.Load()
			if times%u.opts.cronJobErrorSupressTimes == 0 {
				logutil.Error(
					"CDCWatermarkUpdater-Error",
					zap.Error(err),
					zap.Uint64("error-times", times),
				)
			}
		}
	}()

	err = u.ForceFlush(ctx)
}
