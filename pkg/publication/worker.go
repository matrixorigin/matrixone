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

package publication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

const (
	PublicationWorkerThread  = 10
	FilterObjectWorkerThread = 1000
	GetChunkWorkerThread     = 10
	WriteObjectWorkerThread  = 100

	SubmitRetryTimes    = 1000
	SubmitRetryDuration = time.Hour

	StatsPrintInterval = 10 * time.Second
)

// GetChunkJobDuration holds duration info for a GetChunk job
type GetChunkJobDuration struct {
	ObjectName string
	ChunkIndex int64
	Duration   time.Duration
}

// WriteObjectJobDuration holds duration info for a WriteObject job
type WriteObjectJobDuration struct {
	ObjectName string
	Size       int64
	Duration   time.Duration
}

// JobStats holds statistics for job tracking using atomic counters for thread safety
type JobStats struct {
	FilterObjectPending   atomic.Int64
	FilterObjectRunning   atomic.Int64
	FilterObjectCompleted atomic.Int64

	GetChunkPending   atomic.Int64
	GetChunkRunning   atomic.Int64
	GetChunkCompleted atomic.Int64

	GetMetaPending   atomic.Int64
	GetMetaRunning   atomic.Int64
	GetMetaCompleted atomic.Int64

	WriteObjectPending   atomic.Int64
	WriteObjectRunning   atomic.Int64
	WriteObjectCompleted atomic.Int64

	// Top 3 longest GetChunk jobs
	getChunkDurationMu   sync.Mutex
	getChunkTopDurations []*GetChunkJobDuration

	// Top 3 longest WriteObject jobs
	writeObjectDurationMu   sync.Mutex
	writeObjectTopDurations []*WriteObjectJobDuration
}

// Global job stats instance
var globalJobStats = &JobStats{}

// GetJobStats returns the global job stats
func GetJobStats() *JobStats {
	return globalJobStats
}

// IncrementFilterObjectPending increments the filter object pending counter
func (s *JobStats) IncrementFilterObjectPending() {
	s.FilterObjectPending.Add(1)
}

// IncrementFilterObjectRunning increments the filter object running counter and decrements pending
func (s *JobStats) IncrementFilterObjectRunning() {
	s.FilterObjectPending.Add(-1)
	s.FilterObjectRunning.Add(1)
}

// DecrementFilterObjectRunning decrements the filter object running counter
func (s *JobStats) DecrementFilterObjectRunning() {
	s.FilterObjectRunning.Add(-1)
	s.FilterObjectCompleted.Add(1)
}

// IncrementGetChunkPending increments the get chunk pending counter
func (s *JobStats) IncrementGetChunkPending() {
	s.GetChunkPending.Add(1)
}

// IncrementGetChunkRunning increments the get chunk running counter and decrements pending
func (s *JobStats) IncrementGetChunkRunning() {
	s.GetChunkPending.Add(-1)
	s.GetChunkRunning.Add(1)
}

// DecrementGetChunkRunning decrements the get chunk running counter
func (s *JobStats) DecrementGetChunkRunning() {
	s.GetChunkRunning.Add(-1)
	s.GetChunkCompleted.Add(1)
}

// IncrementGetMetaPending increments the get meta pending counter
func (s *JobStats) IncrementGetMetaPending() {
	s.GetMetaPending.Add(1)
}

// IncrementGetMetaRunning increments the get meta running counter and decrements pending
func (s *JobStats) IncrementGetMetaRunning() {
	s.GetMetaPending.Add(-1)
	s.GetMetaRunning.Add(1)
}

// DecrementGetMetaRunning decrements the get meta running counter
func (s *JobStats) DecrementGetMetaRunning() {
	s.GetMetaRunning.Add(-1)
	s.GetMetaCompleted.Add(1)
}

// IncrementWriteObjectPending increments the write object pending counter
func (s *JobStats) IncrementWriteObjectPending() {
	s.WriteObjectPending.Add(1)
}

// IncrementWriteObjectRunning increments the write object running counter and decrements pending
func (s *JobStats) IncrementWriteObjectRunning() {
	s.WriteObjectPending.Add(-1)
	s.WriteObjectRunning.Add(1)
}

// DecrementWriteObjectRunning decrements the write object running counter
func (s *JobStats) DecrementWriteObjectRunning() {
	s.WriteObjectRunning.Add(-1)
	s.WriteObjectCompleted.Add(1)
}

// RecordGetChunkDuration records a GetChunk job duration and keeps top 3 longest
func (s *JobStats) RecordGetChunkDuration(objectName string, chunkIndex int64, duration time.Duration) {
	s.getChunkDurationMu.Lock()
	defer s.getChunkDurationMu.Unlock()

	newEntry := &GetChunkJobDuration{
		ObjectName: objectName,
		ChunkIndex: chunkIndex,
		Duration:   duration,
	}

	// Add new entry
	s.getChunkTopDurations = append(s.getChunkTopDurations, newEntry)

	// Sort by duration descending
	for i := len(s.getChunkTopDurations) - 1; i > 0; i-- {
		if s.getChunkTopDurations[i].Duration > s.getChunkTopDurations[i-1].Duration {
			s.getChunkTopDurations[i], s.getChunkTopDurations[i-1] = s.getChunkTopDurations[i-1], s.getChunkTopDurations[i]
		} else {
			break
		}
	}

	// Keep only top 3
	if len(s.getChunkTopDurations) > 3 {
		s.getChunkTopDurations = s.getChunkTopDurations[:3]
	}
}

// GetTopGetChunkDurations returns a copy of top 3 longest GetChunk job durations
func (s *JobStats) GetTopGetChunkDurations() []*GetChunkJobDuration {
	s.getChunkDurationMu.Lock()
	defer s.getChunkDurationMu.Unlock()

	result := make([]*GetChunkJobDuration, len(s.getChunkTopDurations))
	copy(result, s.getChunkTopDurations)
	return result
}

// ResetTopGetChunkDurations resets the top durations (called after printing)
func (s *JobStats) ResetTopGetChunkDurations() {
	s.getChunkDurationMu.Lock()
	defer s.getChunkDurationMu.Unlock()
	s.getChunkTopDurations = nil
}

// RecordWriteObjectDuration records a WriteObject job duration and keeps top 3 longest
func (s *JobStats) RecordWriteObjectDuration(objectName string, size int64, duration time.Duration) {
	s.writeObjectDurationMu.Lock()
	defer s.writeObjectDurationMu.Unlock()

	newEntry := &WriteObjectJobDuration{
		ObjectName: objectName,
		Size:       size,
		Duration:   duration,
	}

	// Add new entry
	s.writeObjectTopDurations = append(s.writeObjectTopDurations, newEntry)

	// Sort by duration descending
	for i := len(s.writeObjectTopDurations) - 1; i > 0; i-- {
		if s.writeObjectTopDurations[i].Duration > s.writeObjectTopDurations[i-1].Duration {
			s.writeObjectTopDurations[i], s.writeObjectTopDurations[i-1] = s.writeObjectTopDurations[i-1], s.writeObjectTopDurations[i]
		} else {
			break
		}
	}

	// Keep only top 3
	if len(s.writeObjectTopDurations) > 3 {
		s.writeObjectTopDurations = s.writeObjectTopDurations[:3]
	}
}

// GetTopWriteObjectDurations returns a copy of top 3 longest WriteObject job durations
func (s *JobStats) GetTopWriteObjectDurations() []*WriteObjectJobDuration {
	s.writeObjectDurationMu.Lock()
	defer s.writeObjectDurationMu.Unlock()

	result := make([]*WriteObjectJobDuration, len(s.writeObjectTopDurations))
	copy(result, s.writeObjectTopDurations)
	return result
}

// ResetTopWriteObjectDurations resets the top durations (called after printing)
func (s *JobStats) ResetTopWriteObjectDurations() {
	s.writeObjectDurationMu.Lock()
	defer s.writeObjectDurationMu.Unlock()
	s.writeObjectTopDurations = nil
}

type Worker interface {
	Submit(taskID string, lsn uint64, state int8) error
	Stop()
	// RegisterSyncProtection registers a sync protection job for keepalive
	RegisterSyncProtection(jobID string, ttlExpireTS int64)
	// UnregisterSyncProtection unregisters a sync protection job
	UnregisterSyncProtection(jobID string)
	// GetSyncProtectionTTL returns the current TTL expiration timestamp for a job
	GetSyncProtectionTTL(jobID string) int64
}

// FilterObjectWorker is the interface for filter object worker pool
type FilterObjectWorker interface {
	SubmitFilterObject(job Job) error
	Stop()
}

// GetChunkWorker is the interface for get upstream chunk worker pool
type GetChunkWorker interface {
	SubmitGetChunk(job Job) error
	Stop()
}

// WriteObjectWorker is the interface for write object worker pool
type WriteObjectWorker interface {
	SubmitWriteObject(job Job) error
	Stop()
}

// GetChunkJobInfo is the interface for jobs that have object name and chunk index
type GetChunkJobInfo interface {
	GetObjectName() string
	GetChunkIndex() int64
}

// WriteObjectJobInfo is the interface for jobs that have object name and size
type WriteObjectJobInfo interface {
	GetObjectName() string
	GetObjectSize() int64
}

// syncProtectionEntry represents a registered sync protection job
type syncProtectionEntry struct {
	jobID       string
	ttlExpireTS atomic.Int64
}

type worker struct {
	cnUUID                   string
	cnEngine                 engine.Engine
	cnTxnClient              client.TxnClient
	mp                       *mpool.MPool
	upstreamSQLHelperFactory UpstreamSQLHelperFactory
	taskChan                 chan *TaskContext
	wg                       sync.WaitGroup
	cancel                   context.CancelFunc
	ctx                      context.Context
	closed                   atomic.Bool
	filterObjectWorker       FilterObjectWorker
	getChunkWorker           GetChunkWorker
	writeObjectWorker        WriteObjectWorker

	// Sync protection keepalive management
	syncProtectionMu      sync.RWMutex
	syncProtectionJobs    map[string]*syncProtectionEntry
	syncProtectionTicker  *time.Ticker
	syncProtectionStarted atomic.Bool
}

type TaskContext struct {
	TaskID string
	LSN    uint64
}

func NewWorker(
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	mp *mpool.MPool,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
) Worker {
	worker := &worker{
		cnUUID:                   cnUUID,
		cnEngine:                 cnEngine,
		cnTxnClient:              cnTxnClient,
		taskChan:                 make(chan *TaskContext, 10000),
		mp:                       mp,
		upstreamSQLHelperFactory: upstreamSQLHelperFactory,
		filterObjectWorker:       NewFilterObjectWorker(),
		getChunkWorker:           NewGetChunkWorker(),
		writeObjectWorker:        NewWriteObjectWorker(),
		syncProtectionJobs:       make(map[string]*syncProtectionEntry),
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	go worker.Run()
	go worker.RunStatsPrinter()
	go worker.RunSyncProtectionKeepAlive()
	return worker
}

// RunStatsPrinter prints job stats every 10 seconds
func (w *worker) RunStatsPrinter() {
	ticker := time.NewTicker(StatsPrintInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			stats := GetJobStats()
			logutil.Info("ccpr-worker-stats",
				zap.Int64("filter_object_pending", stats.FilterObjectPending.Load()),
				zap.Int64("filter_object_running", stats.FilterObjectRunning.Load()),
				zap.Int64("filter_object_completed", stats.FilterObjectCompleted.Load()),
				zap.Int64("get_chunk_pending", stats.GetChunkPending.Load()),
				zap.Int64("get_chunk_running", stats.GetChunkRunning.Load()),
				zap.Int64("get_chunk_completed", stats.GetChunkCompleted.Load()),
				zap.Int64("get_meta_pending", stats.GetMetaPending.Load()),
				zap.Int64("get_meta_running", stats.GetMetaRunning.Load()),
				zap.Int64("get_meta_completed", stats.GetMetaCompleted.Load()),
				zap.Int64("write_object_pending", stats.WriteObjectPending.Load()),
				zap.Int64("write_object_running", stats.WriteObjectRunning.Load()),
				zap.Int64("write_object_completed", stats.WriteObjectCompleted.Load()),
			)

			// Print top 3 longest GetChunk jobs
			topDurations := stats.GetTopGetChunkDurations()
			if len(topDurations) > 0 {
				fields := make([]zap.Field, 0, len(topDurations)*3)
				for i, d := range topDurations {
					idx := i + 1
					fields = append(fields,
						zap.String(fmt.Sprintf("object_name_%d", idx), d.ObjectName),
						zap.Int64(fmt.Sprintf("chunk_index_%d", idx), d.ChunkIndex),
						zap.Duration(fmt.Sprintf("duration_%d", idx), d.Duration),
					)
				}
				logutil.Info("ccpr-worker-stats-top3-get-chunk-duration", fields...)
			}

			// Print top 3 longest WriteObject jobs
			topWriteDurations := stats.GetTopWriteObjectDurations()
			if len(topWriteDurations) > 0 {
				fields := make([]zap.Field, 0, len(topWriteDurations)*3)
				for i, d := range topWriteDurations {
					idx := i + 1
					fields = append(fields,
						zap.String(fmt.Sprintf("object_name_%d", idx), d.ObjectName),
						zap.Int64(fmt.Sprintf("size_%d", idx), d.Size),
						zap.Duration(fmt.Sprintf("duration_%d", idx), d.Duration),
					)
				}
				logutil.Info("ccpr-worker-stats-top3-write-object-duration", fields...)
			}

			// Reset top durations for next interval
			stats.ResetTopGetChunkDurations()
			stats.ResetTopWriteObjectDurations()
		}
	}
}

func (w *worker) Run() {
	for i := 0; i < PublicationWorkerThread; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case task := <-w.taskChan:
					w.onItem(task)
				}
			}
		}()
	}
}

func (w *worker) Submit(taskID string, lsn uint64, state int8) error {
	if w.closed.Load() {
		return moerr.NewInternalError(context.Background(), "Publication-Worker is closed")
	}
	w.taskChan <- &TaskContext{
		TaskID: taskID,
		LSN:    lsn,
	}
	return nil
}

func (w *worker) onItem(taskCtx *TaskContext) {
	// Create retry option for executor operations
	executorRetryOpt := &ExecutorRetryOption{
		RetryTimes:    SubmitRetryTimes,
		RetryInterval: DefaultRetryInterval,
		RetryDuration: SubmitRetryDuration,
	}

	err := retryPublication(
		w.ctx,
		func() error {
			// Ensure ccpr state is set to pending before executing iteration
			if err := w.updateIterationState(w.ctx, taskCtx.TaskID, IterationStateRunning); err != nil {
				return err
			}
			return nil
		},
		executorRetryOpt,
	)
	if err != nil {
		logutil.Error(
			"Publication-Task update iteration state to running failed",
			zap.String("taskID", taskCtx.TaskID),
			zap.Uint64("lsn", taskCtx.LSN),
			zap.Error(err),
		)
		return
	}
	err = ExecuteIteration(
		w.ctx,
		w.cnUUID,
		w.cnEngine,
		w.cnTxnClient,
		taskCtx.TaskID,
		taskCtx.LSN,
		w.upstreamSQLHelperFactory,
		w.mp,
		nil, // utHelper
		0,   // snapshotFlushInterval (use default 1min)
		w.filterObjectWorker,
		w.getChunkWorker,
		w.writeObjectWorker,
		w,   // syncProtectionWorker (pass worker itself for keepalive management)
		nil, // syncProtectionRetryOpt (use default: 1s initial, x2 backoff, 5min max)
		nil, // sqlExecutorRetryOpt (use default)
	)
	// Task failure is usually caused by CN UUID or LSN validation errors.
	// The state will be reset by another CN node.
	if err != nil {
		logutil.Error(
			"Publication-Task execute iteration failed",
			zap.String("taskID", taskCtx.TaskID),
			zap.Uint64("lsn", taskCtx.LSN),
			zap.Error(err),
		)
	}
}

func (w *worker) Stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.taskChan)
	if w.filterObjectWorker != nil {
		w.filterObjectWorker.Stop()
	}
	if w.getChunkWorker != nil {
		w.getChunkWorker.Stop()
	}
	if w.writeObjectWorker != nil {
		w.writeObjectWorker.Stop()
	}
	// Stop sync protection ticker
	if w.syncProtectionTicker != nil {
		w.syncProtectionTicker.Stop()
	}
}

// ============================================================================
// Sync Protection KeepAlive Management
// ============================================================================

// RunSyncProtectionKeepAlive runs a single goroutine that manages keepalive for all registered sync protection jobs
func (w *worker) RunSyncProtectionKeepAlive() {
	w.syncProtectionTicker = time.NewTicker(SyncProtectionRenewInterval)
	w.syncProtectionStarted.Store(true)
	defer w.syncProtectionTicker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			logutil.Info("ccpr-worker sync protection keepalive stopped due to context cancellation")
			return
		case <-w.syncProtectionTicker.C:
			w.renewAllSyncProtections()
		}
	}
}

// renewAllSyncProtections renews TTL for all registered sync protection jobs
func (w *worker) renewAllSyncProtections() {
	w.syncProtectionMu.RLock()
	jobs := make([]*syncProtectionEntry, 0, len(w.syncProtectionJobs))
	for _, job := range w.syncProtectionJobs {
		jobs = append(jobs, job)
	}
	w.syncProtectionMu.RUnlock()

	if len(jobs) == 0 {
		return
	}

	// Create an internal SQL executor for renewal operations
	executor, err := NewInternalSQLExecutor(
		w.cnUUID,
		w.cnTxnClient,
		w.cnEngine,
		catalog.System_Account,
		&SQLExecutorRetryOption{
			MaxRetries:    DefaultSQLExecutorRetryOption().MaxRetries,
			RetryInterval: DefaultSQLExecutorRetryOption().RetryInterval,
			Classifier:    NewDownstreamCommitClassifier(),
		},
		true,
	)
	if err != nil {
		logutil.Warn("ccpr-worker failed to create executor for sync protection renewal",
			zap.Error(err),
		)
		return
	}
	defer executor.Close()

	var jobsToRemove []string

	for _, job := range jobs {
		newTTLExpireTS := time.Now().UnixNano()
		err := w.renewSyncProtectionWithRetry(executor, job.jobID, newTTLExpireTS)
		if err != nil {
			// Handle specific errors - non-retryable errors
			if IsSyncProtectionNotFoundError(err) || IsSyncProtectionSoftDeleteError(err) ||
				IsSyncProtectionInvalidError(err) {
				// Job no longer exists or invalid, remove from worker
				logutil.Warn("ccpr-worker sync protection not found/soft deleted/invalid, removing from keepalive",
					zap.String("job_id", job.jobID),
					zap.Error(err),
				)
				jobsToRemove = append(jobsToRemove, job.jobID)
			} else {
				// Retryable errors (GC running, max count) were already retried
				// Just log and continue, will retry in next interval
				logutil.Warn("ccpr-worker sync protection renew failed after retries",
					zap.String("job_id", job.jobID),
					zap.Error(err),
				)
			}
		} else {
			job.ttlExpireTS.Store(newTTLExpireTS)
			logutil.Debug("ccpr-worker sync protection renewed",
				zap.String("job_id", job.jobID),
				zap.Int64("new_ttl_expire_ts", newTTLExpireTS),
			)
		}
	}

	// Remove jobs that are no longer valid
	if len(jobsToRemove) > 0 {
		w.syncProtectionMu.Lock()
		for _, jobID := range jobsToRemove {
			delete(w.syncProtectionJobs, jobID)
			logutil.Info("ccpr-worker removed invalid sync protection job",
				zap.String("job_id", jobID),
			)
		}
		w.syncProtectionMu.Unlock()
	}
}

// renewSyncProtectionWithRetry renews sync protection with retry for retryable errors
// Retryable errors: GC is running, max count reached
// Non-retryable errors: not found, soft deleted, invalid
func (w *worker) renewSyncProtectionWithRetry(executor SQLExecutor, jobID string, newTTLExpireTS int64) error {
	const maxRetries = 3
	const retryInterval = 10 * time.Second

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := RenewSyncProtection(w.ctx, executor, jobID, newTTLExpireTS)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if IsGCRunningError(err) || IsSyncProtectionMaxCountError(err) {
			logutil.Warn("ccpr-worker sync protection renew retryable error, will retry",
				zap.String("job_id", jobID),
				zap.Int("attempt", i+1),
				zap.Int("max_retries", maxRetries),
				zap.Error(err),
			)
			// Wait before retry
			select {
			case <-w.ctx.Done():
				return w.ctx.Err()
			case <-time.After(retryInterval):
			}
			continue
		}

		// Non-retryable error, return immediately
		return err
	}

	return lastErr
}

// RegisterSyncProtection registers a sync protection job for keepalive
func (w *worker) RegisterSyncProtection(jobID string, ttlExpireTS int64) {
	w.syncProtectionMu.Lock()
	defer w.syncProtectionMu.Unlock()

	entry := &syncProtectionEntry{
		jobID: jobID,
	}
	entry.ttlExpireTS.Store(ttlExpireTS)
	w.syncProtectionJobs[jobID] = entry

	logutil.Info("ccpr-worker registered sync protection",
		zap.String("job_id", jobID),
		zap.Int64("ttl_expire_ts", ttlExpireTS),
	)
}

// UnregisterSyncProtection unregisters a sync protection job
func (w *worker) UnregisterSyncProtection(jobID string) {
	w.syncProtectionMu.Lock()
	defer w.syncProtectionMu.Unlock()

	delete(w.syncProtectionJobs, jobID)

	logutil.Info("ccpr-worker unregistered sync protection",
		zap.String("job_id", jobID),
	)
}

// GetSyncProtectionTTL returns the current TTL expiration timestamp for a job
func (w *worker) GetSyncProtectionTTL(jobID string) int64 {
	w.syncProtectionMu.RLock()
	defer w.syncProtectionMu.RUnlock()

	if entry, exists := w.syncProtectionJobs[jobID]; exists {
		return entry.ttlExpireTS.Load()
	}
	return 0
}

func (w *worker) updateIterationState(ctx context.Context, taskID string, iterationState int8) error {
	executor, err := NewInternalSQLExecutor(
		w.cnUUID,
		w.cnTxnClient,
		w.cnEngine,
		catalog.System_Account,
		&SQLExecutorRetryOption{
			MaxRetries:    DefaultSQLExecutorRetryOption().MaxRetries,
			RetryInterval: DefaultSQLExecutorRetryOption().RetryInterval,
			Classifier:    NewDownstreamCommitClassifier(),
		},
		true,
	)
	if err != nil {
		return err
	}

	updateSQL := PublicationSQLBuilder.UpdateMoCcprLogIterationStateAndCnUuidSQL(
		taskID,
		iterationState,
		w.cnUUID,
	)

	systemCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	result, cancel, err := executor.ExecSQL(systemCtx, nil, catalog.System_Account, updateSQL, false, false, time.Minute)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to update iteration state to pending: %v", err)
	}
	if result != nil {
		defer result.Close()
	}
	cancel()

	return nil
}

// ============================================================================
// FilterObjectWorker implementation
// ============================================================================

type filterObjectWorker struct {
	jobChan chan Job
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	ctx     context.Context
	closed  atomic.Bool
}

// NewFilterObjectWorker creates a new filter object worker pool
func NewFilterObjectWorker() FilterObjectWorker {
	w := &filterObjectWorker{
		jobChan: make(chan Job, 10000),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	go w.Run()
	return w
}

func (w *filterObjectWorker) Run() {
	for i := 0; i < FilterObjectWorkerThread; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case job := <-w.jobChan:
					globalJobStats.IncrementFilterObjectRunning()
					job.Execute()
					globalJobStats.DecrementFilterObjectRunning()
				}
			}
		}()
	}
}

func (w *filterObjectWorker) SubmitFilterObject(job Job) error {
	if w.closed.Load() {
		return moerr.NewInternalError(context.Background(), "FilterObjectWorker is closed")
	}
	globalJobStats.IncrementFilterObjectPending()
	w.jobChan <- job
	return nil
}

func (w *filterObjectWorker) Stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.jobChan)
}

// ============================================================================
// GetChunkWorker implementation
// ============================================================================

type getChunkWorker struct {
	jobChan chan Job
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	ctx     context.Context
	closed  atomic.Bool
}

// NewGetChunkWorker creates a new get chunk worker pool
func NewGetChunkWorker() GetChunkWorker {
	w := &getChunkWorker{
		jobChan: make(chan Job, 10000),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	go w.Run()
	return w
}

func (w *getChunkWorker) Run() {
	workerThreadCount := GetChunkWorkerThread
	for i := 0; i < workerThreadCount; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case job := <-w.jobChan:
					jobType := job.GetType()
					switch jobType {
					case JobTypeGetMeta:
						globalJobStats.IncrementGetMetaRunning()
						job.Execute()
						globalJobStats.DecrementGetMetaRunning()
					default:
						// JobTypeGetChunk
						globalJobStats.IncrementGetChunkRunning()
						startTime := time.Now()
						job.Execute()
						duration := time.Since(startTime)
						globalJobStats.DecrementGetChunkRunning()

						// Record duration for GetChunk jobs
						if chunkJobInfo, ok := job.(GetChunkJobInfo); ok {
							globalJobStats.RecordGetChunkDuration(
								chunkJobInfo.GetObjectName(),
								chunkJobInfo.GetChunkIndex(),
								duration,
							)
						}
					}
				}
			}
		}()
	}
}

func (w *getChunkWorker) SubmitGetChunk(job Job) error {
	if w.closed.Load() {
		return moerr.NewInternalError(context.Background(), "GetChunkWorker is closed")
	}
	jobType := job.GetType()
	switch jobType {
	case JobTypeGetMeta:
		globalJobStats.IncrementGetMetaPending()
	default:
		globalJobStats.IncrementGetChunkPending()
	}
	w.jobChan <- job
	return nil
}

func (w *getChunkWorker) Stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.jobChan)
}

// ============================================================================
// simpleJobWorker - generic worker implementation for reuse
// ============================================================================

type simpleJobWorker struct {
	name             string
	jobChan          chan Job
	wg               sync.WaitGroup
	cancel           context.CancelFunc
	ctx              context.Context
	closed           atomic.Bool
	onPending        func()
	onRunningStart   func()
	onRunningFinish  func()
	onRecordDuration func(job Job, duration time.Duration)
}

// newSimpleJobWorker creates a new simple job worker pool
func newSimpleJobWorker(
	name string,
	threadCount int,
	onPending func(),
	onRunningStart func(),
	onRunningFinish func(),
	onRecordDuration func(job Job, duration time.Duration),
) *simpleJobWorker {
	w := &simpleJobWorker{
		name:             name,
		jobChan:          make(chan Job, 10000),
		onPending:        onPending,
		onRunningStart:   onRunningStart,
		onRunningFinish:  onRunningFinish,
		onRecordDuration: onRecordDuration,
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	go w.run(threadCount)
	return w
}

func (w *simpleJobWorker) run(threadCount int) {
	for i := 0; i < threadCount; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case job := <-w.jobChan:
					w.onRunningStart()
					startTime := time.Now()
					job.Execute()
					duration := time.Since(startTime)
					w.onRunningFinish()
					if w.onRecordDuration != nil {
						w.onRecordDuration(job, duration)
					}
				}
			}
		}()
	}
}

func (w *simpleJobWorker) submit(job Job) error {
	if w.closed.Load() {
		return moerr.NewInternalErrorf(context.Background(), "%s is closed", w.name)
	}
	w.onPending()
	w.jobChan <- job
	return nil
}

func (w *simpleJobWorker) stop() {
	w.closed.Store(true)
	w.cancel()
	w.wg.Wait()
	close(w.jobChan)
}

// ============================================================================
// WriteObjectWorker implementation using simpleJobWorker
// ============================================================================

type writeObjectWorker struct {
	*simpleJobWorker
}

// NewWriteObjectWorker creates a new write object worker pool
func NewWriteObjectWorker() WriteObjectWorker {
	return &writeObjectWorker{
		simpleJobWorker: newSimpleJobWorker(
			"WriteObjectWorker",
			WriteObjectWorkerThread,
			globalJobStats.IncrementWriteObjectPending,
			globalJobStats.IncrementWriteObjectRunning,
			globalJobStats.DecrementWriteObjectRunning,
			func(job Job, duration time.Duration) {
				if writeJobInfo, ok := job.(WriteObjectJobInfo); ok {
					globalJobStats.RecordWriteObjectDuration(
						writeJobInfo.GetObjectName(),
						writeJobInfo.GetObjectSize(),
						duration,
					)
				}
			},
		),
	}
}

func (w *writeObjectWorker) SubmitWriteObject(job Job) error {
	return w.submit(job)
}

func (w *writeObjectWorker) Stop() {
	w.stop()
}
