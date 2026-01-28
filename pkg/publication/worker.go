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
	FilterObjectWorkerThread = 100
	GetChunkWorkerThread     = 1000

	SubmitRetryTimes    = 1000
	SubmitRetryDuration = time.Hour

	StatsPrintInterval = 10 * time.Second
)

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

type Worker interface {
	Submit(taskID uint64, lsn uint64, state int8) error
	Stop()
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
}

type TaskContext struct {
	TaskID uint64
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
	}
	worker.ctx, worker.cancel = context.WithCancel(context.Background())
	go worker.Run()
	go worker.RunStatsPrinter()
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
			)
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

func (w *worker) Submit(taskID uint64, lsn uint64, state int8) error {
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
			zap.Uint64("taskID", taskCtx.TaskID),
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
		nil, // sqlExecutorRetryOpt (use default)
	)
	// Task failure is usually caused by CN UUID or LSN validation errors.
	// The state will be reset by another CN node.
	if err != nil {
		logutil.Error(
			"Publication-Task execute iteration failed",
			zap.Uint64("taskID", taskCtx.TaskID),
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
}

func (w *worker) updateIterationState(ctx context.Context, taskID uint64, iterationState int8) error {
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
	result, err := executor.ExecSQL(systemCtx, nil, updateSQL, false, false, 0)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "failed to update iteration state to pending: %v", err)
	}
	if result != nil {
		defer result.Close()
	}

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
	for i := 0; i < GetChunkWorkerThread; i++ {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				select {
				case <-w.ctx.Done():
					return
				case job := <-w.jobChan:
					jobType := job.GetType()
					if jobType == JobTypeGetMeta {
						globalJobStats.IncrementGetMetaRunning()
						job.Execute()
						globalJobStats.DecrementGetMetaRunning()
					} else {
						globalJobStats.IncrementGetChunkRunning()
						job.Execute()
						globalJobStats.DecrementGetChunkRunning()
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
	if jobType == JobTypeGetMeta {
		globalJobStats.IncrementGetMetaPending()
	} else {
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
