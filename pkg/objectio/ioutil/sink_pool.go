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

package ioutil

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

// SinkPool is a global worker pool for parallelizing Sinker serialization
// (Sink) and IO (Sync) operations. All Sinkers with pipeline mode enabled
// share the same pool, so concurrent queries do not multiply memory usage.
//
// Architecture:
//
//	Main goroutine → [sinkChan] → N sink workers → [syncChan] → M sync workers
//
// Memory is bounded globally:
//
//	(sinkChan cap + N) × 128MB raw + (syncChan cap + M) × compressed size
type SinkPool struct {
	sinkChan chan *poolSinkJob
	syncChan chan *poolSyncJob

	sinkWg sync.WaitGroup
	syncWg sync.WaitGroup

	done    chan struct{}
	closeMu sync.RWMutex // protects sinkChan from concurrent Send+Close
}

type poolSinkJob struct {
	data    []*batch.Batch
	factory FileSinkerFactory
	mp      *mpool.MPool
	fs      fileservice.FileService
	result  *pipelineResult
}

type poolSyncJob struct {
	fSinker FileSinker
	result  *pipelineResult
}

// pipelineResult tracks accumulated results and errors for one Sinker's
// pipeline operations. Multiple jobs from the same Sinker share one result.
type pipelineResult struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	persisted []objectio.ObjectStats
	err       error

	pending sync.WaitGroup // tracks in-flight jobs for this Sinker

	sinkNs int64 // atomic, total serialization time across workers
	syncNs int64 // atomic, total IO time across workers
}

var (
	defaultPool     *SinkPool
	defaultPoolOnce sync.Once
)

// GetDefaultSinkPool returns the process-global sink pool, creating it
// lazily on first call. Sink workers = min(NumCPU, 8), sync workers = 2.
func GetDefaultSinkPool() *SinkPool {
	defaultPoolOnce.Do(func() {
		sinkWorkers := runtime.NumCPU()
		if sinkWorkers > 8 {
			sinkWorkers = 8
		}
		if sinkWorkers < 2 {
			sinkWorkers = 2
		}
		defaultPool = NewSinkPool(sinkWorkers, 2)
	})
	return defaultPool
}

// NewSinkPool creates a new worker pool with the given number of
// serialization (sink) workers and IO (sync) workers.
func NewSinkPool(sinkWorkers, syncWorkers int) *SinkPool {
	pool := &SinkPool{
		sinkChan: make(chan *poolSinkJob, sinkWorkers),
		syncChan: make(chan *poolSyncJob, syncWorkers),
		done:     make(chan struct{}),
	}
	for i := 0; i < sinkWorkers; i++ {
		pool.sinkWg.Add(1)
		go pool.runSinkWorker()
	}
	for i := 0; i < syncWorkers; i++ {
		pool.syncWg.Add(1)
		go pool.runSyncWorker()
	}
	return pool
}

func (p *SinkPool) runSinkWorker() {
	defer p.sinkWg.Done()
	for job := range p.sinkChan {
		r := job.result
		if r.hasError() {
			freeBatches(job.data, job.mp)
			r.pending.Done()
			continue
		}

		sinkStart := time.Now()
		fSinker := job.factory(job.mp, job.fs)
		var sinkErr error
		for _, bat := range job.data {
			if err := fSinker.Sink(r.ctx, bat); err != nil {
				sinkErr = err
				break
			}
		}
		atomic.AddInt64(&r.sinkNs, int64(time.Since(sinkStart)))
		freeBatches(job.data, job.mp)

		if sinkErr != nil {
			fSinker.Close()
			r.setError(sinkErr)
			r.pending.Done()
			continue
		}

		// Forward to sync worker. Do NOT call r.pending.Done() here;
		// the sync worker calls it after completing the IO.
		select {
		case p.syncChan <- &poolSyncJob{fSinker: fSinker, result: r}:
		case <-r.ctx.Done():
			fSinker.Close()
			r.setError(context.Cause(r.ctx))
			r.pending.Done()
		case <-p.done:
			fSinker.Close()
			r.setError(context.Canceled)
			r.pending.Done()
		}
	}
}

func (p *SinkPool) runSyncWorker() {
	defer p.syncWg.Done()
	for job := range p.syncChan {
		r := job.result
		if r.hasError() {
			job.fSinker.Close()
			r.pending.Done()
			continue
		}

		syncStart := time.Now()
		stats, err := job.fSinker.Sync(r.ctx)
		atomic.AddInt64(&r.syncNs, int64(time.Since(syncStart)))
		job.fSinker.Close()

		if err != nil {
			r.setError(err)
		} else {
			r.mu.Lock()
			r.persisted = append(r.persisted, *stats)
			r.mu.Unlock()
		}
		r.pending.Done()
	}
}

// Submit enqueues a sink job for async processing. The job's result.pending
// WaitGroup is incremented; it will be decremented when the job fully
// completes (after both Sink and Sync).
func (p *SinkPool) Submit(job *poolSinkJob) error {
	r := job.result
	if err := r.getError(); err != nil {
		freeBatches(job.data, job.mp)
		return err
	}
	r.pending.Add(1)

	p.closeMu.RLock()
	defer p.closeMu.RUnlock()

	// Fast path: if the pool is already closed (p.done is closed), sinkChan
	// may also be closed. Return immediately to avoid send-on-closed-channel.
	// This check under RLock is safe because Close() cannot close sinkChan
	// while any RLock is held.
	select {
	case <-p.done:
		r.pending.Done()
		freeBatches(job.data, job.mp)
		return context.Canceled
	default:
	}

	// Fast path: if the pipeline context is already cancelled, don't enqueue.
	select {
	case <-r.ctx.Done():
		r.pending.Done()
		freeBatches(job.data, job.mp)
		if err := r.getError(); err != nil {
			return err
		}
		return context.Cause(r.ctx)
	default:
	}

	select {
	case p.sinkChan <- job:
		return nil
	case <-r.ctx.Done():
		r.pending.Done()
		freeBatches(job.data, job.mp)
		if err := r.getError(); err != nil {
			return err
		}
		return context.Cause(r.ctx)
	case <-p.done:
		r.pending.Done()
		freeBatches(job.data, job.mp)
		return context.Canceled
	}
}

// Close shuts down the pool, waiting for all workers to finish.
func (p *SinkPool) Close() {
	close(p.done)
	// Wait for all in-flight Submit calls to finish before closing sinkChan,
	// preventing send-on-closed-channel panics.
	p.closeMu.Lock()
	close(p.sinkChan)
	p.closeMu.Unlock()
	p.sinkWg.Wait()
	close(p.syncChan)
	p.syncWg.Wait()
}

func (r *pipelineResult) hasError() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err != nil
}

func (r *pipelineResult) getError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *pipelineResult) setError(err error) {
	r.mu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.mu.Unlock()
	r.cancel()
}

func freeBatches(batches []*batch.Batch, mp *mpool.MPool) {
	for _, bat := range batches {
		if bat != nil {
			bat.Clean(mp)
		}
	}
}
