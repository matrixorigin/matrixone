// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	// diskCacheAsyncQueueSize is the most asynchronous updates pending at once, each
	// holding an open file.
	diskCacheAsyncQueueSize = 512
	// diskCacheAsyncWorkers is the number of asynchronous updates published at once.
	diskCacheAsyncWorkers        = 4
	diskCacheWriteErrorLogKey    = "disk-cache-write-error"
	diskCacheWriteRecoveryLogKey = "disk-cache-write-recovery"
	diskCacheWriteErrorLogPeriod = time.Minute
)

type diskCacheAsyncUpdate struct {
	diskPath    string
	finalize    *diskCacheAsyncFileFinalize
	releaseOnce sync.Once
}

// pendingKey returns the key of the async publication of the file at diskPath into
// target: the publications of an object into the disk cache and into the index-file cache
// are pending apart.
func pendingKey(target fillTarget, diskPath string) string {
	if t, ok := target.(indexFillTarget); ok {
		return "index-file\x00" + t.name
	}
	return diskPath
}

// pendingKey returns the key its publication is pending under.
func (j *diskCacheAsyncUpdate) pendingKey() string {
	return pendingKey(j.finalize.target, j.diskPath)
}

type diskCacheAsyncFileFinalize struct {
	file   *os.File
	target fillTarget
	owner  *pathOwner
	// deletes, when set, is the delete count a whole-object download started at.
	deletes   *uint64
	attempted bool
	err       error
}

type diskCacheAsyncState struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan *diskCacheAsyncUpdate
	slots  chan struct{}
	done   chan struct{}

	startOnce  sync.Once
	closeOnce  sync.Once
	submitters sync.WaitGroup

	mu struct {
		sync.Mutex
		closed  bool
		pending map[string]struct{}
		dropped int64
		idle    chan struct{}
		// freed, when set, is closed once a slot is released.
		freed freedSignal
	}
}

type diskCacheWriteFailureState struct {
	failed          atomic.Bool
	logger          *logutil.RateLimitedLogger
	recoveryLimiter *logutil.EventRateLimiter
}

type diskCacheSourceError struct {
	err error
}

func (e *diskCacheSourceError) Error() string { return e.err.Error() }
func (e *diskCacheSourceError) Unwrap() error { return e.err }

type diskCacheSourceReader struct {
	io.Reader
	err error
}

func (r *diskCacheSourceReader) Read(buf []byte) (int, error) {
	n, err := r.Reader.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}
	return n, err
}

func (d *DiskCache) initAsyncUpdates() {
	d.async.ctx, d.async.cancel = context.WithCancel(context.Background())
	d.async.jobs = make(chan *diskCacheAsyncUpdate, diskCacheAsyncQueueSize)
	d.async.slots = make(chan struct{}, diskCacheAsyncQueueSize)
	d.async.done = make(chan struct{})
	d.async.mu.pending = make(map[string]struct{})
	d.async.mu.idle = make(chan struct{})
	close(d.async.mu.idle)
}

func (d *DiskCache) initWriteFailureState() {
	d.writeFailures.logger = logutil.NewRateLimitedLoggerWithConfig(
		logutil.GetGlobalLogger(),
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)
	d.writeFailures.recoveryLimiter = logutil.NewEventRateLimiter(
		logutil.RateLimitedLoggerConfig{MaxKeys: 1},
	)
}

func (d *DiskCache) observeWriteResult(diskPath string, err error) error {
	if err == nil {
		if d.writeFailures.failed.CompareAndSwap(true, false) {
			decision, ok := d.writeFailures.recoveryLimiter.Allow(
				diskCacheWriteRecoveryLogKey,
				logutil.RateLimitConfig{
					Interval:   diskCacheWriteErrorLogPeriod,
					BurstCount: 1,
				},
			)
			if ok {
				fields := logutil.EventFieldsWithDecision([]zap.Field{
					zap.String("path", diskPath),
					zap.String("cache-path", d.path),
				}, decision)
				d.writeFailures.logger.Logger().Info(
					"disk cache write recovered",
					fields...,
				)
			}
		}
		return nil
	}
	var sourceErr *diskCacheSourceError
	if errors.As(err, &sourceErr) {
		return sourceErr.err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	metric.FSDiskCacheErrorCounter.Inc()
	d.writeFailures.failed.Store(true)
	d.writeFailures.logger.WarnWithConfig(
		diskCacheWriteErrorLogKey,
		"write disk cache error",
		logutil.RateLimitConfig{
			Interval:   diskCacheWriteErrorLogPeriod,
			BurstCount: 1,
		},
		zap.Error(err),
		zap.String("path", diskPath),
		zap.String("cache-path", d.path),
	)
	// Disk cache is an optional acceleration layer. Filesystem failures are
	// observable above but remain fail-open for the storage read/write result.
	return nil
}

func (d *DiskCache) mergeWriteCleanupError(diskPath string, primary *error, cleanupErr error) {
	if cleanupErr == nil {
		return
	}
	var sourceErr *diskCacheSourceError
	if *primary != nil && (errors.As(*primary, &sourceErr) ||
		errors.Is(*primary, context.Canceled) ||
		errors.Is(*primary, context.DeadlineExceeded)) {
		// Preserve the source/control-path contract while still surfacing an
		// independent local cleanup failure.
		_ = d.observeWriteResult(diskPath, cleanupErr)
		return
	}
	*primary = errors.Join(*primary, cleanupErr)
}

func cleanupDiskCacheTempFile(file *os.File) error {
	closeErr := file.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		closeErr = nil
	}
	removeErr := os.Remove(file.Name())
	if os.IsNotExist(removeErr) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}

// reserveAsyncFileFinalize reserves a slot for the async publication pending under key,
// waiting for one to be free. It reports false when the cache is closed, when the
// publication is already pending, when ctx ends first, or once deleted, when set,
// reports true; a delete of an object wakes the reservations waiting.
func (d *DiskCache) reserveAsyncFileFinalize(ctx context.Context, key string, deleted func() bool) bool {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	for {
		if a.mu.closed {
			a.dropAsyncUpdateLocked()
			return false
		}
		if deleted != nil && deleted() {
			return false
		}
		if _, ok := a.mu.pending[key]; ok {
			return false
		}
		if len(a.slots) < cap(a.slots) {
			a.slots <- struct{}{}
			break
		}
		freed := a.mu.freed.waitLocked()
		a.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
		case <-a.ctx.Done():
		}
		a.mu.Lock()
		if ctx.Err() != nil {
			a.dropAsyncUpdateLocked()
			return false
		}
	}
	if len(a.mu.pending) == 0 {
		a.mu.idle = make(chan struct{})
	}
	a.mu.pending[key] = struct{}{}
	// The reservation spans the foreground source copy. Close seals admission
	// under the same mutex before waiting for submitters, so it cannot miss a
	// finalizer that was admitted but has not reached the jobs channel yet.
	a.submitters.Add(1)
	return true
}

func (d *DiskCache) releaseAsyncFileFinalizeReservation(key string) {
	a := &d.async
	a.mu.Lock()
	delete(a.mu.pending, key)
	<-a.slots
	a.wakeFreedLocked()
	if len(a.mu.pending) == 0 {
		close(a.mu.idle)
	}
	a.mu.Unlock()
	a.submitters.Done()
}

func (d *DiskCache) scheduleReservedAsyncFileFinalize(
	diskPath string,
	file *os.File,
	owner *pathOwner,
	target fillTarget,
	deletes *uint64,
) {
	a := &d.async
	a.startOnce.Do(func() {
		d.startAsyncWorkers()
	})

	job := &diskCacheAsyncUpdate{
		diskPath: diskPath,
		finalize: &diskCacheAsyncFileFinalize{
			file:    file,
			target:  target,
			owner:   owner,
			deletes: deletes,
		},
	}
	select {
	case a.jobs <- job:
	case <-a.ctx.Done():
		// The foreground source copy is complete and ownership has transferred.
		// File close/remove may block, so terminal cleanup must not run in the
		// submitting S3/LocalFS goroutine.
		go d.releaseAsyncUpdate(job)
	}
	a.submitters.Done()
}

func (d *DiskCache) claimAsyncPublication() bool {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	// This mutex is the publication/Close linearization boundary. If Close
	// sealed first, the temp file stays private and is cleaned up. If this claim
	// wins first, publication belongs to the admitted generation; a Close whose
	// context expires may return before that already-claimed operation drains.
	return !a.mu.closed
}

// finalizeQueuedFile publishes a file the async workers finalize. The file of an entry is
// not published when the entry's object was deleted after the entry was read, nor the
// file of a whole-object download when its object was deleted after the download started.
func (d *DiskCache) finalizeQueuedFile(diskPath string, finalize *diskCacheAsyncFileFinalize) error {
	f, target := finalize.file, finalize.target
	var err error
	write := func(claim func() bool) (bool, error) {
		err = d.finalizeFileTo(d.async.ctx, diskPath, f, claim, target)
		return err == nil, err
	}
	var published bool
	if t, ok := target.(entryFillTarget); ok {
		published, _ = d.publishUnlessDeleted(t.filePath, IOEntry{readDeletes: t.readDeletes}, d.claimAsyncPublication, write)
	} else if finalize.deletes != nil {
		published, _ = d.publishUnlessDeletedSince(diskPath, *finalize.deletes, d.claimAsyncPublication, write)
	} else {
		return d.finalizeFileTo(d.async.ctx, diskPath, f, d.claimAsyncPublication, target)
	}
	if !published && errors.Is(err, context.Canceled) && d.async.ctx.Err() == nil {
		// The object was deleted.
		return nil
	}
	return err
}

// wakeFreedLocked wakes the updates waiting for a slot.
func (a *diskCacheAsyncState) wakeFreedLocked() {
	a.mu.freed.notifyLocked()
}

func (a *diskCacheAsyncState) dropAsyncUpdateLocked() {
	a.mu.dropped++
	metric.FSDiskCacheAsyncUpdateDroppedCounter.Inc()
}

// startAsyncWorkers starts the workers that write asynchronous updates; done is closed
// once all of them exit.
func (d *DiskCache) startAsyncWorkers() {
	var workers sync.WaitGroup
	for range diskCacheAsyncWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			d.runAsyncUpdates()
		}()
	}
	go func() {
		workers.Wait()
		close(d.async.done)
	}()
}

func (d *DiskCache) runAsyncUpdates() {
	for {
		select {
		case <-d.async.ctx.Done():
			return
		case job := <-d.async.jobs:
			if d.async.ctx.Err() == nil {
				job.finalize.attempted = true
				job.finalize.err = d.finalizeQueuedFile(job.diskPath, job.finalize)
				full := d
				if t := job.finalize.target; t != nil {
					full = t.cache()
				}
				full.evictForDiskFull(d.async.ctx, job.finalize.err)
			}
			d.releaseAsyncUpdate(job)
		}
	}
}

func (d *DiskCache) releaseAsyncUpdate(job *diskCacheAsyncUpdate) {
	job.releaseOnce.Do(func() {
		cleanupErr := errors.Join(
			cleanupDiskCacheTempFile(job.finalize.file),
			job.finalize.owner.release(),
		)
		if job.finalize.attempted {
			resultErr := job.finalize.err
			d.mergeWriteCleanupError(job.diskPath, &resultErr, cleanupErr)
			_ = d.observeWriteResult(
				job.diskPath,
				resultErr,
			)
		} else if cleanupErr != nil {
			// A finalizer canceled before its first write attempt has not
			// demonstrated recovery. Only surface an actual cleanup failure.
			_ = d.observeWriteResult(job.diskPath, cleanupErr)
		}
		a := &d.async
		a.mu.Lock()
		delete(a.mu.pending, job.pendingKey())
		<-a.slots
		a.wakeFreedLocked()
		if len(a.mu.pending) == 0 {
			close(a.mu.idle)
		}
		a.mu.Unlock()
	})
}

func (d *DiskCache) flushAsyncUpdates(ctx context.Context) {
	d.async.mu.Lock()
	idle := d.async.mu.idle
	d.async.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
	}
}

func (d *DiskCache) closeAsyncUpdates(ctx context.Context) {
	a := &d.async
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.mu.closed = true
		a.mu.Unlock()
		a.cancel()

		// Waiting for submitters and releasing canceled jobs can execute file
		// close/remove cleanup. Keep that work off the Close caller so its
		// context remains a real upper bound even when a filesystem call stalls.
		go func() {
			a.submitters.Wait()
			for {
				select {
				case job := <-a.jobs:
					d.releaseAsyncUpdate(job)
				default:
					a.startOnce.Do(func() {
						close(a.done)
					})
					return
				}
			}
		}()
	})

	d.flushAsyncUpdates(ctx)
	select {
	case <-a.done:
	case <-ctx.Done():
	}
}
