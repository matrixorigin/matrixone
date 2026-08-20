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
	"bytes"
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
	diskCacheAsyncQueueSize       = 64
	diskCacheAsyncMaxPendingBytes = int64(64 << 20)
	diskCacheWriteErrorLogKey     = "disk-cache-write-error"
	diskCacheWriteRecoveryLogKey  = "disk-cache-write-recovery"
	diskCacheWriteErrorLogPeriod  = time.Minute
)

type diskCacheAsyncUpdate struct {
	filePath    string
	diskPath    string
	entry       IOEntry
	callbacks   []OnDiskCacheWrittenFunc
	finalize    *diskCacheAsyncFileFinalize
	releaseOnce sync.Once
}

type diskCacheAsyncFileFinalize struct {
	file       *os.File
	doneUpdate func() error
	attempted  bool
	err        error
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
		closed          bool
		pending         map[string]struct{}
		pendingBytes    int64
		maxPendingBytes int64
		dropped         int64
		idle            chan struct{}
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
	d.async.mu.maxPendingBytes = diskCacheAsyncMaxPendingBytes
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

func (d *DiskCache) scheduleAsyncUpdate(
	filePath string,
	diskPath string,
	entry IOEntry,
	callbacks []OnDiskCacheWrittenFunc,
) {
	dataBytes := int64(len(entry.Data))
	a := &d.async
	a.mu.Lock()
	if a.mu.closed {
		a.dropAsyncUpdateLocked()
		a.mu.Unlock()
		return
	}
	if _, ok := a.mu.pending[diskPath]; ok {
		// FileService objects are immutable. One pending write for the same
		// canonical range is sufficient and avoids copying duplicate data.
		a.mu.Unlock()
		return
	}
	select {
	case a.slots <- struct{}{}:
	default:
		a.dropAsyncUpdateLocked()
		a.mu.Unlock()
		return
	}
	if dataBytes > a.mu.maxPendingBytes-a.mu.pendingBytes {
		<-a.slots
		a.dropAsyncUpdateLocked()
		a.mu.Unlock()
		return
	}
	if len(a.mu.pending) == 0 {
		a.mu.idle = make(chan struct{})
	}
	a.mu.pending[diskPath] = struct{}{}
	a.mu.pendingBytes += dataBytes
	a.submitters.Add(1)
	a.mu.Unlock()

	a.startOnce.Do(func() {
		go d.runAsyncUpdates()
	})

	job := &diskCacheAsyncUpdate{
		filePath: filePath,
		diskPath: diskPath,
		entry: IOEntry{
			Offset: entry.Offset,
			Size:   entry.Size,
			Data:   bytes.Clone(entry.Data),
		},
		callbacks: append([]OnDiskCacheWrittenFunc(nil), callbacks...),
	}
	select {
	case a.jobs <- job:
	case <-a.ctx.Done():
		d.releaseAsyncUpdate(job)
	}
	a.submitters.Done()
}

func (d *DiskCache) tryReserveAsyncFileFinalize() bool {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu.closed {
		a.dropAsyncUpdateLocked()
		return false
	}
	select {
	case a.slots <- struct{}{}:
		return true
	default:
		a.dropAsyncUpdateLocked()
		return false
	}
}

func (d *DiskCache) releaseAsyncFileFinalizeReservation() {
	<-d.async.slots
}

func (d *DiskCache) scheduleReservedAsyncFileFinalize(
	diskPath string,
	file *os.File,
	doneUpdate func() error,
) bool {
	a := &d.async
	a.mu.Lock()
	if a.mu.closed {
		a.dropAsyncUpdateLocked()
		a.mu.Unlock()
		return false
	}
	if _, ok := a.mu.pending[diskPath]; ok {
		a.mu.Unlock()
		return false
	}
	if len(a.mu.pending) == 0 {
		a.mu.idle = make(chan struct{})
	}
	a.mu.pending[diskPath] = struct{}{}
	a.submitters.Add(1)
	a.mu.Unlock()

	a.startOnce.Do(func() {
		go d.runAsyncUpdates()
	})

	job := &diskCacheAsyncUpdate{
		diskPath: diskPath,
		finalize: &diskCacheAsyncFileFinalize{
			file:       file,
			doneUpdate: doneUpdate,
		},
	}
	select {
	case a.jobs <- job:
	case <-a.ctx.Done():
		d.releaseAsyncUpdate(job)
	}
	a.submitters.Done()
	return true
}

func (a *diskCacheAsyncState) dropAsyncUpdateLocked() {
	a.mu.dropped++
	metric.FSDiskCacheAsyncUpdateDroppedCounter.Inc()
}

func (d *DiskCache) runAsyncUpdates() {
	defer close(d.async.done)
	for {
		select {
		case <-d.async.ctx.Done():
			return
		case job := <-d.async.jobs:
			if d.async.ctx.Err() == nil {
				if job.finalize != nil {
					job.finalize.attempted = true
					job.finalize.err = d.finalizeFile(
						d.async.ctx,
						job.diskPath,
						job.finalize.file,
					)
					if isDiskFull(job.finalize.err) {
						d.cache.ForceEvict(d.async.ctx, d.capacityFunc()/10)
					}
				} else {
					_ = d.updateEntry(
						d.async.ctx,
						job.filePath,
						job.diskPath,
						job.entry,
						job.callbacks,
					)
				}
			}
			d.releaseAsyncUpdate(job)
		}
	}
}

func (d *DiskCache) releaseAsyncUpdate(job *diskCacheAsyncUpdate) {
	job.releaseOnce.Do(func() {
		if job.finalize != nil {
			cleanupErr := errors.Join(
				cleanupDiskCacheTempFile(job.finalize.file),
				job.finalize.doneUpdate(),
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
		}
		a := &d.async
		a.mu.Lock()
		delete(a.mu.pending, job.diskPath)
		a.mu.pendingBytes -= int64(len(job.entry.Data))
		<-a.slots
		job.entry.Data = nil
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
