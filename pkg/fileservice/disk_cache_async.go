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
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
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

var eventDiskCacheAsyncCallbackPanic = logutil.Event{
	Name:    "fileservice.disk-cache.async-callback-panic",
	Message: "disk cache async callback panicked",
}

type diskCacheAsyncUpdate struct {
	filePath     string
	diskPath     string
	entry        IOEntry
	callbacks    []OnDiskCacheWrittenFunc
	finalize     *diskCacheAsyncFileFinalize
	releaseOnce  sync.Once
	resourceOnce sync.Once
}

type diskCacheAsyncFileFinalize struct {
	file       *os.File
	doneUpdate func() error
	attempted  bool
	err        error
}

type diskCacheAsyncState struct {
	ctx       context.Context
	cancel    context.CancelFunc
	jobs      chan *diskCacheAsyncUpdate
	slots     chan struct{}
	done      chan struct{}
	blockSize int64 // allocation unit for full-file staging

	startOnce    sync.Once
	closeOnce    sync.Once
	submitters   sync.WaitGroup
	callbackMu   sync.Mutex
	callbackTail chan struct{}

	mu struct {
		sync.Mutex
		closed          bool
		pending         map[string]diskCacheAdmission
		stagedBytes     int64 // full-file temporary disk space
		pendingBytes    int64
		maxPendingBytes int64
		dropped         int64
		idle            chan struct{}
	}
}

// A non-nil done revokes publication and signals when the admission drains.
type diskCacheAdmission struct {
	object      string // canonical object path of a range update
	done        chan struct{}
	stagedBytes int64
}

func (a *diskCacheAsyncState) finishAdmissionLocked(path string) {
	admission := a.mu.pending[path]
	a.mu.stagedBytes -= admission.stagedBytes
	if done := admission.done; done != nil {
		close(done)
	}
	delete(a.mu.pending, path)
}

func (d *DiskCache) diskPathsForDelete(paths []string) map[string]chan struct{} {
	targets := make(map[string]chan struct{}, len(paths))
	objects := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		objects[path] = struct{}{}
		targets[d.pathForFile(path)] = nil
	}
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	for diskPath, admission := range a.mu.pending {
		_, full := targets[diskPath]
		if _, ok := objects[admission.object]; !ok && !full {
			continue
		}
		if admission.done == nil {
			admission.done = make(chan struct{})
			a.mu.pending[diskPath] = admission
		}
		targets[diskPath] = admission.done
	}
	return targets
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

type diskCacheDestinationWriter struct {
	io.Writer
	err error
}

func (w *diskCacheDestinationWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
	}
	return n, err
}

// Each async full-file writer spends its admission's disk reservation before
// writing. If the source outgrows the budget, keep consuming it: a streaming
// Read captures the caller's entries as it goes and must still see source errors.
type diskCacheStagingWriter struct {
	cache   *DiskCache
	path    string
	file    *os.File
	ctx     context.Context
	credit  int64 // reserved space not yet written by this file
	skipped bool
}

func (w *diskCacheStagingWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if w.skipped {
		return len(p), nil
	}
	if need := int64(len(p)) - w.credit; need > 0 {
		granted := w.cache.reserveStagingBytes(w.ctx, w.path, need)
		if granted == 0 {
			if err := w.file.Truncate(0); err != nil {
				return 0, err
			}
			// The rest of a slow source no longer needs its partial file.
			a := &w.cache.async
			a.mu.Lock()
			admission := a.mu.pending[w.path]
			a.mu.stagedBytes -= admission.stagedBytes
			admission.stagedBytes = 0
			a.mu.pending[w.path] = admission
			a.mu.Unlock()
			w.skipped = true
			return len(p), nil
		}
		w.credit += granted
	}
	n, err := w.file.Write(p)
	w.credit -= int64(n)
	return n, err
}

func (d *DiskCache) reserveStagingBytes(ctx context.Context, path string, need int64) int64 {
	a := &d.async
	blocks := 1 + (need-1)/a.blockSize
	capacity := max(int64(0), d.cache.Capacity())
	a.mu.Lock()
	available := capacity - a.mu.stagedBytes
	if a.mu.closed || available <= 0 || blocks > available/a.blockSize {
		a.dropAsyncUpdateLocked()
		a.mu.Unlock()
		return 0
	}
	granted := blocks * a.blockSize
	admission := a.mu.pending[path]
	admission.stagedBytes += granted
	a.mu.pending[path] = admission
	a.mu.stagedBytes += granted
	a.mu.Unlock()

	// Make room among published entries without holding the admission lock
	// across eviction callbacks. Concurrent stagers share the same limit.
	if target := available - granted; d.cache.Used() > target {
		d.cache.EvictToTargetWithWait(ctx, target)
	}
	return granted
}

func (d *DiskCache) sourceError(ctx context.Context, path string, err error) error {
	if (err != context.Canceled && err != context.DeadlineExceeded) || ctx.Err() != err {
		metric.FSDiskCacheErrorCounter.Inc()
		d.writeFailures.logger.WarnWithConfig(diskCacheWriteErrorLogKey, "write disk cache error",
			logutil.RateLimitConfig{Interval: diskCacheWriteErrorLogPeriod, BurstCount: 1},
			zap.Error(err), zap.String("path", path))
	}
	return &diskCacheSourceError{err: err}
}

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
	d.async.blockSize = int64(os.Getpagesize())
	var stat syscall.Statfs_t
	if err := syscall.Statfs(d.path, &stat); err == nil && stat.Bsize > 0 {
		d.async.blockSize = int64(stat.Bsize)
	}
	d.async.ctx, d.async.cancel = context.WithCancel(context.Background())
	d.async.jobs = make(chan *diskCacheAsyncUpdate, diskCacheAsyncQueueSize)
	d.async.slots = make(chan struct{}, diskCacheAsyncQueueSize)
	d.async.done = make(chan struct{})
	d.async.mu.pending = make(map[string]diskCacheAdmission)
	d.async.mu.maxPendingBytes = diskCacheAsyncMaxPendingBytes
	d.async.mu.idle = make(chan struct{})
	close(d.async.mu.idle)
	d.async.callbackTail = make(chan struct{})
	close(d.async.callbackTail)
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
	filePath, object, diskPath string,
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
	a.mu.pending[diskPath] = diskCacheAdmission{object: object}
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

func (d *DiskCache) tryReserveAsyncFileFinalize(diskPath string) bool {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mu.closed {
		a.dropAsyncUpdateLocked()
		return false
	}
	if _, ok := a.mu.pending[diskPath]; ok {
		return false
	}
	select {
	case a.slots <- struct{}{}:
	default:
		a.dropAsyncUpdateLocked()
		return false
	}
	if len(a.mu.pending) == 0 {
		a.mu.idle = make(chan struct{})
	}
	a.mu.pending[diskPath] = diskCacheAdmission{}
	// The reservation spans the foreground source copy. Close seals admission
	// under the same mutex before waiting for submitters, so it cannot miss a
	// finalizer that was admitted but has not reached the jobs channel yet.
	a.submitters.Add(1)
	return true
}

func (d *DiskCache) releaseAsyncFileFinalizeReservation(diskPath string) {
	a := &d.async
	a.mu.Lock()
	a.finishAdmissionLocked(diskPath)
	<-a.slots
	if len(a.mu.pending) == 0 {
		close(a.mu.idle)
	}
	a.mu.Unlock()
	a.submitters.Done()
}

func (d *DiskCache) scheduleReservedAsyncFileFinalize(
	diskPath string,
	file *os.File,
	doneUpdate func() error,
) {
	a := &d.async
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
		// The foreground source copy is complete and ownership has transferred.
		// File close/remove may block, so terminal cleanup must not run in the
		// submitting S3/LocalFS goroutine.
		go d.releaseAsyncUpdate(job)
	}
	a.submitters.Done()
}

func (d *DiskCache) claimAsyncPublication(diskPath string) bool {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	// Close seals the whole instance; Delete revokes only the observed
	// admission. If publication already won, the path owner completes the
	// deletion before it lets a reader or another writer enter.
	admission, ok := a.mu.pending[diskPath]
	return ok && !a.mu.closed && admission.done == nil
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
			retainForCallbacks := false
			if d.async.ctx.Err() == nil {
				claim := func() bool { return d.claimAsyncPublication(job.diskPath) }
				if job.finalize != nil {
					job.finalize.attempted = true
					job.finalize.err = d.finalizeFile(
						d.async.ctx,
						job.diskPath,
						job.finalize.file,
						claim,
					)
					if isDiskFull(job.finalize.err) {
						d.cache.ForceEvictWithWait(d.async.ctx, d.capacityFunc()/10)
					}
				} else {
					written, _ := d.writeEntry(
						d.async.ctx,
						job.diskPath,
						job.entry,
						claim,
					)
					retainForCallbacks = written && len(job.callbacks) > 0
				}
			}
			d.completeAsyncUpdate(job, retainForCallbacks)
			if retainForCallbacks {
				// The original admission slot and byte budget stay owned by this
				// callback goroutine. Callbacks are therefore independent of the
				// cache worker/Flush/Close wait graph, while blocked callbacks remain
				// bounded by the same admission limits as cache updates.
				d.scheduleAsyncUpdateCallbacks(job)
			}
		}
	}
}

func (d *DiskCache) releaseAsyncUpdate(job *diskCacheAsyncUpdate) {
	d.completeAsyncUpdate(job, false)
}

func (d *DiskCache) completeAsyncUpdate(
	job *diskCacheAsyncUpdate,
	retainForCallbacks bool,
) {
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
		a.finishAdmissionLocked(job.diskPath)
		if !retainForCallbacks {
			d.releaseAsyncUpdateResourcesLocked(job)
		}
		if len(a.mu.pending) == 0 {
			close(a.mu.idle)
		}
		a.mu.Unlock()
	})
}

func (d *DiskCache) scheduleAsyncUpdateCallbacks(job *diskCacheAsyncUpdate) {
	a := &d.async
	a.callbackMu.Lock()
	previous := a.callbackTail
	done := make(chan struct{})
	a.callbackTail = done
	a.callbackMu.Unlock()

	go func() {
		defer close(done)
		defer d.releaseAsyncUpdateResources(job)
		<-previous
		for i, fn := range job.callbacks {
			d.runAsyncUpdateCallback(job, i, fn)
		}
	}()
}

// runAsyncUpdateCallback is the panic boundary for callbacks moved from the
// Update caller onto a DiskCache-owned goroutine. One broken observer must not
// terminate the CN or skip later callbacks in the ordered chain.
func (d *DiskCache) runAsyncUpdateCallback(
	job *diskCacheAsyncUpdate,
	index int,
	fn OnDiskCacheWrittenFunc,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			metric.FSDiskCacheAsyncCallbackPanicCounter.Inc()
			eventDiskCacheAsyncCallbackPanic.ErrorLazy(func() []zap.Field {
				fields := []zap.Field{
					zap.String("panic-type", fmt.Sprintf("%T", recovered)),
					zap.ByteString("stack", debug.Stack()),
					zap.String("file-path", job.filePath),
					zap.String("disk-path", job.diskPath),
					zap.Int("callback-index", index),
				}
				// Do not call Error, String, MarshalJSON, or any other method on
				// a callback-owned panic value while handling the panic.
				if message, ok := recovered.(string); ok {
					fields = append(fields, zap.String("panic", message))
				}
				return fields
			})
		}
	}()
	fn(job.filePath, job.entry)
}

func (d *DiskCache) releaseAsyncUpdateResources(job *diskCacheAsyncUpdate) {
	a := &d.async
	a.mu.Lock()
	defer a.mu.Unlock()
	d.releaseAsyncUpdateResourcesLocked(job)
}

func (d *DiskCache) releaseAsyncUpdateResourcesLocked(job *diskCacheAsyncUpdate) {
	job.resourceOnce.Do(func() {
		d.async.mu.pendingBytes -= int64(len(job.entry.Data))
		<-d.async.slots
		job.entry.Data = nil
		job.callbacks = nil
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
