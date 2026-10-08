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

package fulltext2

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var errBaseFilePoolCapacity = errors.New("fulltext2 base file pool capacity admission failed")

// These sentinels classify failures at the optional-file layer.  Callers must
// use errors.Is instead of matching messages: capacity and a recoverable cache
// failure may fall back to the ordinary loader, while a closed owner or a
// source-side error must be returned to the query unchanged.
var (
	errBaseFilePoolClosed         = errors.New("fulltext2 base file pool is closed")
	errBaseFilePoolLeaderCanceled = errors.New("fulltext2 base file fill leader canceled")
	errBaseFilePoolReadyCorrupt   = errors.New("fulltext2 ready base file is corrupt")
)

type baseFilePoolError struct {
	kind  error
	cause error
}

func (e *baseFilePoolError) Error() string {
	if e == nil {
		return "fulltext2 base file pool error"
	}
	if e.cause == nil {
		return e.kind.Error()
	}
	return fmt.Sprintf("%s: %v", e.kind, e.cause)
}

func (e *baseFilePoolError) Unwrap() error { return e.cause }

func (e *baseFilePoolError) Is(target error) bool {
	return e != nil && (target == e.kind || errors.Is(e.cause, target))
}

func wrapBaseFilePoolError(kind, cause error) error {
	if cause == nil {
		return kind
	}
	return &baseFilePoolError{kind: kind, cause: cause}
}

// baseFileKey identifies one immutable Base file.  The metadata identity is
// deliberately part of the key: a checksum is an integrity check, not an
// identity, and must not by itself deduplicate files from different indexes.
// The owner and account are captured from the loading CN when available; the
// database/table identity remains part of the key for deterministic tests.
type baseFileKey struct {
	owner    string
	account  uint32
	db       string
	src      string
	index    string
	metadata string
	pkey     string
	id       string
	checksum string
	size     int64
}

type baseFileState uint8

const (
	baseFileFilling baseFileState = iota + 1
	baseFileReady
)

type baseFileEntry struct {
	key      baseFileKey
	handle   baseFileHandle
	state    baseFileState
	users    int
	waiters  int
	err      error
	retired  bool
	fileGone bool
	lastUsed uint64
	done     chan struct{}
	mapMu    sync.Mutex // protects fallback mmapReadOnly implementations that seek the shared fd
	cancel   context.CancelFunc
}

type deferredBaseMapping struct {
	data []byte
	size int64
}

type baseFileHandle struct {
	file *os.File
	path string
	// validationData remains owned by the fill until it is successfully unmapped.
	// It is only non-nil on the exceptional validation-release failure path.
	validationData []byte
	// validated is a one-shot proof produced by the fill transaction. It avoids
	// hashing the same bytes twice for the first lease; later READY hits always
	// re-check the complete file before mapping.
	validated bool
}

// baseFilePool is a deliberately small, request-independent pool of immutable
// Base files.  It owns files, not decoded Segments.  The mutex protects only
// the directory, byte/FD accounting and lease state; SQL, checksum and mmap
// work happens outside it.
type baseFilePool struct {
	mu       sync.Mutex
	retryMu  sync.Mutex
	maxBytes int64
	maxFiles int
	bytes    int64
	reserved int64
	// deferredBytes accounts for validation mappings whose munmap failed after
	// their fill reservation was released. They remain charged until retry succeeds.
	deferredBytes int64
	openFiles     int
	// reservedFiles remains charged from FILLING admission until the fill's
	// handle has actually been closed.  In particular, a failed fill must not
	// open a second FD while its first handle is still in closeHandle.
	reservedFiles    int
	seq              uint64
	closed           bool
	entries          map[baseFileKey]*baseFileEntry
	deferredSegments map[*Segment]struct{}
	deferredMappings map[*deferredBaseMapping]struct{}
}

func newBaseFilePool(maxBytes int64, maxFiles int) *baseFilePool {
	if maxBytes <= 0 {
		maxBytes = 1
	}
	if maxFiles <= 0 {
		maxFiles = 1
	}
	return &baseFilePool{
		maxBytes:         maxBytes,
		maxFiles:         maxFiles,
		entries:          make(map[baseFileKey]*baseFileEntry),
		deferredSegments: make(map[*Segment]struct{}),
		deferredMappings: make(map[*deferredBaseMapping]struct{}),
	}
}

// baseFileLease pins one pool file.  A lease has at most one mapping; every
// loaded Segment gets its own mapping and its own lease.
type baseFileLease struct {
	pool     *baseFilePool
	entry    *baseFileEntry
	once     sync.Once
	mu       sync.Mutex
	data     []byte
	err      error
	mapped   bool
	released bool
}

func (p *baseFilePool) acquire(ctx context.Context, key baseFileKey, fill func(context.Context) (*baseFileHandle, error)) (*baseFileLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errBaseFilePoolClosed
		}
		if e := p.entries[key]; e != nil {
			if e.state == baseFileReady {
				p.seq++
				e.lastUsed = p.seq
				e.users++
				p.mu.Unlock()
				return &baseFileLease{pool: p, entry: e}, nil
			}
			done := e.done
			e.waiters++
			p.mu.Unlock()
			select {
			case <-done:
				p.mu.Lock()
				if e.waiters > 0 {
					e.waiters--
				}
				fillErr := e.err
				p.mu.Unlock()
				if fillErr != nil {
					return nil, fillErr
				}
				continue
			case <-ctx.Done():
				p.mu.Lock()
				if e.waiters > 0 {
					e.waiters--
				}
				p.mu.Unlock()
				return nil, ctx.Err()
			}
		}

		if key.size <= 0 || key.size > p.maxBytes {
			p.mu.Unlock()
			return nil, fmt.Errorf("%w: base file %s exceeds pool capacity", errBaseFilePoolCapacity, key.id)
		}
		if err := p.makeRoomLocked(key.size); err != nil {
			p.mu.Unlock()
			return nil, err
		}
		e := &baseFileEntry{
			key:   key,
			state: baseFileFilling,
			done:  make(chan struct{}),
		}
		fillCtx, cancel := context.WithCancel(ctx)
		e.cancel = cancel
		p.entries[key] = e
		p.reserved += key.size
		p.reservedFiles++
		p.mu.Unlock()

		handle, err := fill(fillCtx)
		callerErr := ctx.Err()
		cancel()
		if err != nil {
			err = p.classifyFillError(ctx, err)
			p.finishFill(e, handle, err)
			return nil, err
		}
		if handle == nil || handle.file == nil {
			err = fmt.Errorf("fulltext2 base file %s fill returned nil file", key.id)
			p.finishFill(e, nil, err)
			return nil, err
		}
		if callerErr != nil {
			err = wrapBaseFilePoolError(errBaseFilePoolLeaderCanceled, callerErr)
			p.finishFill(e, handle, err)
			return nil, err
		}
		p.mu.Lock()
		closed := p.closed
		if !closed {
			e.handle = *handle
			e.state = baseFileReady
			e.users = 1
			p.seq++
			e.lastUsed = p.seq
			p.bytes += key.size
			p.openFiles++
			p.reserved -= key.size
			p.reservedFiles--
		}
		if closed {
			e.err = errBaseFilePoolClosed
		}
		close(e.done)
		delete(p.entries, key)
		if !closed {
			p.entries[key] = e
		}
		p.mu.Unlock()
		if closed {
			p.closeHandle(handle)
			p.mu.Lock()
			p.reserved -= key.size
			p.reservedFiles--
			p.mu.Unlock()
			return nil, e.err
		}
		return &baseFileLease{pool: p, entry: e}, nil
	}
}

func (p *baseFilePool) classifyFillError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return wrapBaseFilePoolError(errBaseFilePoolClosed, err)
	}
	// A fill context is derived from the leader's context.  Only that leader's
	// cancellation is eligible for waiter-side ordinary-loader fallback; source
	// errors that happen to wrap context.Canceled are preserved as source errors.
	if ctx != nil && ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return wrapBaseFilePoolError(errBaseFilePoolLeaderCanceled, err)
	}
	return err
}

func (p *baseFilePool) finishFill(e *baseFileEntry, handle *baseFileHandle, err error) {
	p.mu.Lock()
	e.err = err
	if p.entries[e.key] == e {
		delete(p.entries, e.key)
		close(e.done)
	}
	p.mu.Unlock()
	p.closeHandle(handle)
	// Keep both byte and FD reservations until closeHandle has returned.  A
	// failing fill may already own an open file even though its directory entry
	// is gone, so releasing either budget earlier admits an actual overage.
	p.mu.Lock()
	p.reserved -= e.key.size
	p.reservedFiles--
	p.mu.Unlock()
	_ = err
}

func (p *baseFilePool) makeRoomLocked(need int64) error {
	for p.bytes+p.reserved+p.deferredBytes+need > p.maxBytes || p.fileCountLocked()+1 > p.maxFiles {
		var victim *baseFileEntry
		for _, e := range p.entries {
			if e.state != baseFileReady || e.users != 0 {
				continue
			}
			if victim == nil || e.lastUsed < victim.lastUsed {
				victim = e
			}
		}
		if victim == nil {
			return errBaseFilePoolCapacity
		}
		delete(p.entries, victim.key)
		victim.retired = true
		p.closeEntryLocked(victim)
	}
	return nil
}

func (p *baseFilePool) closeEntryLocked(e *baseFileEntry) {
	if e == nil || e.fileGone {
		return
	}
	e.fileGone = true
	p.bytes -= e.key.size
	if e.state == baseFileReady && p.openFiles > 0 {
		p.openFiles--
	}
	if e.handle.validationData != nil {
		p.deferMappingLocked(e.handle.validationData)
		e.handle.validationData = nil
	}
	_ = e.handle.file.Close()
	if e.handle.path != "" {
		_ = os.Remove(e.handle.path)
	}
}

func (p *baseFilePool) retire(e *baseFileEntry) {
	if e == nil {
		return
	}
	p.mu.Lock()
	if !e.retired {
		e.retired = true
		if p.entries[e.key] == e {
			delete(p.entries, e.key)
		}
	}
	if e.users == 0 {
		p.closeEntryLocked(e)
	}
	p.mu.Unlock()
}

func (p *baseFilePool) fileCountLocked() int {
	return p.openFiles + p.reservedFiles
}

func (p *baseFilePool) deferSegment(s *Segment) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	p.deferredSegments[s] = struct{}{}
	p.mu.Unlock()
}

func (p *baseFilePool) undeferSegment(s *Segment) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	delete(p.deferredSegments, s)
	p.mu.Unlock()
}

func (p *baseFilePool) deferMappingLocked(data []byte) {
	size := int64(len(data))
	if size <= 0 {
		return
	}
	p.deferredMappings[&deferredBaseMapping{data: data, size: size}] = struct{}{}
	p.deferredBytes += size
}

// baseFilePoolBeforeCloseHandle is test-only synchronization for the failure
// accounting tests. Production leaves it nil; the hook runs before any mmap or
// FD is released so a second acquisition can observe the reservation.
var baseFilePoolBeforeCloseHandle func()

func (p *baseFilePool) closeHandle(handle *baseFileHandle) {
	if handle == nil {
		return
	}
	if hook := baseFilePoolBeforeCloseHandle; hook != nil {
		hook()
	}
	if handle.validationData != nil {
		if err := munmap(handle.validationData); err != nil {
			p.mu.Lock()
			p.deferMappingLocked(handle.validationData)
			p.mu.Unlock()
		}
		handle.validationData = nil
	}
	if handle.file != nil {
		_ = handle.file.Close()
	}
	if handle.path != "" {
		_ = os.Remove(handle.path)
	}
}

// retryDeferred retries mappings and Segment frees outside the pool mutex. A failed
// munmap remains owned by the pool and is retried on the next Destroy/Close boundary.
func (p *baseFilePool) retryDeferred() {
	if p == nil {
		return
	}
	p.retryMu.Lock()
	defer p.retryMu.Unlock()
	p.mu.Lock()
	segments := make([]*Segment, 0, len(p.deferredSegments))
	for s := range p.deferredSegments {
		segments = append(segments, s)
	}
	mappings := make([]*deferredBaseMapping, 0, len(p.deferredMappings))
	for m := range p.deferredMappings {
		mappings = append(mappings, m)
	}
	p.mu.Unlock()

	for _, s := range segments {
		s.Free()
	}
	for _, m := range mappings {
		if err := munmap(m.data); err == nil {
			p.mu.Lock()
			if _, ok := p.deferredMappings[m]; ok {
				delete(p.deferredMappings, m)
				p.deferredBytes -= m.size
				if p.deferredBytes < 0 {
					p.deferredBytes = 0
				}
			}
			p.mu.Unlock()
		}
	}
}

func (p *baseFilePool) deferredCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.deferredSegments) + len(p.deferredMappings)
}

func (l *baseFileLease) MapReadOnly() ([]byte, error) {
	if l == nil || l.pool == nil || l.entry == nil {
		return nil, fmt.Errorf("fulltext2 nil base file lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil, fmt.Errorf("fulltext2 base file lease already released")
	}
	if l.mapped {
		return l.data, l.err
	}
	l.mapped = true
	l.entry.mapMu.Lock()
	if l.entry.handle.validated {
		// The fill path already checksummed and decoded this immutable file. Consume
		// that proof for one first mapping; later READY hits re-check the complete
		// file before mapping so a damaged cached file is retired.
		l.entry.handle.validated = false
		l.data, l.err = mmapReadOnly(l.entry.handle.file)
	} else {
		checksum, checksumErr := checksumFile(l.entry.handle.file, l.entry.key.size)
		if checksumErr != nil {
			l.err = wrapBaseFilePoolError(errBaseFilePoolReadyCorrupt, checksumErr)
		} else if checksum != l.entry.key.checksum {
			l.err = wrapBaseFilePoolError(errBaseFilePoolReadyCorrupt,
				fmt.Errorf("fulltext2 base file %s checksum mismatch", l.entry.key.id))
		} else {
			l.data, l.err = mmapReadOnly(l.entry.handle.file)
		}
	}
	l.entry.mapMu.Unlock()
	if l.err != nil {
		l.pool.retire(l.entry)
	}
	return l.data, l.err
}

func checksumFile(f *os.File, size int64) (string, error) {
	if f == nil || size < 0 {
		return "", fmt.Errorf("fulltext2 invalid base file for checksum")
	}
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() != size {
		return "", fmt.Errorf("fulltext2 base file size mismatch: got %d want %d", info.Size(), size)
	}
	h := md5.New()
	if _, err := io.CopyN(h, io.NewSectionReader(f, 0, size), size); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (l *baseFileLease) Invalidate() {
	if l == nil {
		return
	}
	l.pool.retire(l.entry)
}

func (l *baseFileLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.mu.Lock()
		l.released = true
		l.mu.Unlock()
		p := l.pool
		p.mu.Lock()
		if l.entry.users > 0 {
			l.entry.users--
		}
		if l.entry.users == 0 && (p.closed || l.entry.retired) {
			if p.entries[l.entry.key] == l.entry {
				delete(p.entries, l.entry.key)
			}
			p.closeEntryLocked(l.entry)
		}
		p.mu.Unlock()
	})
}

// Close rejects new acquisitions and closes all unpinned files. Pinned files
// stay alive until their Segment releases its lease.
func (p *baseFilePool) Close() {
	p.retryDeferred()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for key, e := range p.entries {
		if e.state == baseFileFilling && e.cancel != nil {
			e.cancel()
		}
		if e.state == baseFileReady && e.users == 0 {
			delete(p.entries, key)
			e.retired = true
			p.closeEntryLocked(e)
		} else if e.state == baseFileReady {
			e.retired = true
		}
	}
	p.mu.Unlock()
}
