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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// DecodeSharing opts a scoped reader into sharing immutable conversion results.
// Codec and Parameters must identify all conversion/validation semantics. The
// caller must consume every borrowed decoded view before releasing the complete
// IOVector; only owned results may escape that scope.
// A zero descriptor is ignored. Only write-once, non-reused paths are eligible.
// Results without trustworthy logical and retained-size information use the
// ordinary unshared path.
type DecodeSharing struct {
	Codec      string
	Parameters [2]uint64
}

const (
	sharedDecodeMaxBytes            = 64 << 20
	sharedDecodeMaxEntries          = 64
	sharedDecodeMaxParticipants     = 128
	sharedDecodeWait                = 200 * time.Millisecond
	sharedDecodeFillMaxGenerations  = 64
	sharedDecodeFillMaxParticipants = 128
	sharedDecodeFillWait            = time.Second
)

type decodedReadKey struct {
	path                  string
	offset, size, decoded int64
	policy                Policy
	codec                 DecodeSharing
}

type decodedRead struct {
	key          decodedReadKey
	done         chan struct{}
	finished     bool
	data         fscache.Data // one registry reference, never a wrapper/copy
	participants int
	bytes        int64
}

type decodedFill struct {
	key          decodedReadKey
	done         chan struct{}
	finished     bool
	participants int
}

type decodedReadRegistry struct {
	mu        sync.Mutex
	entries   map[decodedReadKey]*decodedRead
	fills     map[decodedReadKey]*decodedFill
	limit     int64
	bytes     int64
	count     int // includes detached generations awaiting final release
	fillCount int // includes completed generations awaiting final release
	reads     int // guarded S3FS reads, not Top-K consumers
	closed    bool
	retire    func()
}

type decodedReadLease struct {
	registry *decodedReadRegistry
	entry    *decodedRead
	released atomic.Bool
}

// decodedReadScope owns the selected entry's temporary converter and lease for
// one guarded S3FS read. It exists only after that read opts into sharing.
type decodedReadScope struct {
	registry           *decodedReadRegistry
	vector             *IOVector
	index              int
	original           func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error)
	lease              *decodedReadLease
	bypassSharing      bool
	closedDuringDecode bool
}

func (scope *decodedReadScope) finish() {
	entry := &scope.vector.Entries[scope.index]
	entry.ToCacheData = scope.original
	if entry.CachedData != nil {
		entry.decodeLease = scope.lease
	} else {
		scope.lease.release()
	}
	scope.registry.endRead()
}

type decodedFillTicket struct {
	registry   *decodedReadRegistry
	generation *decodedFill
	leader     bool
	released   atomic.Bool
}

func newDecodedReadRegistry(limit int64) *decodedReadRegistry {
	return &decodedReadRegistry{
		entries: make(map[decodedReadKey]*decodedRead),
		fills:   make(map[decodedReadKey]*decodedFill),
		limit:   max(0, min(limit, sharedDecodeMaxBytes)),
	}
}

func sharedDecodeClosed() error {
	return moerr.NewInvalidStateNoCtx("file service closed during shared decompression")
}

func (r *decodedReadRegistry) beginRead() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.reads++
	return true
}

func (r *decodedReadRegistry) endRead() {
	r.mu.Lock()
	r.reads--
	var retire func()
	if r.closed && r.reads == 0 {
		retire, r.retire = r.retire, nil
	}
	r.mu.Unlock()
	if retire != nil {
		retire()
	}
}

// acquireFill admits a read-scope notification generation. It deliberately
// carries no decoded data or byte reservation; the memory cache remains the
// only publication and ownership boundary.
func (r *decodedReadRegistry) acquireFill(key decodedReadKey, mayStart bool) (*decodedFillTicket, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, sharedDecodeClosed()
	}
	if generation := r.fills[key]; generation != nil {
		if generation.participants >= sharedDecodeFillMaxParticipants {
			return nil, nil
		}
		generation.participants++
		return &decodedFillTicket{registry: r, generation: generation}, nil
	}
	if !mayStart || r.fillCount >= sharedDecodeFillMaxGenerations {
		return nil, nil
	}
	generation := &decodedFill{
		key:          key,
		done:         make(chan struct{}),
		participants: 1,
	}
	r.fills[key] = generation
	r.fillCount++
	return &decodedFillTicket{registry: r, generation: generation, leader: true}, nil
}

func (r *decodedReadRegistry) completeFill(generation *decodedFill) {
	if generation == nil {
		return
	}
	r.mu.Lock()
	if !generation.finished {
		generation.finished = true
		if r.fills[generation.key] == generation {
			delete(r.fills, generation.key)
		}
		close(generation.done)
	}
	r.mu.Unlock()
}

func (ticket *decodedFillTicket) wait(ctx context.Context, timeout time.Duration) error {
	if ticket == nil || ticket.leader {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticket.generation.done:
		return ticket.waitResult(ctx)
	case <-timer.C:
		return ticket.waitResult(ctx)
	}
}

func (ticket *decodedFillTicket) waitResult(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ticket.registry.mu.Lock()
	closed := ticket.registry.closed
	ticket.registry.mu.Unlock()
	if closed {
		return sharedDecodeClosed()
	}
	return nil
}

func (ticket *decodedFillTicket) finish() {
	if ticket == nil {
		return
	}
	if ticket.leader {
		ticket.registry.completeFill(ticket.generation)
	}
	ticket.release()
}

func (ticket *decodedFillTicket) release() {
	if ticket == nil || !ticket.released.CompareAndSwap(false, true) {
		return
	}
	r, generation := ticket.registry, ticket.generation
	r.mu.Lock()
	generation.participants--
	if generation.participants == 0 {
		if !generation.finished && r.fills[generation.key] == generation {
			delete(r.fills, generation.key)
		}
		r.fillCount--
	}
	r.mu.Unlock()
}

func (r *decodedReadRegistry) close(retire func()) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	for key, entry := range r.entries {
		delete(r.entries, key)
		if !entry.finished {
			entry.finished = true
			close(entry.done)
		}
	}
	for key, generation := range r.fills {
		delete(r.fills, key)
		if !generation.finished {
			generation.finished = true
			close(generation.done)
		}
	}
	if r.reads != 0 {
		r.retire = retire
		retire = nil
	}
	r.mu.Unlock()
	if retire != nil {
		retire()
	}
}

// acquire returns no lease when sharing cannot be admitted. No allocator or
// blocking cache operation runs under the registry mutex.
func (r *decodedReadRegistry) acquire(key decodedReadKey, bytes int64) (lease *decodedReadLease, leader bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, sharedDecodeClosed()
	}
	if entry := r.entries[key]; entry != nil {
		if entry.participants >= sharedDecodeMaxParticipants {
			return nil, false, nil
		}
		entry.participants++
		return &decodedReadLease{registry: r, entry: entry}, false, nil
	}
	if bytes <= 0 || bytes > r.limit-r.bytes || r.count >= sharedDecodeMaxEntries {
		return nil, false, nil
	}
	entry := &decodedRead{key: key, done: make(chan struct{}), participants: 1, bytes: bytes}
	r.entries[key] = entry
	r.bytes += bytes
	r.count++
	metric.SharedDecodeActive.Inc()
	metric.SharedDecodeReserved.Add(float64(bytes))
	return &decodedReadLease{registry: r, entry: entry}, true, nil
}

// finish seals one generation. Nil data wakes followers to perform their own
// conversion, including when a leader fails or unwinds a panic.
func (r *decodedReadRegistry) finish(entry *decodedRead, data fscache.Data) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry.finished {
		return false
	}
	entry.finished = true
	if data != nil {
		data.Retain()
		entry.data = data
	} else if r.entries[entry.key] == entry {
		delete(r.entries, entry.key)
	}
	close(entry.done)
	return true
}

func (lease *decodedReadLease) release() {
	if lease == nil || !lease.released.CompareAndSwap(false, true) {
		return
	}
	r, entry := lease.registry, lease.entry
	r.mu.Lock()
	entry.participants--
	if entry.participants != 0 {
		r.mu.Unlock()
		return
	}
	if r.entries[entry.key] == entry {
		delete(r.entries, entry.key)
	}
	data := entry.data
	entry.data = nil
	r.mu.Unlock()
	// Keep quota charged until the retained reference has actually been released.
	if data != nil {
		data.Release()
	}
	r.mu.Lock()
	r.bytes -= entry.bytes
	r.count--
	metric.SharedDecodeReserved.Sub(float64(entry.bytes))
	metric.SharedDecodeActive.Dec()
	r.mu.Unlock()
}

func (r *decodedReadRegistry) convert(ctx context.Context, convert func() (fscache.Data, error)) (fscache.Data, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if closed {
		return nil, sharedDecodeClosed()
	}
	metric.SharedDecodeConversions.Inc()
	return convert()
}

func (r *decodedReadRegistry) decode(ctx context.Context, key decodedReadKey, bytes int64, convert func() (fscache.Data, error)) (data fscache.Data, lease *decodedReadLease, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	run := func() (fscache.Data, error) { return r.convert(ctx, convert) }
	// The default allocator uses power-of-two classes (or an allocation no
	// larger than that class). Charge the class until the shared reference is
	// released, and bypass sharing if a result cannot prove its retained size.
	backing, ok := malloc.ClassAllocationSize(uint64(max(bytes, 0)))
	if !ok {
		metric.SharedDecodeAdmission.Inc()
		data, err = run()
		return
	}
	lease, leader, err := r.acquire(key, int64(backing))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		metric.SharedDecodeClosed.Inc()
		return
	}
	if lease == nil {
		metric.SharedDecodeAdmission.Inc()
		data, err = run()
		return
	}
	owned := true
	defer func() {
		if owned {
			lease.release()
			lease = nil
		}
	}()
	entry := lease.entry
	if leader {
		metric.SharedDecodeLeaders.Inc()
		defer r.finish(entry, nil)
		data, err = run()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			if data != nil {
				data.Release()
				data = nil
			}
			r.finish(entry, nil)
			return
		}
		logicalSize, retainedSize := int64(-1), int64(-1)
		if sized, ok := data.(interface {
			Size() int64
			BackingSize() int64
		}); ok {
			retainedSize = sized.BackingSize()
			if retainedSize >= 0 {
				logicalSize = sized.Size()
			}
		}
		if logicalSize < 0 || logicalSize > bytes ||
			retainedSize < logicalSize || retainedSize > int64(backing) {
			metric.SharedDecodeAdmission.Inc()
			r.finish(entry, nil)
			return
		}
		if !r.finish(entry, data) {
			data.Release()
			data = nil
			err = sharedDecodeClosed()
			return
		}
		owned = false
		return
	}
	timer := time.NewTimer(sharedDecodeWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-timer.C:
		lease.release()
		lease = nil
		owned = false
		metric.SharedDecodeTimeout.Inc()
		data, err = run()
		return
	case <-entry.done:
	}
	r.mu.Lock()
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if r.closed {
		err = sharedDecodeClosed()
	} else {
		if entry.data != nil {
			data = entry.data
			data.Retain()
		}
	}
	r.mu.Unlock()
	if err != nil {
		return
	}
	if data != nil {
		owned = false
		metric.SharedDecodeReuses.Inc()
		return
	}
	lease.release()
	lease = nil
	owned = false
	metric.SharedDecodeAbandoned.Inc()
	data, err = run()
	return
}

// This is only an admission hint: used cache data may be evicted, and a
// later update can still be rejected. Existing fills may accept joins even
// when pressure starts after the leader was admitted.
func sharedDecodeFillMayStart(m *MemCache, decodedSize int64) bool {
	capacity := m.cache.Capacity()
	if decodedSize <= 0 || decodedSize > capacity {
		return false
	}
	_, pressureActive := memoryCachePressureTarget(capacity)
	return !pressureActive
}

func sharedDecodeDiskFillAvailable(d *DiskCache, path string, vector *IOVector) bool {
	if d == nil || d.cache == nil {
		return false
	}
	if d.cache.Contains(d.pathForFile(path)) {
		return true
	}
	return len(vector.Entries) == 1 && d.cache.Contains(d.pathForIOEntry(path, vector.Entries[0]))
}

// prepareSharedDecodeForRead is called only after a memory-cache miss. The wrapper
// shares conversion, not I/O or cache-update outcomes. Its finalizer runs after
// the enclosing read's deferred cache updates and transfers the ticket to the
// final entry (which helper reads may have replaced).
func (s *S3FS) prepareSharedDecodeForRead(vector *IOVector) (*decodedReadScope, *decodedFillTicket, bool, error) {
	if s.decodedReads == nil || len(vector.Caches) != 0 || vector.Policy.Any(SkipMemoryCache) {
		return nil, nil, false, nil
	}
	index := -1
	for i := range vector.Entries {
		if vector.Entries[i].DecodeSharing.Codec == "" {
			continue
		}
		// One selected entry per request. Do not grow per-entry wrapper state
		// or silently choose a winner when a caller marks multiple columns.
		if index >= 0 {
			return nil, nil, false, nil
		}
		index = i
	}
	if index < 0 {
		return nil, nil, false, nil
	}
	entry := &vector.Entries[index]
	sharing := entry.DecodeSharing
	if sharing.Codec == "" || len(sharing.Codec) > 64 || len(vector.FilePath) > 4096 ||
		entry.Offset < 0 || entry.Size <= 0 || entry.CachedDataSize <= 0 || entry.CachedDataSize > int64(^uint(0)>>1) ||
		entry.ToCacheData == nil || entry.WriterForRead != nil || entry.ReadCloserForRead != nil || entry.ReaderForWrite != nil ||
		entry.done || entry.Data != nil || entry.CachedData != nil || entry.decodeLease != nil {
		return nil, nil, false, nil
	}
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return nil, nil, false, err
	}
	if !s.decodedReads.beginRead() {
		return nil, nil, false, sharedDecodeClosed()
	}
	key := decodedReadKey{path: path.File, offset: entry.Offset, size: entry.Size, decoded: entry.CachedDataSize, policy: vector.Policy, codec: sharing}
	var fill *decodedFillTicket
	fillAttempted := false
	if !vector.Policy.Any(SkipDiskCacheReads) &&
		sharedDecodeDiskFillAvailable(s.diskCache, path.File, vector) {
		mayStart := sharedDecodeFillMayStart(s.memCache, entry.CachedDataSize)
		fill, err = s.decodedReads.acquireFill(key, mayStart)
		fillAttempted = mayStart || fill != nil
		if err != nil {
			s.decodedReads.endRead()
			return nil, nil, false, err
		}
	}
	scope := &decodedReadScope{registry: s.decodedReads, vector: vector, index: index, original: entry.ToCacheData}
	entry.ToCacheData = func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
		var result fscache.Data
		var err error
		if scope.bypassSharing {
			result, err = scope.registry.convert(ctx, func() (fscache.Data, error) { return scope.original(ctx, reader, data, allocator) })
		} else if int64(len(data)) != key.size {
			result, err = scope.original(ctx, reader, data, allocator)
		} else {
			var ticket *decodedReadLease
			result, ticket, err = scope.registry.decode(ctx, key, key.decoded, func() (fscache.Data, error) { return scope.original(ctx, reader, data, allocator) })
			scope.lease = ticket
			// A timeout, failure, or admission bypass must not rejoin and wait
			// again if this read falls through to a different storage tier.
			scope.bypassSharing = ticket == nil
		}
		if err != nil {
			// DiskCache treats conversion errors as misses. Remember a close
			// observed by this failed conversion so S3FS does not start another tier.
			scope.registry.mu.Lock()
			scope.closedDuringDecode = scope.closedDuringDecode || scope.registry.closed
			scope.registry.mu.Unlock()
		}
		return result, err
	}
	return scope, fill, fillAttempted, nil
}
