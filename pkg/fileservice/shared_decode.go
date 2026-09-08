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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// DecodeSharing opts a scoped reader into sharing immutable conversion results.
// Codec and Parameters must identify all conversion/validation semantics. The
// caller must release the complete IOVector before any decoded view escapes.
// A zero descriptor is ignored. Only write-once, non-reused paths are eligible.
type DecodeSharing struct {
	Codec      string
	Parameters [2]uint64
}

const (
	sharedDecodeMaxBytes        = 64 << 20
	sharedDecodeMaxEntries      = 64
	sharedDecodeMaxParticipants = 128
	sharedDecodeWait            = 200 * time.Millisecond
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
	err          error
	data         fscache.Data // one registry reference, never a wrapper/copy
	participants int
	bytes        int64
}

type decodedReadRegistry struct {
	mu      sync.Mutex
	entries map[decodedReadKey]*decodedRead
	limit   int64
	bytes   int64
	count   int // includes detached generations awaiting final release
	reads   int // guarded S3FS reads, not Top-K consumers
	closed  bool
	retire  func()
}

type decodedReadLease struct {
	registry *decodedReadRegistry
	entry    *decodedRead
	released atomic.Bool
}

func newDecodedReadRegistry(limit int64) *decodedReadRegistry {
	return &decodedReadRegistry{entries: make(map[decodedReadKey]*decodedRead), limit: max(0, min(limit, sharedDecodeMaxBytes))}
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
			entry.finished, entry.err = true, sharedDecodeClosed()
			close(entry.done)
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

// finish seals one generation. A nil data/nil error result means bypass; it
// also retires an abandoned conversion while unwinding a panic, without recover.
func (r *decodedReadRegistry) finish(entry *decodedRead, data fscache.Data, err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry.finished {
		return false
	}
	entry.finished, entry.err = true, err
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

func (r *decodedReadRegistry) decode(ctx context.Context, key decodedReadKey, bytes int64, convert func() (fscache.Data, error)) (data fscache.Data, lease *decodedReadLease, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	run := func() (fscache.Data, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return nil, sharedDecodeClosed()
		}
		metric.SharedDecodeConversions.Inc()
		return convert()
	}
	lease, leader, err := r.acquire(key, bytes)
	if err != nil {
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
		defer r.finish(entry, nil, nil)
		data, err = run()
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			if data != nil {
				data.Release()
				data = nil
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				r.finish(entry, nil, nil)
			} else {
				r.finish(entry, nil, err)
			}
			return
		}
		if data == nil || data.Capacity() > bytes {
			metric.SharedDecodeAdmission.Inc()
			r.finish(entry, nil, nil)
			return
		}
		if !r.finish(entry, data, nil) {
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
	if r.closed {
		err = sharedDecodeClosed()
	} else if ctx.Err() != nil {
		err = ctx.Err()
	} else {
		err = entry.err
		if err == nil && entry.data != nil {
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

// prepareSharedDecode is called only after a memory-cache miss. The wrapper
// shares conversion, not I/O or cache-update outcomes. Its finalizer runs after
// the enclosing read's deferred cache updates and transfers the ticket to the
// final entry (which helper reads may have replaced).
func (s *S3FS) prepareSharedDecode(vector *IOVector) (func(), error) {
	if s.decodedReads == nil || len(vector.Entries) != 1 || len(vector.Caches) != 0 || vector.Policy.Any(SkipMemoryCache) {
		return nil, nil
	}
	entry := &vector.Entries[0]
	sharing := entry.DecodeSharing
	if sharing == nil || sharing.Codec == "" || len(sharing.Codec) > 64 || len(vector.FilePath) > 4096 ||
		entry.Offset < 0 || entry.Size <= 0 || entry.CachedDataSize <= 0 || entry.CachedDataSize > int64(^uint(0)>>1) ||
		entry.ToCacheData == nil || entry.WriterForRead != nil || entry.ReadCloserForRead != nil || entry.ReaderForWrite != nil ||
		entry.Data != nil || entry.CachedData != nil || entry.decodeLease != nil {
		return nil, nil
	}
	path, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil {
		return nil, err
	}
	if !s.decodedReads.beginRead() {
		return nil, sharedDecodeClosed()
	}
	key := decodedReadKey{path: path.File, offset: entry.Offset, size: entry.Size, decoded: entry.CachedDataSize, policy: vector.Policy, codec: *sharing}
	original := entry.ToCacheData
	var lease *decodedReadLease
	entry.ToCacheData = func(ctx context.Context, reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
		if int64(len(data)) != key.size {
			return original(ctx, reader, data, allocator)
		}
		bytes := int64(max(s.memCache.BackingSize(int(key.decoded)), s.memCache.uncachedAllocator.BackingSize(int(key.decoded))))
		result, ticket, err := s.decodedReads.decode(ctx, key, bytes, func() (fscache.Data, error) { return original(ctx, reader, data, allocator) })
		lease = ticket
		return result, err
	}
	return func() {
		entry := &vector.Entries[0]
		entry.ToCacheData = original
		if entry.CachedData != nil {
			entry.decodeLease = lease
		} else {
			lease.release()
		}
		s.decodedReads.endRead()
	}, nil
}
