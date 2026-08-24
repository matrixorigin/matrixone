// Copyright 2026 Matrix Origin
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

package fulltext2

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// The pool is deliberately bounded independently of VectorIndexCache. A cache
	// entry can expire while an immutable base is still reusable, but that reuse
	// must not turn into a process-lifetime cache for every index ever queried.
	basePoolMaxEntries = 1024
	basePoolMaxBytes   = int64(8 << 30)
	basePoolIdleTTL    = 15 * time.Minute
)

// baseKey identifies the immutable bytes of one tag=0 segment. Recency is
// deliberately excluded: a metadata rewrite may change recency while the
// segment bytes remain identical, and each generation gets its own Segment
// header with the new recency.
type baseKey struct {
	index    string
	id       string
	checksum string
	filesize int64
}

type baseEntry struct {
	ready    chan struct{}
	lease    *segmentLease
	err      error
	loading  bool
	retired  bool
	lastUsed time.Time
}

// segmentLease owns one mmap and hands out shallow Segment views. The views
// have independent mutable headers (notably AvgDocLen and Recency) while all
// serialized slices/FST pointers remain shared and read-only.
type segmentLease struct {
	mu       sync.Mutex
	template *Segment
	refs     int // pool reference plus one reference per returned view
	pooled   bool
	retired  bool
}

func newSegmentLease(template *Segment) *segmentLease {
	return &segmentLease{template: template, refs: 1, pooled: true}
}

func (l *segmentLease) acquire(recency int64) *Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.template == nil {
		return nil
	}
	l.refs++
	view := *l.template
	if recency == 0 {
		recency = l.template.Recency
	}
	view.Recency = recency
	view.lease = l
	return &view
}

func (l *segmentLease) release() {
	template := func() *Segment {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.refs <= 0 {
			return nil
		}
		l.refs--
		if l.refs != 0 {
			return nil
		}
		template := l.template
		l.template = nil
		return template
	}()
	if template != nil {
		template.freeOwned()
	}
}

// retire removes the pool's reference. Active generations keep the mapping
// alive until their Segment views are freed.
func (l *segmentLease) retire() {
	template := func() *Segment {
		l.mu.Lock()
		defer l.mu.Unlock()
		if !l.pooled {
			return nil
		}
		l.pooled = false
		l.retired = true
		l.refs-- // drop the pool reference
		if l.refs != 0 {
			return nil
		}
		template := l.template
		l.template = nil
		return template
	}()
	if template != nil {
		template.freeOwned()
	}
}

type immutableBasePool struct {
	mu         sync.Mutex
	entries    map[baseKey]*baseEntry
	totalBytes int64
	maxEntries int
	maxBytes   int64
	idleTTL    time.Duration
}

var loadedBasePool = newImmutableBasePool()

func newImmutableBasePool() *immutableBasePool {
	return &immutableBasePool{
		entries:    make(map[baseKey]*baseEntry),
		maxEntries: basePoolMaxEntries,
		maxBytes:   basePoolMaxBytes,
		idleTTL:    basePoolIdleTTL,
	}
}

func (p *immutableBasePool) limits() (int, int64, time.Duration) {
	maxEntries, maxBytes, idleTTL := p.maxEntries, p.maxBytes, p.idleTTL
	if maxEntries <= 0 {
		maxEntries = basePoolMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = basePoolMaxBytes
	}
	if idleTTL <= 0 {
		idleTTL = basePoolIdleTTL
	}
	return maxEntries, maxBytes, idleTTL
}

func baseEntryBytes(key baseKey) int64 {
	if key.filesize <= 0 {
		return 0
	}
	return key.filesize
}

// evictLocked retires pool ownership for idle or over-cap entries. It must be
// called with p.mu held; the returned leases are retired after unlocking so
// Segment.Free never runs while the pool map is locked.
func (p *immutableBasePool) evictLocked(now time.Time) (retire []*segmentLease) {
	_, _, idleTTL := p.limits()
	remove := func(key baseKey, entry *baseEntry) {
		delete(p.entries, key)
		p.totalBytes -= baseEntryBytes(key)
		if p.totalBytes < 0 {
			p.totalBytes = 0
		}
		if entry.lease != nil {
			retire = append(retire, entry.lease)
		}
	}
	for key, entry := range p.entries {
		if entry.loading || entry.lease == nil || entry.lastUsed.IsZero() {
			continue
		}
		if now.Sub(entry.lastUsed) >= idleTTL {
			remove(key, entry)
		}
	}
	maxEntries, maxBytes, _ := p.limits()
	for len(p.entries) > maxEntries || p.totalBytes > maxBytes {
		var oldestKey baseKey
		var oldest *baseEntry
		for key, entry := range p.entries {
			if entry.loading || entry.lease == nil {
				continue
			}
			if oldest == nil || entry.lastUsed.Before(oldest.lastUsed) {
				oldestKey, oldest = key, entry
			}
		}
		if oldest == nil {
			break // a loading entry will be accounted for when its load completes
		}
		remove(oldestKey, oldest)
	}
	return retire
}

// retireLoadingLocked marks an in-flight entry so its loader will not repool
// the result when it completes. The entry stays in the map until the loader
// closes ready; this keeps existing waiters from being stranded on a channel
// that can never be signaled.
func (p *immutableBasePool) retireLoadingLocked(entry *baseEntry) {
	if entry != nil && entry.loading {
		entry.retired = true
	}
}

func retireBaseLeases(leases []*segmentLease) {
	for _, lease := range leases {
		lease.retire()
	}
}

func (p *immutableBasePool) evict(now time.Time) {
	retire := func() []*segmentLease {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.evictLocked(now)
	}()
	retireBaseLeases(retire)
}

func (p *immutableBasePool) acquire(ctx context.Context, key baseKey, load func() (*Segment, error), recency int64) (*Segment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.evict(time.Now())
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		e, ready, view, found := func() (*baseEntry, chan struct{}, *Segment, bool) {
			p.mu.Lock()
			defer p.mu.Unlock()
			if e, ok := p.entries[key]; ok {
				if e.loading {
					return nil, e.ready, nil, true
				}
				e.lastUsed = time.Now()
				return nil, nil, e.lease.acquire(recency), true
			}
			e := &baseEntry{ready: make(chan struct{}), loading: true, lastUsed: time.Now()}
			p.entries[key] = e
			return e, nil, nil, false
		}()
		if found {
			if ready != nil {
				select {
				case <-ready:
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
				continue
			}
			if view == nil {
				continue
			}
			return view, nil
		}

		template, err := func() (template *Segment, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = baseLeaseError(fmt.Sprintf("fulltext2 base load panic: %v", r))
				}
			}()
			return load()
		}()
		retired, resultErr, directTemplate := func() (bool, error, *Segment) {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.loading = false
			e.err = err
			if e.retired {
				delete(p.entries, key)
				e.err = errBaseLeaseGone
				close(e.ready)
				if err != nil {
					return true, err, template
				}
				if template == nil {
					return true, errBaseLeaseGone, nil
				}
				return true, nil, template
			}
			if err == nil {
				if template == nil {
					err = errBaseLeaseGone
					e.err = err
				} else {
					e.lease = newSegmentLease(template)
					e.lastUsed = time.Now()
					p.totalBytes += baseEntryBytes(key)
				}
			}
			close(e.ready)
			if err != nil {
				delete(p.entries, key)
			}
			return false, err, nil
		}()
		if retired {
			if directTemplate != nil && resultErr != nil {
				directTemplate.Free()
			}
			if resultErr != nil {
				return nil, resultErr
			}
			if directTemplate == nil {
				return nil, errBaseLeaseGone
			}
			if ctxErr := context.Cause(ctx); ctxErr != nil {
				directTemplate.Free()
				return nil, ctxErr
			}
			// The invalidation retired the pool entry while this query was
			// loading it. Let this in-flight query finish with direct ownership;
			// the stale closure result is never repooled or retried here.
			return directTemplate, nil
		}
		if resultErr != nil {
			if template != nil {
				template.Free()
			}
			return nil, resultErr
		}
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		view = func() *Segment {
			p.mu.Lock()
			defer p.mu.Unlock()
			e.lastUsed = time.Now()
			return e.lease.acquire(recency)
		}()
		if view == nil {
			continue
		}
		p.evict(time.Now())
		return view, nil
	}
}

// commit keeps only the base keys used by the successfully assembled
// generation. A retired lease remains alive for old readers but cannot be
// acquired by a later generation.
func (p *immutableBasePool) commit(index string, used map[baseKey]struct{}) {
	retire := func() []*segmentLease {
		p.mu.Lock()
		defer p.mu.Unlock()
		retire := make([]*segmentLease, 0, len(p.entries))
		for key, entry := range p.entries {
			if key.index != index {
				continue
			}
			if entry.loading {
				p.retireLoadingLocked(entry)
				continue
			}
			if _, ok := used[key]; ok {
				entry.lastUsed = time.Now()
				continue
			}
			delete(p.entries, key)
			p.totalBytes -= baseEntryBytes(key)
			retire = append(retire, entry.lease)
		}
		return append(retire, p.evictLocked(time.Now())...)
	}()
	retireBaseLeases(retire)
}

func (p *immutableBasePool) clearIndex(index string) {
	p.commit(index, nil)
}

func (p *immutableBasePool) clearAll() {
	retire := func() []*segmentLease {
		p.mu.Lock()
		defer p.mu.Unlock()
		retire := make([]*segmentLease, 0, len(p.entries))
		for key, entry := range p.entries {
			if entry.loading {
				p.retireLoadingLocked(entry)
				continue
			}
			delete(p.entries, key)
			if entry.lease != nil {
				retire = append(retire, entry.lease)
			}
		}
		p.totalBytes = 0
		return retire
	}()
	retireBaseLeases(retire)
}

// errBaseLeaseGone is only reachable if a pool entry is retired concurrently
// with an acquire. The caller can retry the metadata/load sequence safely.
var errBaseLeaseGone = baseLeaseError("fulltext2 base lease retired")

type baseLeaseError string

func (e baseLeaseError) Error() string { return string(e) }
