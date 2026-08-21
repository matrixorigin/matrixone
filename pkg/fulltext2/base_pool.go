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
	"fmt"
	"sync"
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
	ready   chan struct{}
	lease   *segmentLease
	err     error
	loading bool
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
	if l.template == nil {
		l.mu.Unlock()
		return nil
	}
	l.refs++
	view := *l.template
	if recency == 0 {
		recency = l.template.Recency
	}
	view.Recency = recency
	view.lease = l
	l.mu.Unlock()
	return &view
}

func (l *segmentLease) release() {
	l.mu.Lock()
	if l.refs <= 0 {
		l.mu.Unlock()
		return
	}
	l.refs--
	if l.refs != 0 {
		l.mu.Unlock()
		return
	}
	template := l.template
	l.template = nil
	l.mu.Unlock()
	if template != nil {
		template.freeOwned()
	}
}

// retire removes the pool's reference. Active generations keep the mapping
// alive until their Segment views are freed.
func (l *segmentLease) retire() {
	l.mu.Lock()
	if !l.pooled {
		l.mu.Unlock()
		return
	}
	l.pooled = false
	l.retired = true
	l.refs-- // drop the pool reference
	if l.refs != 0 {
		l.mu.Unlock()
		return
	}
	template := l.template
	l.template = nil
	l.mu.Unlock()
	if template != nil {
		template.freeOwned()
	}
}

type immutableBasePool struct {
	mu      sync.Mutex
	entries map[baseKey]*baseEntry
}

var loadedBasePool = &immutableBasePool{entries: make(map[baseKey]*baseEntry)}

func (p *immutableBasePool) acquire(key baseKey, load func() (*Segment, error), recency int64) (*Segment, error) {
	for {
		p.mu.Lock()
		if e, ok := p.entries[key]; ok {
			if e.loading {
				ready := e.ready
				p.mu.Unlock()
				<-ready
				continue
			}
			view := e.lease.acquire(recency)
			p.mu.Unlock()
			if view == nil {
				continue
			}
			return view, nil
		}
		e := &baseEntry{ready: make(chan struct{}), loading: true}
		p.entries[key] = e
		p.mu.Unlock()

		template, err := func() (template *Segment, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = baseLeaseError(fmt.Sprintf("fulltext2 base load panic: %v", r))
				}
			}()
			return load()
		}()
		if err != nil && template != nil {
			template.Free()
		}
		p.mu.Lock()
		e.loading = false
		e.err = err
		if err == nil {
			if template == nil {
				err = errBaseLeaseGone
				e.err = err
			} else {
				e.lease = newSegmentLease(template)
			}
		}
		close(e.ready)
		if err != nil {
			delete(p.entries, key)
		}
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		view := e.lease.acquire(recency)
		p.mu.Unlock()
		if view == nil {
			continue
		}
		return view, nil
	}
}

// commit keeps only the base keys used by the successfully assembled
// generation. A retired lease remains alive for old readers but cannot be
// acquired by a later generation.
func (p *immutableBasePool) commit(index string, used map[baseKey]struct{}) {
	var retire []*segmentLease
	p.mu.Lock()
	for key, entry := range p.entries {
		if key.index != index || entry.loading {
			continue
		}
		if _, ok := used[key]; ok {
			continue
		}
		delete(p.entries, key)
		retire = append(retire, entry.lease)
	}
	p.mu.Unlock()
	for _, l := range retire {
		l.retire()
	}
}

func (p *immutableBasePool) clearIndex(index string) {
	p.commit(index, nil)
}

// errBaseLeaseGone is only reachable if a pool entry is retired concurrently
// with an acquire. The caller can retry the metadata/load sequence safely.
var errBaseLeaseGone = baseLeaseError("fulltext2 base lease retired")

type baseLeaseError string

func (e baseLeaseError) Error() string { return string(e) }
