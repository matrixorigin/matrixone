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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

const (
	tailPoolMaxEntries = 256
	tailPoolMaxBytes   = int64(1 << 30)
	tailPoolIdleTTL    = 15 * time.Minute
)

type tailState struct {
	baseGeneration int64
	maxChunk       int64
	segments       []*segmentLease
	deletes        map[any]int64
	bytes          int64
	lastUsed       time.Time
}

type tailPool struct {
	mu         sync.Mutex
	states     map[string]*tailState
	totalBytes int64
	maxEntries int
	maxBytes   int64
	idleTTL    time.Duration
}

var loadedTailPool = newTailPool()

func newTailPool() *tailPool {
	return &tailPool{
		states:     make(map[string]*tailState),
		maxEntries: tailPoolMaxEntries,
		maxBytes:   tailPoolMaxBytes,
		idleTTL:    tailPoolIdleTTL,
	}
}

func (p *tailPool) limits() (int, int64, time.Duration) {
	maxEntries, maxBytes, idleTTL := p.maxEntries, p.maxBytes, p.idleTTL
	if maxEntries <= 0 {
		maxEntries = tailPoolMaxEntries
	}
	if maxBytes <= 0 {
		maxBytes = tailPoolMaxBytes
	}
	if idleTTL <= 0 {
		idleTTL = tailPoolIdleTTL
	}
	return maxEntries, maxBytes, idleTTL
}

// estimateSegmentBytes is intentionally conservative. Tail segments are
// build-side Go objects, so their exact allocator footprint is runtime-specific;
// this estimate still gives the pool a deterministic byte bound instead of
// treating a state containing millions of postings as one small map entry.
func estimateSegmentBytes(s *Segment) int64 {
	if s == nil {
		return 0
	}
	n := int64(len(s.pks))*24 + int64(len(s.pkOffsets))*4 + int64(len(s.pkRaw)) +
		int64(len(s.docLen))*4 + int64(len(s.includeTypes))*4 + int64(len(s.includeRaw)) +
		int64(len(s.includeVarOffsets))*4
	for _, values := range s.includeVals {
		n += int64(len(values)) * 16
	}
	for word, postings := range s.terms {
		n += int64(len(word)) + 64
		if postings == nil {
			continue
		}
		n += int64(len(postings.docIDs))*8 + int64(len(postings.tfs))*1 +
			int64(len(postings.positions))*24
		for _, positions := range postings.positions {
			n += int64(len(positions)) * 4
		}
	}
	return n
}

func estimateTailStateBytes(segs []*Segment, deletes map[any]int64) int64 {
	n := int64(len(segs)) * 8
	for _, seg := range segs {
		n += estimateSegmentBytes(seg)
	}
	n += int64(len(deletes)) * 32
	return n
}

func retireTailStates(states []*tailState) {
	for _, state := range states {
		for _, lease := range state.segments {
			lease.retire()
		}
	}
}

func (p *tailPool) evictLocked(now time.Time) (retire []*tailState) {
	_, _, idleTTL := p.limits()
	remove := func(index string, state *tailState) {
		delete(p.states, index)
		p.totalBytes -= state.bytes
		if p.totalBytes < 0 {
			p.totalBytes = 0
		}
		retire = append(retire, state)
	}
	for index, state := range p.states {
		if state.lastUsed.IsZero() {
			continue
		}
		if now.Sub(state.lastUsed) >= idleTTL {
			remove(index, state)
		}
	}
	maxEntries, maxBytes, _ := p.limits()
	for len(p.states) > maxEntries || p.totalBytes > maxBytes {
		var oldestIndex string
		var oldest *tailState
		for index, state := range p.states {
			if oldest == nil || state.lastUsed.Before(oldest.lastUsed) {
				oldestIndex, oldest = index, state
			}
		}
		if oldest == nil {
			break
		}
		remove(oldestIndex, oldest)
	}
	return retire
}

func (p *tailPool) evict(now time.Time) {
	p.mu.Lock()
	retire := p.evictLocked(now)
	p.mu.Unlock()
	retireTailStates(retire)
}

func cloneDeletes(src map[any]int64) map[any]int64 {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[any]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mergeTailDeletes(base, delta map[any]int64) map[any]int64 {
	merged := cloneDeletes(base)
	if merged == nil && len(delta) > 0 {
		merged = make(map[any]int64, len(delta))
	}
	for k, v := range delta {
		if cur, ok := merged[k]; !ok || v > cur {
			merged[k] = v
		}
	}
	return merged
}

func (p *tailPool) state(index string) *tailState {
	p.mu.Lock()
	retire := p.evictLocked(time.Now())
	state := p.states[index]
	if state != nil {
		state.lastUsed = time.Now()
	}
	p.mu.Unlock()
	retireTailStates(retire)
	return state
}

func (p *tailPool) acquireViews(index string, expected *tailState) ([]*Segment, map[any]int64, bool) {
	p.mu.Lock()
	retire := p.evictLocked(time.Now())
	if p.states[index] != expected {
		p.mu.Unlock()
		retireTailStates(retire)
		return nil, nil, false
	}
	expected.lastUsed = time.Now()
	segs, deletes := acquireTailViews(expected)
	p.mu.Unlock()
	retireTailStates(retire)
	return segs, deletes, true
}

func (p *tailPool) current(index string, expected *tailState) bool {
	p.mu.Lock()
	ok := p.states[index] == expected
	if ok {
		expected.lastUsed = time.Now()
	}
	p.mu.Unlock()
	return ok
}

func (p *tailPool) installDeltaAndAcquire(index string, expected, state *tailState) ([]*Segment, map[any]int64, bool) {
	p.mu.Lock()
	retire := p.evictLocked(time.Now())
	if p.states[index] != expected {
		p.mu.Unlock()
		retireTailStates(retire)
		return nil, nil, false
	}
	state.lastUsed = time.Now()
	p.totalBytes -= expected.bytes
	p.states[index] = state
	p.totalBytes += state.bytes
	segs, deletes := acquireTailViews(state)
	retire = append(retire, p.evictLocked(time.Now())...)
	p.mu.Unlock()
	retireTailStates(retire)
	return segs, deletes, true
}

func (p *tailPool) installAndAcquire(index string, state *tailState) ([]*Segment, map[any]int64) {
	p.mu.Lock()
	retire := p.evictLocked(time.Now())
	old := p.states[index]
	if old != nil {
		p.totalBytes -= old.bytes
	}
	state.lastUsed = time.Now()
	p.states[index] = state
	p.totalBytes += state.bytes
	segs, deletes := acquireTailViews(state)
	retire = append(retire, p.evictLocked(time.Now())...)
	p.mu.Unlock()
	if old == nil || old == state {
		retireTailStates(retire)
		return segs, deletes
	}
	// Appending a delta transfers old segment leases into the new state. A full
	// reset (MERGE/REBUILD or tail regression) has no shared leases to transfer.
	if len(state.segments) > 0 && len(old.segments) > 0 && state.segments[0] == old.segments[0] {
		retireTailStates(retire)
		return segs, deletes
	}
	retire = append(retire, old)
	retireTailStates(retire)
	return segs, deletes
}

func (p *tailPool) clear(index string) {
	p.mu.Lock()
	old := p.states[index]
	delete(p.states, index)
	if old != nil {
		p.totalBytes -= old.bytes
		if p.totalBytes < 0 {
			p.totalBytes = 0
		}
	}
	p.mu.Unlock()
	if old != nil {
		retireTailStates([]*tailState{old})
	}
}

func (p *tailPool) clearAll() {
	p.mu.Lock()
	states := make([]*tailState, 0, len(p.states))
	for index, state := range p.states {
		delete(p.states, index)
		states = append(states, state)
	}
	p.totalBytes = 0
	p.mu.Unlock()
	retireTailStates(states)
}

func acquireTailViews(state *tailState) ([]*Segment, map[any]int64) {
	if state == nil {
		return nil, nil
	}
	segs := make([]*Segment, 0, len(state.segments))
	for _, lease := range state.segments {
		if view := lease.acquire(0); view != nil {
			segs = append(segs, view)
		}
	}
	return segs, cloneDeletes(state.deletes)
}

func newTailState(baseGeneration, maxChunk int64, segs []*Segment, deletes map[any]int64) *tailState {
	leases := make([]*segmentLease, 0, len(segs))
	for _, seg := range segs {
		leases = append(leases, newSegmentLease(seg))
	}
	state := &tailState{
		baseGeneration: baseGeneration,
		maxChunk:       maxChunk,
		segments:       leases,
		deletes:        cloneDeletes(deletes),
		lastUsed:       time.Now(),
	}
	state.bytes = estimateTailStateBytes(segs, state.deletes)
	return state
}

// loadTailWithReuse reuses the prior tail generation when the durable tail
// maximum is unchanged, or reads only chunks after the prior maximum when it
// advanced. A base generation change or tail regression forces a full tail
// reload and retires the old tail leases after active readers release them.
func loadTailWithReuse(sqlproc *sqlexec.SqlProcess, cfg TableConfig, baseGeneration, tailMax int64, trace *loadTrace) ([]*Segment, map[any]int64, int64, error) {
	index := cfg.DbName + "." + cfg.IndexTable
	old := loadedTailPool.state(index)
	if old != nil && old.baseGeneration == baseGeneration && tailMax == old.maxChunk {
		segs, deletes, ok := loadedTailPool.acquireViews(index, old)
		if ok {
			return segs, deletes, old.maxChunk, nil
		}
		old = loadedTailPool.state(index)
	}

	after := int64(-1)
	if old != nil && old.baseGeneration == baseGeneration && tailMax > old.maxChunk {
		if loadedTailPool.current(index, old) {
			after = old.maxChunk
		} else {
			old = nil
		}
	}
	newSegs, newDeletes, appliedMax, err := loadTailSegmentsAfter(sqlproc, cfg, after, trace)
	if err != nil {
		if after < 0 {
			return nil, nil, 0, err
		}
		// A delta can begin in the middle of a multi-chunk frame when the
		// previous snapshot ended before the frame became visible. Retry the
		// complete tail so the frame header/prefix is available; persistent
		// corruption still fails on this second pass.
		newSegs, newDeletes, appliedMax, err = loadTailSegmentsAfter(sqlproc, cfg, -1, trace)
		if err != nil {
			return nil, nil, 0, err
		}
		old = nil
		after = -1
	}
	state := newTailState(baseGeneration, appliedMax, newSegs, newDeletes)
	if after >= 0 && old != nil {
		// Transfer old leases into the new state and append only the new frames.
		state.segments = append(append([]*segmentLease(nil), old.segments...), state.segments...)
		state.deletes = mergeTailDeletes(old.deletes, newDeletes)
		state.bytes = estimateTailStateBytes(nil, state.deletes)
		for _, lease := range state.segments {
			lease.mu.Lock()
			template := lease.template
			lease.mu.Unlock()
			state.bytes += estimateSegmentBytes(template)
		}
	}
	if after >= 0 && old != nil {
		segs, deletes, ok := loadedTailPool.installDeltaAndAcquire(index, old, state)
		if ok {
			return segs, deletes, appliedMax, nil
		}
		// Another invalidation replaced the state while the delta was being
		// read. Do not publish a delta-only state without its predecessor.
		for _, lease := range state.segments[len(old.segments):] {
			lease.retire()
		}
		newSegs, newDeletes, appliedMax, err = loadTailSegmentsAfter(sqlproc, cfg, -1, trace)
		if err != nil {
			return nil, nil, 0, err
		}
		state = newTailState(baseGeneration, appliedMax, newSegs, newDeletes)
	}
	segs, deletes := loadedTailPool.installAndAcquire(index, state)
	return segs, deletes, appliedMax, nil
}
