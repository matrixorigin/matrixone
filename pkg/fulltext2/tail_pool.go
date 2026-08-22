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

	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type tailState struct {
	baseGeneration int64
	maxChunk       int64
	segments       []*segmentLease
	deletes        map[any]int64
}

type tailPool struct {
	mu     sync.Mutex
	states map[string]*tailState
}

var loadedTailPool = &tailPool{states: make(map[string]*tailState)}

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
	defer p.mu.Unlock()
	return p.states[index]
}

func (p *tailPool) acquireViews(index string, expected *tailState) ([]*Segment, map[any]int64, bool) {
	p.mu.Lock()
	if p.states[index] != expected {
		p.mu.Unlock()
		return nil, nil, false
	}
	segs, deletes := acquireTailViews(expected)
	p.mu.Unlock()
	return segs, deletes, true
}

func (p *tailPool) current(index string, expected *tailState) bool {
	p.mu.Lock()
	ok := p.states[index] == expected
	p.mu.Unlock()
	return ok
}

func (p *tailPool) installDeltaAndAcquire(index string, expected, state *tailState) ([]*Segment, map[any]int64, bool) {
	p.mu.Lock()
	if p.states[index] != expected {
		p.mu.Unlock()
		return nil, nil, false
	}
	p.states[index] = state
	segs, deletes := acquireTailViews(state)
	p.mu.Unlock()
	return segs, deletes, true
}

func (p *tailPool) installAndAcquire(index string, state *tailState) ([]*Segment, map[any]int64) {
	p.mu.Lock()
	old := p.states[index]
	p.states[index] = state
	segs, deletes := acquireTailViews(state)
	p.mu.Unlock()
	if old == nil || old == state {
		return segs, deletes
	}
	// Appending a delta transfers old segment leases into the new state. A full
	// reset (MERGE/REBUILD or tail regression) has no shared leases to transfer.
	if len(state.segments) > 0 && len(old.segments) > 0 && state.segments[0] == old.segments[0] {
		return segs, deletes
	}
	for _, lease := range old.segments {
		lease.retire()
	}
	return segs, deletes
}

func (p *tailPool) clear(index string) {
	p.mu.Lock()
	old := p.states[index]
	delete(p.states, index)
	p.mu.Unlock()
	if old != nil {
		for _, lease := range old.segments {
			lease.retire()
		}
	}
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
	return &tailState{
		baseGeneration: baseGeneration,
		maxChunk:       maxChunk,
		segments:       leases,
		deletes:        cloneDeletes(deletes),
	}
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
