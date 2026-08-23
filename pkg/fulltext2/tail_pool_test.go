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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTailPoolTransfersOnlyCurrentState(t *testing.T) {
	p := &tailPool{states: make(map[string]*tailState)}
	first := newTailState(1, 10, []*Segment{NewSegment("tail-10", 0)}, map[any]int64{"old": 10})
	second := newTailState(1, 11, []*Segment{NewSegment("tail-11", 0)}, map[any]int64{"old": 10, "new": 11})
	index := "db.store"
	p.mu.Lock()
	p.states[index] = first
	p.mu.Unlock()

	second.segments = append(append([]*segmentLease(nil), first.segments...), second.segments...)
	views, deletes, ok := p.installDeltaAndAcquire(index, first, second)
	require.True(t, ok)
	require.Len(t, views, 2)
	require.Equal(t, int64(10), deletes["old"])
	require.Equal(t, int64(11), deletes["new"])
	for _, view := range views {
		view.Free()
	}

	p.clear(index)
	require.Nil(t, first.segments[0].template)
	require.Nil(t, second.segments[1].template)
}

func TestTailPoolRejectsStaleDeltaInstall(t *testing.T) {
	p := &tailPool{states: make(map[string]*tailState)}
	index := "db.store"
	first := newTailState(1, 10, []*Segment{NewSegment("tail-10", 0)}, nil)
	other := newTailState(1, 12, []*Segment{NewSegment("tail-12", 0)}, nil)
	state := newTailState(1, 11, []*Segment{NewSegment("tail-11", 0)}, nil)
	state.segments = append(append([]*segmentLease(nil), first.segments...), state.segments...)
	p.mu.Lock()
	p.states[index] = other
	p.mu.Unlock()

	views, _, ok := p.installDeltaAndAcquire(index, first, state)
	require.False(t, ok)
	require.Nil(t, views)
	for _, lease := range state.segments[1:] {
		lease.retire()
	}
	p.clear(index)
}

func TestMergeTailDeletesInitializesInsertOnlyDelta(t *testing.T) {
	merged := mergeTailDeletes(nil, map[any]int64{"new": 11})
	require.Equal(t, map[any]int64{"new": int64(11)}, merged)
}

func TestTailPoolBoundsIndexesAndEvictsIdle(t *testing.T) {
	p := &tailPool{
		states:     make(map[string]*tailState),
		maxEntries: 1,
		maxBytes:   1 << 20,
		idleTTL:    time.Hour,
	}
	for i, index := range []string{"db.one", "db.two"} {
		state := newTailState(1, int64(i), []*Segment{NewSegment(index, 0)}, nil)
		views, _ := p.installAndAcquire(index, state)
		for _, view := range views {
			view.Free()
		}
	}
	require.LessOrEqual(t, len(p.states), 1)

	p.idleTTL = time.Nanosecond
	p.evict(time.Now().Add(time.Second))
	require.Empty(t, p.states)
	p.clearAll()
}
