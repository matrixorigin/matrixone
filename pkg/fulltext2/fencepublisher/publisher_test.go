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

package fencepublisher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/require"
)

func testPublisher() *Publisher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Publisher{
		ctx: ctx, cancel: cancel, wake: make(chan struct{}, workerCount), rpcSem: make(chan struct{}, rpcParallel),
		pending: make(map[string]pendingFence), active: make(map[string]fulltext2.Generation), delays: []time.Duration{0, 0},
	}
}

func TestConcurrentBroadcastsShareRPCParallelBound(t *testing.T) {
	p := testPublisher()
	nodes := make([]metadata.CNService, rpcParallel+4)
	for i := range nodes {
		nodes[i] = metadata.CNService{ServiceID: fmt.Sprintf("n-%d", i), QueryAddress: "q"}
	}
	p.nodesFn = func(context.Context) ([]metadata.CNService, error) { return nodes, nil }
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	p.sendFn = func(pendingFence, metadata.CNService) bool {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		<-release
		return true
	}
	done := make(chan struct{}, 2)
	for _, name := range []string{"a", "b"} {
		go func(name string) {
			p.broadcast(pendingFence{identity: testIdentity(name), generation: fulltext2.Generation{BaseTimestamp: 1}})
			done <- struct{}{}
		}(name)
	}
	require.Eventually(t, func() bool { return active.Load() == rpcParallel }, time.Second, time.Millisecond)
	require.Equal(t, int32(rpcParallel), maximum.Load())
	close(release)
	<-done
	<-done
}

func TestActiveBroadcastCoalescesAndYieldsToNewestGeneration(t *testing.T) {
	p := testPublisher()
	id := testIdentity("s")
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 1})
	active, ok := p.pop()
	require.True(t, ok)
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 2})
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 3})
	_, ok = p.pop()
	require.False(t, ok)
	require.True(t, p.superseded(active))
	p.finish(active)
	newest, ok := p.pop()
	require.True(t, ok)
	require.Equal(t, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 3}, newest.generation)
}

func testIdentity(name string) fulltext2.CacheIdentity {
	return fulltext2.CacheIdentity{AccountID: 1, Database: "db", StorageTable: name, MetadataTable: "m-" + name}
}

func TestQueueCoalescesNewestGeneration(t *testing.T) {
	p := testPublisher()
	id := testIdentity("s")
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 1})
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 3})
	p.Enqueue(id, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 2})
	item, ok := p.pop()
	require.True(t, ok)
	require.Equal(t, fulltext2.Generation{BaseTimestamp: 1, TailChunk: 3}, item.generation)
}

func TestQueueCapacityIsBounded(t *testing.T) {
	p := testPublisher()
	for i := 0; i < queueCapacity+1; i++ {
		p.Enqueue(testIdentity(fmt.Sprintf("s-%d", i)), fulltext2.Generation{BaseTimestamp: 1})
	}
	p.mu.Lock()
	require.Len(t, p.pending, queueCapacity)
	p.mu.Unlock()
}

func TestBroadcastRetriesOnlyFailedTargets(t *testing.T) {
	p := testPublisher()
	nodes := []metadata.CNService{{ServiceID: "a", QueryAddress: "a"}, {ServiceID: "b", QueryAddress: "b"}}
	p.nodesFn = func(context.Context) ([]metadata.CNService, error) { return nodes, nil }
	var mu sync.Mutex
	calls := map[string]int{}
	p.sendFn = func(_ pendingFence, cn metadata.CNService) bool {
		mu.Lock()
		defer mu.Unlock()
		calls[cn.ServiceID]++
		return cn.ServiceID == "a" || calls[cn.ServiceID] == 2
	}
	p.broadcast(pendingFence{identity: testIdentity("s"), generation: fulltext2.Generation{BaseTimestamp: 1}})
	require.Equal(t, 1, calls["a"])
	require.Equal(t, 2, calls["b"])
}

func TestBroadcastRefreshesLiveTargetsBetweenAttempts(t *testing.T) {
	p := testPublisher()
	lookup := 0
	p.nodesFn = func(context.Context) ([]metadata.CNService, error) {
		lookup++
		if lookup == 1 {
			return []metadata.CNService{{ServiceID: "a", QueryAddress: "a"}}, nil
		}
		return []metadata.CNService{{ServiceID: "a", QueryAddress: "a"}, {ServiceID: "joining", QueryAddress: "joining"}}, nil
	}
	calls := map[string]int{}
	var callsMu sync.Mutex
	p.sendFn = func(_ pendingFence, cn metadata.CNService) bool {
		callsMu.Lock()
		defer callsMu.Unlock()
		calls[cn.ServiceID]++
		return cn.ServiceID == "joining" || calls[cn.ServiceID] == 2
	}
	p.broadcast(pendingFence{identity: testIdentity("s"), generation: fulltext2.Generation{BaseTimestamp: 1}})
	require.Equal(t, 2, lookup)
	require.Equal(t, 2, calls["a"])
	require.Equal(t, 1, calls["joining"])
}

func TestBroadcastAllFailureIsBoundedAndCancelable(t *testing.T) {
	p := testPublisher()
	p.nodesFn = func(context.Context) ([]metadata.CNService, error) {
		return []metadata.CNService{{ServiceID: "a", QueryAddress: "a"}}, nil
	}
	calls := 0
	p.sendFn = func(pendingFence, metadata.CNService) bool { calls++; return false }
	p.broadcast(pendingFence{identity: testIdentity("s")})
	require.Equal(t, len(p.delays), calls)

	p.delays = []time.Duration{time.Hour}
	done := make(chan struct{})
	go func() {
		p.broadcast(pendingFence{identity: testIdentity("cancel")})
		close(done)
	}()
	p.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast did not stop on cancellation")
	}
}

func TestAckRequiresClaimAndSufficientGeneration(t *testing.T) {
	want := fulltext2.Generation{BaseTimestamp: 4, TailChunk: 8}
	require.False(t, ackAccepts(querypb.Fulltext2CacheFenceResponse{
		RequiredBaseTimestamp: 4, RequiredTailChunk: 8,
	}, want))
	require.False(t, ackAccepts(querypb.Fulltext2CacheFenceResponse{
		RequiredBaseTimestamp: 4, RequiredTailChunk: 7, EvictionClaimed: true,
	}, want))
	require.True(t, ackAccepts(querypb.Fulltext2CacheFenceResponse{
		RequiredBaseTimestamp: 5, RequiredTailChunk: -1, EvictionClaimed: true,
	}, want))
}
