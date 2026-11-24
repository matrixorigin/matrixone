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

package checkpoint

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

type enqueueContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

// Observe the cancellation being read by the real catalog scanner.
type observedReplayContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *observedReplayContext) Value(key any) any {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Value(key)
}

func TestReplayCatalogWaitsForAcceptedTasksOnCancellation(t *testing.T) {
	cat := catalog.MockCatalog(nil)
	r := &CkpReplayer{
		r:              &runner{catalog: cat},
		ckpEntries:     []*CheckpointEntry{{}},
		objectCountMap: make(map[uint64]int),
	}
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	q := sm.NewSafeQueue(1, 1, func(items ...any) {
		for _, item := range items {
			item.(func())()
			r.wg.Done()
		}
	})
	r.objectReplayWorker = []sm.ContextQueue{q}
	q.Start()
	var replayDone chan struct{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		q.Stop()
		if replayDone != nil {
			select {
			case <-replayDone:
			case <-time.After(5 * time.Second):
				t.Error("catalog replay goroutine did not stop")
				return
			}
		}
		cat.Close()
	})
	require.NoError(t, r.Submit(context.Background(), 1, func() {
		close(started)
		<-release
	}))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted replay task did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observed := &observedReplayContext{Context: ctx, observed: make(chan struct{})}
	result := make(chan error, 1)
	replayDone = make(chan struct{})
	go func() {
		defer close(replayDone)
		result <- r.ReplayCatalog(observed, txnbase.MockTxnReaderWithNow(), "test")
	}()
	select {
	case <-observed.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog scanner did not observe cancellation")
	}
	select {
	case err := <-result:
		t.Fatalf("catalog replay returned before the accepted task finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("catalog replay did not return after the accepted task finished")
	}
}

func (c *enqueueContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestReplaySubmissionCancellationDrainsAcceptedWork(t *testing.T) {
	r := &CkpReplayer{objectCountMap: make(map[uint64]int)}
	q := sm.NewSafeQueue(1, 1, func(items ...any) {
		for _, item := range items {
			item.(func())()
			r.wg.Done()
		}
	})
	r.objectReplayWorker = []sm.ContextQueue{q}
	q.Start()
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		q.Stop()
	})

	started := make(chan struct{})
	var ran atomic.Int32
	require.NoError(t, r.Submit(context.Background(), 1, func() {
		close(started)
		<-release
		ran.Add(1)
	}))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first replay task did not start")
	}
	require.NoError(t, r.Submit(context.Background(), 2, func() { ran.Add(1) }))

	waiting := &enqueueContext{Context: ctx, entered: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- r.Submit(waiting, 3, func() { ran.Add(100) }) }()
	select {
	case <-waiting.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("third replay task did not reach the full queue")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled submission remained blocked")
	}
	require.Equal(t, map[uint64]int{1: 1, 2: 1}, r.objectCountMap)

	releaseOnce.Do(func() { close(release) })
	drained := make(chan struct{})
	go func() { r.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted replay tasks did not drain")
	}
	require.EqualValues(t, 2, ran.Load())
}
