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

package fileservice

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// occupied is the room of each governor that occupy holds.
var occupied sync.Map

func occupiedRoom(g *governor) *atomic.Int64 {
	n, _ := occupied.LoadOrStore(g, new(atomic.Int64))
	return n.(*atomic.Int64)
}

// occupy holds n room of class c of g until the returned function frees it.
func occupy(t *testing.T, g *governor, c requestClass, n int) func() {
	t.Helper()
	for range n {
		require.NoError(t, g.wait(context.Background(), c))
		occupiedRoom(g).Add(1)
	}
	return sync.OnceFunc(func() {
		for range n {
			g.done(c)
			occupiedRoom(g).Add(-1)
		}
	})
}

// limitRoom holds room of g until the test ends or the returned function frees it, so
// that n of it is free, at most background of it for background requests.
func limitRoom(t *testing.T, g *governor, n, background int) func() {
	t.Helper()
	freeBackground := occupy(t, g, backgroundRequest, g.backgroundMax-background)
	freeForeground := occupy(t, g, foregroundRequest, g.capacity-g.backgroundMax-(n-background))
	free := sync.OnceFunc(func() {
		freeForeground()
		freeBackground()
	})
	t.Cleanup(free)
	return free
}

// waitingFor returns whether n requests of class c wait for room of g.
func waitingFor(g *governor, c requestClass, n int) func() bool {
	return func() bool {
		_, waiting := g.counts()
		return waiting[c] == n
	}
}

// heldRoom returns the room of g requests hold, beyond the room occupy holds.
func heldRoom(g *governor) int {
	running, _ := g.counts()
	return running[foregroundRequest] + running[backgroundRequest] - int(occupiedRoom(g).Load())
}

// openerStorage is object storage whose reads open their range through its function.
type openerStorage struct {
	ObjectStorage
	open func(ctx context.Context, start, end int64) (io.ReadCloser, error)
}

func (o openerStorage) Read(ctx context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	end := int64(-1)
	if max != nil {
		end = *max
	}
	return o.open(ctx, *min, end)
}

// openerSource returns the source of an object whose ranges open through open.
func openerSource(open func(ctx context.Context, start, end int64) (io.ReadCloser, error)) objectSource {
	return objectSource{g: newGovernor(openerStorage{open: open}, 1024), key: "object", end: -1}
}

// admitted reports whether a wait of class c on g is admitted within a short time.
func admitted(g *governor, c requestClass) (func(), bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if g.wait(ctx, c) != nil {
		return nil, false
	}
	return func() { g.done(c) }, true
}

// Waiting requests are admitted foreground first, then background, each in arrival order.
func TestGovernorOrder(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	defer occupy(t, g, foregroundRequest, minRequestRoom-1)()
	free := occupy(t, g, foregroundRequest, 1)
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	start := func(name string, c requestClass, waiting int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, g.wait(context.Background(), c))
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			g.done(c)
		}()
		require.Eventually(t, waitingFor(g, c, waiting), 5*time.Second, time.Millisecond)
	}
	start("b1", backgroundRequest, 1)
	start("f1", foregroundRequest, 1)
	start("b2", backgroundRequest, 2)
	start("f2", foregroundRequest, 2)
	free()
	wg.Wait()
	require.Equal(t, []string{"f1", "f2", "b1", "b2"}, order)
	require.Zero(t, heldRoom(g))
}

// A background request arriving while foreground waits is not admitted before it.
func TestGovernorBackgroundWaitsBehindForeground(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	free := occupy(t, g, foregroundRequest, minRequestRoom)
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.NoError(t, g.wait(context.Background(), foregroundRequest))
		g.done(foregroundRequest)
	}()
	require.Eventually(t, waitingFor(g, foregroundRequest, 1), 5*time.Second, time.Millisecond)
	_, ok := admitted(g, backgroundRequest)
	require.False(t, ok)
	free()
	<-done
	require.Zero(t, heldRoom(g))
}

// Background requests run at most half of the room; foreground may use the rest.
func TestGovernorBackgroundCap(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	free := occupy(t, g, backgroundRequest, minRequestRoom/2)
	_, ok := admitted(g, backgroundRequest)
	require.False(t, ok, "background holds its half")
	freeForeground := occupy(t, g, foregroundRequest, minRequestRoom/2)
	_, ok = admitted(g, foregroundRequest)
	require.False(t, ok, "the room is full")
	freeForeground()
	free()
	require.Zero(t, heldRoom(g))
}

// A room below minRequestRoom is raised to it, half of it for background requests.
func TestGovernorRoomAtLeastMinimum(t *testing.T) {
	for _, room := range []int{-1, 0, 1, 2, minRequestRoom - 1, minRequestRoom} {
		g := newGovernor(nil, room)
		require.Equal(t, minRequestRoom, g.capacity, "room %d", room)
		require.Equal(t, minRequestRoom/2, g.backgroundMax, "room %d", room)
	}
	g := newGovernor(nil, 3*minRequestRoom+1)
	require.Equal(t, 3*minRequestRoom+1, g.capacity)
	require.Equal(t, (3*minRequestRoom+1)/2, g.backgroundMax)
}

// A running request is never stopped: a foreground request waits for a background one to
// end.
func TestGovernorNoPreemption(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	free := occupy(t, g, backgroundRequest, minRequestRoom/2)
	freeForeground := occupy(t, g, foregroundRequest, minRequestRoom/2)
	defer freeForeground()
	_, ok := admitted(g, foregroundRequest)
	require.False(t, ok)
	running, _ := g.counts()
	require.Equal(t, minRequestRoom/2, running[backgroundRequest])
	free()
	release, ok := admitted(g, foregroundRequest)
	require.True(t, ok)
	release()
}

// A wait canceled while waiting, or racing its admission, leaves no room held.
func TestGovernorCancel(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	free := occupy(t, g, foregroundRequest, minRequestRoom)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- g.wait(ctx, foregroundRequest) }()
	require.Eventually(t, waitingFor(g, foregroundRequest, 1), 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
	free()
	require.Zero(t, heldRoom(g))
	_, waiting := g.counts()
	require.Zero(t, waiting[foregroundRequest])

	for range 200 {
		free := occupy(t, g, foregroundRequest, minRequestRoom)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { errCh <- g.wait(ctx, foregroundRequest) }()
		require.Eventually(t, waitingFor(g, foregroundRequest, 1), 5*time.Second, time.Microsecond)
		go free()
		cancel()
		if err := <-errCh; err == nil {
			g.done(foregroundRequest)
		}
		require.Eventually(t, func() bool { return heldRoom(g) == 0 }, 5*time.Second, time.Microsecond)
	}
}

type panicStorage struct{ ObjectStorage }

func (panicStorage) Stat(context.Context, string) (int64, error) { panic("stat") }

// A request whose call panics frees its room.
func TestGovernorPanicFreesRoom(t *testing.T) {
	g := newGovernor(panicStorage{}, minRequestRoom)
	defer occupy(t, g, foregroundRequest, minRequestRoom-1)()
	require.Panics(t, func() { _, _ = g.stat(context.Background(), "key") })
	require.Zero(t, heldRoom(g))
	release, ok := admitted(g, foregroundRequest)
	require.True(t, ok)
	release()
}

// Close ends the waits for room; running requests keep theirs until they end.
func TestGovernorCloseWithWaiters(t *testing.T) {
	g := newGovernor(nil, minRequestRoom)
	free := occupy(t, g, foregroundRequest, minRequestRoom)
	errCh := make(chan error, 2)
	for _, c := range []requestClass{foregroundRequest, backgroundRequest} {
		go func() { errCh <- g.wait(context.Background(), c) }()
		require.Eventually(t, waitingFor(g, c, 1), 5*time.Second, time.Millisecond)
	}
	g.close()
	require.ErrorIs(t, <-errCh, errGovernorClosed)
	require.ErrorIs(t, <-errCh, errGovernorClosed)
	running, _ := g.counts()
	require.Equal(t, minRequestRoom, running[foregroundRequest])
	free()
	require.Zero(t, heldRoom(g))
	require.ErrorIs(t, g.wait(context.Background(), foregroundRequest), errGovernorClosed)
}

type retryStorage struct {
	ObjectStorage
	g     *governor
	held  []int
	fails int
}

func (r *retryStorage) Stat(ctx context.Context, key string) (int64, error) {
	return DoWithRetryContext(ctx, "stat", func() (int64, error) {
		r.held = append(r.held, heldRoom(r.g))
		if len(r.held) <= r.fails {
			return 0, io.ErrUnexpectedEOF
		}
		return 1, nil
	}, maxRetryAttemps, IsRetryableError)
}

// A retry waits with no room held, and its next attempt holds room again: a request
// waiting behind it runs during the wait.
func TestGovernorRetryWaitsWithoutRoom(t *testing.T) {
	s := &retryStorage{fails: 2}
	g := newGovernor(s, minRequestRoom)
	s.g = g
	ctx := context.Background()
	freeOne := occupy(t, g, foregroundRequest, 1)
	defer occupy(t, g, foregroundRequest, minRequestRoom-1)()
	type result struct {
		size int64
		err  error
	}
	statDone := make(chan result, 1)
	go func() {
		size, err := g.stat(ctx, "key")
		statDone <- result{size, err}
	}()
	require.Eventually(t, waitingFor(g, foregroundRequest, 1), 5*time.Second, time.Millisecond)
	attemptsBefore := make(chan int, 1)
	go func() {
		if err := g.wait(ctx, foregroundRequest); err != nil {
			attemptsBefore <- -1
			return
		}
		attemptsBefore <- len(s.held)
		g.done(foregroundRequest)
	}()
	require.Eventually(t, waitingFor(g, foregroundRequest, 2), 5*time.Second, time.Millisecond)
	freeOne()
	r := <-statDone
	require.NoError(t, r.err)
	require.Equal(t, int64(1), r.size)
	require.Equal(t, 1, <-attemptsBefore, "the request behind runs during the first retry's wait")
	require.Equal(t, []int{1, 1, 1}, s.held, "each attempt holds room")
	require.Zero(t, heldRoom(g))
}

// readFlight reads the range of fl through open.
func readFlight(fl *memoryPrefetchObject, open func(ctx context.Context, start, end int64) (io.ReadCloser, error)) error {
	fl.src = openerSource(open)
	return fl.read(context.Background())
}

// getterStorage is object storage whose reads open their range through its function.
type getterStorage struct {
	ObjectStorage
	get func(ctx context.Context, min, max *int64) (io.ReadCloser, error)
}

func (g getterStorage) Read(ctx context.Context, _ string, min, max *int64) (io.ReadCloser, error) {
	return g.get(ctx, min, max)
}

// getterSource returns the source of a whole object whose ranges open through get.
func getterSource(get func(ctx context.Context, min, max *int64) (io.ReadCloser, error)) objectSource {
	return objectSource{g: newGovernor(getterStorage{get: get}, 1024), key: "object", end: -1}
}
