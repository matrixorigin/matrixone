// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeForeignConn struct {
	mu     sync.Mutex
	closed int
}

func (c *fakeForeignConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	return nil
}

func (c *fakeForeignConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func TestSessionForeignConnCache(t *testing.T) {
	ses := &Session{}

	// empty cache
	_, ok := ses.GetForeignConn("h1")
	require.False(t, ok)
	_, ok = ses.RemoveForeignConn("h1")
	require.False(t, ok)
	ses.closeForeignConns() // no-op on empty

	// put + get; Put returns the stored conn
	c1 := &fakeForeignConn{}
	w, err := ses.PutForeignConn(context.TODO(), "h1", c1)
	require.NoError(t, err)
	require.Same(t, c1, w)
	got, ok := ses.GetForeignConn("h1")
	require.True(t, ok)
	require.Same(t, c1, got)

	// first-wins: a racing Put under the same handle keeps the existing entry
	// and returns it; the cache never closes either connection (the losing
	// caller closes its own).
	c2 := &fakeForeignConn{}
	w, err = ses.PutForeignConn(context.TODO(), "h1", c2)
	require.NoError(t, err)
	require.Same(t, c1, w)
	require.Equal(t, 0, c1.closeCount())
	require.Equal(t, 0, c2.closeCount())
	got, ok = ses.GetForeignConn("h1")
	require.True(t, ok)
	require.Same(t, c1, got)

	// remove detaches without closing (caller closes)
	removed, ok := ses.RemoveForeignConn("h1")
	require.True(t, ok)
	require.Same(t, c1, removed)
	require.Equal(t, 0, c1.closeCount())
	_, ok = ses.GetForeignConn("h1")
	require.False(t, ok)

	// after remove, a new Put stores the new conn
	w, err = ses.PutForeignConn(context.TODO(), "h1", c2)
	require.NoError(t, err)
	require.Same(t, c2, w)
	removed, ok = ses.RemoveForeignConn("h1")
	require.True(t, ok)
	require.Same(t, c2, removed)

	// closeForeignConns closes everything left and clears the cache
	c3, c4 := &fakeForeignConn{}, &fakeForeignConn{}
	ses.PutForeignConn(context.TODO(), "h3", c3)
	ses.PutForeignConn(context.TODO(), "h4", c4)
	ses.closeForeignConns()
	require.Equal(t, 1, c3.closeCount())
	require.Equal(t, 1, c4.closeCount())
	_, ok = ses.GetForeignConn("h3")
	require.False(t, ok)

	// cache remains usable after closeForeignConns
	c5 := &fakeForeignConn{}
	ses.PutForeignConn(context.TODO(), "h5", c5)
	_, ok = ses.GetForeignConn("h5")
	require.True(t, ok)
	ses.closeForeignConns()
	require.Equal(t, 1, c5.closeCount())
}

// TestSessionForeignConnCacheConcurrent exercises the cache from many
// goroutines, including a concurrent session close, under the race detector.
func TestSessionForeignConnCacheConcurrent(t *testing.T) {
	ses := &Session{}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			h := "h" + string(rune('a'+g))
			for i := 0; i < 200; i++ {
				ses.PutForeignConn(context.TODO(), h, &fakeForeignConn{})
				ses.GetForeignConn(h)
				if c, ok := ses.RemoveForeignConn(h); ok {
					_ = c.Close()
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ses.closeForeignConns()
		}
	}()
	wg.Wait()
	ses.closeForeignConns()
}

// TestSessionForeignConnCacheBound proves admission is bounded: the cap'th+1
// distinct handle is rejected with an actionable error and nothing is stored,
// and removing an entry frees a slot.
func TestSessionForeignConnCacheBound(t *testing.T) {
	ses := &Session{}
	for i := 0; i < maxForeignConns; i++ {
		_, err := ses.PutForeignConn(context.TODO(), fmt.Sprintf("h%d", i), &fakeForeignConn{})
		require.NoError(t, err)
	}
	// over the cap: rejected, not stored
	rejected := &fakeForeignConn{}
	_, err := ses.PutForeignConn(context.TODO(), "overflow", rejected)
	require.ErrorContains(t, err, "disconnect unused handles")
	_, ok := ses.GetForeignConn("overflow")
	require.False(t, ok)
	// an existing handle still resolves (first-wins path is not an admission)
	w, err := ses.PutForeignConn(context.TODO(), "h0", &fakeForeignConn{})
	require.NoError(t, err)
	require.NotNil(t, w)
	// removing one frees a slot
	_, ok = ses.RemoveForeignConn("h1")
	require.True(t, ok)
	_, err = ses.PutForeignConn(context.TODO(), "overflow", rejected)
	require.NoError(t, err)
	ses.closeForeignConns()
}
