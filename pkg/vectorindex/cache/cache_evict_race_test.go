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

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockingDestroySearch is a cache entry whose teardown blocks until released, so a test can hold an
// eviction "in flight" (entry already removed from the map, Destroy not yet finished) and observe
// what a concurrent evictor does in that window.
type blockingDestroySearch struct {
	MockSearch
	started chan struct{} // closed when Destroy begins
	release chan struct{} // Destroy returns only after this is closed
}

func (m *blockingDestroySearch) Destroy() {
	close(m.started)
	<-m.release
}

// TestEvictKeyWaitsForInFlightEviction proves EvictKey does not report a premature completion when
// it loses the claim to a concurrent evictor (#28985). Evictor A wins the key and parks inside
// Destroy (entry already removed from the map, native handle still alive). Evictor B, finding the
// entry gone, must WAIT for A's teardown to finish before returning 0 -- not return immediately
// while the old resource is still live.
func TestEvictKeyWaitsForInFlightEviction(t *testing.T) {
	c := NewVectorIndexCache()
	t.Cleanup(func() { c.Destroy() })

	const key = "victim"
	mock := &blockingDestroySearch{started: make(chan struct{}), release: make(chan struct{})}
	c.IndexMap.Store(key, newVectorIndexSearch(mock))

	// A wins the eviction and blocks inside Destroy.
	aReturned := make(chan int64, 1)
	go func() { aReturned <- c.EvictKey(key) }()
	<-mock.started // A has removed the entry and is now inside Destroy (in-flight).

	// B loses the claim (entry gone) and must block until A's teardown completes.
	bReturned := make(chan int64, 1)
	go func() { bReturned <- c.EvictKey(key) }()

	// B must NOT report completion while A's Destroy is still running.
	select {
	case <-bReturned:
		t.Fatal("EvictKey returned before the in-flight eviction finished (premature success)")
	case <-aReturned:
		t.Fatal("A's Destroy returned though it was still blocked")
	case <-time.After(200 * time.Millisecond):
		// still blocked, as required.
	}

	// Let A's teardown finish; now both calls settle.
	close(mock.release)
	require.Equal(t, int64(1), <-aReturned, "A performed the eviction")
	select {
	case got := <-bReturned:
		require.Equal(t, int64(0), got, "B removed nothing, but only returned after the teardown settled")
	case <-time.After(5 * time.Second):
		t.Fatal("EvictKey blocked forever after the in-flight eviction completed")
	}

	// The key is fully gone and no in-flight eviction remains registered.
	require.Zero(t, c.CountKey(key))
	_, stillInFlight := c.evictInFlight.Load(key)
	require.False(t, stillInFlight, "the in-flight eviction registry entry must be cleared")
}
