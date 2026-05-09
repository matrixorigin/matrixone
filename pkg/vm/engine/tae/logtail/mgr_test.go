// Copyright 2021 Matrix Origin
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

package logtail

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// goSubmit runs fn in a fresh goroutine; used as submit in tests to avoid
// pulling in the ants pool dependency.
func goSubmit(fn func()) { go fn() }

// syncSubmit runs fn synchronously; exercises the degenerate case where
// collect effectively runs on the caller.
func syncSubmit(fn func()) { fn() }

func TestOrderedCollectAndPublish_Empty(t *testing.T) {
	called := false
	orderedCollectAndPublish(
		0,
		func(int) bool { return false },
		goSubmit,
		func(int) *txnWithLogtails { called = true; return nil },
		func(*txnWithLogtails) { called = true },
	)
	require.False(t, called, "neither collect nor publish should run for n=0")
}

func TestOrderedCollectAndPublish_AllSkip(t *testing.T) {
	var collectCalls, publishCalls atomic.Int32
	orderedCollectAndPublish(
		3,
		func(int) bool { return true }, // skip all
		goSubmit,
		func(int) *txnWithLogtails {
			collectCalls.Add(1)
			return &txnWithLogtails{}
		},
		func(*txnWithLogtails) { publishCalls.Add(1) },
	)
	require.Zero(t, collectCalls.Load())
	require.Zero(t, publishCalls.Load())
}

func TestOrderedCollectAndPublish_HappyPath(t *testing.T) {
	const n = 5
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 1, 2, 3, 4}, publishOrder,
		"publish must follow strictly ascending index order")
}

func TestOrderedCollectAndPublish_ReverseReadyOrder(t *testing.T) {
	// Slot n-1 completes first (no sleep); earlier slots sleep longer the
	// lower their index. Publisher must still observe 0, 1, ..., n-1.
	const n = 4
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails {
			// slot 0 sleeps longest, slot n-1 returns immediately.
			time.Sleep(time.Duration(n-1-i) * 20 * time.Millisecond)
			return tails[i]
		},
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 1, 2, 3}, publishOrder,
		"publish must be in index order even when later slots finish first")
}

func TestOrderedCollectAndPublish_NilResultSkipped(t *testing.T) {
	// Slot 1 returns nil (simulating rollback); only slots 0 and 2 should publish.
	const n = 3
	tails := make([]*txnWithLogtails, n)
	tails[0] = &txnWithLogtails{}
	tails[1] = nil
	tails[2] = &txnWithLogtails{}

	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		goSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt != nil && tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown or nil txnWithLogtails")
		},
	)
	require.Equal(t, []int{0, 2}, publishOrder,
		"slot returning nil must be skipped without breaking order")
}

func TestOrderedCollectAndPublish_SkipInterleaved(t *testing.T) {
	// Even slots are skipped; odd slots publish. Publisher sees 1, 3.
	const n = 5
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}

	var collectCalls atomic.Int32
	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(i int) bool { return i%2 == 0 },
		goSubmit,
		func(i int) *txnWithLogtails {
			collectCalls.Add(1)
			return tails[i]
		},
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
			t.Fatalf("published unknown txnWithLogtails")
		},
	)
	require.Equal(t, int32(2), collectCalls.Load(), "skip(i) must prevent collect")
	require.Equal(t, []int{1, 3}, publishOrder)
}

func TestOrderedCollectAndPublish_SyncSubmit(t *testing.T) {
	// Exercise the path where submit runs fn inline. This guards against
	// regressions where readyCh[i] capacity assumption breaks if the
	// publisher happens to drain a slot while the producer is still
	// enqueueing later slots.
	const n = 3
	tails := make([]*txnWithLogtails, n)
	for i := range tails {
		tails[i] = &txnWithLogtails{}
	}
	var mu sync.Mutex
	var publishOrder []int
	orderedCollectAndPublish(
		n,
		func(int) bool { return false },
		syncSubmit,
		func(i int) *txnWithLogtails { return tails[i] },
		func(v *txnWithLogtails) {
			mu.Lock()
			defer mu.Unlock()
			for idx, tt := range tails {
				if tt == v {
					publishOrder = append(publishOrder, idx)
					return
				}
			}
		},
	)
	require.Equal(t, []int{0, 1, 2}, publishOrder)
}
