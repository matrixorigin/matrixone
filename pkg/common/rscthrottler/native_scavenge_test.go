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

package rscthrottler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func nativePressureThrottler() *memThrottler {
	m := &memThrottler{limitRate: .90, coordinator: newMemoryCoordinator()}
	m.options.enableRSSScavenging = true
	m.actualTotalMemory.Store(100 * mpool.GB)
	m.limit.Store(90 * mpool.GB)
	m.rss.Store(90 * mpool.GB)
	// High visible bytes suppress the Go-only scavenger. Native free pages
	// still need independent reclamation in this state.
	m.reserved.Store(90 * mpool.GB)
	return m
}

func TestMemThrottlerNativeRSSScavenging(t *testing.T) {
	oldNative, oldGo := releaseNativeMemory, freeOSMemory
	t.Cleanup(func() { releaseNativeMemory, freeOSMemory = oldNative, oldGo })
	var nativeCalls, goCalls atomic.Int32
	releaseNativeMemory = func() bool { nativeCalls.Add(1); return true }
	freeOSMemory = func() { goCalls.Add(1) }
	m := nativePressureThrottler()
	now := time.Now().UnixNano()
	m.tryScavengeRSS(now, 84*mpool.GB)
	require.Zero(t, nativeCalls.Load())
	m.tryScavengeRSS(now, 85*mpool.GB)
	require.Equal(t, int32(1), nativeCalls.Load())
	m.tryScavengeRSS(now+int64(time.Second), 90*mpool.GB)
	require.Equal(t, int32(1), nativeCalls.Load())
	now += int64(rssNativeScavengeInterval) + 1
	m.tryScavengeRSS(now, 90*mpool.GB)
	require.Equal(t, int32(2), nativeCalls.Load())
	m.tryScavengeRSS(now+1, 93*mpool.GB)
	require.Equal(t, int32(3), nativeCalls.Load(), "hard pressure escalates immediately")
	m.tryScavengeRSS(now+1, 93*mpool.GB)
	require.Equal(t, int32(3), nativeCalls.Load())
	m.tryScavengeRSS(now+2, 80*mpool.GB)
	m.tryScavengeRSS(now+3, 85*mpool.GB)
	require.Equal(t, int32(3), nativeCalls.Load(), "soft re-entry still obeys the interval")
	m.tryScavengeRSS(now+int64(rssNativeScavengeInterval)+2, 90*mpool.GB)
	require.Equal(t, int32(4), nativeCalls.Load())
	require.Zero(t, goCalls.Load(), "native reclamation must not depend on forcing Go GC")
	m.options.enableRSSScavenging = false
	m.tryScavengeRSS(now+2*int64(rssNativeScavengeInterval), 95*mpool.GB)
	require.Equal(t, int32(4), nativeCalls.Load())
}

func TestMemThrottlerConcurrentNativeRSSScavenging(t *testing.T) {
	oldNative := releaseNativeMemory
	t.Cleanup(func() { releaseNativeMemory = oldNative })
	var calls atomic.Int32
	releaseNativeMemory = func() bool { calls.Add(1); return true }
	m := nativePressureThrottler()
	now := time.Now().UnixNano()
	for _, rss := range []int64{90 * mpool.GB, 93 * mpool.GB} {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; m.tryScavengeRSS(now, rss) }()
		}
		close(start)
		wg.Wait()
	}
	require.Equal(t, int32(2), calls.Load(), "one soft reclamation and one hard escalation")
}
