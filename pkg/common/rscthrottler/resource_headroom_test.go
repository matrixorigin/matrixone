// Copyright 2025 Matrix Origin
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

package rscthrottler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

// Explicit inputs cover the pressure contract without a large-memory fixture.
func TestMemThrottlerPhysicalHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name           string
		rss, available uint64
		measured       bool
		want           rssPressureState
	}{
		{"incident-low-rss-low-headroom", 66869882880, 4502700032, true, rssPressureHard},
		{"same-rss-with-headroom", 66869882880, 30 * mpool.GB, true, rssPressureNone},
		{"large-clean-page-cache-is-reclaimable", 32 * mpool.GB, 48 * mpool.GB, true, rssPressureNone},
		{"measurement-unavailable-keeps-rss", 62 * mpool.GB, 0, false, rssPressureNone},
		{"zero-measured-headroom-is-pressure", 62 * mpool.GB, 0, true, rssPressureHard},
		{"rss-protection-still-applies", 78 * mpool.GB, 30 * mpool.GB, true, rssPressureHard},
		{"headroom-above-limit-does-not-underflow", 62 * mpool.GB, ^uint64(0), true, rssPressureNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &memThrottler{}
			m.options.enableRSSScavenging = true
			m.actualTotalMemory.Store(84 * mpool.GB)
			m.rss.Store(int64(tc.rss))
			// These visible bytes make FreeOSMemory ineligible, as before.
			m.reserved.Store(60 * mpool.GB)
			var target int64
			m.options.rssCacheTargetSetter = func(v int64) { target = v }
			m.tryScavengeRSS(int64(2*time.Minute), int64(tc.rss), tc.available, tc.measured)
			require.Equal(t, tc.want, rssPressureState(m.rssPressureState.Load()))
			require.Equal(t, rssPressureCacheTarget(tc.want), target)
			require.Equal(t, int64(tc.rss), m.rss.Load(), "do not overwrite real RSS for S3 admission")
			require.Equal(t, int64(60*mpool.GB), m.reserved.Load())
		})
	}
}

func TestMemThrottlerPhysicalHeadroomRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	oldFree := freeOSMemory
	completed := make(chan struct{}, 2)
	freeOSMemory = func() { completed <- struct{}{} }
	t.Cleanup(func() { freeOSMemory = oldFree })
	m := &memThrottler{}
	m.options.enableRSSScavenging = true
	m.actualTotalMemory.Store(84 * mpool.GB)
	m.limit.Store(80 * mpool.GB)
	m.rss.Store(62 * mpool.GB)
	m.reserved.Store(60 * mpool.GB)
	var target atomic.Int64
	target.Store(100)
	var evictions atomic.Int64
	m.options.rssCacheTargetSetter = target.Store
	m.options.rssCacheTargetClearer = func() { target.Store(100) }
	m.options.rssCacheEvictor = func(evictCtx context.Context, percent int64) {
		if _, ok := evictCtx.Deadline(); !ok {
			panic("eviction must retain its timeout")
		}
		evictions.Add(1)
	}
	now := int64(2 * time.Minute)
	m.tryScavengeRSS(now, 62*mpool.GB, 4*mpool.GB, true)
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Equal(t, int64(50), target.Load())
	require.Equal(t, int64(1), evictions.Load())
	// No repeated eviction just because low headroom is sampled again.
	m.tryScavengeRSS(now+int64(time.Second), 62*mpool.GB, 4*mpool.GB, true)
	require.Equal(t, int64(1), evictions.Load())
	// Losing the measurement is not evidence that earlier physical pressure ended.
	m.tryScavengeRSS(now+2*int64(time.Second), 62*mpool.GB, 0, false)
	require.Equal(t, rssPressureHard, rssPressureState(m.rssPressureState.Load()))
	require.Equal(t, int64(50), target.Load())
	// Existing hysteresis: hard -> soft -> normal, with the original cache target release.
	m.tryScavengeRSS(now+3*int64(time.Second), 62*mpool.GB, 12*mpool.GB, true)
	require.Equal(t, rssPressureSoft, rssPressureState(m.rssPressureState.Load()))
	require.Equal(t, int64(80), target.Load())
	require.Equal(t, int64(1), evictions.Load())
	m.tryScavengeRSS(now+4*int64(time.Second), 62*mpool.GB, 25*mpool.GB, true)
	require.Equal(t, rssPressureNone, rssPressureState(m.rssPressureState.Load()))
	require.Equal(t, int64(100), target.Load())
	require.Equal(t, int64(1), evictions.Load())
}
