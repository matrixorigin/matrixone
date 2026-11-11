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
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkingSetTryNoWaitPhysicalPressureAndActiveOwner(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "ownerless_residency", true: "active_owner"}[active], func(t *testing.T) {
			rss := int64(950)
			if active {
				rss = 400
			}
			m, c, queued := admissionFixture(t, 1000, 900, rss)
			if active {
				_, ok := m.Acquire(550)
				require.True(t, ok)
				t.Cleanup(func() { m.Release(550) })
			}
			var reclaims atomic.Int32
			c.reclaim = func(*memThrottler) { reclaims.Add(1) }
			w, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(32, 80, 40))
			if w != nil {
				t.Cleanup(w.Close)
			}
			require.NoError(t, err)
			require.False(t, granted)
			require.Nil(t, w)
			require.Empty(t, queued)
			require.Empty(t, c.queue)
			require.Empty(t, c.tickets)
			require.Nil(t, c.workerStop)
			require.Zero(t, reclaims.Load())
		})
	}
}

func TestWorkingSetTryNoWaitPreservesExistingFIFO(t *testing.T) {
	m, c, queued := admissionFixture(t, 1000, 900, 100)
	_, ok := m.Acquire(700)
	require.True(t, ok)
	t.Cleanup(func() { m.Release(700) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan candidateResult, 1)
	go func() {
		w, err := m.ReserveWorkingSetCandidatesUntilFit(ctx, []WorkingSetSpec{candidateSpec(64, 250, 50)})
		result <- candidateResult{w, err}
	}()
	consumed := false
	t.Cleanup(func() {
		cancel()
		if !consumed {
			actual := await(t, result)
			if actual.working != nil {
				actual.working.Close()
			}
		}
		c.mu.Lock()
		done := c.workerDone
		c.mu.Unlock()
		if done != nil {
			await(t, done)
		}
	})
	require.Equal(t, "working-set", await(t, queued))
	c.mu.Lock()
	before := len(c.queue)
	done := c.workerDone
	c.mu.Unlock()
	w, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(1, 10, 10))
	if w != nil {
		t.Cleanup(w.Close)
	}
	require.NoError(t, err)
	require.False(t, granted, "fitting bytes cannot bypass an existing FIFO head")
	require.Nil(t, w)
	c.mu.Lock()
	after := len(c.queue)
	c.mu.Unlock()
	require.Equal(t, 1, before)
	require.Equal(t, before, after, "the denied try must not add a waiter")
	require.Empty(t, queued)
	cancel()
	actual := await(t, result)
	consumed = true
	if actual.working != nil {
		t.Cleanup(actual.working.Close)
	}
	require.ErrorIs(t, actual.err, context.Canceled)
	if done != nil {
		await(t, done)
	}
}
