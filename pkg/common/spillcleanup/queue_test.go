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

package spillcleanup

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQueueRetainsFailuresWithoutBlockingHealthyOwners(t *testing.T) {
	q := &queue{delay: time.Millisecond}
	var blocked atomic.Bool
	blocked.Store(true)
	var recovered, healthy atomic.Int32
	q.enqueue(func() error {
		if blocked.Load() {
			return errors.New("filesystem unavailable")
		}
		recovered.Add(1)
		return nil
	})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); q.enqueue(func() error { healthy.Add(1); return nil }) }()
	}
	wg.Wait()
	require.Eventually(t, func() bool { return healthy.Load() == 64 }, time.Second, time.Millisecond)
	require.Zero(t, recovered.Load())
	blocked.Store(false)
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return !q.running && len(q.pending) == 0
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), recovered.Load())
	// A fully drained queue must restart for a later query.
	q.enqueue(func() error { healthy.Add(1); return nil })
	require.Eventually(t, func() bool { return healthy.Load() == 65 }, time.Second, time.Millisecond)
}
