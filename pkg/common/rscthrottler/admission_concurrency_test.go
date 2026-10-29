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

	"github.com/stretchr/testify/require"
)

func TestConcurrentDefaultAdmission(t *testing.T) {
	const limit int64 = 1024
	m := &memThrottler{}
	m.limit.Store(limit)
	m.actualTotalMemory.Store(1 << 30)
	m.lastRefresh.Store(time.Now().UnixNano())
	const workers = 32
	var wg sync.WaitGroup
	var granted atomic.Int64
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, ok := m.Acquire(limit); ok {
				granted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(1), granted.Load())
	require.Equal(t, int64(0), m.Available())
	require.Equal(t, limit, m.Release(granted.Load()*limit))
}
