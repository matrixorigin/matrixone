// Copyright 2024 Matrix Origin
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

package disttae

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPkCheckSemaphore_LimitsConcurrency(t *testing.T) {
	// Verify the semaphore actually limits concurrent goroutines.
	const workers = 100
	semCap := cap(pkCheckSemaphore) // should be 16

	var maxConcurrent atomic.Int32
	var current atomic.Int32
	var wg sync.WaitGroup

	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case pkCheckSemaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-pkCheckSemaphore }()

			c := current.Add(1)
			for {
				old := maxConcurrent.Load()
				if c <= old || maxConcurrent.CompareAndSwap(old, c) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			current.Add(-1)
		}()
	}

	wg.Wait()

	assert.LessOrEqual(t, int(maxConcurrent.Load()), semCap,
		"concurrent goroutines should not exceed semaphore capacity %d", semCap)
	assert.Greater(t, int(maxConcurrent.Load()), 1,
		"should have some concurrency")
}

func TestPkCheckSemaphore_RespectsContextCancellation(t *testing.T) {
	// Fill the semaphore completely.
	for i := 0; i < cap(pkCheckSemaphore); i++ {
		pkCheckSemaphore <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(pkCheckSemaphore); i++ {
			<-pkCheckSemaphore
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should not block — context is already cancelled.
	select {
	case pkCheckSemaphore <- struct{}{}:
		t.Fatal("should not have acquired semaphore")
		<-pkCheckSemaphore
	case <-ctx.Done():
		// expected
	}
}
