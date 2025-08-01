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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	t.Run("A", func(t *testing.T) {
		throttler := NewMemThrottler("TestBasic", 1)

		throttler.PrintUsage()
		avail1 := throttler.Available()

		for i := 0; i < 10; i++ {
			throttler.Acquire(10)
			throttler.PrintUsage()

			throttler.Release(10)
			throttler.PrintUsage()
		}

		avail2 := throttler.Available()

		require.Equal(t, avail1, avail2)
	})

	t.Run("B", func(t *testing.T) {
		total := int64(mpool.KB)

		throttler := NewMemThrottler(
			"TestBasic",
			1,
			WithConstLimit(total),
		)

		throttler.PrintUsage()
		avail1 := throttler.Available()
		require.Equal(t, total, avail1)

		for i := 0; i < 10; i++ {
			throttler.Acquire(10)
			throttler.PrintUsage()

			throttler.Release(10)
			throttler.PrintUsage()
		}

		avail2 := throttler.Available()
		require.Equal(t, avail1, avail2)

		left, ok := throttler.Acquire(1000)
		require.True(t, ok)
		require.Equal(t, total-1000, left)

		throttler.PrintUsage()

		left, ok = throttler.Acquire(1000)
		require.False(t, ok)
		require.Equal(t, total-1000, left)

		throttler.PrintUsage()
	})

}

func TestParallel(t *testing.T) {
	throttler := NewMemThrottler("TestParallel", 50.0/100.0)

	throttler.PrintUsage()
	available := throttler.Available()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
			}()

			for j := 0; j < 1000*10; j++ {
				avail := throttler.Available()
				rnd := rand.Intn(int(avail+5)/5) + 1000

				for {
					if _, ok := throttler.Acquire(int64(rnd)); ok {
						break
					}
				}

				time.Sleep(time.Microsecond)
				throttler.Release(int64(rnd))
			}
		}()
	}

	wg.Wait()

	throttler.PrintUsage()
	available2 := throttler.Available()
	require.Equal(t, available, available2)
}

func BenchmarkThrottler(b *testing.B) {
	throttler := NewMemThrottler("BenchmarkThrottler", 50.0/100.0)

	for i := 0; i < b.N; i++ {
		throttler.Acquire(10)
		throttler.Release(10)
		throttler.Available()
	}
}
