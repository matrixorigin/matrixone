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

package fscache

import (
	"sync"
	"testing"
	"time"
)

func TestCacheCapacity(t *testing.T) {
	fn := func() int64 {
		return time.Now().UnixNano()
	}
	duration := time.Millisecond * 10
	cached := CachingCapacity(fn, duration)

	var got sync.Map

	n := 16
	wg := new(sync.WaitGroup)
	wg.Add(n)
	t0 := time.Now()
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			for {
				if time.Since(t0) > duration {
					return
				}
				got.Store(cached(), true)
			}

		}()
	}
	wg.Wait()

	c := 0
	got.Range(func(k, v any) bool {
		c++
		return true
	})
	if c > 4 {
		t.Fatalf("got %v", c)
	}
}
