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

package malloc

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestShardedCounter(t *testing.T) {
	counter := NewShardedCounter[int64, atomic.Int64](128)
	wg := new(sync.WaitGroup)
	n := 128
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1024; i++ {
				counter.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := counter.Load(); n != 128*1024 {
		t.Fatalf("got %v\n", n)
	}
}

func BenchmarkShardedCounter(b *testing.B) {
	counter := NewShardedCounter[int64, atomic.Int64](runtime.GOMAXPROCS(0))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Add(1)
		}
	})
}

func BenchmarkAtomicCounter(b *testing.B) {
	counter := new(atomic.Int64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter.Add(1)
		}
	})
}
