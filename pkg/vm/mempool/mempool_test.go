// Copyright 2021 Matrix Origin
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

package mempool

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

var pt = fmt.Printf

func TestMempool(t *testing.T) {
	n := 100
	wg := new(sync.WaitGroup)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool := New()
			for i := 0; i < 512; i++ {
				size := rand.Intn(65536)
				data := pool.Alloc(size)
				x := uint8(rand.Intn(256))
				for i := range data {
					data[i] = x
				}
				for _, b := range data {
					if b != x {
						panic("race")
					}
				}
			}
			pool.Release()
		}()
	}
	wg.Wait()
}

func BenchmarkMempool(b *testing.B) {
	var nextX uint32
	b.RunParallel(func(pb *testing.PB) {
		pool := New()
		for pb.Next() {
			data := pool.Alloc(128)
			x := uint8(atomic.AddUint32(&nextX, 1) % 256)
			for i := range data {
				data[i] = x
			}
			for _, b := range data {
				if b != x {
					panic("race")
				}
			}
			pool.Free(data)
		}
		pool.Release()
	})
}
