// Copyright 2022 Matrix Origin
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

import "time"

func init() {
	go func() {
		maxIdle := time.Second * 37
		lastNumAllocs := make(map[[2]int]int64)
		for range time.NewTicker(maxIdle).C {
			evict(lastNumAllocs)
		}
	}()
}

func evict(stats map[[2]int]int64) {
	for shardIndex := 0; shardIndex < numShards; shardIndex++ {
		for poolIndex := 0; poolIndex < len(classSizes); poolIndex++ {
			pool := &shards[shardIndex].pools[poolIndex]
			numAllocs := pool.numAlloc.Load()
			key := [2]int{shardIndex, poolIndex}
			if numAllocs == stats[key] {
				// not active, flush
				pool.flush()
			}
			stats[key] = numAllocs
		}
	}
}

func (p *Pool) flush() {
	for {
		select {
		case <-p.ch:
		default:
			return
		}
	}
}

func cachingObjects() (ret int) {
	for i := 0; i < numShards; i++ {
		for j := 0; j < len(classSizes); j++ {
			ret += len(shards[i].pools[j].ch)
		}
	}
	return
}
