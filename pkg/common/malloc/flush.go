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
		lastNumAllocs := make([]int64, numShards)
		for range time.NewTicker(time.Second * 37).C {
			for i := 0; i < numShards; i++ {
				numAllocs := shards[i].numAlloc.Load()
				if numAllocs == lastNumAllocs[i] {
					// not active, flush
					shards[i].flush()
				}
				lastNumAllocs[i] = numAllocs
			}
		}
	}()
}

func (s *Shard) flush() {
	for _, ch := range s.pools {
	loop:
		for {
			select {
			case <-ch:
			default:
				break loop
			}
		}
	}
}
