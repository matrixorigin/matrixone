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
	"sync/atomic"
	"time"
)

type CapacityFunc func() int64

func ConstCapacity(capacity int64) CapacityFunc {
	return func() int64 {
		return capacity
	}
}

func CachingCapacity(
	upstream CapacityFunc,
	duration time.Duration,
) CapacityFunc {
	var capacity atomic.Pointer[int64]
	c := upstream()
	capacity.Store(&c)
	t0 := time.Now()
	var last atomic.Pointer[time.Duration]
	elapsed := time.Since(t0)
	last.Store(&elapsed)
	return func() int64 {
		for {
			alignedElapsed := time.Since(t0) / duration * duration
			ptr := last.Load()
			if alignedElapsed == *ptr {
				// use cache
				return *capacity.Load()
			}
			// update
			if last.CompareAndSwap(ptr, &alignedElapsed) {
				c := upstream()
				capacity.Store(&c)
				return c
			}
		}
	}
}
