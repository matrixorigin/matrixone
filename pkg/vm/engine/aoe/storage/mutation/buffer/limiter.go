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

package buffer

import (
	"fmt"
	"sync/atomic"
)

type sizeLimiter struct {
	maxactivesize, activesize uint64
}

func newSizeLimiter(maxactivesize uint64) *sizeLimiter {
	return &sizeLimiter{
		maxactivesize: maxactivesize,
	}
}

func (l *sizeLimiter) RetuernQuota(size uint64) uint64 {
	return atomic.AddUint64(&l.activesize, ^uint64(size-1))
}

func (l *sizeLimiter) ApplyQuota(size uint64) bool {
	pre := atomic.LoadUint64(&l.activesize)
	post := pre + size
	if post > l.maxactivesize {
		return false
	}
	for !atomic.CompareAndSwapUint64(&l.activesize, pre, post) {
		pre = atomic.LoadUint64(&l.activesize)
		post = pre + size
		if post > l.maxactivesize {
			return false
		}
	}
	return true
}

func (l *sizeLimiter) Total() uint64 {
	return atomic.LoadUint64(&l.activesize)
}

func (l *sizeLimiter) String() string {
	s := fmt.Sprintf("<sizeLimiter>[Size=(%d/%d)]",
		l.Total(), l.maxactivesize)
	return s
}
