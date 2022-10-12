// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"fmt"
	"sync/atomic"
)

type sizeLimiter struct {
	maxactivesize uint64
	activesize    atomic.Uint64
}

func newSizeLimiter(maxactivesize uint64) *sizeLimiter {
	return &sizeLimiter{
		maxactivesize: maxactivesize,
	}
}

func (l *sizeLimiter) RetuernQuota(size uint64) uint64 {
	return l.activesize.Add(^uint64(size - 1))
}

func (l *sizeLimiter) ApplyQuota(size uint64) bool {
	pre := l.activesize.Load()
	post := pre + size
	if post > l.maxactivesize {
		return false
	}
	for !l.activesize.CompareAndSwap(pre, post) {
		pre = l.activesize.Load()
		post = pre + size
		if post > l.maxactivesize {
			return false
		}
	}
	return true
}

func (l *sizeLimiter) Total() uint64 {
	return l.activesize.Load()
}

func (l *sizeLimiter) String() string {
	s := fmt.Sprintf("<sizeLimiter>[Size=(%d/%d)]",
		l.Total(), l.maxactivesize)
	return s
}
