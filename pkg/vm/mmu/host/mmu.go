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

package host

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/mmu"
)

func New(limit int64) *Mmu {
	return &Mmu{
		limit: limit,
	}
}

func (m *Mmu) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

func (m *Mmu) Free(size int64) {
	atomic.AddInt64(&m.size, size*-1)
}

func (m *Mmu) Alloc(size int64) error {
	if atomic.LoadInt64(&m.size)+size > m.limit {
		return mmu.ErrOutOfMemory
	}
	for v := atomic.LoadInt64(&m.size); !atomic.CompareAndSwapInt64(&m.size, v, v+size); v = atomic.LoadInt64(&m.size) {
	}
	return nil
}
