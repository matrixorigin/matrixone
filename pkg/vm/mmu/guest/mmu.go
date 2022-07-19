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

package guest

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/mmu"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
)

func New(limit int64, mmu *host.Mmu) *Mmu {
	return &Mmu{
		Mmu:   mmu,
		Limit: limit,
	}
}

func (m *Mmu) Size() int64 {
	return atomic.LoadInt64(&m.size)
}

func (m *Mmu) HostSize() int64 {
	return m.Mmu.Size()
}

func (m *Mmu) Free(size int64) {
	if m.Size() == 0 {
		return
	}
	atomic.AddInt64(&m.size, size*-1)
	m.Mmu.Free(size)
}

func (m *Mmu) Alloc(size int64) error {
	if atomic.LoadInt64(&m.size)+size > m.Limit {
		return mmu.ErrOutOfMemory
	}
	if err := m.Mmu.Alloc(size); err != nil {
		return err
	}
	for v := atomic.LoadInt64(&m.size); !atomic.CompareAndSwapInt64(&m.size, v, v+size); v = atomic.LoadInt64(&m.size) {
	}
	return nil
}
