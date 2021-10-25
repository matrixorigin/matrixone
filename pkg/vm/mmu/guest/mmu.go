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
	return m.size
}

func (m *Mmu) HostSize() int64 {
	return m.Mmu.Size()
}

func (m *Mmu) Free(size int64) {
	if size == 0 {
		return
	}
	m.size -= size
	m.Mmu.Free(size)
}

func (m *Mmu) Alloc(size int64) error {
	if size == 0 {
		return nil
	}
	if m.size+size > m.Limit {
		return mmu.OutOfMemory
	}
	if err := m.Mmu.Alloc(size); err != nil {
		return err
	}
	m.size += size
	return nil
}
