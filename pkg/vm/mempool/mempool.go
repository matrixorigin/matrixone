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
	"github.com/matrixorigin/matrixone/pkg/vm/malloc"
)

func New() *Mempool {
	m := &Mempool{buckets: make([]bucket, 0, 10)}
	for size := PageSize; size <= MaxSize; size *= Factor {
		m.buckets = append(m.buckets, bucket{
			size:  size,
			slots: make([][]byte, 0, 8),
		})
	}
	return m
}

func Realloc(data []byte, size int64) int64 {
	if data == nil {
		return size
	}
	n := int64(cap(data))
	if size <= n {
		return n
	}
	newcap := n
	doublecap := n + n
	if size > doublecap {
		newcap = size
	} else {
		if len(data) < 1024 {
			newcap = doublecap
		} else {
			for 0 < newcap && newcap < size {
				newcap += newcap / 4
			}
			if newcap <= 0 {
				newcap = size
			}
		}
	}
	return newcap
}

func (m *Mempool) Alloc(size int) []byte {
	if size <= MaxSize {
		for i, b := range m.buckets {
			if b.size >= size {
				if len(b.slots) > 0 {
					data := b.slots[0]
					m.buckets[i].slots[0] = m.buckets[i].slots[len(m.buckets[i].slots)-1]
					m.buckets[i].slots[len(m.buckets[i].slots)-1] = nil
					m.buckets[i].slots = m.buckets[i].slots[:len(m.buckets[i].slots)-1]
					return data
				}
				return malloc.Malloc(b.size)
			}
		}
	}
	for i, buf := range m.buffers {
		if cap(buf) >= size {
			m.size -= cap(buf)
			m.buffers[i] = m.buffers[len(m.buffers)-1]
			m.buffers[len(m.buffers)-1] = nil
			m.buffers = m.buffers[:len(m.buffers)-1]
			return buf[:size]
		}
	}
	return malloc.Malloc(size)
}

func (m *Mempool) Free(data []byte) {
	size := cap(data)
	if size <= MaxSize {
		for i, j := 0, len(m.buckets); i < j; i++ {
			if size == m.buckets[i].size {
				m.buckets[i].slots = append(m.buckets[i].slots, data)
				return
			}
		}
		return
	}
	if m.size+cap(data) > Limit {
		return
	}
	m.size += cap(data)
	m.buffers = append(m.buffers, data)
	return
}
