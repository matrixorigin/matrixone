/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mheap

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

// XXX I moved this from types.go to mheap.go.
// Reason: when you look at the type, you can see all its method.
type Mheap struct {
	// SelectList, temporarily stores the row number list in the execution of operators
	// and it can be reused in the future execution.
	pool *sync.Pool
	Gm   *guest.Mmu
	Mp   *mempool.Mempool
}

func New(gm *guest.Mmu) *Mheap {
	return &Mheap{
		Gm: gm,
		Mp: mempool.New(),
		pool: &sync.Pool{
			New: func() any {
				sels := make([]int64, 0, 16)
				return &sels
			},
		},
	}
}

// XXX How many times you need to repeat yourself.
func Size(m *Mheap) int64 {
	return m.Gm.Size()
}
func (m *Mheap) Size() int64 {
	return Size(m)
}

func HostSize(m *Mheap) int64 {
	return m.Gm.HostSize()
}
func (m *Mheap) HostSize() int64 {
	return HostSize(m)
}

func Free(m *Mheap, data []byte) {
	// The Grow() may be called with nil *Mheap
	// So need to check mheap before call Free()
	if m != nil {
		m.Gm.Free(int64(cap(data)))
	}
}

func (m *Mheap) Free(data []byte) {
	Free(m, data)
}

func Alloc(m *Mheap, size int64) ([]byte, error) {
	var data []byte
	if m == nil {
		data = make([]byte, size)
	} else {
		data = mempool.Alloc(m.Mp, int(size))
		if err := m.Gm.Alloc(int64(cap(data))); err != nil {
			return nil, err
		}
	}
	return data[:size], nil
}
func (m *Mheap) Alloc(size int64) ([]byte, error) {
	return Alloc(m, size)
}

// XXX Why this is called Grow?
// XXX If we decided to define Alloc and Free, why don't we define
// Calloc, Realloc, etc?

// Grow to size, not by size
func Grow(m *Mheap, old []byte, size int64) ([]byte, error) {
	var data []byte
	var err error
	if m == nil {
		data = make([]byte, size)
	} else {
		// XXX: WTF moment.  mempool Realloc only compute realloc size but does not
		// do any alloc ..., so we call Alloc
		if data, err = Alloc(m, mempool.Realloc(old, size)); err != nil {
			return nil, err
		}
	}
	copy(data, old)
	return data[:size], nil
}
func (m *Mheap) Grow(old []byte, size int64) ([]byte, error) {
	return Grow(m, old, size)
}

// Grow2 grows old to size and copy in old2.
func Grow2(m *Mheap, old []byte, old2 []byte, size int64) ([]byte, error) {
	data, err := Alloc(m, mempool.Realloc(old, size))
	if err != nil {
		return nil, err
	}
	len1 := len(old)
	len2 := len(old2)
	copy(data[0:len1], old)
	copy(data[len1:len1+len2], old2)
	return data[:size], nil
}

// XXX Increase and Decrease just did the accounting.
// XXX Why Gm's method is called Alloc and Free, if
// it does neither alloc nor free?
func (m *Mheap) Decrease(size int64) {
	m.Gm.Free(size)
}

func (m *Mheap) Increase(size int64) error {
	return m.Gm.Alloc(size)
}

func (m *Mheap) PutSels(sels []int64) {
	m.pool.Put(&sels)
}

func (m *Mheap) GetSels() []int64 {
	sels := m.pool.Get().(*[]int64)
	return (*sels)[:0]
}
