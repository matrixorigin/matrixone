// Copyright 2021 - 2023 Matrix Origin
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

package buffer

import (
	"math"
	"unsafe"
)

var pageHeaderSize int

func init() {
	var p page

	pageHeaderSize = int(unsafe.Sizeof(p))
}

func (ph *pageHeader) alloc(sz int, b *Buffer) []byte {
	p := pop(ph.freeList)
	if p == nil {
		p = ph.newPage(sz, b)
	}
	if p == nil {
		tempList := make([]*page, 0, 1)
		for {
			tmp := pop(ph.fullList)
			if tmp == nil {
				for i := range tempList {
					push(tempList[i], ph.fullList)
				}
				panic("Out of Memory")
			}
			tmp.mu.Lock()
			if tmp.head == math.MaxUint64 {
				tmp.mu.Unlock()
				tempList = append(tempList, tmp)
				continue
			}
			p = tmp
			for i := range tempList {
				push(tempList[i], ph.fullList)
			}
			break
		}
	} else {
		p.mu.Lock()
	}
	data := p.alloc()
	if p.head == math.MaxUint64 {
		push(p, ph.fullList)
	} else {
		push(p, ph.freeList)
	}
	p.mu.Unlock()
	return data[:sz:sz]
}

func (ph *pageHeader) free(data []byte, b *Buffer) {
	ptr := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	headerData := b.data[((ptr-b.ptr)/PageSize)*PageSize:][:PageSize]
	p := (*page)(unsafe.Pointer(unsafe.SliceData(headerData)))
	p.mu.Lock()
	p.free(ptr)
	p.mu.Unlock()
}

func (ph *pageHeader) newPage(sz int, b *Buffer) *page {
	p := pop(b.freeList)
	if p == nil {
		return nil
	}
	p.init(sz)
	return p
}

func (p *page) init(sz int) {
	for i := 0; i+sz < len(p.data); i += sz {
		*(*uintptr)(unsafe.Pointer(&p.data[i])) = p.head
		p.head = uintptr(i)
	}
}

// p.head = p.head->next
func (p *page) alloc() []byte {
	head := p.head
	p.head = *(*uintptr)(unsafe.Pointer(&p.data[head]))
	return p.data[head:]
}

// head = p->head
// p->head = ptr
// ptr->next = head
func (p *page) free(ptr uintptr) {
	ptr -= p.ptr
	head := p.head
	p.head = ptr
	*(*uintptr)(unsafe.Pointer(&p.data[ptr])) = head
}
