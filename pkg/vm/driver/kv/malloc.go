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

package kv

import (
	"sync"
	"sync/atomic"
)

const (
	K int = 1024
	M int = 1024 * 1024
	G     = K * M
)

var (
	elementSize = []int{
		16,
		32,
		48,
		64,
		80,
		96,
		112,
		128,
		144,
		160,
		176,
		192,
		208,
		224,
		240,
		256,
		272,
		288,
		304,
		320,
		336,
		352,
		368,
		384,
		400,
		416,
		432,
		448,
		464,
		480,
		496,
		512,
		528,
		544,
		560,
		576,
		592,
		608,
		624,
		640,
		656,
		672,
		688,
		704,
		720,
		736,
		752,
		768,
		784,
		800,
		816,
		832,
		848,
		864,
		880,
		896,
		912,
		928,
		944,
		960,
		976,
		992,
		1008,
		1024,
	}
)

type elementPool struct {
	elemSize int
	count    int32
	pool     sync.Pool
}

func (ep *elementPool) malloc(size int) []byte {
	if size == ep.elemSize {
		atomic.AddInt32(&ep.count, 1)
		space := ep.pool.Get()
		return space.([]byte)
	}
	return make([]byte, size)
}

func (ep *elementPool) free(ptr []byte) {
	if ptr == nil {
		return
	}
	if len(ptr) == ep.elemSize {
		atomic.AddInt32(&ep.count, -1)
		ep.pool.Put(ptr)
	}
}

func (ep *elementPool) freeBatch(bat [][]byte) {
	for i := 0; i < len(bat); i++ {
		ep.free(bat[i])
	}
}

func (ep *elementPool) bytes() uint64 {
	ct := atomic.LoadInt32(&ep.count)
	return uint64(ct) * uint64(ep.elemSize)
}

type bytesMalloc struct {
	useMake bool
	pools   []*elementPool
}

func NewBytesMalloc(useMake bool) *bytesMalloc {
	var pools []*elementPool
	for _, e := range elementSize {
		pools = append(pools, &elementPool{
			elemSize: e,
			pool: sync.Pool{New: func() interface{} {
				return make([]byte, e)
			}},
		})
	}
	return &bytesMalloc{
		useMake: useMake,
		pools:   pools,
	}
}

func lower_bound(A []int, t int) int {
	x := 0
	y := len(A)
	for x < y {
		m := x + (y-x)/2
		if t <= A[m] {
			y = m
		} else {
			x = m + 1
		}
	}
	return x
}

func binary_search(A []int, t int) int {
	x := 0
	y := len(A)
	for x < y {
		m := x + (y-x)/2
		if t == A[m] {
			return m
		} else if t < A[m] {
			y = m
		} else {
			x = m + 1
		}
	}
	return -1
}

func findPoolIndex(size int) int {
	p := lower_bound(elementSize, size)
	if p >= len(elementSize) {
		return p
	} else {
		if elementSize[p] == size {
			return p
		} else {
			return p + 1
		}
	}
}

func (bp *bytesMalloc) malloc(size int) []byte {
	if bp.useMake {
		return make([]byte, size)
	}
	if size > elementSize[len(elementSize)-1] {
		return make([]byte, size)
	} else {
		p := findPoolIndex(size)
		if p >= len(elementSize) {
			return make([]byte, size)
		}
		space := bp.pools[p].pool.Get()
		return space.([]byte)
	}
}

func (bp *bytesMalloc) free(ptr []byte) {
	if bp.useMake {
		return
	}
	if ptr == nil {
		return
	}
	if len(ptr) >= elementSize[0] && len(ptr) <= elementSize[len(elementSize)-1] {
		p := binary_search(elementSize, len(ptr))
		if p != -1 {
			bp.pools[p].free(ptr)
		}
	}
}

func (bp *bytesMalloc) freeBatch(bat [][]byte) {
	for i := 0; i < len(bat); i++ {
		bp.free(bat[i])
	}
}

func (bp *bytesMalloc) freeMallocedBuffers(nf *mallocedBuffers) {
	bp.freeBatch(nf.get())
}

func (bp *bytesMalloc) bytes() uint64 {
	ret := uint64(0)
	for _, pool := range bp.pools {
		ret += pool.bytes()
	}
	return ret
}

type mallocedBuffers struct {
	handles [][]byte
}

func (nf *mallocedBuffers) collect(h []byte) {
	nf.handles = append(nf.handles, h)
}

func (nf *mallocedBuffers) get() [][]byte {
	return nf.handles
}
