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

package malloc

import (
	"sync/atomic"
	"unsafe"
)

type PureGoClassAllocator struct {
	classSizes []uint64
	pools      []pureGoClassAllocatorPool
}

type pureGoClassAllocatorPool struct {
	numAlloc atomic.Int64
	numFree  atomic.Int64
	ch       chan *pureGoClassAllocatorHandle
}

type pureGoClassAllocatorHandle struct {
	ptr       unsafe.Pointer
	class     int
	allocator *PureGoClassAllocator
}

var dumbHandle = &pureGoClassAllocatorHandle{
	class: -1,
}

func NewPureGoClassAllocator(
	maxBufferSize uint64,
) *PureGoClassAllocator {
	const (
		minClassSize    = 128
		maxClassSize    = 8 * (1 << 20)
		classSizeFactor = 1.8
	)

	classSizes := func() (ret []uint64) {
		for size := uint64(minClassSize); size <= maxClassSize; size = uint64(float64(size) * classSizeFactor) {
			ret = append(ret, size)
		}
		return
	}()

	classSumSize := func() (ret uint64) {
		for _, size := range classSizes {
			ret += size
		}
		return
	}()

	bufferedObjectsPerClass := func() int {
		n := maxBufferSize / classSumSize
		//logutil.Info("malloc",
		//	zap.Any("max buffer size", maxBufferSize),
		//	zap.Any("num shards", numShards),
		//	zap.Any("classes", len(classSizes)),
		//	zap.Any("min class size", minClassSize),
		//	zap.Any("max class size", maxClassSize),
		//	zap.Any("buffer objects per class", n),
		//)
		return int(n)
	}()

	pools := func() (ret []pureGoClassAllocatorPool) {
		for range classSizes {
			ret = append(
				ret,
				pureGoClassAllocatorPool{
					ch: make(chan *pureGoClassAllocatorHandle, bufferedObjectsPerClass),
				},
			)
		}
		return
	}()

	return &PureGoClassAllocator{
		classSizes: classSizes,
		pools:      pools,
	}
}

func (p *PureGoClassAllocator) requestSizeToClass(size uint64) int {
	for class, classSize := range p.classSizes {
		if classSize >= size {
			return class
		}
	}
	return -1
}

func (p *PureGoClassAllocator) classAllocate(class int) *pureGoClassAllocatorHandle {
	select {
	case handle := <-p.pools[class].ch:
		p.pools[class].numAlloc.Add(1)
		clear(unsafe.Slice((*byte)(handle.ptr), p.classSizes[handle.class]))
		return handle
	default:
		slice := make([]byte, p.classSizes[class])
		return &pureGoClassAllocatorHandle{
			ptr:       unsafe.Pointer(unsafe.SliceData(slice)),
			class:     class,
			allocator: p,
		}
	}
}

func (p *PureGoClassAllocator) Allocate(size uint64) (unsafe.Pointer, Deallocator, error) {
	if size == 0 {
		return nil, dumbHandle, nil
	}
	class := p.requestSizeToClass(size)
	if class == -1 {
		return unsafe.Pointer(unsafe.SliceData(make([]byte, size))), dumbHandle, nil
	}
	handle := p.classAllocate(class)
	return handle.ptr, handle, nil
}

func (h *pureGoClassAllocatorHandle) Deallocate(_ unsafe.Pointer) {
	if h.class < 0 {
		return
	}
	select {
	case h.allocator.pools[h.class].ch <- h:
		h.allocator.pools[h.class].numFree.Add(1)
	default:
	}
}
