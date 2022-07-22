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

package alloc

// #include <stdlib.h>
import "C"

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type node []byte

func (n *node) setBuf(buf []byte) {
	*n = buf
}

func (n *node) GetBuf() []byte {
	return *n
}

func (n *node) Size() int {
	return cap(*n)
}

type allocator struct {
	capacity  int64
	usage     int64
	peakusage int64
}

func NewAllocator(capacity int) stl.MemAllocator {
	return &allocator{
		capacity: int64(capacity),
	}
}

func (a *allocator) Alloc(sz int) stl.MemNode {
	// ptr := C.calloc(C.size_t(sz), 1)
	// if ptr == nil {
	// 	return nil
	// }
	// n := new(node)
	// n.buf = (*[MaxArrayLen]byte)(unsafe.Pointer(ptr))[:sz:sz]
	// return n

	if sz <= 0 {
		return new(node)
	}
	pre := atomic.LoadInt64(&a.usage)
	post := pre + int64(sz)
	if post > a.capacity {
		return nil
	}
	for !atomic.CompareAndSwapInt64(&a.usage, pre, post) {
		pre = atomic.LoadInt64(&a.usage)
		post = pre + int64(sz)
		if post > a.capacity {
			return nil
		}
	}
	peak := atomic.LoadInt64(&a.peakusage)
	if post > peak {
		atomic.CompareAndSwapInt64(&a.peakusage, peak, post)
	}
	ptr := C.calloc(C.size_t(sz), 1)
	if ptr == nil {
		return nil
	}
	n := new(node)
	n.setBuf((*[MaxArrayLen]byte)(unsafe.Pointer(ptr))[:sz:sz])
	return n
}

func (a *allocator) Free(n stl.MemNode) {
	b := n.GetBuf()
	if sz := cap(b); sz != 0 {
		b = b[:cap(b)]
		ptr := unsafe.Pointer(&b[0])
		C.free(ptr)
		atomic.AddInt64(&a.usage, -int64(sz))
	}
}

func (a *allocator) Usage() int {
	return int(atomic.LoadInt64(&a.usage))
}

func (a *allocator) String() string {
	var w bytes.Buffer
	usage := atomic.LoadInt64(&a.usage)
	peak := atomic.LoadInt64(&a.peakusage)
	_, _ = w.WriteString(fmt.Sprintf("<CGO-Alloctor>(Cap=%s)(Usage=%s)(Peak=%s)",
		common.ToH(uint64(a.capacity)),
		common.ToH(uint64(usage)),
		common.ToH(uint64(peak))))
	return w.String()
}
