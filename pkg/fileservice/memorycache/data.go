// Copyright 2022 Matrix Origin
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

package memorycache

import (
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// Data is a reference counted byte buffer
type Data struct {
	size int
	buf  []byte
	// reference counta for the Data, the Data is free
	// when the reference count is 0
	ref         refcnt
	ptr         unsafe.Pointer
	deallocator malloc.Deallocator
	globalSize  *atomic.Int64
}

var _ CacheData = new(Data)

func newData(
	allocator malloc.Allocator,
	size int,
	globalSize *atomic.Int64,
) *Data {
	if size == 0 {
		return nil
	}
	globalSize.Add(int64(size))
	data := &Data{
		size:       size,
		globalSize: globalSize,
	}
	data.ptr, data.deallocator = allocator.Allocate(uint64(size))
	metric.FSMallocLiveObjectsMemoryCache.Inc()
	data.buf = unsafe.Slice((*byte)(data.ptr), size)
	data.ref.init(1)
	return data
}

func (d *Data) free() {
	d.globalSize.Add(-int64(d.size))
	d.buf = nil
	d.deallocator.Deallocate(d.ptr)
	metric.FSMallocLiveObjectsMemoryCache.Dec()
}

func (d *Data) Bytes() []byte {
	if d == nil {
		return nil
	}
	return d.Buf()
}

// Buf returns the underlying buffer of the Data
func (d *Data) Buf() []byte {
	if d == nil {
		return nil
	}
	return d.buf
}

func (d *Data) Slice(n int) CacheData {
	d.buf = d.buf[:n]
	return d
}

func (d *Data) refs() int32 {
	return d.ref.refs()
}

func (d *Data) acquire() {
	d.ref.acquire()
}

func (d *Data) Release() {
	if d != nil && d.ref.release() {
		d.free()
	}
}
