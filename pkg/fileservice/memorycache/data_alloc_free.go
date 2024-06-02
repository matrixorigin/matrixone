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

func newData(allocator malloc.Allocator, size int, counter *atomic.Int64) *Data {
	if size == 0 {
		return nil
	}
	counter.Add(int64(size))
	data := &Data{
		size: size,
	}
	data.ptr, data.deallocator = allocator.Allocate(uint64(size))
	metric.FSMallocLiveObjectsMemoryCache.Inc()
	data.buf = unsafe.Slice((*byte)(data.ptr), size)
	data.ref.init(1)
	return data
}

func (d *Data) free(counter *atomic.Int64) {
	counter.Add(-int64(d.size))
	d.buf = nil
	d.deallocator.Deallocate(d.ptr)
	metric.FSMallocLiveObjectsMemoryCache.Dec()
}
