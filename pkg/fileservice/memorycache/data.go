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

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// Data is a reference counted byte buffer
type Data struct {
	size  int
	bytes []byte
	// reference counta for the Data, the Data is free
	// when the reference count is 0
	ref         refcnt
	deallocator malloc.Deallocator
	globalSize  *atomic.Int64
}

var _ fscache.Data = new(Data)

func newData(
	allocator malloc.Allocator,
	size int,
	globalSize *atomic.Int64,
) *Data {
	globalSize.Add(int64(size))
	data := &Data{
		size:       size,
		globalSize: globalSize,
	}
	if size > 0 {
		var err error
		data.bytes, data.deallocator, err = allocator.Allocate(uint64(size), malloc.NoHints)
		if err != nil {
			panic(err)
		}
	}
	data.ref.init(1)
	return data
}

func (d *Data) free() {
	d.globalSize.Add(-int64(d.size))
	d.bytes = nil
	if d.deallocator != nil {
		d.deallocator.Deallocate(malloc.NoHints)
	}
}

func (d *Data) Bytes() []byte {
	return d.bytes
}

func (d *Data) Slice(n int) fscache.Data {
	d.bytes = d.bytes[:n]
	return d
}

func (d *Data) refs() int32 {
	return d.ref.refs()
}

func (d *Data) acquire() {
	d.ref.acquire()
}

func (d *Data) Release() {
	if d.ref.release() {
		d.free()
	}
}
