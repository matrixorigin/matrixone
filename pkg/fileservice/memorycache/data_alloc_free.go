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
)

func newData(size int, counter *atomic.Int64) *Data {
	if size == 0 {
		return nil
	}
	counter.Add(int64(size))
	data := &Data{
		size: size,
	}
	ptr, handle := malloc.Alloc(size)
	data.bufHandle = handle
	data.buf = unsafe.Slice((*byte)(ptr), size)
	data.ref.init(1)
	return data
}

func (d *Data) free(counter *atomic.Int64) {
	counter.Add(-int64(d.size))
	d.buf = nil
	d.bufHandle.Free()
}
