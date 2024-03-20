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

//go:build !race
// +build !race

package memorycache

import (
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

const dataSize = int(unsafe.Sizeof(Data{}))

func newData(n int, size *atomic.Int64) *Data {
	if n == 0 {
		return nil
	}
	size.Add(int64(n + dataSize))
	b := malloc.Alloc(n + dataSize)
	d := (*Data)(unsafe.Pointer(&b[0]))
	d.buf = b[dataSize:]
	d.ref.init(1)
	return d
}

func (d *Data) free(size *atomic.Int64) {
	n := cap(d.buf) + dataSize
	size.Add(-int64(n))
	buf := unsafe.Slice((*byte)(unsafe.Pointer(d)), n)
	d.buf = nil
	malloc.Free(buf)
}
