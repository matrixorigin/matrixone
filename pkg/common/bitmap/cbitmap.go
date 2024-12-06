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

package bitmap

/*
#include "../../../cgo/mo.h"
*/
import "C"
import (
	_ "github.com/matrixorigin/matrixone/cgo"
	"unsafe"
)

func (n *Bitmap) cPtr() *C.uint64_t {
	return (*C.uint64_t)(unsafe.Pointer(&n.data[0]))
}
func (n *Bitmap) cLen() C.uint64_t {
	return C.uint64_t(n.len)
}

func (n *Bitmap) C_IsEmpty() bool {
	cret := C.Bitmap_IsEmpty(n.cPtr(), n.cLen())
	return bool(cret)
}

func (n *Bitmap) C_Count() uint64 {
	return uint64(C.Bitmap_Count(n.cPtr(), n.cLen()))
}

func (n *Bitmap) C_And(m *Bitmap) {
	n.TryExpand(m)
	C.Bitmap_And(n.cPtr(), n.cPtr(), m.cPtr(), n.cLen())
}

func (n *Bitmap) C_Or(m *Bitmap) {
	n.TryExpand(m)
	C.Bitmap_Or(n.cPtr(), n.cPtr(), m.cPtr(), n.cLen())
}
