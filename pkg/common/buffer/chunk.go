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
	"unsafe"

	"golang.org/x/sys/unix"
)

func init() {
	var c chunk

	ChunkSize = int(unsafe.Sizeof(c))
}

func (c *chunk) alloc(sz int) []byte {
	c.Lock()
	defer c.Unlock()
	if int(c.off)+sz+int(PointerSize) >= len(c.data) {
		c.flag |= FULL
		return nil
	}
	data := c.data[c.off : int(c.off)+sz+int(PointerSize)]
	*((*unsafe.Pointer)(unsafe.Pointer(unsafe.SliceData(data)))) = unsafe.Pointer(c)
	c.off += uint32(sz + int(PointerSize))
	c.numAlloc++
	return data[PointerSize:]
}

func (c *chunk) free() {
	c.Lock()
	if c.numFree = c.numFree + 1; c.numFree == c.numAlloc &&
		c.flag&FULL == FULL { // this chunk is no longer needed
		unix.Munmap(c.data)
		return
	}
	c.Unlock()
}
