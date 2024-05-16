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

package malloc

import (
	"testing"
	"unsafe"
)

func TestAllocFree(t *testing.T) {
	for i := 0; i < 1<<19; i++ {
		ptr, handle := Alloc(i, true)
		bs := unsafe.Slice((*byte)(ptr), i)
		if len(bs) != i {
			t.Fatal()
		}
		handle.Free()
	}

	// evict
	stats := make(map[[2]int]int64)
	evict(stats) // collect infos
	n := cachingObjects()
	if n == 0 {
		t.Fatal()
	}
	evict(stats) // flush
	n = cachingObjects()
	if n != 0 {
		t.Fatalf("got %v", n)
	}
}
