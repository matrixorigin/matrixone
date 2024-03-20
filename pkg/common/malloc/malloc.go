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
	"github.com/nnsgmsone/mmap"
)

// New allocates a slice of size n. The returned slice is from manually managed
// memory and MUST be released by calling Free. Failure to do so will result in
// a memory leak.
func Alloc(n int) []byte {
	return mmap.Alloc(n)
}

func Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	mmap.Free(b)
}
