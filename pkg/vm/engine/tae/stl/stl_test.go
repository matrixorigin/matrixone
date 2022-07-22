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

package stl

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestAlloctor(t *testing.T) {
	allocator := NewSimpleAllocator()
	node := allocator.Alloc(10)
	assert.True(t, int(unsafe.Sizeof(int32(0)))*10 <= allocator.Usage())
	allocator.Free(node)
	assert.Equal(t, 0, allocator.Usage())
}
