// Copyright 2024 Matrix Origin
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
	"sync"
	"unsafe"
)

type chainDeallocator []Deallocator

var _ Deallocator = &chainDeallocator{}

func (c *chainDeallocator) Deallocate(ptr unsafe.Pointer) {
	for i := len(*c) - 1; i >= 0; i-- {
		(*c)[i].Deallocate(ptr)
	}
	*c = (*c)[:0]
	chainDeallocatorPool.Put(c)
}

func ChainDeallocator(dec1 Deallocator, dec2 Deallocator) Deallocator {
	if chain1, ok := dec1.(*chainDeallocator); ok {
		if chain2, ok := dec2.(*chainDeallocator); ok {
			*chain1 = append(*chain1, *chain2...)
			*chain2 = (*chain2)[:0]
			chainDeallocatorPool.Put(chain2)
			return chain1
		} else {
			*chain1 = append(*chain1, dec2)
			return chain1
		}
	}
	if chain2, ok := dec2.(*chainDeallocator); ok {
		chain := chainDeallocatorPool.Get().(*chainDeallocator)
		*chain = append(*chain, dec1)
		*chain = append(*chain, *chain2...)
		*chain2 = (*chain2)[:0]
		chainDeallocatorPool.Put(chain2)
		return chain
	}
	chain := chainDeallocatorPool.Get().(*chainDeallocator)
	*chain = append(*chain, dec1, dec2)
	return chain
}

var chainDeallocatorPool = sync.Pool{
	New: func() any {
		return &chainDeallocator{}
	},
}
