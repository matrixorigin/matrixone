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

package fileservice

// RCBytes represents a reference counting []byte from a pool
// newly created RCBytes' ref count is 1
// owner should call Release to give it back to the pool
// new sharing owner should call Retain to increase ref count
type RCBytes struct {
	*RCPoolItem[[]byte]
}

func (r RCBytes) Bytes() []byte {
	return r.Value
}

func (r RCBytes) Slice(length int) CacheData {
	r.Value = r.Value[:length]
	return r
}

func (r RCBytes) Release() {
	if r.RCPoolItem != nil {
		r.RCPoolItem.Release()
	}
}

func (r RCBytes) Copy() []byte {
	ret := make([]byte, len(r.Value))
	copy(ret, r.Value)
	return ret
}

type rcBytesPool struct {
	sizes []int
	pools []*RCPool[[]byte]
}

const (
	rcBytesPoolMinCap = 1 * 1024
	rcBytesPoolMaxCap = 1 * 1024 * 1024
)

// RCBytesPool is the global RCBytes pool
var RCBytesPool = func() *rcBytesPool {
	ret := &rcBytesPool{}
	for size := rcBytesPoolMinCap; size <= rcBytesPoolMaxCap; size *= 2 {
		size := size
		ret.sizes = append(ret.sizes, size)
		ret.pools = append(ret.pools, NewRCPool(func() []byte {
			return make([]byte, size)
		}))
	}
	return ret
}()

// Get returns an RCBytes
func (r *rcBytesPool) Get(size int) RCBytes {
	if size > rcBytesPoolMaxCap {
		item := RCBytes{
			RCPoolItem: &RCPoolItem[[]byte]{
				Value: make([]byte, size),
			},
		}
		item.Retain()
		return item
	}

	for i, poolSize := range r.sizes {
		if poolSize >= size {
			item := r.pools[i].Get()
			item.Value = item.Value[:size]
			return RCBytes{
				RCPoolItem: item,
			}
		}
	}

	panic("impossible")
}

// GetAndCopy gets an RCBytes and copy data to it
func (r *rcBytesPool) GetAndCopy(from []byte) RCBytes {
	bs := r.Get(len(from))
	copy(bs.Value, from)
	return bs
}

var _ CacheDataAllocator = new(rcBytesPool)

func (r *rcBytesPool) Alloc(size int) CacheData {
	return r.Get(size)
}
