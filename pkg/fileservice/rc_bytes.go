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

type RCBytes = RCPoolItem[[]byte]

type rcBytesPool struct {
	sizes []int
	pools []*RCPool[[]byte]
}

const (
	rcBytesPoolMinCap = 1 * 1024
	rcBytesPoolMaxCap = 1 * 1024 * 1024
)

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

func (r *rcBytesPool) Get(size int) *RCBytes {
	if size > rcBytesPoolMaxCap {
		item := &RCBytes{
			Value: make([]byte, size),
		}
		item.Retain()
		return item
	}

	for i, poolSize := range r.sizes {
		if poolSize >= size {
			item := r.pools[i].Get()
			item.Value = item.Value[:size]
			return item
		}
	}

	panic("impossible")
}
