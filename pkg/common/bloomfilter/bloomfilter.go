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

package bloomfilter

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (bf *BloomFilter) Clean() {
	bf.bitmap.Reset()
	bf.hashSeed = nil
}

func (bf *BloomFilter) TestAndAddForVector(v *vector.Vector, callBack func(exits bool, idx int)) {
	length := v.Length()
	keys := make([][]byte, hashmap.UnitLimit)
	states := make([][3]uint64, hashmap.UnitLimit)
	bitSize := uint64(bf.bitmap.Len())
	var val1, val2, val3 uint64

	for i := 0; i < length; i += hashmap.UnitLimit {
		n := length - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		exitsArr := make([]bool, n)
		for j := 0; j < n; j++ {
			keys[j] = keys[j][:0]
			exitsArr[j] = true
		}
		encodeHashKeys(keys, v, i, n)

		for _, seed := range bf.hashSeed {
			hashtable.BytesBatchGenHashStatesWithSeed(&keys[0], &states[0], n, seed)
			for j := 0; j < n; j++ {
				val1 = states[j][0]
				if val1 > bitSize {
					val1 = val1 % bitSize
				}
				if exitsArr[j] {
					exitsArr[j] = bf.bitmap.Contains(val1)
				}
				bf.bitmap.Add(val1)

				val2 = states[j][1]
				if val2 > bitSize {
					val2 = val2 % bitSize
				}
				if exitsArr[j] {
					exitsArr[j] = bf.bitmap.Contains(val2)
				}
				bf.bitmap.Add(val2)

				val3 = states[j][2]
				if val3 > bitSize {
					val3 = val3 % bitSize
				}
				if exitsArr[j] {
					exitsArr[j] = bf.bitmap.Contains(val3)
				}
				bf.bitmap.Add(val3)
			}
		}

		for j := 0; j < n; j++ {
			callBack(exitsArr[j], i+j)
		}

	}
}
