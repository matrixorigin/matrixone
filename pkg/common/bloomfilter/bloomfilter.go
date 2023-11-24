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
	bf.bitmap = nil
	bf.hashSeed = nil
	bf.keys = nil
	bf.states = nil
	bf.vals = nil
}

func (bf *BloomFilter) Test(v *vector.Vector, callBack func(bool, int)) {
	valLength := len(bf.hashSeed) * 3
	var exits bool
	var vals []uint64
	bf.getValue(v, func(idx int, beginIdx int) {
		exits = true
		vals = bf.vals[idx]
		for j := 0; j < valLength; j++ {
			exits = bf.bitmap.Contains(vals[j])
			if !exits {
				break
			}
		}
		callBack(exits, beginIdx+idx)
	})
}

func (bf *BloomFilter) Add(v *vector.Vector) {
	valLength := len(bf.hashSeed) * 3
	var vals []uint64
	bf.getValue(v, func(idx int, _ int) {
		vals = bf.vals[idx]
		for j := 0; j < valLength; j++ {
			bf.bitmap.Add(vals[j])
		}
	})
}

func (bf *BloomFilter) TestAndAdd(v *vector.Vector, callBack func(bool, int)) {
	valLength := len(bf.hashSeed) * 3
	var exits bool
	var vals []uint64
	var contains bool
	bf.getValue(v, func(idx int, beginIdx int) {
		exits = true
		vals = bf.vals[idx]
		for j := 0; j < valLength; j++ {
			if exits {
				contains = bf.bitmap.Contains(vals[j])
				if !contains {
					bf.bitmap.Add(vals[j])
					exits = false
				}
			} else {
				bf.bitmap.Add(vals[j])
			}
		}
		callBack(exits, beginIdx+idx)
	})
}

func (bf *BloomFilter) getValue(v *vector.Vector, callBack func(int, int)) {
	length := v.Length()
	bitSize := uint64(bf.bitmap.Len())
	lastSeed := len(bf.hashSeed) - 1
	step := hashmap.UnitLimit
	var i, j, n, k, idx int

	getIdxVal := func(v uint64) uint64 {
		if v >= bitSize {
			return v % bitSize
		}
		return v
	}

	for i = 0; i < length; i += step {
		n = length - i
		if n > step {
			n = step
		}

		for j = 0; j < n; j++ {
			bf.keys[j] = bf.keys[j][:0]
		}
		encodeHashKeys(bf.keys, v, i, n)

		for k = 0; k < lastSeed; k++ {
			hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.hashSeed[k])
			idx = k * 3
			for j = 0; j < n; j++ {
				bf.vals[j][idx] = getIdxVal(bf.states[j][0])
				bf.vals[j][idx+1] = getIdxVal(bf.states[j][1])
				bf.vals[j][idx+2] = getIdxVal(bf.states[j][2])
			}
		}
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.hashSeed[lastSeed])
		idx = lastSeed * 3
		for j = 0; j < n; j++ {
			bf.vals[j][idx] = getIdxVal(bf.states[j][0])
			bf.vals[j][idx+1] = getIdxVal(bf.states[j][1])
			bf.vals[j][idx+2] = getIdxVal(bf.states[j][2])
			callBack(j, i)
		}
	}
}
