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

func (bf *BloomFilter) Reset() {
	bf.bitmap.Reset()
	bf.addVals = bf.addVals[:0]
}

func (bf *BloomFilter) Clean() {
	bf.bitmap.Reset()
	bf.bitmap = nil
	bf.hashSeed = nil
	bf.keys = nil
	bf.states = nil
	bf.vals = nil
	bf.addVals = nil
}

func (bf *BloomFilter) Add(v *vector.Vector) {
	length := v.Length()
	bitSize := uint64(bf.bitmap.Len())
	step := hashmap.UnitLimit
	lastSeed := len(bf.hashSeed) - 1

	var i, j, n, k, idx int
	getIdxVal := func(v uint64) uint64 {
		if v >= bitSize {
			return v % bitSize
		}
		return v
	}

	// There is no question of correctness if no distinction is made.
	// However, there is an unacceptable slowdown in calling the Add method.
	for i = 0; i < length; i += step {
		n = length - i
		if n > step {
			n = step
		}
		encodeHashKeys(bf.keys, v, i, n)

		idx = 0
		for k = 0; k < lastSeed; k++ {
			hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.hashSeed[k])
			for j = 0; j < n; j++ {
				bf.addVals[idx] = getIdxVal(bf.states[j][0])
				idx++
				bf.addVals[idx] = getIdxVal(bf.states[j][1])
				idx++
				bf.addVals[idx] = getIdxVal(bf.states[j][2])
				idx++
			}
		}
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.hashSeed[lastSeed])
		for j = 0; j < n; j++ {
			bf.addVals[idx] = getIdxVal(bf.states[j][0])
			idx++
			bf.addVals[idx] = getIdxVal(bf.states[j][1])
			idx++
			bf.addVals[idx] = getIdxVal(bf.states[j][2])
			idx++
			bf.keys[j] = bf.keys[j][:0]
		}
		bf.bitmap.AddMany(bf.addVals[:idx])
	}
}

func (bf *BloomFilter) Test(v *vector.Vector, callBack func(bool, int)) {
	bf.handle(v, func(idx, beginIdx int) {
		exist := true
		vals := bf.vals[idx]
		for j := 0; j < bf.valLength; j++ {
			exist = bf.bitmap.Contains(vals[j])
			if !exist {
				break
			}
		}
		callBack(exist, beginIdx+idx)
	},
	)
}

func (bf *BloomFilter) TestAndAdd(v *vector.Vector, callBack func(bool, int)) {
	bf.handle(v, func(idx, beginIdx int) {
		var contains bool
		exist := true
		vals := bf.vals[idx]
		for j := 0; j < bf.valLength; j++ {
			if exist {
				contains = bf.bitmap.Contains(vals[j])
				if !contains {
					bf.bitmap.Add(vals[j])
					exist = false
				}
			} else {
				bf.bitmap.Add(vals[j])
			}
		}
		callBack(exist, beginIdx+idx)
	})

}

// for an incoming vector, compute the hash value of each of its elements, and manipulate it with func tf.fn
func (bf *BloomFilter) handle(v *vector.Vector, callBack func(int, int)) {
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

	// The reason we need to distinguish whether an operator is an Add or not is
	// because it determines whether we can call tf.fn more efficiently or not.
	//
	// There is no question of correctness if no distinction is made. However, there is an unacceptable slowdown in calling the Add method.
	for i = 0; i < length; i += step {
		n = length - i
		if n > step {
			n = step
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
			bf.keys[j] = bf.keys[j][:0]
			callBack(j, i)
		}
	}
}
