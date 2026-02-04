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
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Clean releases all resources of the bloom filter.
func (bf *BloomFilter) Clean() {
	if bf.shared != nil {
		bf.shared.bitmap.Reset()
		bf.shared.hashSeed = nil
	}
	bf.shared = nil
	bf.keys = nil
	bf.states = nil
	bf.vals = nil
	bf.addVals = nil
}

// NewHandle creates a new BloomFilter handle that shares the underlying data
// but has its own scratch buffers for thread-safe concurrent testing.
func (bf *BloomFilter) NewHandle() *BloomFilter {
	if bf == nil || bf.shared == nil {
		return nil
	}
	atomic.AddInt32(&bf.shared.refCount, 1)

	seedCount := len(bf.shared.hashSeed)
	newBF := &BloomFilter{
		shared:    bf.shared,
		valLength: bf.valLength,
		keys:      make([][]byte, hashmap.UnitLimit),
		states:    make([][3]uint64, hashmap.UnitLimit),
		vals:      make([][]uint64, hashmap.UnitLimit),
		addVals:   make([]uint64, hashmap.UnitLimit*3*seedCount),
	}
	for j := 0; j < hashmap.UnitLimit; j++ {
		newBF.vals[j] = make([]uint64, seedCount*3)
	}
	return newBF
}

// Free decrements the reference count of the shared data and cleans up if it reaches zero.
func (bf *BloomFilter) Free() {
	if bf == nil || bf.shared == nil {
		return
	}
	if atomic.AddInt32(&bf.shared.refCount, -1) == 0 {
		bf.Clean()
	} else {
		bf.shared = nil
		bf.keys = nil
		bf.states = nil
		bf.vals = nil
		bf.addVals = nil
	}
}

func (bf *BloomFilter) Add(v *vector.Vector) {
	length := v.Length()
	bitSize := uint64(bf.shared.bitmap.Len())
	step := hashmap.UnitLimit
	lastSeed := len(bf.shared.hashSeed) - 1

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
		encodeHashKeys(bf.keys, v, i, n)

		idx = 0
		for k = 0; k < lastSeed; k++ {
			hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.shared.hashSeed[k])
			for j = 0; j < n; j++ {
				bf.addVals[idx] = getIdxVal(bf.states[j][0])
				idx++
				bf.addVals[idx] = getIdxVal(bf.states[j][1])
				idx++
				bf.addVals[idx] = getIdxVal(bf.states[j][2])
				idx++
			}
		}
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.shared.hashSeed[lastSeed])
		for j = 0; j < n; j++ {
			bf.addVals[idx] = getIdxVal(bf.states[j][0])
			idx++
			bf.addVals[idx] = getIdxVal(bf.states[j][1])
			idx++
			bf.addVals[idx] = getIdxVal(bf.states[j][2])
			idx++
			bf.keys[j] = bf.keys[j][:0]
		}
		bf.shared.bitmap.AddMany(bf.addVals[:idx])
	}
}

func (bf *BloomFilter) Test(v *vector.Vector, callBack func(bool, int)) {
	bf.handle(v, func(idx, beginIdx int) {
		exist := true
		vals := bf.vals[idx]
		for j := 0; j < bf.valLength; j++ {
			exist = bf.shared.bitmap.Contains(vals[j])
			if !exist {
				break
			}
		}
		callBack(exist, beginIdx+idx)
	})
}

// TestRow tests if a single row might be in the bloom filter.
func (bf *BloomFilter) TestRow(v *vector.Vector, row int) bool {
	if row < 0 || row >= v.Length() {
		return false
	}

	bf.keys[0] = bf.keys[0][:0]
	encodeHashKeys(bf.keys[:1], v, row, 1)

	bitSize := uint64(bf.shared.bitmap.Len())
	lastSeed := len(bf.shared.hashSeed) - 1

	getIdxVal := func(v uint64) uint64 {
		if bitSize == 0 || v < bitSize {
			return v
		} else {
			return v % bitSize
		}
	}

	for k := 0; k < lastSeed; k++ {
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], 1, bf.shared.hashSeed[k])
		idx := k * 3
		bf.vals[0][idx] = getIdxVal(bf.states[0][0])
		bf.vals[0][idx+1] = getIdxVal(bf.states[0][1])
		bf.vals[0][idx+2] = getIdxVal(bf.states[0][2])
	}
	hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], 1, bf.shared.hashSeed[lastSeed])
	idx := lastSeed * 3
	bf.vals[0][idx] = getIdxVal(bf.states[0][0])
	bf.vals[0][idx+1] = getIdxVal(bf.states[0][1])
	bf.vals[0][idx+2] = getIdxVal(bf.states[0][2])

	bf.keys[0] = bf.keys[0][:0]

	vals := bf.vals[0]
	for j := 0; j < bf.valLength; j++ {
		if !bf.shared.bitmap.Contains(vals[j]) {
			return false
		}
	}
	return true
}

// TestRowDebug tests if a single row might be in the bloom filter with debug info.
// Returns (result, encodedKey, hashVals)
func (bf *BloomFilter) TestRowDebug(v *vector.Vector, row int) (bool, []byte, []uint64) {
	if row < 0 || row >= v.Length() {
		return false, nil, nil
	}

	bf.keys[0] = bf.keys[0][:0]
	encodeHashKeys(bf.keys[:1], v, row, 1)

	// Copy the encoded key for debugging
	encodedKey := make([]byte, len(bf.keys[0]))
	copy(encodedKey, bf.keys[0])

	bitSize := uint64(bf.shared.bitmap.Len())
	lastSeed := len(bf.shared.hashSeed) - 1

	getIdxVal := func(v uint64) uint64 {
		if bitSize == 0 || v < bitSize {
			return v
		} else {
			return v % bitSize
		}
	}

	for k := 0; k < lastSeed; k++ {
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], 1, bf.shared.hashSeed[k])
		idx := k * 3
		bf.vals[0][idx] = getIdxVal(bf.states[0][0])
		bf.vals[0][idx+1] = getIdxVal(bf.states[0][1])
		bf.vals[0][idx+2] = getIdxVal(bf.states[0][2])
	}
	hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], 1, bf.shared.hashSeed[lastSeed])
	idx := lastSeed * 3
	bf.vals[0][idx] = getIdxVal(bf.states[0][0])
	bf.vals[0][idx+1] = getIdxVal(bf.states[0][1])
	bf.vals[0][idx+2] = getIdxVal(bf.states[0][2])

	bf.keys[0] = bf.keys[0][:0]

	// Copy hash values for debugging
	hashVals := make([]uint64, bf.valLength)
	copy(hashVals, bf.vals[0][:bf.valLength])

	vals := bf.vals[0]
	for j := 0; j < bf.valLength; j++ {
		if !bf.shared.bitmap.Contains(vals[j]) {
			return false, encodedKey, hashVals
		}
	}
	return true, encodedKey, hashVals
}

func (bf *BloomFilter) TestAndAdd(v *vector.Vector, callBack func(bool, int)) {
	bf.handle(v, func(idx, beginIdx int) {
		var contains bool
		exist := true
		vals := bf.vals[idx]
		for j := 0; j < bf.valLength; j++ {
			if exist {
				contains = bf.shared.bitmap.Contains(vals[j])
				if !contains {
					bf.shared.bitmap.Add(vals[j])
					exist = false
				}
			} else {
				bf.shared.bitmap.Add(vals[j])
			}
		}
		callBack(exist, beginIdx+idx)
	})
}

// Marshal encodes BloomFilter into byte sequence.
func (bf *BloomFilter) Marshal() ([]byte, error) {
	var buf bytes.Buffer

	seedCount := uint32(len(bf.shared.hashSeed))
	buf.Write(types.EncodeUint32(&seedCount))
	for i := 0; i < int(seedCount); i++ {
		buf.Write(types.EncodeUint64(&bf.shared.hashSeed[i]))
	}

	bmBytes := bf.shared.bitmap.Marshal()
	bmLen := uint32(len(bmBytes))
	buf.Write(types.EncodeUint32(&bmLen))
	buf.Write(bmBytes)

	return buf.Bytes(), nil
}

// Unmarshal restores BloomFilter from byte sequence and initializes internal handles.
func (bf *BloomFilter) Unmarshal(data []byte) error {
	if len(data) < 4 {
		return moerr.NewInternalErrorNoCtx("invalid bloomfilter data")
	}

	seedCount := int(types.DecodeUint32(data[:4]))
	data = data[4:]

	if seedCount <= 0 {
		return moerr.NewInternalErrorNoCtx("invalid bloomfilter seed count")
	}

	hashSeed := make([]uint64, seedCount)
	for i := 0; i < seedCount; i++ {
		if len(data) < 8 {
			return moerr.NewInternalErrorNoCtx("invalid bloomfilter data (seed truncated)")
		}
		hashSeed[i] = types.DecodeUint64(data[:8])
		data = data[8:]
	}

	if len(data) < 4 {
		return moerr.NewInternalErrorNoCtx("invalid bloomfilter data (no bitmap length)")
	}
	bmLen := int(types.DecodeUint32(data[:4]))
	data = data[4:]
	if bmLen < 0 || len(data) < bmLen {
		return moerr.NewInternalErrorNoCtx("invalid bloomfilter data (bitmap truncated)")
	}

	var bm bitmap.Bitmap
	bm.Unmarshal(data[:bmLen])

	bf.shared = &sharedData{
		bitmap:   bm,
		hashSeed: hashSeed,
		refCount: 1,
	}

	bf.keys = make([][]byte, hashmap.UnitLimit)
	bf.states = make([][3]uint64, hashmap.UnitLimit)
	bf.vals = make([][]uint64, hashmap.UnitLimit)
	for j := 0; j < hashmap.UnitLimit; j++ {
		bf.vals[j] = make([]uint64, seedCount*3)
	}
	bf.addVals = make([]uint64, hashmap.UnitLimit*3*seedCount)
	bf.valLength = len(hashSeed) * 3

	return nil
}

// GetBitmapLen returns the length of the bitmap for debugging
func (bf *BloomFilter) GetBitmapLen() int64 {
	if bf == nil || bf.shared == nil {
		return 0
	}
	return bf.shared.bitmap.Len()
}

// GetSeedCount returns the number of hash seeds for debugging
func (bf *BloomFilter) GetSeedCount() int {
	if bf == nil || bf.shared == nil {
		return 0
	}
	return len(bf.shared.hashSeed)
}

// GetFirstSeed returns the first hash seed for debugging
func (bf *BloomFilter) GetFirstSeed() uint64 {
	if bf == nil || bf.shared == nil || len(bf.shared.hashSeed) == 0 {
		return 0
	}
	return bf.shared.hashSeed[0]
}

// GetAllSeeds returns all hash seeds for debugging
func (bf *BloomFilter) GetAllSeeds() []uint64 {
	if bf == nil || bf.shared == nil {
		return nil
	}
	return bf.shared.hashSeed
}

// GetBitmapCount returns the number of bits set in the bitmap for debugging
func (bf *BloomFilter) GetBitmapCount() int {
	if bf == nil || bf.shared == nil {
		return 0
	}
	return bf.shared.bitmap.Count()
}

// handle computes the hash value of each element and executes the callback.
func (bf *BloomFilter) handle(v *vector.Vector, callBack func(int, int)) {
	length := v.Length()
	bitSize := uint64(bf.shared.bitmap.Len())
	lastSeed := len(bf.shared.hashSeed) - 1
	step := hashmap.UnitLimit

	var i, j, n, k, idx int
	getIdxVal := func(v uint64) uint64 {
		if bitSize == 0 || v < bitSize {
			return v
		} else {
			return v % bitSize
		}
	}

	for i = 0; i < length; i += step {
		n = length - i
		if n > step {
			n = step
		}

		encodeHashKeys(bf.keys, v, i, n)

		for k = 0; k < lastSeed; k++ {
			hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.shared.hashSeed[k])
			idx = k * 3
			for j := 0; j < n; j++ {
				bf.vals[j][idx] = getIdxVal(bf.states[j][0])
				bf.vals[j][idx+1] = getIdxVal(bf.states[j][1])
				bf.vals[j][idx+2] = getIdxVal(bf.states[j][2])
			}
		}
		hashtable.BytesBatchGenHashStatesWithSeed(&bf.keys[0], &bf.states[0], n, bf.shared.hashSeed[lastSeed])
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
