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
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

// sharedData holds the actual bloom filter data that can be shared among multiple handles.
type sharedData struct {
	bitmap   bitmap.Bitmap
	hashSeed []uint64
	refCount int32
}

// BloomFilter is a handle to the bloom filter data. It contains private scratch buffers
// to allow concurrent testing without locking.
type BloomFilter struct {
	shared    *sharedData
	valLength int

	// Private scratch buffers for vectorized operations
	keys    [][]byte
	states  [][3]uint64
	vals    [][]uint64
	addVals []uint64 // used for Add operations
}

// Valid returns true if the BloomFilter is initialized and usable.
func (bf *BloomFilter) Valid() bool {
	return bf != nil && bf.shared != nil
}

func New(rowCount int64, probability float64) BloomFilter {
	bitSize, seedCount := computeMemAndHashCount(rowCount, probability)
	hashSeed := make([]uint64, seedCount)
	for i := 0; i < seedCount; i++ {
		hashSeed[i] = uint64(rand.Int63())
	}
	bits := bitmap.Bitmap{}
	bits.InitWithSize(bitSize)

	shared := &sharedData{
		bitmap:   bits,
		hashSeed: hashSeed,
		refCount: 1,
	}

	bf := BloomFilter{
		shared:    shared,
		valLength: seedCount * 3,
		keys:      make([][]byte, hashmap.UnitLimit),
		states:    make([][3]uint64, hashmap.UnitLimit),
		vals:      make([][]uint64, hashmap.UnitLimit),
		addVals:   make([]uint64, hashmap.UnitLimit*3*seedCount),
	}

	for j := 0; j < hashmap.UnitLimit; j++ {
		bf.vals[j] = make([]uint64, seedCount*3)
	}

	return bf
}

func (bf *BloomFilter) Reset() {
	if bf.shared == nil {
		return
	}
	end := uint64(bf.shared.bitmap.Len())
	bf.shared.bitmap.RemoveRange(0, end)

	for i := range bf.keys {
		bf.keys[i] = nil
	}
	for i := range bf.vals {
		for j := range bf.vals[i] {
			bf.vals[i][j] = 0
		}
	}
	for i := range bf.addVals {
		bf.addVals[i] = 0
	}
}
