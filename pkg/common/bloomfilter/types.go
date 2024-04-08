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

type BloomFilter struct {
	bitmap   *bitmap.Bitmap
	hashSeed []uint64

	keys      [][]byte
	states    [][3]uint64
	vals      [][]uint64
	valLength int

	addVals []uint64
}

func New(rowCount int64, probability float64) *BloomFilter {
	bitSize, seedCount := computeMemAndHashCount(rowCount, probability)
	hashSeed := make([]uint64, seedCount)
	for i := 0; i < seedCount; i++ {
		hashSeed[i] = rand.Uint64()
	}
	bits := &bitmap.Bitmap{}
	bits.InitWithSize(bitSize)

	vals := make([][]uint64, hashmap.UnitLimit)
	keys := make([][]byte, hashmap.UnitLimit)
	states := make([][3]uint64, hashmap.UnitLimit)
	for j := 0; j < hashmap.UnitLimit; j++ {
		vals[j] = make([]uint64, seedCount*3)
	}

	return &BloomFilter{
		bitmap:   bits,
		hashSeed: hashSeed,

		keys:      keys,
		states:    states,
		vals:      vals,
		addVals:   make([]uint64, hashmap.UnitLimit*3*seedCount),
		valLength: len(hashSeed) * 3,
	}
}
