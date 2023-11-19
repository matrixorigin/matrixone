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
	"math"
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
)

var constLogValue float64

func init() {
	constLogValue = math.Log(1 / math.Pow(2, math.Log(2)))
}

// JoinMap is used for join
type BloomFilter struct {
	bitmap   *bitmap.Bitmap
	hashSeed []uint64

	keys   [][]byte
	states [][3]uint64
	vals   [][]uint64
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

		keys:   keys,
		states: states,
		vals:   vals,
	}
}

func computeMemAndHashCount(rowCount int64, probability float64) (int64, int) {
	if rowCount < 10001 {
		return 64 * 10000, 1
	} else if rowCount < 100001 {
		return 64 * 100000, 1
	} else if rowCount < 1000001 {
		return 16 * 1000000, 1
	} else if rowCount < 10000001 {
		return 38 * 10000000, 2
	} else if rowCount < 100000001 {
		// m := ceil((rowCount * log(0.000001)) / log(1/pow(2, log(2))))
		m := math.Ceil((float64(rowCount) * math.Log(probability)) / constLogValue)
		return int64(m), 3
	} else if rowCount < 1000000001 {
		// m := ceil((rowCount * log(0.000001)) / log(1/pow(2, log(2))))
		m := math.Ceil((float64(rowCount) * math.Log(probability)) / constLogValue)
		return int64(m), 3
	} else if rowCount < 10000000001 {
		// m := ceil((rowCount * log(0.000001)) / log(1/pow(2, log(2))))
		m := math.Ceil((float64(rowCount) * math.Log(probability)) / constLogValue)
		return int64(m), 4
	} else {
		panic("unsupport rowCount")
	}
}
