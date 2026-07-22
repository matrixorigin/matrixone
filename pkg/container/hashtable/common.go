// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"unsafe"
)

const (
	kInitialCellCntBits = 10
	kInitialCellCnt     = 1 << kInitialCellCntBits
	maxBlockSize        = 256 * (1 << 20)
	MB                  = 1 << 20
)

func maxElemCnt(cellCnt, cellSize uint64) uint64 {
	totalSize := cellCnt * cellSize
	if totalSize < 64*MB {
		return cellCnt / 2
	} else if totalSize < 128*MB {
		return cellCnt * 2 / 3
	} else if totalSize < 256*MB {
		return cellCnt * 3 / 4
	}
	return cellCnt * 4 / 5
}

// EstimateInt64HashMapSize returns the cell allocation required by an
// Int64HashMap for the given number of distinct keys.  Keep this calculation
// in the hashtable package so planner memory guards follow the same growth and
// load-factor rules as the runtime implementation.
func EstimateInt64HashMapSize(cardinality uint64) uint64 {
	return estimateHashMapSize(cardinality, intCellSize)
}

// EstimateStringHashMapSize returns the cell allocation required by a
// StringHashMap for the given number of distinct keys.
func EstimateStringHashMapSize(cardinality uint64) uint64 {
	return estimateHashMapSize(cardinality, strCellSize)
}

func estimateHashMapSize(cardinality, cellSize uint64) uint64 {
	const maxUint64 = ^uint64(0)

	cellCnt := uint64(kInitialCellCnt)
	for {
		if cellCnt > maxUint64/cellSize {
			return maxUint64
		}
		if maxElemCnt(cellCnt, cellSize) >= cardinality {
			return cellCnt * cellSize
		}
		if cellCnt > maxUint64/2 {
			return maxUint64
		}
		cellCnt <<= 1
	}
}

func toUnsafePointer[T any](p *T) unsafe.Pointer {
	return unsafe.Pointer(p)
}
