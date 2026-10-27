// Copyright 2026 Matrix Origin
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
	"math"
	"unsafe"
)

// BuildMemoryBound uses the allocator's actual load-factor and power-of-two
// cell policy. Peak includes the previous single block during a resize; once
// block allocation is reached, old blocks are reused rather than copied.
func BuildMemoryBound(rows uint64, stringKeys bool) (retained, peak int64, ok bool) {
	cellSize := intCellSize
	metadata := int64(unsafe.Sizeof(Int64HashMap{}))
	if stringKeys {
		cellSize = strCellSize
		metadata = int64(unsafe.Sizeof(StringHashMap{}))
	}
	cells := uint64(kInitialCellCnt)
	for maxElemCnt(cells, cellSize) < rows {
		if cells > math.MaxInt64/cellSize/2 {
			return 0, 0, false
		}
		cells <<= 1
	}
	if cells > math.MaxInt64/cellSize {
		return 0, 0, false
	}
	retained = int64(cells * cellSize)
	old := retained / 2
	if old > maxBlockSize {
		old = maxBlockSize
	}
	if retained == int64(kInitialCellCnt)*int64(cellSize) {
		old = 0
	}
	// Slice-of-block descriptors are separately allocated Go storage.
	descriptors := int64((uint64(retained)+maxBlockSize-1)/maxBlockSize)*24 + metadata
	if retained > math.MaxInt64-old-descriptors {
		return 0, 0, false
	}
	retained += descriptors
	return retained, retained + old, true
}
