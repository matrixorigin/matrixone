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

package hashmap

import (
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"math"
	"unsafe"
)

// BuildMemoryBound reserves the table's resize peak, two iterators (build/probe)
// and their maximum encoded-key buffers. maxKeyUnitBytes is a checked bound from the
// largest UnitLimit-row key window, including varlen length prefixes.
func BuildMemoryBound(rows uint64, stringKeys bool, maxKeyUnitBytes int64, needSels bool) (int64, bool) {
	_, peak, ok := hashtable.BuildMemoryBound(rows, stringKeys)
	if !ok || maxKeyUnitBytes < 0 {
		return 0, false
	}
	iterator := int64(UnitLimit) * (8 + 4 + 8 + 8 + 8)
	if stringKeys {
		if maxKeyUnitBytes > (math.MaxInt64-int64(UnitLimit)*64-MaxStrIteratorCapacity)/4 {
			return 0, false
		}
		// Small reusable history can coexist with new encoding growth. Larger
		// history uses the exact preallocation/pruning path before encoding.
		iterator = int64(UnitLimit)*(24+8+8+24) + MaxStrIteratorCapacity + maxKeyUnitBytes*4
	}
	metadata := int64(unsafe.Sizeof(StrHashMap{})) + 2*int64(unsafe.Sizeof(strHashmapIterator{}))
	if peak > math.MaxInt64-metadata {
		return 0, false
	}
	peak += metadata
	if iterator > (math.MaxInt64-peak)/2 {
		return 0, false
	}
	peak += 2 * iterator
	if needSels {
		// tmp pairs coexist with offsets and values while Finalize scatters.
		if rows > uint64((math.MaxInt64-peak-8)/16) {
			return 0, false
		}
		peak += int64(rows)*16 + 8
	}
	return peak, true
}
