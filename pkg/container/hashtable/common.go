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
)

func maxElemCnt(cellCnt, cellSize uint64) uint64 {
	if cellCnt*cellSize < maxBlockSize {
		return cellCnt / 2
	} else {
		return cellCnt * 3 / 4
	}
}

type Aggregator interface {
	StateSize() uint8
	ResultSize() uint8
	NeedsInit() bool

	Init(state unsafe.Pointer)
	AddBatch(states []unsafe.Pointer, values unsafe.Pointer)
	MergeBatch(lstates, rstates []unsafe.Pointer)

	Finalize(states, results []unsafe.Pointer)
}
