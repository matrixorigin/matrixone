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

type Int64HashMapCell struct {
	Key    uint64
	Mapped uint64
}

type Int64HashMap struct {
	blockCellCntBits uint8
	cellCntMask      uint64
	//confCnt     uint64

	allCellCnt      uint64
	elemCnt         uint64
	blockMaxCellCnt uint64
	blockMaxElemCnt uint64
	rawData2        [][]byte
	cells2          [][]Int64HashMapCell
}

var intCellSize int64

func init() {
	intCellSize = int64(unsafe.Sizeof(Int64HashMapCell{}))
}

type Int64HashMapIterator struct {
	table *Int64HashMap
	pos   uint64
}

func (it *Int64HashMapIterator) Init(ht *Int64HashMap) {
	it.table = ht
}
