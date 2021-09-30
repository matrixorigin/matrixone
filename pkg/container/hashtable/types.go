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

type FixedTable struct {
	inlineVal  bool
	bucketCnt  uint32
	valSize    uint8
	occupied   []byte
	bucketData []byte
}

type FixedTableIterator struct {
	table     *FixedTable
	bitmap    []uint64
	bitmapIdx uint32
	bitmapVal uint64
}

type HashTable struct {
	inlineKey     bool
	inlineVal     bool
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	keySize       uint8
	valSize       uint8
	valOffset     uint8
	bucketWidth   uint8
	bucketData    []byte
	keyHolder     [][]byte
	valHolder     [][]byte
}

type HashTableIterator struct {
	table  *HashTable
	offset uint64
	end    uint64
}

type StringHashTable struct {
	H0 *FixedTable
	H1 *HashTable
	H2 *HashTable
	H3 *HashTable
	Hs *HashTable
}

type Aggregator interface {
	StateSize() uint8
	ResultSize() uint8

	Init(state, data []byte)
	ArrayInit(array []byte)

	Aggregate(state, data []byte)
	Merge(lstate, rstate []byte)
	ArrayMerge(larray, rarray []byte)

	Finalize(state, result []byte)
}
