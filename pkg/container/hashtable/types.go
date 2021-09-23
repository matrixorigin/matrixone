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
	bucketCnt  uint32
	valueSize  uint8
	occupied   []uint64
	bucketData []byte
	agg        Aggregator
}

type FixedTableIterator struct {
	table     *FixedTable
	bitmapIdx int
	bitmapVal uint64
}

type CKHashTable struct {
	bucketCnt uint
	elemCnt   uint
	//hashBits   uint32
	keySize    uint32
	valSize    uint32
	bucketData []byte
	agg        Aggregator
}

type CKHashTableIterator struct {
	table     *CKHashTable
	bucketIdx int
}

type CKStringHashTable struct {
	h0  *FixedTable
	h1  *CKHashTable
	h2  *CKHashTable
	h3  *CKHashTable
	hs  *CKHashTable
	agg Aggregator
}

type CKStringHashTableIterator struct {
	table     *CKStringHashTable
	tableIdx  int
	bucketIdx int
}

type TwoLevelCKHashTable struct {
	htables []*CKHashTable
	agg     Aggregator
}

type TwoLevelCKHashTableIterator struct {
	table     *TwoLevelCKHashTable
	tableIdx  int
	bucketIdx int
}

type TwoLevelCKStringHashTable struct {
	htables []*CKStringHashTable
	agg     Aggregator
}

type TwoLevelCKStringHashTableIterator struct {
	table     *TwoLevelCKStringHashTable
	tableIdx  int
	bucketIdx int
}

type Aggregator interface {
	StateSize() uint8
	ResultSize() uint8

	Init(state []byte)
	ArrayInit(array []byte)

	Aggregate(state, data []byte)
	Merge(lstate, rstate []byte)
	ArrayMerge(larray, rarray []byte)

	Finalize(state, result []byte)
}
