// Copyright 2021 Matrix Origin
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

package hashmap

import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (itr *strHashmapIterator) Find(start, count int, vecs []*vector.Vector, scales []int32) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	itr.mp.encodeHashKeysWithScale(vecs, start, count, scales)
	itr.mp.hashMap.FindStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	return itr.mp.values[:count], itr.mp.zValues[:count]
}

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector, scales []int32) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	copy(itr.mp.zValues[:count], OneInt64s[:count])
	itr.mp.encodeHashKeysWithScale(vecs, start, count, scales)
	if itr.mp.hasNull {
		itr.mp.hashMap.InsertStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	} else {
		itr.mp.hashMap.InsertStringBatchWithRing(itr.mp.zValues, itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	}
	return itr.mp.values[:count], itr.mp.zValues[:count]
}

func (itr *intHashMapIterator) Find(start, count int, vecs []*vector.Vector, _ []int32) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = 0
		}
		copy(itr.mp.keyOffs[:count], zeroUint32)
	}()
	if err := itr.mp.encodeHashKeys(vecs, start, count); err != nil {
		panic(err)
	}
	copy(itr.mp.hashes[:count], zeroUint64[:count])
	itr.mp.hashMap.FindBatch(count, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values[:count])
	return itr.mp.values[:count], itr.mp.zValues[:count]
}

func (itr *intHashMapIterator) Insert(start, count int, vecs []*vector.Vector, _ []int32) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = 0
		}
		copy(itr.mp.keyOffs[:count], zeroUint32)
	}()

	if !itr.mp.hasNull {
		copy(itr.mp.zValues[:count], OneInt64s[:count])
	}
	if err := itr.mp.encodeHashKeys(vecs, start, count); err != nil {
		panic(err)
	}
	copy(itr.mp.hashes[:count], zeroUint64[:count])

	if itr.mp.hasNull {
		itr.mp.hashMap.InsertBatch(count, itr.mp.hashes, unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values)
	} else {
		itr.mp.hashMap.InsertBatchWithRing(count, itr.mp.zValues, itr.mp.hashes, unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values)
	}
	return itr.mp.values[:count], itr.mp.zValues[:count]
}
