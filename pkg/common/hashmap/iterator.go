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

func (itr *strHashmapIterator) Find(start, count int, vecs []*vector.Vector, inBuckets []uint8) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	copy(itr.mp.zValues[:count], OneInt64s[:count])
	itr.mp.encodeHashKeys(vecs, start, count)
	if itr.nbucket != 0 {
		itr.mp.hashMap.FindStringBatchInBucket(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values, inBuckets, itr.ibucket, itr.nbucket)
	} else {
		itr.mp.hashMap.FindStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	}
	return itr.mp.values[:count], itr.mp.zValues[:count]
}

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error

	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	copy(itr.mp.zValues[:count], OneInt64s[:count])
	itr.mp.encodeHashKeys(vecs, start, count)
	if itr.nbucket != 0 {
		if itr.mp.hasNull {
			err = itr.mp.hashMap.InsertStringBatchInBucket(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values, itr.ibucket, itr.nbucket, itr.m)
		} else {
			err = itr.mp.hashMap.InsertStringBatchWithRingInBucket(itr.mp.zValues, itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values, itr.ibucket, itr.nbucket, itr.m)
		}
	} else {
		if itr.mp.hasNull {
			err = itr.mp.hashMap.InsertStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values, itr.m)
		} else {
			err = itr.mp.hashMap.InsertStringBatchWithRing(itr.mp.zValues, itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values, itr.m)
		}
	}
	vs, zvs := itr.mp.values[:count], itr.mp.zValues[:count]
	updateHashTableRows(itr.mp, vs, zvs)
	return vs, zvs, err
}

func (itr *intHashMapIterator) Find(start, count int, vecs []*vector.Vector, inBuckets []uint8) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = 0
		}
		copy(itr.mp.keyOffs[:count], zeroUint32)
	}()
	copy(itr.mp.zValues[:count], OneInt64s[:count])
	itr.mp.encodeHashKeys(vecs, start, count)
	copy(itr.mp.hashes[:count], zeroUint64[:count])
	if itr.nbucket != 0 {
		itr.mp.hashMap.FindBatchInBucket(count, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values[:count], inBuckets, itr.ibucket, itr.nbucket)
	} else {
		itr.mp.hashMap.FindBatch(count, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values[:count])
	}
	return itr.mp.values[:count], itr.mp.zValues[:count]
}

func (itr *intHashMapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error

	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = 0
		}
		copy(itr.mp.keyOffs[:count], zeroUint32)
	}()

	copy(itr.mp.zValues[:count], OneInt64s[:count])
	itr.mp.encodeHashKeys(vecs, start, count)
	copy(itr.mp.hashes[:count], zeroUint64[:count])
	if itr.nbucket != 0 {
		if itr.mp.hasNull {
			err = itr.mp.hashMap.InsertBatchInBucket(count, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values, itr.ibucket, itr.nbucket, itr.m)
		} else {
			err = itr.mp.hashMap.InsertBatchWithRingInBucket(count, itr.mp.zValues, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values, itr.ibucket, itr.nbucket, itr.m)
		}
	} else {
		if itr.mp.hasNull {
			err = itr.mp.hashMap.InsertBatch(count, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values, itr.m)
		} else {
			err = itr.mp.hashMap.InsertBatchWithRing(count, itr.mp.zValues, itr.mp.hashes[:count], unsafe.Pointer(&itr.mp.keys[0]), itr.mp.values, itr.m)
		}
	}
	vs, zvs := itr.mp.values[:count], itr.mp.zValues[:count]
	updateHashTableRows(itr.mp, vs, zvs)
	return vs, zvs, err
}

func updateHashTableRows(hashMap HashMap, vs []uint64, zvs []int64) {
	groupCount := hashMap.GroupCount()
	if hashMap.HasNull() {
		for _, v := range vs {
			if v > groupCount {
				groupCount++
			}
		}
	} else {
		for i, v := range vs {
			if zvs[i] == 0 {
				continue
			}
			if v > groupCount {
				groupCount++
			}
		}
	}
	count := groupCount - hashMap.GroupCount()
	hashMap.AddGroups(count)
}
