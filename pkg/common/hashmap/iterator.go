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

func (itr *strHashmapIterator) Find(start, count int, vecs []*vector.Vector) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = itr.keys[i][:0]
		}
	}()
	copy(itr.zValues[:count], OneInt64s[:count])
	itr.encodeHashKeys(vecs, start, count)
	itr.mp.hashMap.FindStringBatch(itr.strHashStates, itr.keys[:count], itr.values)
	return itr.values[:count], itr.zValues[:count]
}

// Insert a row from multiple columns into the hashmap, return true if it is new, otherwise false
func (itr *strHashmapIterator) DetectDup(vecs []*vector.Vector, row int) (bool, error) {
	keys := itr.keys
	defer func() { keys[0] = keys[0][:0] }()
	itr.encodeHashKeys(vecs, row, 1)
	if err := itr.mp.hashMap.InsertStringBatch(itr.strHashStates, keys[:1], itr.values[:1]); err != nil {
		return false, err
	}
	if itr.values[0] > itr.mp.rows {
		itr.mp.rows++
		return true, nil
	}
	return false, nil
}

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error

	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = itr.keys[i][:0]
		}
	}()
	copy(itr.zValues[:count], OneInt64s[:count])
	itr.encodeHashKeys(vecs, start, count)

	if itr.mp.hasNull {
		err = itr.mp.hashMap.InsertStringBatch(itr.strHashStates, itr.keys[:count], itr.values)
	} else {
		err = itr.mp.hashMap.InsertStringBatchWithRing(itr.zValues, itr.strHashStates, itr.keys[:count], itr.values)
	}

	vs, zvs := itr.values[:count], itr.zValues[:count]
	updateHashTableRows(itr.mp, vs, zvs)
	return vs, zvs, err
}

func (itr *intHashMapIterator) Find(start, count int, vecs []*vector.Vector) ([]uint64, []int64) {
	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = 0
		}
		copy(itr.keyOffs[:count], zeroUint32)
	}()
	copy(itr.zValues[:count], OneInt64s[:count])
	itr.encodeHashKeys(vecs, start, count)
	copy(itr.hashes[:count], zeroUint64[:count])
	itr.mp.hashMap.FindBatch(count, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values[:count])
	return itr.values[:count], itr.zValues[:count]
}

func (itr *intHashMapIterator) DetectDup(vecs []*vector.Vector, row int) (bool, error) {
	panic("not implemented yet!!!")
}

func (itr *intHashMapIterator) Insert(start, count int, vecs []*vector.Vector) ([]uint64, []int64, error) {
	var err error

	defer func() {
		for i := 0; i < count; i++ {
			itr.keys[i] = 0
		}
		copy(itr.keyOffs[:count], zeroUint32)
	}()

	copy(itr.zValues[:count], OneInt64s[:count])
	itr.encodeHashKeys(vecs, start, count)
	copy(itr.hashes[:count], zeroUint64[:count])
	if itr.mp.hasNull {
		err = itr.mp.hashMap.InsertBatch(count, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values)
	} else {
		err = itr.mp.hashMap.InsertBatchWithRing(count, itr.zValues, itr.hashes[:count], unsafe.Pointer(&itr.keys[0]), itr.values)
	}
	vs, zvs := itr.values[:count], itr.zValues[:count]
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
