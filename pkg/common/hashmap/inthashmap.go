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

	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var zeroUint64 []uint64

var zeroUint32 []uint32

func init() {
	zeroUint64 = make([]uint64, UnitLimit)
	zeroUint32 = make([]uint32, UnitLimit)
}

// IntHashMap key is int64, value is an uint64 (start from 1)
// before you use the IntHashMap, the user should make sure that
// sum of vectors' length equal to 8
type IntHashMap struct {
	hasNull bool

	rows    uint64
	keys    []uint64
	keyOffs []uint32
	values  []uint64
	zValues []int64
	hashes  []uint64

	hashMap *hashtable.Int64HashMap
}

type intHashMapIterator struct {
	ibucket, nbucket uint64
	mp               *IntHashMap
}

func NewIntHashMap(hasNull bool) *IntHashMap {
	mp := &hashtable.Int64HashMap{}
	mp.Init()
	return &IntHashMap{
		rows:    0,
		hasNull: hasNull,
		keys:    make([]uint64, UnitLimit),
		keyOffs: make([]uint32, UnitLimit),
		values:  make([]uint64, UnitLimit),
		zValues: make([]int64, UnitLimit),
		hashes:  make([]uint64, UnitLimit),
		hashMap: mp,
	}
}

func (m *IntHashMap) NewIterator(ibucket, nbucket uint64) *intHashMapIterator {
	return &intHashMapIterator{
		mp:      m,
		ibucket: ibucket,
		nbucket: nbucket,
	}
}

func (m *IntHashMap) GroupCount() uint64 {
	return m.rows
}

func (m *IntHashMap) AddGroup() {
	m.rows++
}

func (m *IntHashMap) AddGroups(rows uint64) {
	m.rows += rows
}

func (m *IntHashMap) Cardinality() uint64 {
	return m.hashMap.Cardinality()
}

func (m *IntHashMap) encodeHashKeys(vecs []*vector.Vector, start, count int) {
	for _, vec := range vecs {
		switch vec.Typ.TypeSize() {
		case 1:
			fillKeys[uint8](m, vec, 1, start, count)
		case 2:
			fillKeys[uint16](m, vec, 2, start, count)
		case 4:
			fillKeys[uint32](m, vec, 4, start, count)
		case 8:
			fillKeys[uint64](m, vec, 8, start, count)
		default:
			fillStrKey(m, vec, start, count)
		}
	}
}

func fillKeys[T any](m *IntHashMap, vec *vector.Vector, size uint32, start int, n int) {
	vs := vector.GetFixedVectorValues[T](vec, int(size))
	keys := m.keys
	keyOffs := m.keyOffs
	if !vec.GetNulls().Any() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i]+1)) = vs[i+start]
			}
			uint32AddScalar(1+size, keyOffs[:n], keyOffs[:n])
		} else {
			for i := 0; i < n; i++ {
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = vs[i+start]
			}
			uint32AddScalar(size, keyOffs[:n], keyOffs[:n])
		}
	} else {
		nsp := vec.GetNulls()
		if m.hasNull {
			for i := 0; i < n; i++ {
				if nsp.Contains(uint64(i + start)) {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 1
					keyOffs[i]++
				} else {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
					*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i]+1)) = vs[i+start]
					keyOffs[i] += 1 + size
				}
			}
		} else {
			for i := 0; i < n; i++ {
				if nsp.Contains(uint64(i + start)) {
					m.zValues[i] = 0
					continue
				}
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = vs[i+start]
				keyOffs[i] += size
			}
		}
	}
}

func fillStrKey(m *IntHashMap, vec *vector.Vector, start int, n int) {
	vData, vOff, vLen := vector.GetStrVectorValues(vec)
	keys := m.keys
	keyOffs := m.keyOffs
	if !vec.GetNulls().Any() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]+1:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
				m.keyOffs[i] += vLen[i+start] + 1
			}
		} else {
			for i := 0; i < n; i++ {
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
				m.keyOffs[i] += vLen[i+start]
			}
		}
	} else {
		nsp := vec.GetNulls()
		if m.hasNull {
			for i := 0; i < n; i++ {
				if nsp.Contains(uint64(i + start)) {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 1
					keyOffs[i]++
				} else {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]+1:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
					m.keyOffs[i] += vLen[i+start] + 1
				}
			}
		} else {
			for i := 0; i < n; i++ {
				if nsp.Contains(uint64(i + start)) {
					m.zValues[i] = 0
					continue
				}
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
				m.keyOffs[i] += vLen[i+start]
			}
		}
	}
}

func uint32AddScalar(x uint32, ys, rs []uint32) []uint32 {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}
