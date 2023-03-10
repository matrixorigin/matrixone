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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func init() {
	zeroUint64 = make([]uint64, UnitLimit)
	zeroUint32 = make([]uint32, UnitLimit)
}

func NewIntHashMap(hasNull bool, ibucket, nbucket uint64, m *mpool.MPool) (*IntHashMap, error) {
	mp := &hashtable.Int64HashMap{}
	if err := mp.Init(m); err != nil {
		return nil, err
	}
	return &IntHashMap{
		m:       m,
		rows:    0,
		hasNull: hasNull,
		ibucket: ibucket,
		nbucket: nbucket,
		keys:    make([]uint64, UnitLimit),
		keyOffs: make([]uint32, UnitLimit),
		values:  make([]uint64, UnitLimit),
		zValues: make([]int64, UnitLimit),
		hashes:  make([]uint64, UnitLimit),
		hashMap: mp,
	}, nil
}

func (m *IntHashMap) NewIterator() *intHashMapIterator {
	return &intHashMapIterator{
		mp:      m,
		m:       m.m,
		ibucket: m.ibucket,
		nbucket: m.nbucket,
	}
}

func (m *IntHashMap) HasNull() bool {
	return m.hasNull
}

func (m *IntHashMap) Free() {
	m.hashMap.Free(m.m)
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

func (m *IntHashMap) Size() int64 {
	// TODO: add the size of the other IntHashMap parts
	if m.hashMap == nil {
		return 0
	}
	return m.hashMap.Size()
}

func (m *IntHashMap) Cardinality() uint64 {
	return m.hashMap.Cardinality()
}

func (m *IntHashMap) encodeHashKeys(vecs []*vector.Vector, start, count int) {
	for _, vec := range vecs {
		switch vec.GetType().TypeSize() {
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

func fillKeys[T types.FixedSizeT](m *IntHashMap, vec *vector.Vector, size uint32, start int, n int) {
	keys := m.keys
	keyOffs := m.keyOffs
	if vec.IsConstNull() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 1
				keyOffs[i]++
			}
		} else {
			for i := 0; i < n; i++ {
				m.zValues[i] = 0
			}
		}
	} else if vec.IsConst() {
		ptr := (*T)(vector.GetPtrAt(vec, 0))
		for i := 0; i < n; i++ {
			*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
			*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i]+1)) = *ptr
		}
		uint32AddScalar(size, keyOffs[:n], keyOffs[:n])
	} else if !vec.GetNulls().Any() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
				ptr := (*T)(vector.GetPtrAt(vec, int64(i+start)))
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i]+1)) = *ptr
			}
			uint32AddScalar(1+size, keyOffs[:n], keyOffs[:n])
		} else {
			for i := 0; i < n; i++ {
				ptr := (*T)(vector.GetPtrAt(vec, int64(i+start)))
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = *ptr
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
					ptr := (*T)(vector.GetPtrAt(vec, int64(i+start)))
					*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i]+1)) = *ptr
					keyOffs[i] += 1 + size
				}
			}
		} else {
			for i := 0; i < n; i++ {
				if nsp.Contains(uint64(i + start)) {
					m.zValues[i] = 0
					continue
				}
				ptr := (*T)(vector.GetPtrAt(vec, int64(i+start)))
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = *ptr
				keyOffs[i] += size
			}
		}
	}
}

func fillStrKey(m *IntHashMap, vec *vector.Vector, start int, n int) {
	keys := m.keys
	keyOffs := m.keyOffs
	if vec.IsConstNull() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 1
				keyOffs[i]++
			}
		} else {
			for i := 0; i < n; i++ {
				m.zValues[i] = 0
			}
		}
	} else if !vec.GetNulls().Any() {
		if m.hasNull {
			for i := 0; i < n; i++ {
				v := vec.GetBytesAt(i + start)
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]+1:], v)
				m.keyOffs[i] += uint32(len(v) + 1)
			}
		} else {
			for i := 0; i < n; i++ {
				v := vec.GetBytesAt(i + start)
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]:], v)
				m.keyOffs[i] += uint32(len(v))
			}
		}
	} else {
		nsp := vec.GetNulls()
		if m.hasNull {
			for i := 0; i < n; i++ {
				v := vec.GetBytesAt(i + start)
				if nsp.Contains(uint64(i + start)) {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 1
					keyOffs[i]++
				} else {
					*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = 0
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]+1:], v)
					m.keyOffs[i] += uint32(len(v) + 1)
				}
			}
		} else {
			for i := 0; i < n; i++ {
				v := vec.GetBytesAt(i + start)
				if nsp.Contains(uint64(i + start)) {
					m.zValues[i] = 0
					continue
				}
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), 8)[m.keyOffs[i]:], v)
				m.keyOffs[i] += uint32(len(v))
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
