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
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"unsafe"
)

var wrongUseOfIntHashTable = errors.New(errno.InternalError, "wrong use of IntHashMap")

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
	mp *IntHashMap
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

func (m *IntHashMap) NewIterator() *intHashMapIterator {
	return &intHashMapIterator{mp: m}
}

func (m *IntHashMap) GroupCount() uint64 {
	return m.rows
}

func (m *IntHashMap) AddGroup() {
	m.rows++
}

func (m *IntHashMap) encodeHashKeys(vecs []*vector.Vector, start, count int) error {
	targetLength := 8
	if m.hasNull {
		targetLength -= len(vecs)
	}
	for _, vec := range vecs {
		targetLength -= vec.Typ.TypeSize()
	}
	if targetLength != 0 {
		return wrongUseOfIntHashTable
	}

	for _, vec := range vecs {
		switch vec.Typ.Oid.FixedLength() {
		case 1:
			fillKeys[uint8](m, vec, 1, start, count)
		case 2:
			fillKeys[uint16](m, vec, 2, start, count)
		case 4:
			fillKeys[uint32](m, vec, 4, start, count)
		case 8:
			fillKeys[uint64](m, vec, 8, start, count)
		//case -8:
		//	fillKeys[types.Decimal64](m, vec, 8, start, count)
		default:
			return wrongUseOfIntHashTable
		}
	}
	return nil
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
			add.Uint32AddScalar(1+size, keyOffs[:n], keyOffs[:n])
		} else {
			for i := 0; i < n; i++ {
				*(*T)(unsafe.Add(unsafe.Pointer(&keys[i]), keyOffs[i])) = vs[i+start]
			}
			add.Uint32AddScalar(size, keyOffs[:n], keyOffs[:n])
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
