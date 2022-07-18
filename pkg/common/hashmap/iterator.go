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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (itr *strHashmapIterator) Find(start, count int, vecs []*vector.Vector) []uint64 {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	for _, vec := range vecs {
		switch typLen := vec.Typ.TypeSize(); typLen {
		case 1:
			fillGroupStr[uint8](itr.mp, vec, count, 1, start)
		case 2:
			fillGroupStr[uint16](itr.mp, vec, count, 2, start)
		case 4:
			fillGroupStr[uint32](itr.mp, vec, count, 4, start)
		case 8:
			fillGroupStr[uint64](itr.mp, vec, count, 8, start)
		case 16:
			fillGroupStr[types.Decimal128](itr.mp, vec, count, 16, start)
		default:
			fillStringGroupStr(itr.mp, vec, count, start)
		}
	}
	for i := 0; i < count; i++ {
		if l := len(itr.mp.keys[i]); l < 16 {
			itr.mp.keys[i] = append(itr.mp.keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
	itr.mp.hashMap.FindStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	return itr.mp.values
}

func (itr *strHashmapIterator) Insert(start, count int, vecs []*vector.Vector) []uint64 {
	defer func() {
		for i := 0; i < count; i++ {
			itr.mp.keys[i] = itr.mp.keys[i][:0]
		}
	}()
	copy(itr.mp.zValues[:count], OneInt64s[:count])
	for _, vec := range vecs {
		switch typLen := vec.Typ.TypeSize(); typLen {
		case 1:
			fillGroupStr[uint8](itr.mp, vec, count, 1, start)
		case 2:
			fillGroupStr[uint16](itr.mp, vec, count, 2, start)
		case 4:
			fillGroupStr[uint32](itr.mp, vec, count, 4, start)
		case 8:
			fillGroupStr[uint64](itr.mp, vec, count, 8, start)
		case 16:
			fillGroupStr[types.Decimal128](itr.mp, vec, count, 16, start)
		default:
			fillStringGroupStr(itr.mp, vec, count, start)
		}
	}
	for i := 0; i < count; i++ {
		if l := len(itr.mp.keys[i]); l < 16 {
			itr.mp.keys[i] = append(itr.mp.keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
	if itr.mp.hasNull {
		itr.mp.hashMap.InsertStringBatchWithRing(itr.mp.zValues, itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	} else {
		itr.mp.hashMap.InsertStringBatch(itr.mp.strHashStates, itr.mp.keys[:count], itr.mp.values)
	}
	return itr.mp.values
}
