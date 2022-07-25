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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
)

func init() {
	OneInt64s = make([]int64, UnitLimit)
	for i := range OneInt64s {
		OneInt64s[i] = 1
	}
}

func NewStrMap(hasNull bool) *StrHashMap {
	mp := &hashtable.StringHashMap{}
	mp.Init()
	return &StrHashMap{
		hashMap:         mp,
		hasNull:         hasNull,
		values:          make([]uint64, UnitLimit),
		zValues:         make([]int64, UnitLimit),
		keys:            make([][]byte, UnitLimit),
		strHashStates:   make([][3]uint64, UnitLimit),
		decimal64Slice:  make([]types.Decimal64, UnitLimit),
		decimal128Slice: make([]types.Decimal128, UnitLimit),
	}
}

func (m *StrHashMap) NewIterator() Iterator {
	return &strHashmapIterator{mp: m}
}

func (m *StrHashMap) GroupCount() uint64 {
	return m.rows
}

func (m *StrHashMap) AddGroup() {
	m.rows++
}

// InsertValue insert a value, return true if it is new, otherwise false
// never handle null
func (m *StrHashMap) InsertValue(val any) bool {
	defer func() { m.keys[0] = m.keys[0][:0] }()
	if m.hasNull {
		m.keys[0] = append(m.keys[0], byte(0))
	}
	switch v := val.(type) {
	case uint8:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case uint16:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case uint32:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case uint64:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case int8:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case int16:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case int32:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case int64:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case float32:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case float64:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case []byte:
		m.keys[0] = append(m.keys[0], v...)
	case types.Decimal64:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	case types.Decimal128:
		m.keys[0] = append(m.keys[0], encoding.EncodeFixed(v)...)
	}
	if l := len(m.keys[0]); l < 16 {
		m.keys[0] = append(m.keys[0], hashtable.StrKeyPadding[l:]...)
	}
	m.hashMap.InsertStringBatch(m.strHashStates, m.keys[:1], m.values[:1])
	if m.values[0] > m.rows {
		m.rows++
		return true
	}
	return false
}

// Insert a row from multiple columns into the hashmap, return true if it is new, otherwise false
func (m *StrHashMap) Insert(vecs []*vector.Vector, row int) bool {
	defer func() { m.keys[0] = m.keys[0][:0] }()
	m.encodeHashKeys(vecs, row, 1)
	m.hashMap.InsertStringBatch(m.strHashStates, m.keys[:1], m.values[:1])
	if m.values[0] > m.rows {
		m.rows++
		return true
	}
	return false
}

func (m *StrHashMap) encodeHashKeysWithScale(vecs []*vector.Vector, start, count int, scales []int32) {
	for i, vec := range vecs {
		switch typLen := vec.Typ.TypeSize(); typLen {
		case 1:
			fillGroupStr[uint8](m, vec, count, 1, start, scales[i])
		case 2:
			fillGroupStr[uint16](m, vec, count, 2, start, scales[i])
		case 4:
			fillGroupStr[uint32](m, vec, count, 4, start, scales[i])
		case 8:
			fillGroupStr[uint64](m, vec, count, 8, start, scales[i])
		case 16:
			fillGroupStr[types.Decimal128](m, vec, count, 16, start, scales[i])
		default:
			fillStringGroupStr(m, vec, count, start)
		}
	}
	for i := 0; i < count; i++ {
		if l := len(m.keys[i]); l < 16 {
			m.keys[i] = append(m.keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

func (m *StrHashMap) encodeHashKeys(vecs []*vector.Vector, start, count int) {
	for _, vec := range vecs {
		switch typLen := vec.Typ.TypeSize(); typLen {
		case 1:
			fillGroupStr[uint8](m, vec, count, 1, start, 0)
		case 2:
			fillGroupStr[uint16](m, vec, count, 2, start, 0)
		case 4:
			fillGroupStr[uint32](m, vec, count, 4, start, 0)
		case 8:
			fillGroupStr[uint64](m, vec, count, 8, start, 0)
		case 16:
			fillGroupStr[types.Decimal128](m, vec, count, 16, start, 0)
		default:
			fillStringGroupStr(m, vec, count, start)
		}
	}
	for i := 0; i < count; i++ {
		if l := len(m.keys[i]); l < 16 {
			m.keys[i] = append(m.keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

func fillStringGroupStr(m *StrHashMap, vec *vector.Vector, n int, start int) {
	vs := vector.GetStrColumn(vec)
	if !vec.GetNulls().Any() {
		for i := 0; i < n; i++ {
			if m.hasNull {
				m.keys[i] = append(m.keys[i], byte(0))
			}
			m.keys[i] = append(m.keys[i], vs.Get(int64(i+start))...)
		}
	} else {
		nsp := vec.GetNulls()
		for i := 0; i < n; i++ {
			hasNull := nsp.Contains(uint64(i + start))
			if m.hasNull {
				if hasNull {
					m.keys[i] = append(m.keys[i], byte(1))
				} else {
					m.keys[i] = append(m.keys[i], byte(0))
					m.keys[i] = append(m.keys[i], vs.Get(int64(i+start))...)
				}
			} else {
				if hasNull {
					m.zValues[i] = 0
					continue
				}
				m.keys[i] = append(m.keys[i], vs.Get(int64(i+start))...)
			}
		}
	}
}

func fillGroupStr[T any](m *StrHashMap, vec *vector.Vector, n int, sz int, start int, scale int32) {
	var data []byte

	if scale > 0 {
		if vec.Typ.Oid == types.T_decimal64 {
			src := vector.GetFixedVectorValues[types.Decimal64](vec, sz)
			vs := types.AlignDecimal64UsingScaleDiffBatch(src[start:start+n], m.decimal64Slice[:n], scale)
			data = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
		} else if vec.Typ.Oid == types.T_decimal128 {
			src := vector.GetFixedVectorValues[types.Decimal128](vec, sz)
			vs := m.decimal128Slice[:n]
			types.AlignDecimal128UsingScaleDiffBatch(src[start:start+n], vs, scale)
			data = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
		}
	} else {
		vs := vector.GetFixedVectorValues[T](vec, int(sz))
		data = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
	}
	if !vec.GetNulls().Any() {
		for i := 0; i < n; i++ {
			if m.hasNull {
				m.keys[i] = append(m.keys[i], byte(0))
			}
			m.keys[i] = append(m.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
		}
	} else {
		nsp := vec.GetNulls()
		for i := 0; i < n; i++ {
			isNull := nsp.Contains(uint64(i + start))
			if m.hasNull {
				if isNull {
					m.keys[i] = append(m.keys[i], byte(1))
				} else {
					m.keys[i] = append(m.keys[i], byte(0))
					m.keys[i] = append(m.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
				}
			} else {
				if isNull {
					m.zValues[i] = 0
					continue
				}
				m.keys[i] = append(m.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		}
	}
}
