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

func init() {
	OneInt64s = make([]int64, UnitLimit)
	for i := range OneInt64s {
		OneInt64s[i] = 1
	}
	OneUInt8s = make([]uint8, UnitLimit)
	for i := range OneUInt8s {
		OneUInt8s[i] = 1
	}
}

func NewStrMap(hasNull bool) (*StrHashMap, error) {
	mp := &hashtable.StringHashMap{}
	if err := mp.Init(nil); err != nil {
		return nil, err
	}
	return &StrHashMap{
		hashMap: mp,
		hasNull: hasNull,
	}, nil
}

func (m *StrHashMap) NewIterator() Iterator {
	return &strHashmapIterator{
		mp:            m,
		values:        make([]uint64, UnitLimit),
		zValues:       make([]int64, UnitLimit),
		keys:          make([][]byte, UnitLimit),
		strHashStates: make([][3]uint64, UnitLimit),
	}
}

func (m *StrHashMap) HasNull() bool {
	return m.hasNull
}

func (m *StrHashMap) Free() {
	m.hashMap.Free()
}

func (m *StrHashMap) PreAlloc(n uint64) error {
	return m.hashMap.ResizeOnDemand(n)
}

func (m *StrHashMap) GroupCount() uint64 {
	return m.rows
}

func (m *StrHashMap) AddGroup() {
	m.rows++
}

func (m *StrHashMap) AddGroups(rows uint64) {
	m.rows += rows
}

func (m *StrHashMap) Size() int64 {
	// TODO: add the size of the other StrHashMap parts
	if m.hashMap == nil {
		return 0
	}
	return m.hashMap.Size()
}

func (m *StrHashMap) Cardinality() uint64 {
	return m.hashMap.Cardinality()
}

func (itr *strHashmapIterator) encodeHashKeys(vecs []*vector.Vector, start, count int) {
	for _, vec := range vecs {
		if vec.GetType().IsFixedLen() {
			fillGroupStr(itr, vec, count, vec.GetType().TypeSize(), start, 0, len(vecs))
		} else {
			fillStringGroupStr(itr, vec, count, start, len(vecs))
		}
	}
	keys := itr.keys
	for i := 0; i < count; i++ {
		if l := len(keys[i]); l < 16 {
			keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

// A NULL C
// 01A101C 9 bytes
// for non-NULL value, give 3 bytes, the first byte is always 0, the last two bytes are the length
// of this value,and then append the true bytes of the value
// for NULL value, just only one byte, give one byte(1)
// these are the rules of multi-cols
// for one col, just give the value bytes
func fillStringGroupStr(itr *strHashmapIterator, vec *vector.Vector, n int, start int, lenCols int) {
	keys := itr.keys
	if vec.IsRollup() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(1))
			}
		} else {
			for i := 0; i < n; i++ {
				itr.zValues[i] = 0
			}
		}
		return
	}
	if !vec.GetNulls().Any() {
		if itr.mp.hasNull {
			rsp := vec.GetRollups()
			for i := 0; i < n; i++ {
				hasRollup := rsp.Contains(uint64(i + start))
				if hasRollup {
					keys[i] = append(keys[i], byte(2))
					continue
				}
				bytes := vec.GetBytesAt(i + start)
				// for "a"，"bc" and "ab","c", we need to distinct
				// this is not null value
				keys[i] = append(keys[i], 0)
				// give the length
				length := uint16(len(bytes))
				keys[i] = append(keys[i], unsafe.Slice((*byte)(unsafe.Pointer(&length)), 2)...)
				// append the ture value bytes
				keys[i] = append(keys[i], bytes...)
			}
		} else {
			for i := 0; i < n; i++ {
				bytes := vec.GetBytesAt(i + start)
				// for "a"，"bc" and "ab","c", we need to distinct
				// give the length
				length := uint16(len(bytes))
				keys[i] = append(keys[i], unsafe.Slice((*byte)(unsafe.Pointer(&length)), 2)...)
				// append the ture value bytes
				keys[i] = append(keys[i], bytes...)
			}
		}
	} else {
		nsp := vec.GetNulls()
		rsp := vec.GetRollups()
		for i := 0; i < n; i++ {
			hasNull := nsp.Contains(uint64(i + start))
			hasRollup := rsp.Contains(uint64(i + start))
			if itr.mp.hasNull {
				if hasRollup {
					keys[i] = append(keys[i], byte(2))
				} else if hasNull {
					keys[i] = append(keys[i], byte(1))
				} else {
					bytes := vec.GetBytesAt(i + start)
					// for "a"，"bc" and "ab","c", we need to distinct
					// this is not null value
					keys[i] = append(keys[i], 0)
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], unsafe.Slice((*byte)(unsafe.Pointer(&length)), 2)...)
					// append the ture value bytes
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				if hasNull {
					itr.zValues[i] = 0
					continue
				}
				bytes := vec.GetBytesAt(i + start)
				// for "a"，"bc" and "ab","c", we need to distinct
				// give the length
				length := uint16(len(bytes))
				keys[i] = append(keys[i], unsafe.Slice((*byte)(unsafe.Pointer(&length)), 2)...)
				// append the ture value bytes
				keys[i] = append(keys[i], bytes...)
			}
		}
	}
}

func fillGroupStr(itr *strHashmapIterator, vec *vector.Vector, n int, sz int, start int, scale int32, lenCols int) {
	keys := itr.keys
	if vec.IsRollup() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(1))
			}
		} else {
			for i := 0; i < n; i++ {
				itr.zValues[i] = 0
			}
		}
		return
	}
	if vec.IsConst() {
		data := unsafe.Slice(vector.GetPtrAt[byte](vec, 0), sz)
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], 0)
				keys[i] = append(keys[i], data...)
			}
		} else {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], data...)
			}
		}
		return
	}
	data := unsafe.Slice(vector.GetPtrAt[byte](vec, 0), (n+start)*sz)
	if !vec.GetNulls().Any() {
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				bytes := data[(i+start)*sz : (i+start+1)*sz]
				keys[i] = append(keys[i], 0)
				keys[i] = append(keys[i], bytes...)
			}
		} else {
			for i := 0; i < n; i++ {
				bytes := data[(i+start)*sz : (i+start+1)*sz]
				keys[i] = append(keys[i], bytes...)
			}
		}
	} else {
		nsp := vec.GetNulls()
		rsp := vec.GetRollups()
		for i := 0; i < n; i++ {
			isNull := nsp.Contains(uint64(i + start))
			isRollup := rsp.Contains(uint64(i + start))
			if itr.mp.hasNull {
				if isRollup {
					keys[i] = append(keys[i], 2)
				} else if isNull {
					keys[i] = append(keys[i], 1)
				} else {
					bytes := data[(i+start)*sz : (i+start+1)*sz]
					keys[i] = append(keys[i], 0)
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				if isNull {
					itr.zValues[i] = 0
					continue
				}
				bytes := data[(i+start)*sz : (i+start+1)*sz]
				keys[i] = append(keys[i], bytes...)
			}
		}
	}
}
