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
	"bytes"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

func fillStringGroupStrForConstVec(itr *strHashmapIterator, vec *vector.Vector, n int, start int) {
	keys := itr.keys
	bytes := vec.GetBytesAt(start)
	length := uint16(len(bytes))
	// can't be const null
	if itr.mp.hasNull {
		gsp := vec.GetGrouping()
		for i := 0; i < n; i++ {
			hasGrouping := gsp.Contains(uint64(i + start))
			if hasGrouping {
				keys[i] = append(keys[i], byte(2))
				continue
			}
			// for "a"，"bc" and "ab","c", we need to distinct
			// this is not null value
			keys[i] = append(keys[i], 0)
			// give the length
			keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
			// append the pure value bytes
			keys[i] = append(keys[i], bytes...)
		}
	} else {
		for i := 0; i < n; i++ {
			// for "a"，"bc" and "ab","c", we need to distinct
			// give the length
			keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
			// append the pure value bytes
			keys[i] = append(keys[i], bytes...)
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
func fillStringGroupStr(itr *strHashmapIterator, vec *vector.Vector, lenV int, start int, lenCols int) {
	keys := itr.keys
	if vec.IsGrouping() {
		for i := 0; i < lenV; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		if itr.mp.hasNull {
			for i := 0; i < lenV; i++ {
				keys[i] = append(keys[i], byte(1))
			}
		} else {
			for i := 0; i < lenV; i++ {
				itr.zValues[i] = 0
			}
		}
		return
	}
	if vec.IsConst() {
		fillStringGroupStrForConstVec(itr, vec, lenV, start)
		return
	}

	if !vec.GetNulls().Any() {
		if itr.mp.hasNull {
			gsp := vec.GetGrouping()
			va, area := vector.MustVarlenaRawData(vec)
			if area == nil {
				for i := 0; i < lenV; i++ {
					bytes := va[i+start].ByteSlice()
					hasGrouping := gsp.Contains(uint64(i + start))
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
						continue
					}
					// for "a"，"bc" and "ab","c", we need to distinct
					// this is not null value
					keys[i] = append(keys[i], 0)
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				for i := 0; i < lenV; i++ {
					bytes := va[i+start].GetByteSlice(area)
					hasGrouping := gsp.Contains(uint64(i + start))
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
						continue
					}
					// for "a"，"bc" and "ab","c", we need to distinct
					// this is not null value
					keys[i] = append(keys[i], 0)
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			}
		} else {
			va, area := vector.MustVarlenaRawData(vec)
			if area == nil {
				for i := 0; i < lenV; i++ {
					bytes := va[i+start].ByteSlice()
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				for i := 0; i < lenV; i++ {
					bytes := va[i+start].GetByteSlice(area)
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			}
		}
	} else {
		nsp := vec.GetNulls()
		rsp := vec.GetGrouping()
		va, area := vector.MustVarlenaRawData(vec)
		if area == nil {
			for i := 0; i < lenV; i++ {
				hasNull := nsp.Contains(uint64(i + start))
				hasGrouping := rsp.Contains(uint64(i + start))
				if itr.mp.hasNull {
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
					} else if hasNull {
						keys[i] = append(keys[i], byte(1))
					} else {
						bytes := va[i+start].ByteSlice()
						// for "a"，"bc" and "ab","c", we need to distinct
						// this is not null value
						keys[i] = append(keys[i], 0)
						// give the length
						length := uint16(len(bytes))
						keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
						// append the pure value bytes
						keys[i] = append(keys[i], bytes...)
					}
				} else {
					if hasNull {
						itr.zValues[i] = 0
						continue
					}
					bytes := va[i+start].ByteSlice()
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			}
		} else {
			for i := 0; i < lenV; i++ {
				hasNull := nsp.Contains(uint64(i + start))
				hasGrouping := rsp.Contains(uint64(i + start))
				if itr.mp.hasNull {
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
					} else if hasNull {
						keys[i] = append(keys[i], byte(1))
					} else {
						bytes := va[i+start].GetByteSlice(area)
						// for "a"，"bc" and "ab","c", we need to distinct
						// this is not null value
						keys[i] = append(keys[i], 0)
						// give the length
						length := uint16(len(bytes))
						keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
						// append the pure value bytes
						keys[i] = append(keys[i], bytes...)
					}
				} else {
					if hasNull {
						itr.zValues[i] = 0
						continue
					}
					bytes := va[i+start].GetByteSlice(area)
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint16(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			}
		}
	}
}

func fillGroupStr(itr *strHashmapIterator, vec *vector.Vector, n int, sz int, start int, scale int32, lenCols int) {
	keys := itr.keys
	if vec.IsGrouping() {
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
		data := vec.GetData()[:sz]
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
	data := vec.GetData()[:(n+start)*sz]
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
		gsp := vec.GetGrouping()
		for i := 0; i < n; i++ {
			isNull := nsp.Contains(uint64(i + start))
			isGrouping := gsp.Contains(uint64(i + start))
			if itr.mp.hasNull {
				if isGrouping {
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

func (m *StrHashMap) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	// Serialize hasNull (1 byte)
	if m.hasNull {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	// Serialize rows (8 bytes)
	buf.Write(types.EncodeUint64(&m.rows))

	// Serialize the underlying StringHashMap
	hashMapData, err := m.hashMap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// Write length of hash map data (4 bytes)
	size := uint32(len(hashMapData))
	buf.Write(types.EncodeUint32(&size))
	// Write hash map data
	buf.Write(hashMapData)

	return buf.Bytes(), nil
}

func (m *StrHashMap) UnmarshalBinary(data []byte, allocator malloc.Allocator) error {
	r := bytes.NewReader(data)

	// Deserialize hasNull
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	m.hasNull = b == 1

	// Deserialize rows
	rowsData := make([]byte, 8)
	if _, err := io.ReadFull(r, rowsData); err != nil {
		return err
	}
	m.rows = types.DecodeUint64(rowsData)

	// Deserialize the underlying StringHashMap
	sizeData := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeData); err != nil {
		return err
	}
	hashMapSize := types.DecodeUint32(sizeData)

	hashMapData := make([]byte, hashMapSize)
	if _, err := io.ReadFull(r, hashMapData); err != nil {
		return err
	}

	m.hashMap = &hashtable.StringHashMap{}
	if err := m.hashMap.UnmarshalBinary(hashMapData, allocator); err != nil {
		return err
	}

	return nil
}
