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

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func NewStrHashMap(hasNull bool, memPool *mpool.MPool) (*StrHashMap, error) {
	return NewStrHashMapWithAllocation(hasNull, memPool, nil)
}

func NewStrHashMapWithAllocation(
	hasNull bool,
	memPool *mpool.MPool,
	allocation *hashtable.AllocationAccountSelection,
) (*StrHashMap, error) {
	return NewStrHashMapWithAllocations(
		hasNull,
		memPool,
		allocation,
		nil,
	)
}

func NewStrHashMapWithAllocations(
	hasNull bool,
	memPool *mpool.MPool,
	allocation *hashtable.AllocationAccountSelection,
	iteratorAllocation *IteratorAllocation,
) (*StrHashMap, error) {
	mp := &hashtable.StringHashMap{}
	if err := mp.InitWithAllocation(memPool, allocation); err != nil {
		return nil, err
	}
	return &StrHashMap{
		hashMap:            mp,
		hasNull:            hasNull,
		mp:                 memPool,
		iteratorAllocation: iteratorAllocation,
	}, nil
}

func (m *StrHashMap) NewIterator() Iterator {
	return &strHashmapIterator{
		mp: m,
	}
}

func (m *StrHashMap) NewTransactionalIterator() TransactionalIterator {
	return &transactionalStrIterator{
		strHashmapIterator: m.NewIterator().(*strHashmapIterator),
	}
}

// SetRejectNaN makes FLOAT NaN keys non-matching, as required by SQL join
// equality. It must be selected before inserting the first row.
func (m *StrHashMap) SetRejectNaN() error {
	if m == nil || m.rows != 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	m.rejectNaN = true
	return nil
}

func (itr *strHashmapIterator) prepareHashKeys(
	vecs []*vector.Vector,
	start int,
	count int,
) error {
	if itr == nil || itr.mp == nil || start < 0 || count < 0 ||
		count > UnitLimit {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := validateIteratorVectors(vecs, start, count); err != nil {
		return err
	}
	itr.ensureCapacity(count)
	for i := 0; i < count; i++ {
		itr.zValues[i] = 0
	}
	const maxInt = int(^uint(0) >> 1)
	flatFixedSize := 0
	add := func(row int, size int) error {
		if size < 0 || itr.zValues[row] > int64(maxInt-size) {
			return mpool.ErrAllocationAccountInvalid
		}
		itr.zValues[row] += int64(size)
		return nil
	}
	for _, vec := range vecs {
		withDomain := itr.mp.hasNull || itr.mp.groupingAware
		prefix := 0
		if withDomain {
			prefix = 1
		}
		if vec.IsGrouping() {
			for i := 0; i < count; i++ {
				if err := add(i, 1); err != nil {
					return err
				}
			}
			continue
		}
		if vec.IsConstNull() {
			if itr.mp.hasNull {
				for i := 0; i < count; i++ {
					if err := add(i, 1); err != nil {
						return err
					}
				}
			}
			continue
		}

		// Most join keys are flat and non-null. Size them from the physical
		// representation directly, avoiding repeated type/null/const dispatch
		// before the encoder's required value pass.
		hasGrouping := withDomain && vec.HasGrouping()
		if !hasGrouping && !vec.GetNulls().Any() {
			if vec.GetType().IsFixedLen() {
				size := prefix + vec.GetType().TypeSize()
				if size < 0 || flatFixedSize > maxInt-size {
					return mpool.ErrAllocationAccountInvalid
				}
				flatFixedSize += size
				continue
			}
			if vec.IsConst() {
				value := canonicalVarlenaHashValue(vec.GetType().Oid, vec.GetBytesAt(0))
				valueSize := len(value)
				if vec.GetType().Oid == types.T_json {
					valueSize = keycodec.CanonicalJSONSize(value)
				}
				size := prefix + 4 + valueSize
				for i := 0; i < count; i++ {
					if err := add(i, size); err != nil {
						return err
					}
				}
				continue
			}
			values, area := vector.MustVarlenaRawData(vec)
			if vec.GetType().Oid == types.T_json {
				for i := 0; i < count; i++ {
					value := values[start+i].GetByteSlice(area)
					if err := add(i, prefix+4+keycodec.CanonicalJSONSize(value)); err != nil {
						return err
					}
				}
			} else {
				for i := 0; i < count; i++ {
					value := canonicalVarlenaHashValue(
						vec.GetType().Oid, values[start+i].GetByteSlice(area),
					)
					if err := add(i, prefix+4+len(value)); err != nil {
						return err
					}
				}
			}
			continue
		}

		fixed := vec.GetType().IsFixedLen()
		for i := 0; i < count; i++ {
			row := start + i
			if withDomain && vec.GetGrouping().Contains(uint64(row)) {
				if err := add(i, 1); err != nil {
					return err
				}
				continue
			}
			if vec.GetNulls().Contains(uint64(row)) {
				if itr.mp.hasNull {
					if err := add(i, 1); err != nil {
						return err
					}
				}
				continue
			}
			if fixed {
				if err := add(i, prefix+vec.GetType().TypeSize()); err != nil {
					return err
				}
				continue
			}
			valueRow := row
			if vec.IsConst() {
				valueRow = 0
			}
			value := canonicalVarlenaHashValue(vec.GetType().Oid, vec.GetBytesAt(valueRow))
			valueSize := len(value)
			if vec.GetType().Oid == types.T_json {
				valueSize = keycodec.CanonicalJSONSize(value)
			}
			if err := add(i, prefix+4+valueSize); err != nil {
				return err
			}
		}
	}
	// Fixed, flat columns contribute the same width to every row. Accumulate
	// them once above instead of walking the row set once per key column.
	for i := 0; i < count; i++ {
		if err := add(i, flatFixedSize); err != nil {
			return err
		}
	}

	total := 0
	for i := 0; i < count; i++ {
		if itr.zValues[i] < 16 {
			itr.zValues[i] = 16
		}
		keyLength := int(itr.zValues[i])
		if total > maxInt-keyLength {
			return mpool.ErrAllocationAccountInvalid
		}
		total += keyLength
	}
	if cap(itr.keyBuffer) < total {
		itr.clearKeys()
		if allocation := itr.mp.iteratorAllocation; allocation != nil {
			var next []byte
			var err error
			if cap(itr.keyBuffer) > 0 {
				// Grow owns the capacity policy. Passing a pre-grown capacity
				// would apply that policy twice and falsely inflate admission.
				next, err = itr.mp.mp.Grow(itr.keyBuffer, total, true)
			} else {
				capacity, ok := mpool.GrowCapacity(0, int64(total))
				if !ok || int64(int(capacity)) != capacity {
					return mpool.ErrAllocationAllocatorLimit
				}
				next, err = itr.mp.mp.AllocAccounted(
					int(capacity),
					allocation.account,
					allocation.owner,
					allocation.site,
				)
			}
			if err != nil {
				return err
			}
			itr.keyBuffer = next
			itr.keyBufferMP = itr.mp.mp
			itr.keyBufferAllocation = allocation
		} else {
			itr.keyBuffer = make([]byte, total)
			itr.keyBufferMP = nil
			itr.keyBufferAllocation = nil
		}
	}
	itr.keyBuffer = itr.keyBuffer[:total]
	storage := itr.keyBuffer
	offset := 0
	for i := 0; i < count; i++ {
		end := offset + int(itr.zValues[i])
		itr.keys[i] = storage[offset:offset:end]
		offset = end
	}
	return nil
}

func (m *StrHashMap) HasNull() bool {
	return m.hasNull
}

// SetGroupingAware selects a collision-free key domain for maps that may see
// GROUPING rows. It must be set before the
// first insert. Ordinary columns receive a 0 domain byte and GROUPING columns
// receive 2, so no raw fixed-width value can alias the sentinel.
func (m *StrHashMap) SetGroupingAware() error {
	if m == nil || m.rows != 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	m.groupingAware = true
	return nil
}

func (m *StrHashMap) Free() {
	if m == nil || m.hashMap == nil {
		return
	}
	m.hashMap.Free()
	m.hashMap = nil
}

func (m *StrHashMap) PreAlloc(n uint64) error {
	return m.hashMap.ResizeOnDemand(n)
}

func (m *StrHashMap) SetResizeAdmission(admit hashtable.ResizeAdmission) {
	m.hashMap.SetResizeAdmission(admit)
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
		if itr.mp.groupingAware || itr.mp.hasNull {
			switch vec.GetType().Oid {
			case types.T_json, types.T_array_float32, types.T_array_float64,
				types.T_array_bf16, types.T_array_float16:
				fillCanonicalGroupingAwareVarlena(itr, vec, count, start)
			default:
				fillGroupingAwareStr(itr, vec, count, start)
			}
			continue
		}
		if vec.GetType().IsFixedLen() {
			switch vec.GetType().Oid {
			case types.T_float32:
				fillFloat32GroupStr(itr, vec, count, start)
			case types.T_float64:
				fillFloat64GroupStr(itr, vec, count, start)
			default:
				fillGroupStr(itr, vec, count, vec.GetType().TypeSize(), start, 0, len(vecs))
			}
		} else {
			switch vec.GetType().Oid {
			case types.T_json, types.T_array_float32, types.T_array_float64,
				types.T_array_bf16, types.T_array_float16:
				fillCanonicalStringGroupStr(itr, vec, count, start)
			default:
				fillStringGroupStr(itr, vec, count, start, len(vecs))
			}
		}
	}
	keys := itr.keys
	for i := 0; i < count; i++ {
		if l := len(keys[i]); l < 16 {
			keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
		}
	}
}

func appendVarlenaHashKey(dst []byte, oid types.T, value []byte) []byte {
	value = canonicalVarlenaHashValue(oid, value)
	switch oid {
	case types.T_json:
		return keycodec.AppendCanonicalJSON(dst, value)
	case types.T_array_float32:
		return keycodec.AppendCanonicalVecF32(dst, value)
	case types.T_array_float64:
		return keycodec.AppendCanonicalVecF64(dst, value)
	case types.T_array_bf16, types.T_array_float16:
		return keycodec.AppendCanonicalVecF16(dst, value)
	default:
		return append(dst, value...)
	}
}

func canonicalVarlenaHashValue(oid types.T, value []byte) []byte {
	if oid != types.T_char {
		return value
	}
	for len(value) > 0 && value[len(value)-1] == ' ' {
		value = value[:len(value)-1]
	}
	return value
}

func appendFramedVarlenaHashKey(dst []byte, oid types.T, value []byte) []byte {
	lengthOffset := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	valueOffset := len(dst)
	dst = appendVarlenaHashKey(dst, oid, value)
	length := uint32(len(dst) - valueOffset)
	copy(dst[lengthOffset:valueOffset], util.UnsafeToBytes(&length))
	return dst
}

func fillCanonicalGroupingAwareVarlena(
	itr *strHashmapIterator,
	vec *vector.Vector,
	n int,
	start int,
) {
	keys := itr.keys
	if vec.IsGrouping() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		for i := 0; i < n; i++ {
			row := start + i
			if vec.GetGrouping().Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(2))
			} else if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
		}
		return
	}

	for i := 0; i < n; i++ {
		row := start + i
		if vec.GetGrouping().Contains(uint64(row)) {
			keys[i] = append(keys[i], byte(2))
			continue
		}
		if vec.GetNulls().Contains(uint64(row)) {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
			continue
		}
		keys[i] = append(keys[i], byte(0))
		valueRow := row
		if vec.IsConst() {
			valueRow = 0
		}
		value := vec.GetBytesAt(valueRow)
		keys[i] = appendFramedVarlenaHashKey(keys[i], vec.GetType().Oid, value)
	}
}

func fillCanonicalStringGroupStr(
	itr *strHashmapIterator,
	vec *vector.Vector,
	n int,
	start int,
) {
	keys := itr.keys
	if vec.IsGrouping() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		for i := 0; i < n; i++ {
			itr.zValues[i] = 0
		}
		return
	}
	if vec.IsConst() {
		value := vec.GetBytesAt(0)
		for i := 0; i < n; i++ {
			keys[i] = appendFramedVarlenaHashKey(keys[i], vec.GetType().Oid, value)
		}
		return
	}

	values, area := vector.MustVarlenaRawData(vec)
	nulls := vec.GetNulls()
	if !nulls.Any() {
		if area == nil {
			for i := 0; i < n; i++ {
				value := values[start+i].ByteSlice()
				keys[i] = appendFramedVarlenaHashKey(keys[i], vec.GetType().Oid, value)
			}
		} else {
			for i := 0; i < n; i++ {
				value := values[start+i].GetByteSlice(area)
				keys[i] = appendFramedVarlenaHashKey(keys[i], vec.GetType().Oid, value)
			}
		}
		return
	}
	for i := 0; i < n; i++ {
		row := start + i
		if nulls.Contains(uint64(row)) {
			itr.zValues[i] = 0
			continue
		}
		value := values[row].GetByteSlice(area)
		keys[i] = appendFramedVarlenaHashKey(keys[i], vec.GetType().Oid, value)
	}
}

func fillFloat32GroupStr(itr *strHashmapIterator, vec *vector.Vector, n, start int) {
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
	values := vector.MustFixedColNoTypeCheck[float32](vec)
	codec := keycodec.NewFloat32Codec(vec.GetType().Scale)
	if vec.IsConst() {
		value := codec.CanonicalBytes(values[0])
		for i := 0; i < n; i++ {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(0))
			}
			keys[i] = append(keys[i], value[:]...)
		}
		return
	}

	if !vec.GetNulls().Any() {
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				value := codec.CanonicalBytes(values[i+start])
				keys[i] = append(keys[i], value[:]...)
			}
		} else {
			for i := 0; i < n; i++ {
				value := codec.CanonicalBytes(values[i+start])
				keys[i] = append(keys[i], value[:]...)
			}
		}
		return
	}

	nsp := vec.GetNulls()
	gsp := vec.GetGrouping()
	for i := 0; i < n; i++ {
		row := i + start
		if itr.mp.hasNull {
			if gsp.Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(2))
			} else if nsp.Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(1))
			} else {
				keys[i] = append(keys[i], byte(0))
				value := codec.CanonicalBytes(values[row])
				keys[i] = append(keys[i], value[:]...)
			}
		} else if nsp.Contains(uint64(row)) {
			itr.zValues[i] = 0
		} else {
			value := codec.CanonicalBytes(values[row])
			keys[i] = append(keys[i], value[:]...)
		}
	}
}

func fillGroupingAwareStr(
	itr *strHashmapIterator,
	vec *vector.Vector,
	n int,
	start int,
) {
	keys := itr.keys
	// Nullable and grouping-aware maps use a domain byte before every ordinary
	// value. Most batches contain neither NULL nor GROUPING rows, though. Keep
	// that common case on flat vector data so the hot path does not probe two
	// bitmaps and redispatch the vector representation for every row.
	if vec.GetType().IsFixedLen() && !vec.IsConstNull() &&
		!vec.HasGrouping() && !vec.GetNulls().Any() {
		fillFlatFixedGroupingAwareStr(keys, vec, n, start)
		return
	}
	if vec.IsGrouping() {
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(2))
		}
		return
	}
	if vec.IsConstNull() {
		for i := 0; i < n; i++ {
			row := start + i
			if vec.GetGrouping().Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(2))
			} else if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
		}
		return
	}
	float32Codec := keycodec.NewFloat32Codec(vec.GetType().Scale)
	for i := 0; i < n; i++ {
		row := start + i
		if vec.GetGrouping().Contains(uint64(row)) {
			keys[i] = append(keys[i], byte(2))
			continue
		}
		if vec.GetNulls().Contains(uint64(row)) {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(1))
			} else {
				itr.zValues[i] = 0
			}
			continue
		}
		keys[i] = append(keys[i], byte(0))
		valueRow := row
		if vec.IsConst() {
			valueRow = 0
		}
		switch vec.GetType().Oid {
		case types.T_float32:
			values := vector.MustFixedColNoTypeCheck[float32](vec)
			value := float32Codec.CanonicalBytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			continue
		case types.T_float64:
			values := vector.MustFixedColNoTypeCheck[float64](vec)
			value := keycodec.CanonicalFloat64Bytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			continue
		}
		if vec.GetType().IsFixedLen() {
			size := vec.GetType().TypeSize()
			data := vec.GetData()
			value := data[valueRow*size : (valueRow+1)*size]
			keys[i] = append(keys[i], value...)
			continue
		}
		value := canonicalVarlenaHashValue(vec.GetType().Oid, vec.GetBytesAt(valueRow))
		length := uint32(len(value))
		keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
		keys[i] = append(keys[i], value...)
	}
}

func fillFlatFixedGroupingAwareStr(
	keys [][]byte,
	vec *vector.Vector,
	n int,
	start int,
) {
	valueRow := start
	if vec.IsConst() {
		valueRow = 0
	}
	switch vec.GetType().Oid {
	case types.T_float32:
		values := vector.MustFixedColNoTypeCheck[float32](vec)
		codec := keycodec.NewFloat32Codec(vec.GetType().Scale)
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(0))
			value := codec.CanonicalBytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			if !vec.IsConst() {
				valueRow++
			}
		}
	case types.T_float64:
		values := vector.MustFixedColNoTypeCheck[float64](vec)
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(0))
			value := keycodec.CanonicalFloat64Bytes(values[valueRow])
			keys[i] = append(keys[i], value[:]...)
			if !vec.IsConst() {
				valueRow++
			}
		}
	default:
		width := vec.GetType().TypeSize()
		data := vec.GetData()
		for i := 0; i < n; i++ {
			keys[i] = append(keys[i], byte(0))
			offset := valueRow * width
			keys[i] = append(keys[i], data[offset:offset+width]...)
			if !vec.IsConst() {
				valueRow++
			}
		}
	}
}

func fillFloat64GroupStr(itr *strHashmapIterator, vec *vector.Vector, n, start int) {
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
		values := vector.MustFixedColNoTypeCheck[float64](vec)
		value := keycodec.CanonicalFloat64Bytes(values[0])
		for i := 0; i < n; i++ {
			if itr.mp.hasNull {
				keys[i] = append(keys[i], byte(0))
			}
			keys[i] = append(keys[i], value[:]...)
		}
		return
	}

	values := vector.MustFixedColNoTypeCheck[float64](vec)
	if !vec.GetNulls().Any() {
		if itr.mp.hasNull {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				value := keycodec.CanonicalFloat64Bytes(values[i+start])
				keys[i] = append(keys[i], value[:]...)
			}
		} else {
			for i := 0; i < n; i++ {
				value := keycodec.CanonicalFloat64Bytes(values[i+start])
				keys[i] = append(keys[i], value[:]...)
			}
		}
		return
	}

	nsp := vec.GetNulls()
	gsp := vec.GetGrouping()
	for i := 0; i < n; i++ {
		row := i + start
		if itr.mp.hasNull {
			if gsp.Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(2))
			} else if nsp.Contains(uint64(row)) {
				keys[i] = append(keys[i], byte(1))
			} else {
				keys[i] = append(keys[i], byte(0))
				value := keycodec.CanonicalFloat64Bytes(values[row])
				keys[i] = append(keys[i], value[:]...)
			}
		} else if nsp.Contains(uint64(row)) {
			itr.zValues[i] = 0
		} else {
			value := keycodec.CanonicalFloat64Bytes(values[row])
			keys[i] = append(keys[i], value[:]...)
		}
	}
}

func fillStringGroupStrForConstVec(itr *strHashmapIterator, vec *vector.Vector, n int, start int) {
	keys := itr.keys
	bytes := canonicalVarlenaHashValue(vec.GetType().Oid, vec.GetBytesAt(start))
	length := uint32(len(bytes))
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
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].ByteSlice())
					hasGrouping := gsp.Contains(uint64(i + start))
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
						continue
					}
					// for "a"，"bc" and "ab","c", we need to distinct
					// this is not null value
					keys[i] = append(keys[i], 0)
					// give the length
					length := uint32(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				for i := 0; i < lenV; i++ {
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].GetByteSlice(area))
					hasGrouping := gsp.Contains(uint64(i + start))
					if hasGrouping {
						keys[i] = append(keys[i], byte(2))
						continue
					}
					// for "a"，"bc" and "ab","c", we need to distinct
					// this is not null value
					keys[i] = append(keys[i], 0)
					// give the length
					length := uint32(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			}
		} else {
			va, area := vector.MustVarlenaRawData(vec)
			if area == nil {
				for i := 0; i < lenV; i++ {
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].ByteSlice())
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint32(len(bytes))
					keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
					// append the pure value bytes
					keys[i] = append(keys[i], bytes...)
				}
			} else {
				for i := 0; i < lenV; i++ {
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].GetByteSlice(area))
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint32(len(bytes))
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
						bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].ByteSlice())
						// for "a"，"bc" and "ab","c", we need to distinct
						// this is not null value
						keys[i] = append(keys[i], 0)
						// give the length
						length := uint32(len(bytes))
						keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
						// append the pure value bytes
						keys[i] = append(keys[i], bytes...)
					}
				} else {
					if hasNull {
						itr.zValues[i] = 0
						continue
					}
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].ByteSlice())
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint32(len(bytes))
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
						bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].GetByteSlice(area))
						// for "a"，"bc" and "ab","c", we need to distinct
						// this is not null value
						keys[i] = append(keys[i], 0)
						// give the length
						length := uint32(len(bytes))
						keys[i] = append(keys[i], util.UnsafeToBytes(&length)...)
						// append the pure value bytes
						keys[i] = append(keys[i], bytes...)
					}
				} else {
					if hasNull {
						itr.zValues[i] = 0
						continue
					}
					bytes := canonicalVarlenaHashValue(vec.GetType().Oid, va[i+start].GetByteSlice(area))
					// for "a"，"bc" and "ab","c", we need to distinct
					// give the length
					length := uint32(len(bytes))
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
	if _, err := m.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// MarshalBinarySize returns the exact number of bytes written by WriteTo.
// The wire format contains a one-byte option field, the eight-byte logical
// row count, the eight-byte hash-table element count, and one 32-byte cell per
// group.
func (m *StrHashMap) MarshalBinarySize() (int64, error) {
	const (
		fixedSize = uint64(1 + 8 + 8)
		cellSize  = uint64(32)
		maxInt64  = uint64(^uint64(0) >> 1)
	)
	if m == nil || m.hashMap == nil || m.rows != m.hashMap.Cardinality() ||
		m.rows > (maxInt64-fixedSize)/cellSize {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	return int64(fixedSize + m.rows*cellSize), nil
}

func (m *StrHashMap) UnmarshalBinary(data []byte, mp *mpool.MPool) error {
	r := bytes.NewReader(data)
	_, err := m.UnmarshalFrom(r, mp)
	return err
}

func (m *StrHashMap) WriteTo(w io.Writer) (int64, error) {
	var n int64

	// The low three bits retain the key grammar. Historical payloads used only
	// bit zero, so 0/1 remain backward-compatible.
	flags := byte(0)
	if m.hasNull {
		flags |= 1
	}
	if m.groupingAware {
		flags |= 2
	}
	if m.rejectNaN {
		flags |= 4
	}
	if _, err := w.Write([]byte{flags}); err != nil {
		return 0, err
	}
	n++

	// Serialize rows (8 bytes)
	rowsBytes := types.EncodeUint64(&m.rows)
	wn, err := w.Write(rowsBytes)
	if err != nil {
		return 0, err
	}
	n += int64(wn)

	// Serialize the underlying StringHashMap
	subn, err := m.hashMap.WriteTo(w)
	if err != nil {
		return 0, err
	}
	n += subn

	return n, nil
}

func (m *StrHashMap) UnmarshalFrom(r io.Reader, mp *mpool.MPool) (int64, error) {
	var n int64

	// Deserialize hasNull
	b := make([]byte, 1)
	rn, err := io.ReadFull(r, b)
	if err != nil {
		return 0, err
	}
	n += int64(rn)
	if b[0]&^byte(7) != 0 {
		return 0, mpool.ErrAllocationAccountInvalid
	}
	m.hasNull = b[0]&1 != 0
	m.groupingAware = b[0]&2 != 0
	m.rejectNaN = b[0]&4 != 0

	// Deserialize rows
	rowsData := make([]byte, 8)
	if rn, err = io.ReadFull(r, rowsData); err != nil {
		return 0, err
	}
	n += int64(rn)
	m.rows = types.DecodeUint64(rowsData)
	m.mp = mp

	// Deserialize the underlying StringHashMap
	m.hashMap = &hashtable.StringHashMap{}
	subn, err := m.hashMap.UnmarshalFrom(r, mp)
	if err != nil {
		return 0, err
	}
	n += subn
	if m.rows != m.hashMap.Cardinality() {
		declaredRows := m.rows
		cardinality := m.hashMap.Cardinality()
		m.hashMap.Free()
		m.hashMap = nil
		m.rows = 0
		return 0, moerr.NewInvalidInputNoCtxf(
			"string hash map row count %d does not match cardinality %d",
			declaredRows, cardinality)
	}

	return n, nil
}

func (m *StrHashMap) FillGroupHashes(dst []uint64) []uint64 {
	return m.hashMap.FillGroupHashes(dst)
}
