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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func NewUnaryDistAgg[T1, T2 any](
	op int64,
	priv AggStruct,
	isCount bool,
	ityp, otyp types.Type,
	grows func(int),
	eval func([]T2) ([]T2, error),
	merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool, error),
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool, error)) Agg[*UnaryDistAgg[T1, T2]] {
	return &UnaryDistAgg[T1, T2]{
		op:         op,
		priv:       priv,
		outputType: otyp,
		eval:       eval,
		fill:       fill,
		grows:      grows,
		merge:      merge,
		isCount:    isCount,
		inputTypes: []types.Type{ityp},
	}
}

func (a *UnaryDistAgg[T1, T2]) Free(pool *mpool.MPool) {
	for _, mp := range a.maps {
		if mp != nil {
			mp.Free()
			mp = nil
		}
	}
	if a.outputType.IsVarlen() {
		return
	}
	if a.da != nil {
		pool.Free(a.da)
		a.da = nil
	}
}

func (a *UnaryDistAgg[T1, T2]) OutputType() types.Type {
	return a.outputType
}

func (a *UnaryDistAgg[T1, T2]) InputTypes() []types.Type {
	return a.inputTypes
}

func (a *UnaryDistAgg[T1, T2]) Grows(count int, pool *mpool.MPool) (err error) {
	a.grows(count)

	finalCount := len(a.es) + count
	if a.outputType.IsVarlen() {
		if len(a.es) == 0 {
			a.es = make([]bool, count)
			a.vs = make([]T2, count)
			a.srcs = make([][]T1, count)
			a.maps = make([]*hashmap.StrHashMap, count)
			for i := 0; i < count; i++ {
				a.es[i] = true
				a.srcs[i] = make([]T1, 0, 1)
				a.maps[i], err = hashmap.NewStrMap(false, 0, 0, pool)
				if err != nil {
					return err
				}
			}

		} else {
			var emptyResult T2
			for i := 0; i < count; i++ {
				a.es = append(a.es, true)
				a.vs = append(a.vs, emptyResult)
				a.srcs = append(a.srcs, make([]T1, 0, 1))

				m, err1 := hashmap.NewStrMap(false, 0, 0, pool)
				if err1 != nil {
					return err1
				}
				a.maps = append(a.maps, m)
			}
		}

	} else {
		itemSize := a.outputType.TypeSize()
		if len(a.es) == 0 {
			data, err1 := pool.Alloc(count * itemSize)
			if err1 != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)
			a.es = make([]bool, 0, count)
			a.srcs = make([][]T1, 0, count)
			a.maps = make([]*hashmap.StrHashMap, 0, count)
		} else {
			data, err1 := pool.Grow(a.da, finalCount*itemSize)
			if err1 != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)
		}

		a.vs = a.vs[:finalCount]
		a.da = a.da[:finalCount*itemSize]
		for i := len(a.es); i < finalCount; i++ {
			a.es = append(a.es, true)
			a.srcs = append(a.srcs, make([]T1, 0, 1))

			m, err1 := hashmap.NewStrMap(false, 0, 0, pool)
			if err1 != nil {
				return err1
			}
			a.maps = append(a.maps, m)
		}
	}

	return nil
}

func (a *UnaryDistAgg[T1, T2]) Fill(groupIdx int64, rowIndex int64, vectors []*vector.Vector) (err error) {
	ok, err := a.maps[groupIdx].Insert(vectors, int(rowIndex))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	var value T1
	inputVector := vectors[0]

	if inputVector.IsConst() {
		rowIndex = 0
	}
	isNull := inputVector.IsConstNull() || inputVector.GetNulls().Contains(uint64(rowIndex))
	if !isNull {
		if inputVector.GetType().IsVarlen() {
			getValue := inputVector.GetBytesAt(int(rowIndex))
			storeValue := make([]byte, len(getValue))
			// bad copy.
			copy(storeValue, getValue)

			value = (any)(storeValue).(T1)
		} else {
			value = vector.MustFixedCol[T1](inputVector)[rowIndex]
		}
	}

	a.srcs[groupIdx] = append(a.srcs[groupIdx], value)
	a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], isNull)
	return err
}

func (a *UnaryDistAgg[T1, T2]) BatchFill(offset int64, groupStatus []uint8, groupOfRows []uint64, vectors []*vector.Vector) (err error) {
	// TODO: refer to the comments of UnaryAgg.BatchFill
	var value T1
	var ok bool
	inputVector := vectors[0]
	rowOffset := uint64(offset)
	loopLength := uint64(len(groupStatus))

	if inputVector.GetType().IsVarlen() {
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				getValue := inputVector.GetBytesAt(0)
				storeValue := make([]byte, len(getValue))
				copy(storeValue, getValue)

				value = (any)(storeValue).(T1)
				for i := uint64(0); i < loopLength; i++ {
					if groupOfRows[i] == GroupNotMatch {
						continue
					}
					groupNumber := int64(groupOfRows[i] - 1)

					ok, err = a.maps[groupNumber].InsertValue(value)
					if err != nil {
						return err
					}
					if ok {
						a.srcs[groupNumber] = append(a.srcs[groupNumber], value)
						a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
						if err != nil {
							return err
						}
					}
				}
			}

		} else {
			nulls := inputVector.GetNulls()
			for i := uint64(0); i < loopLength; i++ {
				if groupOfRows[i] == GroupNotMatch {
					continue
				}
				rowIndex := rowOffset + i
				isNull := nulls.Contains(rowIndex)
				if isNull {
					continue
				}
				getValue := inputVector.GetBytesAt(int(rowIndex))

				groupNumber := int64(groupOfRows[i] - 1)
				ok, err = a.maps[groupNumber].InsertValue(getValue)
				if err != nil {
					return err
				}
				if ok {
					storeValue := make([]byte, len(getValue))
					copy(storeValue, getValue)
					value = (any)(storeValue).(T1)

					a.srcs[groupNumber] = append(a.srcs[groupNumber], value)
					a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
					if err != nil {
						return err
					}
				}
			}

		}

	} else {
		values := vector.MustFixedCol[T1](inputVector)
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				value = values[0]
				for i := uint64(0); i < loopLength; i++ {
					if groupOfRows[i] == GroupNotMatch {
						continue
					}
					groupNumber := int64(groupOfRows[i] - 1)

					ok, err = a.maps[groupNumber].InsertValue(value)
					if err != nil {
						return err
					}
					if ok {
						a.srcs[groupNumber] = append(a.srcs[groupNumber], value)
						a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
						if err != nil {
							return err
						}
					}
				}
			}

		} else {
			nulls := inputVector.GetNulls()
			for i := uint64(0); i < loopLength; i++ {
				if groupOfRows[i] == GroupNotMatch {
					continue
				}
				rowIndex := rowOffset + i
				isNull := nulls.Contains(rowIndex)
				if isNull {
					continue
				}
				value = values[rowIndex]
				groupNumber := int64(groupOfRows[i] - 1)
				ok, err = a.maps[groupNumber].InsertValue(value)
				if err != nil {
					return err
				}
				if ok {
					a.srcs[groupNumber] = append(a.srcs[groupNumber], value)
					a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (a *UnaryDistAgg[T1, T2]) BulkFill(groupIdx int64, vectors []*vector.Vector) (err error) {
	var value T1
	var ok bool

	inputVector := vectors[0]

	loopLength := inputVector.Length()
	if inputVector.GetType().IsVarlen() {
		var getValue []byte
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				getValue = inputVector.GetBytesAt(0)
				ok, err = a.maps[groupIdx].InsertValue(getValue)
				if err != nil {
					return err
				}
				if ok {
					storeValue := make([]byte, len(getValue))
					copy(storeValue, getValue)

					value = (any)(storeValue).(T1)
					a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], int64(loopLength), a.es[groupIdx], isNull)
					if err != nil {
						return err
					}

					for i := 0; i < loopLength; i++ {
						a.srcs[groupIdx] = append(a.srcs[groupIdx], value)
					}
				}
			}

		} else {
			nulls := inputVector.GetNulls()
			for i := 0; i < loopLength; i++ {
				isNull := nulls.Contains(uint64(i))
				if isNull {
					continue
				}
				getValue = inputVector.GetBytesAt(i)
				ok, err = a.maps[groupIdx].InsertValue(getValue)
				if err != nil {
					return err
				}
				if ok {
					value = (any)(getValue).(T1)
					a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], false)
					if err != nil {
						return err
					}
					a.srcs[groupIdx] = append(a.srcs[groupIdx], value)
				}
			}
		}

	} else {
		values := vector.MustFixedCol[T1](inputVector)
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				value = values[0]
			}
			ok, err = a.maps[groupIdx].InsertValue(value)
			if err != nil {
				return err
			}
			if ok {
				a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], int64(loopLength), a.es[groupIdx], isNull)
				if err != nil {
					return err
				}

				for i := 0; i < loopLength; i++ {
					a.srcs[groupIdx] = append(a.srcs[groupIdx], value)
				}
			}

		} else {
			nulls := inputVector.GetNulls()
			for i := 0; i < loopLength; i++ {
				isNull := nulls.Contains(uint64(i))
				if isNull {
					continue
				}
				value = values[i]
				ok, err = a.maps[groupIdx].InsertValue(value)
				if err != nil {
					return err
				}
				if ok {
					a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], false)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// Merge a[x] += b[y]
func (a *UnaryDistAgg[T1, T2]) Merge(b Agg[any], groupIdx1, groupIdx2 int64) (err error) {
	a2 := b.(*UnaryDistAgg[T1, T2])
	if a2.es[groupIdx2] {
		return nil
	}

	var ok bool
	for _, v := range a2.srcs[groupIdx2] {
		if ok, err = a.maps[groupIdx1].InsertValue(v); err != nil {
			return err
		}
		if ok {
			a.vs[groupIdx1], a.es[groupIdx1], err = a.fill(groupIdx1, v, a.vs[groupIdx1], 1, a.es[groupIdx1], false)
			if err != nil {
				return err
			}
			a.srcs[groupIdx1] = append(a.srcs[groupIdx1], v)
		}
	}
	return nil
}

func (a *UnaryDistAgg[T1, T2]) BatchMerge(b Agg[any], offset int64, groupStatus []uint8, groupIdxes []uint64) (err error) {
	a2 := b.(*UnaryDistAgg[T1, T2])

	var ok bool
	for i := range groupStatus {
		if groupIdxes[i] == GroupNotMatch {
			continue
		}
		groupIdx1 := int64(groupIdxes[i] - 1)
		groupIdx2 := offset + int64(i)
		if a2.es[groupIdx2] {
			continue
		}

		for _, v := range a2.srcs[groupIdx2] {
			if ok, err = a.maps[groupIdx1].InsertValue(v); err != nil {
				return err
			}
			if ok {
				a.vs[groupIdx1], a.es[groupIdx1], err = a.fill(groupIdx1, v, a.vs[groupIdx1], 1, a.es[groupIdx1], false)
				if err != nil {
					return err
				}
				a.srcs[groupIdx1] = append(a.srcs[groupIdx1], v)
			}
		}
	}

	return nil
}

func (a *UnaryDistAgg[T1, T2]) Eval(pool *mpool.MPool) (vec *vector.Vector, err error) {
	if a.PartialResult != nil {
		if a.isCount {
			var x T1
			a.vs[0], a.es[0], err = a.fill(0, x, a.vs[0], a.PartialResult.(int64), false, false)
			if err != nil {
				return nil, err
			}
		} else {
			a.vs[0], a.es[0], err = a.fill(0, a.PartialResult.(T1), a.vs[0], 1, a.es[0], false)
			if err != nil {
				return nil, err
			}
		}
	}
	a.vs, err = a.eval(a.vs)
	if err != nil {
		return nil, err
	}

	nullList := a.es
	if IsWinOrderFun(a.op) {
		nullList = nil
	}

	vec = vector.NewVec(a.outputType)
	if a.outputType.IsVarlen() {
		vs := (any)(a.vs).([][]byte)
		if err = vector.AppendBytesList(vec, vs, nullList, pool); err != nil {
			vec.Free(pool)
			return nil, err
		}
	} else {
		if err = vector.AppendFixedList[T2](vec, a.vs, nullList, pool); err != nil {
			vec.Free(pool)
			return nil, err
		}
	}

	if a.isCount {
		vec.GetNulls().Reset()
	}
	return vec, nil
}

func (a *UnaryDistAgg[T1, T2]) WildAggReAlloc(m *mpool.MPool) error {
	d, err := m.Alloc(len(a.da))
	if err != nil {
		return err
	}
	copy(d, a.da)
	a.da = d
	setDistAggValues[T1, T2](a, a.outputType)
	return nil
}

func (a *UnaryDistAgg[T1, T2]) IsDistinct() bool {
	return true
}

func (a *UnaryDistAgg[T1, T2]) GetOperatorId() int64 {
	return a.op
}

// todo need improve performance
func (a *UnaryDistAgg[T1, T2]) Dup(m *mpool.MPool) Agg[any] {
	val := &UnaryDistAgg[T1, T2]{
		op:            a.op,
		priv:          a.priv.Dup(),
		vs:            make([]T2, len(a.vs)),
		es:            make([]bool, len(a.es)),
		da:            make([]byte, len(a.da)),
		srcs:          make([][]T1, len(a.srcs)),
		isCount:       a.isCount,
		outputType:    a.outputType,
		inputTypes:    make([]types.Type, len(a.inputTypes)),
		grows:         a.grows,
		eval:          a.eval,
		merge:         a.merge,
		fill:          a.fill,
		PartialResult: a.PartialResult,
		maps:          make([]*hashmap.StrHashMap, len(a.maps)),
	}
	copy(val.vs, a.vs)
	copy(val.es, a.es)
	copy(val.da, a.da)
	copy(val.inputTypes, a.inputTypes)
	for i, src := range a.srcs {
		val.srcs[i] = make([]T1, len(src))
		copy(val.srcs[i], src)
	}
	for i, smap := range a.maps {
		val.maps[i] = smap.Dup(m)
	}

	return val
}

func (a *UnaryDistAgg[T1, T2]) MarshalBinary() ([]byte, error) {
	pData, err := a.priv.MarshalBinary()
	if err != nil {
		return nil, err
	}
	source := &EncodeAggDistinct[T1]{
		Op:         a.op,
		Private:    pData,
		Es:         a.es,
		IsCount:    a.isCount,
		InputTypes: a.inputTypes,
		OutputType: a.outputType,
		Srcs:       a.srcs,
	}
	switch {
	case a.outputType.Oid.IsMySQLString():
		source.Da = types.EncodeStringSlice(getDistAggStrVs(a))
	default:
		source.Da = a.da
	}

	return types.Encode(source)
}

func getDistAggStrVs(strUnaryDistAgg any) []string {
	agg := strUnaryDistAgg.(*UnaryDistAgg[[]byte, []byte])
	result := make([]string, len(agg.vs))
	for i := range result {
		result[i] = string(agg.vs[i])
	}
	return result
}

func (a *UnaryDistAgg[T1, T2]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	decode := new(EncodeAggDistinct[T1])
	if err := types.Decode(copyData, decode); err != nil {
		return err
	}

	// Recover data
	a.op = decode.Op
	a.inputTypes = decode.InputTypes
	a.outputType = decode.OutputType
	a.es = decode.Es
	data = make([]byte, len(decode.Da))
	copy(data, decode.Da)
	a.da = data
	setDistAggValues[T1, T2](a, a.outputType)
	a.srcs = decode.Srcs
	a.maps = make([]*hashmap.StrHashMap, len(a.srcs))
	m := mpool.MustNewZeroNoFixed()
	for i, src := range a.srcs {
		mp, err := hashmap.NewStrMap(true, 0, 0, m)
		if err != nil {
			m.Free(data)
			for j := 0; j < i; j++ {
				a.maps[j].Free()
			}
			return err
		}
		a.maps[i] = mp
		for _, v := range src {
			mp.InsertValue(v)
		}
	}
	return a.priv.UnmarshalBinary(decode.Private)
}

func setDistAggValues[T1, T2 any](agg any, typ types.Type) {
	switch {
	case typ.Oid.IsMySQLString():
		a := agg.(*UnaryDistAgg[[]byte, []byte])
		values := types.DecodeStringSlice(a.da)
		a.vs = make([][]byte, len(values))
		for i := range a.vs {
			a.vs[i] = []byte(values[i])
		}
	default:
		a := agg.(*UnaryDistAgg[T1, T2])
		a.vs = types.DecodeSlice[T2](a.da)
	}
}
