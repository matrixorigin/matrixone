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

func (a *UnaryDistAgg[T1, T2]) Free(pool *mpool.MPool) {
	for _, mp := range a.maps {
		if mp != nil {
			mp.Free()
		}
	}
	if a.otyp.IsVarlen() {
		return
	}
	if a.da != nil {
		pool.Free(a.da)
	}
}

func (a *UnaryDistAgg[T1, T2]) Grows(count int, pool *mpool.MPool) (err error) {
	a.grows(count)

	finalCount := len(a.es) + count
	if a.otyp.IsVarlen() {
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
		itemSize := a.otyp.TypeSize()
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

	a.srcs[rowIndex] = append(a.srcs[rowIndex], value)
	a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], isNull)
	return err
}

func (a *UnaryDistAgg[T1, T2]) BatchFill(offset int64, groupStatus []uint8, groupOfRows []uint64, vectors []*vector.Vector) (err error) {
	// TODO: refer to the comments of UnaryAgg.BatchFill
	var value T1
	var str []byte
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
					if groupOfRows[i] == groupNotMatch {
						continue
					}
					groupNumber := int64(groupOfRows[i] - 1)

					ok, err = a.maps[groupNumber].InsertValue(str)
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
				if groupOfRows[i] == groupNotMatch {
					continue
				}
				rowIndex := rowOffset + i
				isNull := nulls.Contains(rowIndex)
				if isNull {
					continue
				}
				getValue := inputVector.GetBytesAt(int(rowIndex))
				storeValue := make([]byte, len(getValue))
				copy(storeValue, getValue)

				value = (any)(storeValue).(T1)
				groupNumber := int64(groupOfRows[i] - 1)
				ok, err = a.maps[groupNumber].InsertValue(str)
				if err != nil {
					return err
				}
				if ok {
					value = (any)(str).(T1)

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
					if groupOfRows[i] == groupNotMatch {
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
				if groupOfRows[i] == groupNotMatch {
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
		if groupIdxes[i] == groupNotMatch {
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
	a.vs, err = a.eval(a.vs, nil)
	if err != nil {
		return nil, err
	}

	nullList := a.es
	if a.op == WinDenseRank || a.op == WinRank || a.op == WinRowNumber {
		nullList = nil
	}

	vec = vector.NewVec(a.otyp)
	if a.otyp.IsVarlen() {
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
