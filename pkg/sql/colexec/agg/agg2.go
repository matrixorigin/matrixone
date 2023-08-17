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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func (a *UnaryAgg[T1, T2]) Free(pool *mpool.MPool) {
	if a.otyp.IsVarlen() {
		return
	}
	if cap(a.da) > 0 {
		pool.Free(a.da)
	}
}

func (a *UnaryAgg[T1, T2]) Grows(count int, pool *mpool.MPool) error {
	a.grows(count)

	finalCount := len(a.es) + count
	// allocate memory from pool except for string type.
	if a.otyp.IsVarlen() {
		// first time.
		if len(a.es) == 0 {
			a.vs = make([]T2, 0, count)
			a.es = make([]bool, count)
			for i := range a.es {
				a.es[i] = true
			}
		} else {
			var emptyResult T2
			for i := len(a.es); i < finalCount; i++ {
				a.es = append(a.es, true)
				a.vs = append(a.vs, emptyResult)
			}
		}

	} else {
		itemSize := a.otyp.TypeSize()
		if len(a.es) == 0 {
			data, err := pool.Alloc(count * itemSize)
			if err != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)

		} else {
			data, err := pool.Grow(a.da, (count+len(a.es))*itemSize)
			if err != nil {
				return err
			}
			a.da = data
			a.vs = types.DecodeSlice[T2](a.da)
		}

		a.vs = a.vs[:finalCount]
		a.da = a.da[:finalCount*itemSize]
		for i := len(a.es); i < finalCount; i++ {
			a.es = append(a.es, true)
		}
		a.es = a.es[:finalCount]
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) Fill(groupIdx int64, rowIndex int64, vectors []*vector.Vector) (err error) {
	var value T1
	inputVector := vectors[0]

	if inputVector.IsConst() {
		rowIndex = 0
	}
	if inputVector.IsConstNull() || inputVector.GetNulls().Contains(uint64(rowIndex)) {
		a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], true)
	} else {
		if inputVector.GetType().IsVarlen() {
			a.vs[groupIdx], a.es[groupIdx], err = a.fill(
				groupIdx,
				any(inputVector.GetBytesAt(int(rowIndex))).(T1),
				a.vs[groupIdx],
				1,
				a.es[groupIdx],
				false)
		} else {
			a.vs[groupIdx], a.es[groupIdx], err = a.fill(
				groupIdx,
				vector.MustFixedCol[T1](inputVector)[rowIndex],
				a.vs[groupIdx],
				1,
				a.es[groupIdx],
				false)
		}
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) BatchFill(offset int64, groupStatus []uint8, groupOfRows []uint64, vectors []*vector.Vector) (err error) {
	// TODO: batch fill method of aggregation should be redesigned and optimized.
	// if a.batchFill != nil

	var value T1
	inputVector := vectors[0]
	rowOffset := uint64(offset)
	loopLength := uint64(len(groupStatus))

	if inputVector.GetType().IsVarlen() {
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				value = (any)(inputVector.GetBytesAt(0)).(T1)
			}

			for i := uint64(0); i < loopLength; i++ {
				if groupOfRows[i] == groupNotMatch {
					continue
				}
				groupNumber := int64(groupOfRows[i] - 1)

				a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
				if err != nil {
					return err
				}
			}

		} else {
			nulls := inputVector.GetNulls()

			for i := uint64(0); i < loopLength; i++ {
				if groupOfRows[i] == groupNotMatch {
					continue
				}
				groupNumber := int64(groupOfRows[i] - 1)
				rowIndex := rowOffset + i

				isNull := nulls.Contains(rowIndex)
				if !isNull {
					value = (any)(inputVector.GetBytesAt(int(rowIndex))).(T1)
				}
				a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
				if err != nil {
					return err
				}

			}
		}
		return nil
	}

	values := vector.MustFixedCol[T1](inputVector)
	if inputVector.IsConst() {
		isNull := inputVector.IsConstNull()
		if !isNull {
			value = values[0]
		}

		for i := uint64(0); i < loopLength; i++ {
			if groupOfRows[i] == groupNotMatch {
				continue
			}
			groupNumber := int64(groupOfRows[i] - 1)

			a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, value, a.vs[groupNumber], 1, a.es[groupNumber], isNull)
			if err != nil {
				return err
			}
		}
	} else {
		nulls := inputVector.GetNulls()

		for i := uint64(0); i < loopLength; i++ {
			if groupOfRows[i] == groupNotMatch {
				continue
			}
			groupNumber := int64(groupOfRows[i] - 1)
			rowIndex := rowOffset + i

			isNull := nulls.Contains(rowIndex)
			a.vs[groupNumber], a.es[groupNumber], err = a.fill(groupNumber, values[rowIndex], a.vs[groupNumber], 1, a.es[groupNumber], isNull)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *UnaryAgg[T1, T2]) BulkFill(groupIdx int64, vectors []*vector.Vector) (err error) {
	var value T1
	inputVector := vectors[0]

	loopLength := inputVector.Length()
	if inputVector.GetType().IsVarlen() {
		if inputVector.IsConst() {
			isNull := inputVector.IsConstNull()
			if !isNull {
				value = (any)(inputVector.GetBytesAt(0)).(T1)
			}
			a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], int64(loopLength), a.es[groupIdx], isNull)
			if err != nil {
				return err
			}

		} else {
			nulls := inputVector.GetNulls()
			for i := 0; i < loopLength; i++ {
				isNull := nulls.Contains(uint64(i))
				if !isNull {
					value = (any)(inputVector.GetBytesAt(i)).(T1)
				}
				a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], 1, a.es[groupIdx], isNull)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	values := vector.MustFixedCol[T1](inputVector)
	if inputVector.IsConst() {
		isNull := inputVector.IsConstNull()
		if !isNull {
			value = values[0]
		}
		a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, value, a.vs[groupIdx], int64(loopLength), a.es[groupIdx], isNull)
		if err != nil {
			return err
		}

	} else {
		nulls := inputVector.GetNulls()
		for i := 0; i < loopLength; i++ {
			isNull := nulls.Contains(uint64(i))
			a.vs[groupIdx], a.es[groupIdx], err = a.fill(groupIdx, values[i], a.vs[groupIdx], 1, a.es[groupIdx], isNull)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *UnaryAgg[T1, T2]) Merge(b Agg[any], groupIdx1, groupIdx2 int64) (err error) {
	a2 := b.(*UnaryAgg[T1, T2])
	a.vs[groupIdx1], a.es[groupIdx1], err = a.merge(
		groupIdx1, groupIdx2, a.vs[groupIdx1], a2.vs[groupIdx2], a.es[groupIdx1], a2.es[groupIdx2], a2.priv)
	return err
}

func (a *UnaryAgg[T1, T2]) BatchMerge(b Agg[any], offset int64, groupStatus []uint8, groupIdxes []uint64) (err error) {
	a2 := b.(*UnaryAgg[T1, T2])
	for i := range groupStatus {
		if groupIdxes[i] == groupNotMatch {
			continue
		}
		groupIdx1 := int64(groupIdxes[i] - 1)
		groupIdx2 := offset + int64(i)

		a.vs[groupIdx1], a.es[groupIdx1], err = a.merge(groupIdx1, groupIdx2, a.vs[groupIdx1], a2.vs[groupIdx2], a.es[groupIdx1], a2.es[groupIdx2], a2.priv)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *UnaryAgg[T1, T2]) Eval(pool *mpool.MPool) (vec *vector.Vector, err error) {
	a.vs, err = a.eval(a.vs, nil)
	if err != nil {
		return nil, err
	}

	vec = vector.NewVec(a.otyp)
	if a.otyp.IsVarlen() {
		vs := (any)(a.vs).([][]byte)
		if err = vector.AppendBytesList(vec, vs, a.es, pool); err != nil {
			vec.Free(pool)
			return nil, err
		}
	} else {
		if err = vector.AppendFixedList[T2](vec, a.vs, a.es, pool); err != nil {
			vec.Free(pool)
		}
	}

	if a.isCount {
		vec.GetNulls().Reset()
	}
	return vec, nil
}
