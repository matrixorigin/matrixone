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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func NewUnaryAgg[T1, T2 any](op int, priv AggStruct, isCount bool, ityp, otyp types.Type, grows func(int),
	eval func([]T2, error, any) ([]T2, error), merge func(int64, int64, T2, T2, bool, bool, any) (T2, bool, error),
	fill func(int64, T1, T2, int64, bool, bool) (T2, bool, error),
	batchFill func(any, any, int64, int64, []uint64, *nulls.Nulls) error, partialresults any) Agg[*UnaryAgg[T1, T2]] {
	return &UnaryAgg[T1, T2]{
		op:             op,
		priv:           priv,
		otyp:           otyp,
		eval:           eval,
		fill:           fill,
		merge:          merge,
		grows:          grows,
		batchFill:      batchFill,
		isCount:        isCount,
		ityps:          []types.Type{ityp},
		partialresults: partialresults,
		err:            nil,
	}
}

func (a *UnaryAgg[T1, T2]) Free(pool *mpool.MPool) {
	if a.otyp.IsVarlen() {
		return
	}
	if cap(a.da) > 0 {
		pool.Free(a.da)
		a.da = nil
	}
}

func (a *UnaryAgg[T1, T2]) OutputType() types.Type {
	return a.otyp
}

func (a *UnaryAgg[T1, T2]) InputTypes() []types.Type {
	return a.ityps
}

func (a *UnaryAgg[T1, T2]) Grows(count int, pool *mpool.MPool) error {
	a.grows(count)

	finalCount := len(a.es) + count
	// allocate memory from pool except for string type.
	if a.otyp.IsVarlen() {
		// first time.
		if len(a.es) == 0 {
			a.vs = make([]T2, count)
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
			data, err := pool.Grow(a.da, finalCount*itemSize)
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
	return err
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

// Merge a[x] += b[y]
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
	a.vs, err = a.eval(a.vs, nil, a.partialresults)
	if err != nil {
		return nil, err
	}

	// TODO: it's a bad hack here. I will remove it later. and change it to a better way like `a.IsOrderedWindow()`
	nullList := a.es
	if GetFunctionIsWinOrderFunBySpecialId(a.op) {
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

func (a *UnaryAgg[T1, T2]) WildAggReAlloc(m *mpool.MPool) error {
	d, err := m.Alloc(len(a.da))
	if err != nil {
		return err
	}
	copy(d, a.da)
	a.da = d
	setAggValues[T1, T2](a, a.otyp)
	return nil
}

func (a *UnaryAgg[T1, T2]) IsDistinct() bool {
	return false
}

func (a *UnaryAgg[T1, T2]) GetOperatorId() int {
	return a.op
}

func (a *UnaryAgg[T1, T2]) GetInputTypes() []types.Type {
	return a.ityps
}

func (a *UnaryAgg[T1, T2]) MarshalBinary() ([]byte, error) {
	pData, err := a.priv.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// encode the input types.
	source := &EncodeAgg{
		Op:         a.op,
		Private:    pData,
		Es:         a.es,
		InputTypes: types.EncodeSlice(a.ityps),
		OutputType: types.EncodeType(&a.otyp),
		IsCount:    a.isCount,
	}
	switch {
	case a.otyp.Oid.IsMySQLString():
		source.Da = types.EncodeStringSlice(getUnaryAggStrVs(a))
	default:
		source.Da = a.da
	}

	return source.Marshal()
}

func getUnaryAggStrVs(strUnaryAgg any) []string {
	agg := strUnaryAgg.(*UnaryAgg[[]byte, []byte])
	result := make([]string, len(agg.vs))
	for i := range result {
		result[i] = string(agg.vs[i])
	}
	return result
}

func (a *UnaryAgg[T1, T2]) UnmarshalBinary(data []byte) error {
	// avoid resulting errors caused by morpc overusing memory
	copyData := make([]byte, len(data))
	copy(copyData, data)
	decoded := new(EncodeAgg)
	if err := decoded.Unmarshal(copyData); err != nil {
		return err
	}

	// Recover data
	a.ityps = types.DecodeSlice[types.Type](decoded.InputTypes)
	a.otyp = types.DecodeType(decoded.OutputType)
	a.isCount = decoded.IsCount
	a.es = decoded.Es
	data = make([]byte, len(decoded.Da))
	copy(data, decoded.Da)
	a.da = data

	setAggValues[T1, T2](a, a.otyp)

	return a.priv.UnmarshalBinary(decoded.Private)
}

func setAggValues[T1, T2 any](agg any, typ types.Type) {
	switch {
	case typ.Oid.IsMySQLString():
		a := agg.(*UnaryAgg[[]byte, []byte])
		values := types.DecodeStringSlice(a.da)
		a.vs = make([][]byte, len(values))
		for i := range a.vs {
			a.vs[i] = []byte(values[i])
		}
	default:
		a := agg.(*UnaryAgg[T1, T2])
		a.vs = types.DecodeSlice[T2](a.da)
	}
}
