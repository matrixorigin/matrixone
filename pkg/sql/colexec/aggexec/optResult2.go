// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func initAggResultWithFixedTypeResult[T types.FixedSizeTExceptStrType](
	mg AggMemoryManager,
	resultType types.Type,
	setEmptyGroupToNull bool, initialValue T) aggResultWithFixedType[T] {

	res := aggResultWithFixedType[T]{}
	res.init(mg, resultType, setEmptyGroupToNull)
	res.InitialValue = initialValue
	res.values = make([][]T, 1)

	return res
}

func initAggResultWithBytesTypeResult(
	mg AggMemoryManager,
	resultType types.Type,
	setEmptyGroupToNull bool, initialValue string) aggResultWithBytesType {

	res := aggResultWithBytesType{}
	res.init(mg, resultType, setEmptyGroupToNull)
	res.InitialValue = []byte(initialValue)

	return res
}

type aggResultWithFixedType[T types.FixedSizeTExceptStrType] struct {
	optSplitResult

	// the initial value for a new result row.
	InitialValue T

	// for easy get from / set to resultList.
	values [][]T
}

func (r *aggResultWithFixedType[T]) marshal() [][]byte {
	return nil
}

func (r *aggResultWithFixedType[T]) grows(more int) error {
	x1, y1, x2, y2, err := r.resExtend(more)
	if err != nil {
		return err
	}

	r.values[x1] = vector.MustFixedColNoTypeCheck[T](r.resultList[x1])
	for i := x1 + 1; i <= x2; i++ {
		r.values = append(r.values, vector.MustFixedColNoTypeCheck[T](r.resultList[i]))
	}
	setValueFromX1Y1ToX2Y2(r.values, x1, y1, x2, y2, r.InitialValue)
	return nil
}

func (r *aggResultWithFixedType[T]) get() T {
	return r.values[r.accessIdx1][r.accessIdx2]
}

func (r *aggResultWithFixedType[T]) set(value T) {
	r.values[r.accessIdx1][r.accessIdx2] = value
}

type aggResultWithBytesType struct {
	optSplitResult

	// the initial value for a new result row.
	InitialValue []byte
}

func (r *aggResultWithBytesType) marshal() [][]byte {
	return nil
}

func (r *aggResultWithBytesType) grows(more int) error {
	x1, y1, x2, y2, err := r.resExtend(more)
	if err != nil {
		return err
	}

	// copy from function setValueFromX1Y1ToX2Y2.
	if x1 == x2 {
		for y1 < y2 {
			if err = vector.SetBytesAt(r.resultList[x1], y1, r.InitialValue, r.mp); err != nil {
				return err
			}
		}
		return nil
	}

	for i := y1; i < r.optInformation.eachSplitCapacity; i++ {
		if err = vector.SetBytesAt(r.resultList[x1], i, r.InitialValue, r.mp); err != nil {
			return err
		}
	}
	for x := x1 + 1; x < x2; x++ {
		for i := 0; i < r.optInformation.eachSplitCapacity; i++ {
			if err = vector.SetBytesAt(r.resultList[x], i, r.InitialValue, r.mp); err != nil {
				return err
			}
		}
	}
	for i := 0; i < y2; i++ {
		if err = vector.SetBytesAt(r.resultList[x2], i, r.InitialValue, r.mp); err != nil {
			return err
		}
	}
	return nil
}

func (r *aggResultWithBytesType) get() []byte {
	// never return the source pointer directly.
	//
	// if not so, the append action outside like `r = append(r, "more")` will cause memory contamination to other data row.
	newr := r.resultList[r.accessIdx1].GetBytesAt(r.accessIdx2)
	newr = newr[:len(newr):len(newr)]
	return newr
}

func (r *aggResultWithBytesType) set(value []byte) error {
	return vector.SetBytesAt(r.resultList[r.accessIdx1], r.accessIdx2, value, r.mp)
}
