// Copyright 2021 - 2022 Matrix Origin
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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sub"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Minus[T constraints.Integer | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv, rv := vectors[0], vectors[1]
	lvs, rvs := lv.Col.([]T), rv.Col.([]T)
	resultElementSize := lv.Typ.Oid.FixedLength()

	switch {
	case lv.IsConst && rv.IsConst:
		resultVector, err := process.Get(proc, int64(resultElementSize), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.Numeric(lvs, rvs, resultValues))
		resultVector.IsConst = true
		resultVector.Length = lv.Length
		return resultVector, nil
	case lv.IsConst && !rv.IsConst:
		resultVector, err := process.Get(proc, int64(resultElementSize*len(rvs)), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericScalar[T](lvs[0], rvs, resultValues))
		return resultVector, nil
	case !lv.IsConst && rv.IsConst:
		resultVector, err := process.Get(proc, int64(resultElementSize*len(lvs)), lv.Typ)
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
		nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
		vector.SetCol(resultVector, sub.NumericScalar[T](rvs[0], lvs, resultValues))
		return resultVector, nil
	}
	resultVector, err := process.Get(proc, int64(resultElementSize*len(lvs)), lv.Typ)
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeFixedSlice[T](resultVector.Data, resultElementSize)
	nulls.Or(lv.Nsp, rv.Nsp, resultVector.Nsp)
	vector.SetCol(resultVector, sub.Numeric[T](lvs, rvs, resultValues))
	return resultVector, nil
}
