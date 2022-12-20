// Copyright 2022 Matrix Origin
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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
// when implicit cast from varchar to date is ready, get rid of this
func ExtractFromString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.Type{Oid: types.T_uint32, Size: 4}
	resultElementSize := int(resultType.Size)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		leftValues, rightValues := left.Col.(*types.Bytes), right.Col.(*types.Bytes)
		resultVector := vector.NewConst(resultType)
		resultValues := make([]uint32, 1)
		unit := string(leftValues.Data)
		inputDate, err := types.ParseDateCast(string(rightValues.Get(0)))
		if err != nil {
			return nil, moerr.NewInternalError("invalid input")
		}
		resultValues, err = extract.ExtractFromDate(unit, []types.Date{inputDate}, resultValues)
		if err != nil {
			return nil, moerr.NewInternalError("invalid input")
		}
		vector.SetCol(resultVector, resultValues)
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		leftValues, rightValues := left.Col.(*types.Bytes), right.Col.(*types.Bytes)
		unit := string(leftValues.Data)
		resultValues, err := proc.AllocVector(resultType, int64(resultElementSize) * int64(len(rightValues.Lengths)))
		if

		result, resultNsp, err := extract.ExtractFromInputBytes(unit, rightValues, right.Nsp, )

	default:
		return nil, moerr.NewInternalError("invalid input")
	}
}
*/

func ExtractFromDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.Type{Oid: types.T_uint32, Size: 4}
	resultElementSize := int(resultType.Size)
	leftValues, rightValues := vector.MustStrCols(left), vector.MustTCols[types.Date](right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := vector.MustTCols[uint32](resultVector)
		unit := leftValues[0]
		_, err := extract.ExtractFromDate(unit, rightValues, resultValues)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return resultVector, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(rightValues)))
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[uint32](resultVector)
		unit := leftValues[0]
		_, err = extract.ExtractFromDate(unit, rightValues, resultValues)
		if err != nil {
			return nil, err
		}
		return resultVector, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}

func ExtractFromDatetime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	resultType := types.Type{Oid: types.T_varchar, Size: 24, Width: types.MaxVarcharLen}
	leftValues, rightValues := vector.MustStrCols(left), vector.MustTCols[types.Datetime](right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, 1)
		unit := leftValues[0]
		resultValues, err := extract.ExtractFromDatetime(unit, rightValues, resultValues)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return vector.NewConstString(resultType, 1, resultValues[0], proc.Mp()), nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, len(rightValues))
		unit := leftValues[0]
		resultValues, err := extract.ExtractFromDatetime(unit, rightValues, resultValues)
		if err != nil {
			return nil, err
		}
		return vector.NewWithStrings(resultType, resultValues, right.Nsp, proc.Mp()), nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}
