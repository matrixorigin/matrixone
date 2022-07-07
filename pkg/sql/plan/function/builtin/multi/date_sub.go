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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/date_sub"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Date](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	resultType := types.Type{Oid: types.T_date, Size: 4}
	resultElementSize := int(resultType.Size)
	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Date, 1)
		res, err := date_sub.DateSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*maxLen))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDateSlice(resultVector.Data)
		resultValues = resultValues[:maxLen]
		res, err := date_sub.DateSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, err
	}
}

func DatetimeSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Datetime](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	resultType := types.Type{Oid: types.T_datetime, Precision: firstVector.Typ.Precision, Size: 8}
	resultElementSize := int(resultType.Size)
	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Datetime, 1)
		res, err := date_sub.DatetimeSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*maxLen))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDatetimeSlice(resultVector.Data)
		resultValues = resultValues[:maxLen]
		res, err := date_sub.DatetimeSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, err
	}
}

func DateStringSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustBytesCols(vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])
	resultType := types.Type{Oid: types.T_datetime, Precision: 6, Size: 8}
	resultElementSize := int(resultType.Size)

	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Datetime, 1)
		res, err := date_sub.DateStringSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues.Lengths) > len(secondValues) {
			maxLen = len(firstValues.Lengths)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*maxLen))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeDatetimeSlice(resultVector.Data)
		resultValues = resultValues[:maxLen]
		res, err := date_sub.DateStringSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, err
	}
}

func TimeStampSub(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vectors[0]
	secondVector := vectors[1]
	firstValues := vector.MustTCols[types.Timestamp](vectors[0])
	secondValues := vector.MustTCols[int64](vectors[1])
	thirdValues := vector.MustTCols[int64](vectors[2])

	resultType := types.Type{Oid: types.T_timestamp, Precision: firstVector.Typ.Precision, Size: 8}
	resultElementSize := int(resultType.Size)
	if firstVector.IsScalar() && secondVector.IsScalar() {
		if firstVector.IsScalarNull() || secondVector.IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := proc.AllocScalarVector(resultType)
		rs := make([]types.Timestamp, 1)
		res, err := date_sub.TimestampSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, rs)
		vector.SetCol(resultVector, res)
		return resultVector, err
	} else {
		var maxLen int
		if len(firstValues) > len(secondValues) {
			maxLen = len(firstValues)
		} else {
			maxLen = len(secondValues)
		}
		resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*maxLen))
		if err != nil {
			return nil, err
		}
		resultValues := encoding.DecodeTimestampSlice(resultVector.Data)
		resultValues = resultValues[:maxLen]
		nulls.Set(resultVector.Nsp, firstVector.Nsp)
		resultValues, err = date_sub.TimestampSub(firstValues, secondValues, thirdValues, firstVector.Nsp, secondVector.Nsp, resultVector.Nsp, resultValues)
		vector.SetCol(resultVector, resultValues)
		return resultVector, err
	}
}
