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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/date_format"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// function overview:
// date_format() function used to formated the date value according to the format string. If either argument is NULL, the function returns NULL.
// Input parameter type: date type: datatime, format type: string(constant)
// return type: string
// reference linking:https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format

//  DateFromat
//  @Description: Formats the date value according to the format string. If either argument is NULL, the function returns NULL.
func DateFormat(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	formatVector := vectors[1]

	resultType := types.T_varchar.ToType()
	if !formatVector.IsScalar() {
		return nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "the second parameter of function to_date must be char/varchar constant\n"+usage)
	}

	if dateVector.IsScalarNull() || formatVector.IsScalarNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	// get the format string.
	formatMask := string(formatVector.GetString(0))

	if dateVector.IsScalar() {
		resultVector := proc.AllocScalarVector(resultType)
		resCol := &types.Bytes{
			Data:    make([]byte, 0, 1),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}

		datetimes := dateVector.Col.([]types.Datetime)
		date_format.DateFromat(datetimes, formatMask, dateVector.Nsp, resCol)
		vector.SetCol(resultVector, resCol)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, 0)
		if err != nil {
			return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
		}

		rowCount := vector.Length(dateVector)
		resCol := &types.Bytes{
			Data:    make([]byte, 0, rowCount),
			Offsets: make([]uint32, rowCount),
			Lengths: make([]uint32, rowCount),
		}
		resultVector.Nsp = dateVector.Nsp

		datetimes := dateVector.Col.([]types.Datetime)
		date_format.DateFromat(datetimes, formatMask, dateVector.Nsp, resCol)
		vector.SetCol(resultVector, resCol)
		return resultVector, nil
	}
}
