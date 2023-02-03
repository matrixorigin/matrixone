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

package ctl

import (
	"regexp"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const ZeroDate = "0001-01-01"

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func MOLogDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_date, Size: 4}
	inputValues := vector.MustStrCols(inputVector)

	// regexp
	parsedInput := make([]string, len(inputValues))
	reg := regexp.MustCompile(`\d{4}/\d{1,2}/\d{1,2}`)
	for idx, ori := range inputValues {
		parsedInput[idx] = reg.FindString(ori)
		if len(parsedInput[idx]) == 0 {
			parsedInput[idx] = ZeroDate
		}
	}
	// end of regexp

	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := make([]types.Date, 1)
		result, err := date.DateStringToDate(parsedInput, resultValues)
		vector.SetCol(resultVector, result)
		return resultVector, err
	} else {
		resultVector, err := proc.AllocVectorOfRows(resultType, int64(len(parsedInput)), inputVector.Nsp)
		if err != nil {
			return nil, err
		}
		resultValues := vector.MustTCols[types.Date](resultVector)
		_, err = date.DateStringToDate(parsedInput, resultValues)
		return resultVector, err
	}
}
