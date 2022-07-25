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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// var usage = "to_date usage: "
var usage = ""

func ToDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if !vectors[1].IsScalar() {
		return nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "the second parameter of function to_date must be char/varchar constant\n"+usage)
	}
	inputBytes0 := vector.MustBytesCols(vectors[0])
	inputBytes1 := vector.MustBytesCols(vectors[1])
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	if vectors[0].IsScalar() && vectors[1].IsScalar() {
		resultVector := vector.NewConst(resultType, 1)
		results := &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		format := string(inputBytes1.Data)
		inputNsp := vectors[0].Nsp
		result, resultNsp, err := ToDateInputBytes(inputBytes0, format, inputNsp, results)
		if err != nil {
			return nil, err
		}
		nulls.Set(resultVector.Nsp, resultNsp)
		vector.SetCol(resultVector, result)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, 0) // I think we can calculate the bytes needed ahead and get rid of append though
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, len(inputBytes0.Lengths)),
			Lengths: make([]uint32, len(inputBytes0.Lengths)),
		}
		format := string(inputBytes1.Data)
		inputNsp := vectors[0].Nsp
		result, resultNsp, err := ToDateInputBytes(inputBytes0, format, inputNsp, results)
		if err != nil {
			return nil, err
		}
		nulls.Set(resultVector.Nsp, resultNsp)
		vector.SetCol(resultVector, result)
		return resultVector, nil
	}
}

var otherFormats = map[string]string{
	"MMDDYYYY":        "01022006",
	"DDMMYYYY":        "02012006",
	"MM-DD-YYYY":      "01-02-2006",
	"DD-MM-YYYY":      "02-01-2006",
	"YYYY-MM-DD":      "2006-01-02",
	"YYYYMMDD":        "20060102",
	"YYYYMMDD HHMMSS": "20060102 15:04:05",
}

func ToDateInputBytes(inputBytes *types.Bytes, format string, inputNsp *nulls.Nulls, resultBytes *types.Bytes) (*types.Bytes, *nulls.Nulls, error) {
	resultValueLen := uint32(len("2006-01-02"))
	resultOffset := uint32(0)
	resultNsp := new(nulls.Nulls)
	for i := range inputBytes.Lengths {
		if nulls.Contains(inputNsp, uint64(i)) {
			resultBytes.Offsets[i] = resultOffset
			resultBytes.Lengths[i] = 0
			nulls.Add(resultNsp, uint64(i))
			continue
		}
		offset := inputBytes.Offsets[i]
		length := inputBytes.Lengths[i]
		inputValue := inputBytes.Data[offset : offset+length]
		if val, ok := otherFormats[format]; ok {
			t, err := time.Parse(val, string(inputValue))
			if err != nil {
				return nil, nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, string(inputValue)+"is not is a valid "+format+" format.")
			}
			resultValue := []byte(t.Format("2006-01-02")) // this is our output format
			resultBytes.Data = append(resultBytes.Data, resultValue...)
			resultBytes.Offsets[i] = resultOffset
			resultBytes.Lengths[i] = resultValueLen
			resultOffset += resultValueLen
		} else {
			t, err := time.Parse(val, string(inputValue))
			if err != nil {
				return nil, nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "invalid inputs for function 'to_date()'"+string(inputValue)+" "+format)
			}
			resultValue := []byte(t.Format("2006-01-02")) // this is our output format
			resultBytes.Data = append(resultBytes.Data, resultValue...)
			resultBytes.Offsets[i] = resultOffset
			resultBytes.Lengths[i] = resultValueLen
			resultOffset += resultValueLen
		}
	}
	return resultBytes, resultNsp, nil
}
