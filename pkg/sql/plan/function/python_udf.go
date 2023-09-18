// Copyright 2023 Matrix Origin
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

package function

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// param inputs has four parts:
//  1. inputs[0]: udf, function self
//  2. inputs[1 : size+1]: receivedArgs, args which function received
//  3. inputs[size+1 : 2*size+1]: requiredArgs, args which function required
//  4. inputs[2*size+1]: ret, function ret
//     which size = (len(inputs) - 2) / 2
func checkPythonUdf(overloads []overload, inputs []types.Type) checkResult {

	if len(inputs)%2 == 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if len(inputs) == 2 {
		return newCheckResultWithSuccess(0)
	}
	size := (len(inputs) - 2) / 2
	receivedArgs := inputs[1 : size+1]
	requiredArgs := inputs[size+1 : 2*size+1]
	needCast := false
	for i := 0; i < size; i++ {
		if receivedArgs[i].Oid != requiredArgs[i].Oid {
			canCast, _ := fixedImplicitTypeCast(receivedArgs[i], requiredArgs[i].Oid)
			if !canCast {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}
			needCast = true
		}
	}
	if needCast {
		castType := make([]types.Type, size+1)
		castType[0] = inputs[0]
		for i, typ := range requiredArgs {
			castType[i+1] = typ
		}
		return newCheckResultWithCast(0, castType)
	}
	return newCheckResultWithSuccess(0)
}

// param parameters is same with param inputs in function checkPythonUdf
func pythonUdfRetType(parameters []types.Type) types.Type {
	return parameters[len(parameters)-1]
}

// param parameters has two parts:
//  1. parameters[0]: const vector udf
//  2. parameters[1:]: data vectors
func runPythonUdf(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	// udf
	u := &Udf{}
	bytes, _ := vector.GenerateFunctionStrParameter(parameters[0]).GetStrValue(0)
	err := json.Unmarshal(bytes, u)
	if err != nil {
		return err
	}

	// request
	body := &NonSqlUdfBody{}
	err = json.Unmarshal([]byte(u.Body), body)
	if err != nil {
		return err
	}
	request := &udf.Request{
		Udf: &udf.Udf{
			Handler:      body.Handler,
			IsImport:     body.Import,
			Body:         body.Body,
			RetType:      t2DataType[u.GetRetType().Oid],
			Language:     udf.LanguagePython,
			Db:           u.Db,
			ModifiedTime: u.ModifiedTime,
		},
		Vectors: make([]*udf.DataVector, len(parameters)-1),
		Length:  int64(length),
		Type:    udf.RequestType_DataRequest,
	}
	for i := 1; i < len(parameters); i++ {
		dataVector, _ := vector2DataVector(parameters[i])
		request.Vectors[i-1] = dataVector
	}

	// getPkg
	getPkg := func() (pkg []byte, err error) {
		ioVector := &fileservice.IOVector{
			FilePath: request.Udf.Body,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   -1,
				},
			},
		}
		err = proc.FileService.Read(proc.Ctx, ioVector)
		return ioVector.Entries[0].Data, err
	}

	// run
	response, err := proc.UdfService.Run(proc.Ctx, request, getPkg)
	if err != nil {
		return err
	}

	// response
	err = writeResponse(response, result)
	if err != nil {
		return err
	}

	return nil
}
