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
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type computeFn func([]byte, []*bytejson.Path) (*bytejson.ByteJson, error)

func computeJson(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(paths), nil
}
func computeString(json []byte, paths []*bytejson.Path) (*bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return nil, err
	}
	return bj.Query(paths), nil
}

func JsonExtract(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	jsonVec := parameters[0]
	var fn computeFn
	switch jsonVec.GetType().Oid {
	case types.T_json:
		fn = computeJson
	default:
		fn = computeString
	}
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	pathWrapers := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters)-1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	paths := make([]*bytejson.Path, len(parameters)-1)
	for i := 0; i < len(parameters)-1; i++ {
		pathWrapers[i] = vector.GenerateFunctionStrParameter(parameters[i+1])
	}
	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, jIsNull := jsonWrapper.GetStrValue(i)
		if jIsNull {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		skip := false
		for j := 0; j < len(parameters)-1; j++ {
			pathBytes, pIsNull := pathWrapers[j].GetStrValue(i)
			if pIsNull {
				skip = true
				break
			}
			p, err := types.ParseStringToPath(string(pathBytes))
			if err != nil {
				return err
			}
			paths[j] = &p
		}
		if skip {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		out, err := fn(jsonBytes, paths)
		if err != nil {
			return err
		}
		if out.IsNull() {
			err := rs.AppendBytes(nil, true)
			if err != nil {
				return err
			}
			continue
		}
		dt, _ := out.Marshal()
		err = rs.AppendBytes(dt, false)
		if err != nil {
			return err
		}
	}
	return nil
}
