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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type opBuiltInJsonExtract struct {
	allConst bool
	npath    int
	pathStrs []string
	paths    []*bytejson.Path
}

func newOpBuiltInJsonExtract() *opBuiltInJsonExtract {
	return &opBuiltInJsonExtract{}
}

// JSON_EXTRACT
func jsonExtractCheckFn(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		ts := make([]types.Type, 0, len(inputs))
		allMatch := true
		for _, input := range inputs {
			if input.Oid == types.T_json || input.Oid.IsMySQLString() {
				ts = append(ts, input)
			} else {
				if canCast, _ := fixedImplicitTypeCast(input, types.T_varchar); canCast {
					ts = append(ts, types.T_varchar.ToType())
					allMatch = false
				} else {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
			}
		}
		if allMatch {
			return newCheckResultWithSuccess(0)
		}
		return newCheckResultWithCast(0, ts)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

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

func (op *opBuiltInJsonExtract) buildPath(params []*vector.Vector, length int) error {
	op.npath = len(params) - 1
	if op.npath == 0 {
		return nil
	}

	// the only case we care: all paths are constant
	op.allConst = true
	for i := 0; i < op.npath; i++ {
		if !params[i+1].IsConst() {
			op.allConst = false
			break
		}
	}

	if op.allConst {
		if len(op.pathStrs) != op.npath {
			op.pathStrs = make([]string, op.npath)
			op.paths = make([]*bytejson.Path, op.npath)
		} else {
			// check if the paths are the same
			match := true
			for i := 0; i < op.npath; i++ {
				// if the path is null, we treat it as empty string
				if params[i+1].IsNull(0) {
					op.pathStrs[i] = ""
					op.paths[i] = nil
					continue
				}
				// check if the path is the same
				if op.pathStrs[i] != string(params[i+1].UnsafeGetStringAt(0)) {
					match = false
					break
				}
			}
			if match {
				return nil
			}
		}
	} else {
		op.pathStrs = make([]string, op.npath*length)
		op.paths = make([]*bytejson.Path, op.npath*length)
	}

	// Do it!
	pathWrapers := make([]vector.FunctionParameterWrapper[types.Varlena], op.npath)
	for i := 0; i < op.npath; i++ {
		pathWrapers[i] = vector.GenerateFunctionStrParameter(params[i+1])
	}

	if op.allConst {
		return op.buildOnePath(pathWrapers, 0, op.pathStrs, op.paths)
	} else {
		for i := 0; i < length; i++ {
			strs := op.pathStrs[i*op.npath : (i+1)*op.npath]
			paths := op.paths[i*op.npath : (i+1)*op.npath]
			if err := op.buildOnePath(pathWrapers, i, strs, paths); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opBuiltInJsonExtract) getPaths(i uint64) []*bytejson.Path {
	if op.allConst {
		return op.paths
	}
	return op.paths[i*uint64(op.npath) : (i+1)*uint64(op.npath)]
}

func (op *opBuiltInJsonExtract) buildOnePath(paramWrappers []vector.FunctionParameterWrapper[types.Varlena], i int, strs []string, paths []*bytejson.Path) error {
	skip := false
	for j := 0; j < len(paramWrappers); j++ {
		pathBytes, pIsNull := paramWrappers[j].GetStrValue(uint64(i))
		if pIsNull {
			skip = true
			break
		}

		strs[j] = string(pathBytes)
		p, err := types.ParseStringToPath(strs[j])
		if err != nil {
			return err
		}
		paths[j] = &p
	}

	if skip {
		for j := 0; j < len(paramWrappers); j++ {
			strs[j] = ""
			paths[j] = nil
		}
	}

	return nil
}

func (op *opBuiltInJsonExtract) jsonExtract(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	jsonVec := parameters[0]
	var fn computeFn
	switch jsonVec.GetType().Oid {
	case types.T_json:
		fn = computeJson
	default:
		fn = computeString
	}
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	// build all paths
	if err = op.buildPath(parameters, length); err != nil {
		return err
	}

	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, jIsNull := jsonWrapper.GetStrValue(i)
		if jIsNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		paths := op.getPaths(i)
		if len(paths) == 0 || paths[0] == nil {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		} else {
			out, err := fn(jsonBytes, paths)
			if err != nil {
				return err
			}
			if out.IsNull() {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				dt, _ := out.Marshal()
				if err = rs.AppendBytes(dt, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
