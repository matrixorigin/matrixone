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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	simple   bool
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

type computeFn func([]byte, []*bytejson.Path) (bytejson.ByteJson, error)

func computeJson(json []byte, paths []*bytejson.Path) (bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Query(paths), nil
}

func computeString(json []byte, paths []*bytejson.Path) (bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return bytejson.Null, err
	}
	return bj.Query(paths), nil
}

func computeJsonSimple(json []byte, paths []*bytejson.Path) (bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.QuerySimple(paths), nil
}

func computeStringSimple(json []byte, paths []*bytejson.Path) (bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return bytejson.Null, err
	}
	return bj.QuerySimple(paths), nil
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
		if err := op.buildOnePath(pathWrapers, 0, op.pathStrs, op.paths); err != nil {
			return err
		}
		op.simple = true
		for _, p := range op.paths {
			op.simple = op.simple && p.IsSimple()
		}
		return nil
	} else {
		op.simple = false
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
	var fn computeFn

	jsonVec := parameters[0]
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	// build all paths
	if err = op.buildPath(parameters, length); err != nil {
		return err
	}

	if op.simple {
		if jsonVec.GetType().Oid == types.T_json {
			fn = computeJsonSimple
		} else {
			fn = computeStringSimple
		}
	} else {
		if jsonVec.GetType().Oid == types.T_json {
			fn = computeJson
		} else {
			fn = computeString
		}
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
				if err = rs.AppendByteJson(out, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// JSON_EXTRACT_STRING: extract a string value from a json object
func (op *opBuiltInJsonExtract) jsonExtractString(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	var fn computeFn

	jsonVec := parameters[0]
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	// build all paths
	if err = op.buildPath(parameters, length); err != nil {
		return err
	}

	if !op.simple || op.simple && len(op.paths) > 1 {
		return moerr.NewInvalidInput(proc.Ctx, "json_extract_value should use a path that retrives a single value")
	}
	if jsonVec.GetType().Oid == types.T_json {
		fn = computeJsonSimple
	} else {
		fn = computeStringSimple
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
				if out.TYPE() == "STRING" {
					outstr := out.GetString()
					if err = rs.AppendBytes([]byte(outstr), false); err != nil {
						return err
					}
				} else {
					// append null
					if err = rs.AppendBytes(nil, true); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// JSON_EXTRACT_FLOAT64: extract a float64 value from a json object
func (op *opBuiltInJsonExtract) jsonExtractFloat64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error
	var fn computeFn

	jsonVec := parameters[0]
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	rs := vector.MustFunctionResult[float64](result)

	// build all paths
	if err = op.buildPath(parameters, length); err != nil {
		return err
	}
	if !op.simple || op.simple && len(op.paths) > 1 {
		return moerr.NewInvalidInput(proc.Ctx, "json_extract_value should use a path that retrives a single value")
	}

	if jsonVec.GetType().Oid == types.T_json {
		fn = computeJsonSimple
	} else {
		fn = computeStringSimple
	}

	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, jIsNull := jsonWrapper.GetStrValue(i)
		if jIsNull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		paths := op.getPaths(i)
		if len(paths) == 0 || paths[0] == nil {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		} else {
			out, err := fn(jsonBytes, paths)
			if err != nil {
				return err
			}
			if out.IsNull() {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			} else {
				var fv float64
				// XXX: here we expect we can get a single numeric value, and we expect we can cast
				// it to the target type. No error checking for overflow etc.  Seems this is what
				// customer wants.  If this is not true, we should do a strict, type, range checked
				// version and a try_json_extract_value version for the current behavior.
				if out.TYPE() == "INTEGER" {
					i64 := out.GetInt64()
					fv = float64(i64)
				} else if out.TYPE() == "DOUBLE" {
					fv = out.GetFloat64()
				} else {
					// append null
					if err = rs.Append(0, true); err != nil {
						return err
					}
					continue
				}
				if err = rs.Append(fv, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
