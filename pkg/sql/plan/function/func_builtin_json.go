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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/xeipuuv/gojsonschema"
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

type computeJsonFn func([]byte, []*bytejson.Path, []bytejson.ByteJson) (bytejson.ByteJson, error)

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

func computeJsonSet(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Modify(paths, newVal, bytejson.JsonModifySet)
}

func computeStringJsonSet(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return bytejson.Null, err
	}
	return bj.Modify(paths, newVal, bytejson.JsonModifySet)
}

func computeJsonInsert(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Modify(paths, newVal, bytejson.JsonModifyInsert)
}

func computeStringJsonInsert(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return bytejson.Null, err
	}
	return bj.Modify(paths, newVal, bytejson.JsonModifyInsert)
}

func computeJsonReplace(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj := types.DecodeJson(json)
	return bj.Modify(paths, newVal, bytejson.JsonModifyReplace)
}

func computeStringJsonReplace(json []byte, paths []*bytejson.Path, newVal []bytejson.ByteJson) (bytejson.ByteJson, error) {
	bj, err := types.ParseSliceToByteJson(json)
	if err != nil {
		return bytejson.Null, err
	}
	return bj.Modify(paths, newVal, bytejson.JsonModifyReplace)
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

	if !op.simple || (op.simple && len(op.paths) > 1) {
		return moerr.NewInvalidInput(proc.Ctx, "json_extract_string should use a path that retrives a single value")
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
	if !op.simple || (op.simple && len(op.paths) > 1) {
		return moerr.NewInvalidInput(proc.Ctx, "json_extract_float64 should use a path that retrives a single value")
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

type opBuiltInJsonSet struct {
}

func newOpBuiltInJsonSet() *opBuiltInJsonSet {
	return &opBuiltInJsonSet{}
}

// JSON_SET
func jsonSetCheckFn(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 2 {
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

func (op *opBuiltInJsonSet) buildJsonSet(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.buildJsonFunction(parameters, result, proc, length, selectList, bytejson.JsonModifySet)
}

func (op *opBuiltInJsonSet) buildJsonInsert(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.buildJsonFunction(parameters, result, proc, length, selectList, bytejson.JsonModifyInsert)
}

func (op *opBuiltInJsonSet) buildJsonReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.buildJsonFunction(parameters, result, proc, length, selectList, bytejson.JsonModifyReplace)
}

func (op *opBuiltInJsonSet) buildJsonFunction(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, jsonFuncType bytejson.JsonModifyType) error {
	// implement json_set function
	// the first parameter is the json object
	// the rest of the parameters are the path-value pairs
	// the path is a string, the value is a json object
	var err error
	var fn computeJsonFn

	jsonVec := parameters[0]
	jsonWrapper := vector.GenerateFunctionStrParameter(jsonVec)
	rs := vector.MustFunctionResult[types.Varlena](result)

	switch jsonFuncType {
	case bytejson.JsonModifySet:
		if jsonVec.GetType().Oid == types.T_json {
			fn = computeJsonSet
		} else {
			fn = computeStringJsonSet
		}
	case bytejson.JsonModifyInsert:
		if jsonVec.GetType().Oid == types.T_json {
			fn = computeJsonInsert
		} else {
			fn = computeStringJsonInsert
		}
	case bytejson.JsonModifyReplace:
		if jsonVec.GetType().Oid == types.T_json {
			fn = computeJsonReplace
		} else {
			fn = computeStringJsonReplace
		}
	default:
		return moerr.NewInvalidInput(proc.Ctx, "invalid json function type")
	}

	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, jIsNull := jsonWrapper.GetStrValue(i)
		if jIsNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return err
		}

		// build all paths
		pathExprs := make([]*bytejson.Path, 0, (len(parameters)-1)/2+1)
		for j := 1; j < len(parameters); j += 2 {
			pathBytes, pIsNull := vector.GenerateFunctionStrParameter(parameters[j]).GetStrValue(uint64(i))
			if pIsNull {
				if err = rs.AppendBytes(nil, true); err != nil {
					return err
				}
				return err
			}

			pathStr := string(pathBytes)
			p, err := types.ParseStringToPath(pathStr)
			if err != nil {
				return err
			}

			pathExprs = append(pathExprs, &p)
		}

		// build all values
		valExprs := make([]bytejson.ByteJson, 0, (len(parameters)-1)/2+1)
		for j := 2; j < len(parameters); j += 2 {
			valBytes, vIsNull := vector.GenerateFunctionStrParameter(parameters[j]).GetStrValue(uint64(i))
			if vIsNull {
				var expr bytejson.ByteJson
				expr, err = bytejson.CreateByteJSON(nil)
				if err != nil {
					return err
				}
				valExprs = append(valExprs, expr)
				continue
			}
			valString := string(valBytes)

			_, parserErr := strconv.ParseInt(valString, 10, 64)
			var val bytejson.ByteJson
			if len(valString) > 0 && (valString[0] == '{' || valString[0] == '[' || parserErr == nil) {
				val, err = types.ParseStringToByteJson(valString)
				if err != nil {
					return err
				}

			} else {
				val, err = bytejson.CreateByteJSON(valString)
				if err != nil {
					return err
				}
			}
			valExprs = append(valExprs, val)
		}

		out, err := fn(jsonBytes, pathExprs, valExprs)
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
	return nil
}

type opBuiltInJsonArray struct{}

func newOpBuiltInJsonArray() *opBuiltInJsonArray {
	return &opBuiltInJsonArray{}
}

func (op *opBuiltInJsonArray) jsonArray(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for j := 0; j < length; j++ {
		elems := make([]any, 0, len(params))
		for i := 0; i < len(params); i++ {
			elem, err := op.convertToAny(proc.Ctx, params[i], j)
			if err != nil {
				return err
			}
			elems = append(elems, elem)
		}

		bj, err := bytejson.CreateByteJSON(elems)
		if err != nil {
			return err
		}
		dt, err := bj.Marshal()
		if err != nil {
			return err
		}
		if selectList.Contains(uint64(j)) {
			rs.AppendBytes(nil, true)
		} else {
			rs.AppendBytes(dt, false)
		}
	}
	return nil
}

func (op *opBuiltInJsonArray) convertToAny(ctx context.Context, v *vector.Vector, row int) (any, error) {
	fromType := v.GetType()
	switch fromType.Oid {
	case types.T_bool:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return vector.GetFixedAtNoTypeCheck[bool](v, row), nil
	case types.T_int8:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return int64(vector.GetFixedAtNoTypeCheck[int8](v, row)), nil
	case types.T_int16:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return int64(vector.GetFixedAtNoTypeCheck[int16](v, row)), nil
	case types.T_int32:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return int64(vector.GetFixedAtNoTypeCheck[int32](v, row)), nil
	case types.T_int64:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return vector.GetFixedAtNoTypeCheck[int64](v, row), nil
	case types.T_uint8:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return uint64(vector.GetFixedAtNoTypeCheck[uint8](v, row)), nil
	case types.T_uint16:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return uint64(vector.GetFixedAtNoTypeCheck[uint16](v, row)), nil
	case types.T_uint32:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return uint64(vector.GetFixedAtNoTypeCheck[uint32](v, row)), nil
	case types.T_uint64:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return vector.GetFixedAtNoTypeCheck[uint64](v, row), nil
	case types.T_float32:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return float64(vector.GetFixedAtNoTypeCheck[float32](v, row)), nil
	case types.T_float64:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return vector.GetFixedAtNoTypeCheck[float64](v, row), nil
	case types.T_char, types.T_varchar, types.T_text:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return string(v.GetBytesAt(row)), nil
	case types.T_json:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		data := v.GetBytesAt(row)
		if len(data) == 0 {
			return nil, nil
		}
		bj := types.DecodeJson(data)
		return bj, nil
	case types.T_date:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return newTypedByteJson(bytejson.TpCodeDate, vector.GetFixedAtNoTypeCheck[types.Date](v, row).String()), nil
	case types.T_time:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return newTypedByteJson(bytejson.TpCodeTime, vector.GetFixedAtNoTypeCheck[types.Time](v, row).String()), nil
	case types.T_datetime:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return newTypedByteJson(bytejson.TpCodeDatetime, vector.GetFixedAtNoTypeCheck[types.Datetime](v, row).String()), nil
	case types.T_timestamp:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return newTypedByteJson(bytejson.TpCodeDatetime, vector.GetFixedAtNoTypeCheck[types.Timestamp](v, row).String()), nil
	case types.T_decimal64:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](v, row)
		return newTypedByteJson(bytejson.TpCodeDecimal, string(val.Format(fromType.Scale))), nil
	case types.T_decimal128:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](v, row)
		return newTypedByteJson(bytejson.TpCodeDecimal, string(val.Format(fromType.Scale))), nil
	case types.T_binary, types.T_varbinary, types.T_blob:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		data := v.GetBytesAt(row)
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(dst, data)
		return newTypedByteJson(bytejson.TpCodeBlob, string(dst)), nil
	case types.T_decimal256:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		val := vector.GetFixedAtNoTypeCheck[types.Decimal256](v, row)
		return newTypedByteJson(bytejson.TpCodeDecimal, string(val.Format(fromType.Scale))), nil
	case types.T_year:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		val := vector.GetFixedAtNoTypeCheck[int16](v, row)
		return strconv.FormatInt(int64(val), 10), nil
	case types.T_bit:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return newTypedByteJson(bytejson.TpCodeBlob, strconv.FormatUint(vector.GetFixedAtNoTypeCheck[uint64](v, row), 10)), nil
	case types.T_enum:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		val := vector.GetFixedAtNoTypeCheck[types.Enum](v, row)
		return val.String(), nil
	case types.T_geometry:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		data := v.GetBytesAt(row)
		return string(data), nil
	case types.T_uuid:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return vector.GetFixedAtNoTypeCheck[types.Uuid](v, row).String(), nil
	case types.T_array_float32:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		arr := types.BytesToArray[float32](v.GetBytesAt(row))
		out := make([]any, len(arr))
		for i, x := range arr {
			out[i] = float64(x)
		}
		return out, nil
	case types.T_array_float64:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		arr := types.BytesToArray[float64](v.GetBytesAt(row))
		out := make([]any, len(arr))
		for i, x := range arr {
			out[i] = x
		}
		return out, nil
	default:
		if v.IsNull(uint64(row)) {
			return nil, nil
		}
		return nil, moerr.NewInvalidInputf(ctx, "unsupported type for json_array: %v", fromType.String())
	}
}

type opBuiltInJsonObject struct{}

func newOpBuiltInJsonObject() *opBuiltInJsonObject {
	return &opBuiltInJsonObject{}
}

func (op *opBuiltInJsonObject) jsonObject(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	arrayOp := &opBuiltInJsonArray{}

	for j := 0; j < length; j++ {
		obj := make(map[string]any, len(params)/2)

		for i := 0; i < len(params); i += 2 {
			// key must not be NULL
			if params[i].IsNull(uint64(j)) {
				return moerr.NewInvalidInputf(proc.Ctx, "JSON documents may not contain NULL member names")
			}
			// key may be any type, convert to string representation.
			keyAny, err := arrayOp.convertToAny(proc.Ctx, params[i], j)
			if err != nil {
				return err
			}
			var key string
			switch v := keyAny.(type) {
			case bytejson.ByteJson:
				// Use JSON text form, not internal binary, for key display.
				// GetString() assumes a string-encoded binary layout and panics
				// on numeric/object/array JSON values.
				if bj, err := v.MarshalJSON(); err == nil {
					key = string(bj)
					// Strip surrounding quotes for JSON string values.
					if len(key) >= 2 && key[0] == '"' && key[len(key)-1] == '"' {
						key = key[1 : len(key)-1]
					}
				} else {
					key = fmt.Sprint(v)
				}
			case nil:
				return moerr.NewInvalidInputf(proc.Ctx, "JSON documents may not contain NULL member names")
			default:
				key = fmt.Sprint(v)
			}

			elem, err := arrayOp.convertToAny(proc.Ctx, params[i+1], j)
			if err != nil {
				return err
			}
			obj[key] = elem
		}

		bj, err := bytejson.CreateByteJSON(obj)
		if err != nil {
			return err
		}
		dt, err := bj.Marshal()
		if err != nil {
			return err
		}
		if selectList.Contains(uint64(j)) {
			rs.AppendBytes(nil, true)
		} else {
			rs.AppendBytes(dt, false)
		}
	}
	return nil
}

type opBuiltInJsonType struct{}

func newOpBuiltInJsonType() *opBuiltInJsonType {
	return &opBuiltInJsonType{}
}

func (op *opBuiltInJsonType) jsonType(params []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	jsonVec := params[0]
	isBinary := jsonVec.GetType().Oid == types.T_json
	p := vector.GenerateFunctionStrParameter(jsonVec)

	for j := uint64(0); j < uint64(length); j++ {
		val, null := p.GetStrValue(j)
		if null || selectList.Contains(j) {
			rs.AppendBytes(nil, true)
			continue
		}
		var bj bytejson.ByteJson
		var err error
		if isBinary {
			bj = types.DecodeJson(val)
		} else {
			bj, err = types.ParseStringToByteJson(string(val))
			if err != nil {
				return err
			}
		}
		tn := bj.TYPE()
		if tn == "LITERAL" {
			if bj.Data[0] == bytejson.LiteralNull {
				tn = "NULL"
			} else {
				tn = "BOOLEAN"
			}
		}
		rs.AppendBytes([]byte(tn), false)
	}
	return nil
}

func newTypedByteJson(tp bytejson.TpCode, s string) bytejson.ByteJson {
	l := len(s)
	data := make([]byte, binary.MaxVarintLen64+l)
	n := binary.PutUvarint(data, uint64(l))
	copy(data[n:], s)
	return bytejson.ByteJson{Type: tp, Data: data[:n+l]}
}

// JSON_VALID
func JsonValid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// T_json input: internal representation is always valid JSON.
	jsonValid := func(v []byte) (bool, error) {
		return true, nil
	}
	// String input: try to parse as JSON.
	strValid := func(v []byte) (bool, error) {
		_, err := types.ParseSliceToByteJson(v)
		return err == nil, nil
	}
	fn := jsonValid
	if ivecs[0].GetType().Oid.IsMySQLString() {
		fn = strValid
	}
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, fn, selectList)
}

// JSON_LENGTH
func JsonLength(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) == 2 {
		return jsonLengthWithPath(ivecs, result, proc, length, selectList)
	}
	return jsonLengthRoot(ivecs, result, proc, length, selectList)
}

func jsonLengthRoot(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	jsonFn := func(v []byte) (int64, error) {
		bj := types.DecodeJson(v)
		return jsonValueLength(bj), nil
	}
	strFn := func(v []byte) (int64, error) {
		bj, err := types.ParseSliceToByteJson(v)
		if err != nil {
			return 0, moerr.NewInvalidArg(proc.Ctx, "json_length", "invalid JSON document")
		}
		return jsonValueLength(bj), nil
	}
	fn := jsonFn
	if ivecs[0].GetType().Oid.IsMySQLString() {
		fn = strFn
	}
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, fn, selectList)
}

func jsonLengthWithPath(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[int64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	p2 := vector.OptGetBytesParamFromWrapper(rs, 1, ivecs[1])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	isJson := !ivecs[0].GetType().Oid.IsMySQLString()
	c1, c2 := ivecs[0].IsConst(), ivecs[1].IsConst()

	// selectList: ignore all rows
	if selectList != nil && selectList.IgnoreAllRow() {
		nulls.AddRange(rsNull, 0, uint64(length))
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		// selectList: skip non-selected rows
		if selectList != nil && !selectList.ShouldEvalAllRow() && !selectList.Contains(i) {
			rsNull.Add(i)
			continue
		}
		jsonBytes, null1 := p1.GetStrValue(i)
		pathBytes, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rsNull.Add(i)
			continue
		}
		var bj bytejson.ByteJson
		var err error
		if isJson {
			bj = types.DecodeJson(jsonBytes)
		} else {
			bj, err = types.ParseSliceToByteJson(jsonBytes)
		}
		if err != nil {
			if c1 && c2 {
				return moerr.NewInvalidArg(proc.Ctx, "json_length", "invalid JSON document")
			}
			rsNull.Add(i)
			continue
		}
		path, err := types.ParseStringToPath(string(pathBytes))
		if err != nil {
			if c1 && c2 {
				return moerr.NewInvalidArg(proc.Ctx, "json_length", "invalid path expression")
			}
			rsNull.Add(i)
			continue
		}
		val := bj.Query([]*bytejson.Path{&path})
		if val.IsNull() {
			rsNull.Add(i)
			continue
		}
		rss[int(i)] = jsonValueLength(val)
	}
	return nil
}

func jsonValueLength(bj bytejson.ByteJson) int64 {
	switch bj.Type {
	case bytejson.TpCodeObject:
		return int64(bj.GetElemCnt())
	case bytejson.TpCodeArray:
		return int64(bj.GetElemCnt())
	default:
		return 1
	}
}

// JSON_KEYS
func JsonKeys(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) == 2 {
		return jsonKeysWithPath(ivecs, result, proc, length, selectList)
	}
	return jsonKeysRoot(ivecs, result, proc, length, selectList)
}

func jsonKeysRoot(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	isString := ivecs[0].GetType().Oid.IsMySQLString()
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			rs.AppendMustNullForBytesResult()
			continue
		}
		var bj bytejson.ByteJson
		var err error
		if isString {
			bj, err = types.ParseSliceToByteJson(v)
		} else {
			bj = types.DecodeJson(v)
		}
		if err != nil {
			rs.AppendMustNullForBytesResult()
			continue
		}
		keysArray, err := buildJsonKeysArray(bj)
		if err != nil || keysArray == nil {
			rs.AppendMustNullForBytesResult()
			continue
		}
		rs.AppendMustBytesValue(keysArray)
	}
	return nil
}

func jsonKeysWithPath(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	p2 := vector.OptGetBytesParamFromWrapper(rs, 1, ivecs[1])

	isJson := !ivecs[0].GetType().Oid.IsMySQLString()
	c1, c2 := ivecs[0].IsConst(), ivecs[1].IsConst()

	for i := uint64(0); i < uint64(length); i++ {
		jsonBytes, null1 := p1.GetStrValue(i)
		pathBytes, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rs.AppendMustNullForBytesResult()
			continue
		}
		var bj bytejson.ByteJson
		var err error
		if isJson {
			bj = types.DecodeJson(jsonBytes)
		} else {
			bj, err = types.ParseSliceToByteJson(jsonBytes)
		}
		if err != nil {
			if c1 && c2 {
				return moerr.NewInvalidArg(proc.Ctx, "json_keys", "invalid JSON document")
			}
			rs.AppendMustNullForBytesResult()
			continue
		}
		path, err := types.ParseStringToPath(string(pathBytes))
		if err != nil {
			if c1 && c2 {
				return moerr.NewInvalidArg(proc.Ctx, "json_keys", "invalid path expression")
			}
			rs.AppendMustNullForBytesResult()
			continue
		}
		val := bj.Query([]*bytejson.Path{&path})
		if val.IsNull() || val.Type != bytejson.TpCodeObject {
			rs.AppendMustNullForBytesResult()
			continue
		}
		keysArray, err := buildJsonKeysArray(val)
		if err != nil {
			rs.AppendMustNullForBytesResult()
			continue
		}
		rs.AppendMustBytesValue(keysArray)
	}
	return nil
}

func buildJsonKeysArray(bj bytejson.ByteJson) ([]byte, error) {
	if bj.Type != bytejson.TpCodeObject {
		return nil, nil // not an object → return nil (will be NULL)
	}
	cnt := bj.GetElemCnt()
	keys := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		keys[i] = string(bj.GetObjectKey(i))
	}
	raw, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// JSON_PRETTY
func JsonPretty(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	isString := ivecs[0].GetType().Oid.IsMySQLString()
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			rs.AppendMustNullForBytesResult()
			continue
		}
		var bj bytejson.ByteJson
		var err error
		if isString {
			bj, err = types.ParseSliceToByteJson(v)
		} else {
			bj = types.DecodeJson(v)
		}
		if err != nil {
			return moerr.NewInvalidArg(proc.Ctx, "json_pretty", "invalid JSON document")
		}
		out, err := jsonPrettyPrint(bj, 0)
		if err != nil {
			return err
		}
		rs.AppendMustBytesValue(out)
	}
	return nil
}

func jsonPrettyPrint(bj bytejson.ByteJson, depth int) ([]byte, error) {
	var buf bytes.Buffer
	err := jsonPrettyPrintTo(&buf, bj, depth)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func jsonPrettyPrintTo(w *bytes.Buffer, bj bytejson.ByteJson, depth int) error {
	switch bj.Type {
	case bytejson.TpCodeObject:
		return prettyPrintObject(w, bj, depth)
	case bytejson.TpCodeArray:
		return prettyPrintArray(w, bj, depth)
	default:
		return prettyPrintScalar(w, bj)
	}
}

func prettyPrintObject(w *bytes.Buffer, bj bytejson.ByteJson, depth int) error {
	cnt := bj.GetElemCnt()
	if cnt == 0 {
		w.WriteString("{}")
		return nil
	}
	indent := strings.Repeat("  ", depth+1)
	w.WriteString("{\n")
	for i := 0; i < cnt; i++ {
		key := bj.GetObjectKey(i)
		// Escape key the same way JSON_QUOTE would.
		keyJSON, _ := json.Marshal(string(key))
		w.WriteString(indent)
		w.Write(keyJSON)
		w.WriteString(": ")
		val := bj.GetObjectVal(i)
		if err := jsonPrettyPrintTo(w, val, depth+1); err != nil {
			return err
		}
		if i < cnt-1 {
			w.WriteString(",")
		}
		w.WriteString("\n")
	}
	w.WriteString(strings.Repeat("  ", depth))
	w.WriteString("}")
	return nil
}

func prettyPrintArray(w *bytes.Buffer, bj bytejson.ByteJson, depth int) error {
	cnt := bj.GetElemCnt()
	if cnt == 0 {
		w.WriteString("[]")
		return nil
	}
	indent := strings.Repeat("  ", depth+1)
	w.WriteString("[\n")
	for i := 0; i < cnt; i++ {
		w.WriteString(indent)
		elem := bj.GetArrayElem(i)
		if err := jsonPrettyPrintTo(w, elem, depth+1); err != nil {
			return err
		}
		if i < cnt-1 {
			w.WriteString(",")
		}
		w.WriteString("\n")
	}
	w.WriteString(strings.Repeat("  ", depth))
	w.WriteString("]")
	return nil
}

func prettyPrintScalar(w *bytes.Buffer, bj bytejson.ByteJson) error {
	// Use MarshalJSON to get properly formatted/escaped scalar value.
	text, err := bj.MarshalJSON()
	if err != nil {
		return err
	}
	w.Write(text)
	return nil
}

// JSON_SCHEMA_VALID
func JsonSchemaValid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[bool](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	p2 := vector.OptGetBytesParamFromWrapper(rs, 1, ivecs[1])

	schemaIsStr := ivecs[0].GetType().Oid.IsMySQLString()
	docIsStr := ivecs[1].GetType().Oid.IsMySQLString()

	for i := uint64(0); i < uint64(length); i++ {
		schemaBytes, null1 := p1.GetStrValue(i)
		docBytes, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rs.AppendMustNull()
			continue
		}
		v, err := doJsonSchemaValidate(schemaBytes, docBytes, schemaIsStr, docIsStr, true, proc)
		if err != nil {
			return err
		}
		rs.AppendMustValue(v.(bool))
	}
	return nil
}

// JSON_SCHEMA_VALIDATION_REPORT
func JsonSchemaValidationReport(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(2)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	p2 := vector.OptGetBytesParamFromWrapper(rs, 1, ivecs[1])

	schemaIsStr := ivecs[0].GetType().Oid.IsMySQLString()
	docIsStr := ivecs[1].GetType().Oid.IsMySQLString()

	for i := uint64(0); i < uint64(length); i++ {
		schemaBytes, null1 := p1.GetStrValue(i)
		docBytes, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rs.AppendMustNullForBytesResult()
			continue
		}
		v, err := doJsonSchemaValidate(schemaBytes, docBytes, schemaIsStr, docIsStr, false, proc)
		if err != nil {
			return err
		}
		rs.AppendMustBytesValue(v.([]byte))
	}
	return nil
}

func doJsonSchemaValidate(schemaBytes, docBytes []byte, schemaIsStr, docIsStr bool, boolResult bool, proc *process.Process) (interface{}, error) {
	var schemaBJ, docBJ bytejson.ByteJson
	var err error
	if schemaIsStr {
		schemaBJ, err = types.ParseSliceToByteJson(schemaBytes)
	} else {
		schemaBJ = types.DecodeJson(schemaBytes)
	}
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "json_schema_valid", "invalid schema JSON")
	}
	if schemaBJ.Type != bytejson.TpCodeObject {
		return nil, moerr.NewInvalidArg(proc.Ctx, "json_schema_valid", "schema must be a JSON object")
	}
	if schemaBJ.HasRef() {
		return nil, moerr.NewNotSupportedf(proc.Ctx, "json_schema_valid: $ref is not supported")
	}
	if docIsStr {
		docBJ, err = types.ParseSliceToByteJson(docBytes)
	} else {
		docBJ = types.DecodeJson(docBytes)
	}
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "json_schema_valid", "invalid document JSON")
	}
	schemaJSON, err := schemaBJ.MarshalJSON()
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "json_schema_valid", "invalid schema JSON")
	}
	docJSON, err := docBJ.MarshalJSON()
	if err != nil {
		return nil, moerr.NewInvalidArg(proc.Ctx, "json_schema_valid", "invalid document JSON")
	}
	sl := gojsonschema.NewBytesLoader(schemaJSON)
	dl := gojsonschema.NewBytesLoader(docJSON)
	validationResult, err := gojsonschema.Validate(sl, dl)
	if err != nil {
		if boolResult {
			return true, nil // invalid schema → MySQL returns true
		}
		return []byte(`{"valid": true}`), nil
	}
	if boolResult {
		return validationResult.Valid(), nil
	}
	return buildSchemaValidationReport(validationResult), nil
}

func buildSchemaValidationReport(result *gojsonschema.Result) []byte {
	if result.Valid() {
		return []byte(`{"valid": true}`)
	}
	var buf bytes.Buffer
	buf.WriteString(`{"valid": false, "errors": [`)
	for i, e := range result.Errors() {
		if i > 0 {
			buf.WriteString(", ")
		}
		errObj, _ := json.Marshal(map[string]string{
			"property": e.Field(),
			"message":  e.Description(),
		})
		buf.Write(errObj)
	}
	buf.WriteString("]}")
	return buf.Bytes()
}
