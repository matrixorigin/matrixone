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
	"context"
	"encoding/json"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/udf/pythonservice"
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
	udf := &Udf{}
	bytes, _ := vector.GenerateFunctionStrParameter(parameters[0]).GetStrValue(0)
	err := json.Unmarshal(bytes, udf)
	if err != nil {
		return err
	}

	// request
	body := &tree.PythonFunctionBody{}
	err = json.Unmarshal([]byte(udf.Body), body)
	if err != nil {
		return err
	}
	request := &pythonservice.PythonUdfRequest{
		Udf: &pythonservice.PythonUdf{
			Handler: body.Handler,
			AsFun:   body.As,
			RetType: t2DataType[udf.GetRetType().Oid],
		},
		Vectors: make([]*pythonservice.DataVector, len(parameters)-1),
		Length:  int64(length),
	}
	for i := 1; i < len(parameters); i++ {
		dataVector, _ := vector2DataVector(parameters[i])
		request.Vectors[i-1] = dataVector
	}

	// run
	response, err := proc.UdfService.RunPythonUdf(context.Background(), request)
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

func getDataFromDataVector(v *pythonservice.DataVector, i int) *pythonservice.Data {
	if v == nil {
		return nil
	}
	if v.Const {
		return v.Data[0]
	}
	return v.Data[i]
}

func vector2DataVector(v *vector.Vector) (*pythonservice.DataVector, error) {
	if v == nil {
		return nil, nil
	}
	dv := &pythonservice.DataVector{
		Const:  v.IsConst(),
		Length: int64(v.Length()),
		Type:   t2DataType[v.GetType().Oid],
		Scale:  v.GetType().Scale,
	}
	size := v.Length()
	if dv.Const {
		size = 1
	}
	dv.Data = make([]*pythonservice.Data, size)
	switch v.GetType().Oid {
	case types.T_bool:
		p := vector.GenerateFunctionFixedTypeParameter[bool](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_BoolVal{BoolVal: val}}
			}
		}
	case types.T_int8:
		p := vector.GenerateFunctionFixedTypeParameter[int8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int16:
		p := vector.GenerateFunctionFixedTypeParameter[int16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int32:
		p := vector.GenerateFunctionFixedTypeParameter[int32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_IntVal{IntVal: val}}
			}
		}
	case types.T_int64:
		p := vector.GenerateFunctionFixedTypeParameter[int64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_Int64Val{Int64Val: val}}
			}
		}
	case types.T_uint8:
		p := vector.GenerateFunctionFixedTypeParameter[uint8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint16:
		p := vector.GenerateFunctionFixedTypeParameter[uint16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint32:
		p := vector.GenerateFunctionFixedTypeParameter[uint32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_UintVal{UintVal: val}}
			}
		}
	case types.T_uint64:
		p := vector.GenerateFunctionFixedTypeParameter[uint64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_Uint64Val{Uint64Val: val}}
			}
		}
	case types.T_float32:
		p := vector.GenerateFunctionFixedTypeParameter[float32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_FloatVal{FloatVal: val}}
			}
		}
	case types.T_float64:
		p := vector.GenerateFunctionFixedTypeParameter[float64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_DoubleVal{DoubleVal: val}}
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: string(val)}}
			}
		}
	case types.T_json:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: types.DecodeJson(val).String()}}
			}
		}
	case types.T_uuid:
		p := vector.GenerateFunctionFixedTypeParameter[types.Uuid](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.ToString()}}
			}
		}
	case types.T_time:
		p := vector.GenerateFunctionFixedTypeParameter[types.Time](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_date:
		p := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.String()}}
			}
		}
	case types.T_datetime:
		p := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_timestamp:
		p := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.String2(time.Local, v.GetType().Scale)}}
			}
		}
	case types.T_decimal64:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_decimal128:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &pythonservice.Data{Val: &pythonservice.Data_BytesVal{BytesVal: val}}
			}
		}
	default:
		return nil, moerr.NewInvalidArgNoCtx("python udf arg type", v.GetType().String())
	}
	return dv, nil
}

func writeResponse(response *pythonservice.PythonUdfResponse, result vector.FunctionResultWrapper) error {
	var err error
	retType := dataType2T[response.Vector.Type]
	length := int(response.Vector.Length)
	scale := response.Vector.Scale

	result.GetResultVector().SetTypeScale(scale)

	switch retType {
	case types.T_bool:
		res := vector.MustFunctionResult[bool](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(false, true)
			} else {
				err = res.Append(data.GetBoolVal(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_int8:
		res := vector.MustFunctionResult[int8](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(int8(data.GetIntVal()), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_int16:
		res := vector.MustFunctionResult[int16](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(int16(data.GetIntVal()), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_int32:
		res := vector.MustFunctionResult[int32](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetIntVal(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_int64:
		res := vector.MustFunctionResult[int64](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetInt64Val(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_uint8:
		res := vector.MustFunctionResult[uint8](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(uint8(data.GetUintVal()), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_uint16:
		res := vector.MustFunctionResult[uint16](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(uint16(data.GetUintVal()), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_uint32:
		res := vector.MustFunctionResult[uint32](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetUintVal(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_uint64:
		res := vector.MustFunctionResult[uint64](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetUint64Val(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_float32:
		res := vector.MustFunctionResult[float32](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetFloatVal(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_float64:
		res := vector.MustFunctionResult[float64](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.Append(0, true)
			} else {
				err = res.Append(data.GetDoubleVal(), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		res := vector.MustFunctionResult[types.Varlena](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				err = res.AppendBytes([]byte(data.GetStringVal()), false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_json:
		res := vector.MustFunctionResult[types.Varlena](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				bytes, err2 := bytejson.ParseJsonByteFromString(data.GetStringVal())
				if err2 != nil {
					return err2
				}
				err = res.AppendBytes(bytes, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_uuid:
		res := vector.MustFunctionResult[types.Uuid](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				uuid, err2 := types.ParseUuid(data.GetStringVal())
				if err2 != nil {
					return err2
				}
				err = res.Append(uuid, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_time:
		res := vector.MustFunctionResult[types.Time](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				t, err2 := types.ParseTime(data.GetStringVal(), scale)
				if err2 != nil {
					return err2
				}
				err = res.Append(t, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_date:
		res := vector.MustFunctionResult[types.Date](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				d, err2 := types.ParseDateCast(data.GetStringVal())
				if err2 != nil {
					return err2
				}
				err = res.Append(d, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_datetime:
		res := vector.MustFunctionResult[types.Datetime](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				dt, err2 := types.ParseDatetime(data.GetStringVal(), scale)
				if err2 != nil {
					return err2
				}
				err = res.Append(dt, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_timestamp:
		res := vector.MustFunctionResult[types.Timestamp](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				ts, err2 := types.ParseTimestamp(time.Local, data.GetStringVal(), scale)
				if err2 != nil {
					return err2
				}
				err = res.Append(ts, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_decimal64, types.T_decimal128:
		res := vector.MustFunctionResult[types.Decimal128](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				decimal, err2 := types.ParseDecimal128(data.GetStringVal(), 38, scale)
				if err2 != nil {
					return err2
				}
				err = res.Append(decimal, false)
			}
			if err != nil {
				return err
			}
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		res := vector.MustFunctionResult[types.Varlena](result)
		for i := 0; i < length; i++ {
			data := getDataFromDataVector(response.Vector, i)
			if data == nil || data.Val == nil {
				err = res.AppendBytes(nil, true)
			} else {
				err = res.AppendBytes(data.GetBytesVal(), false)
			}
			if err != nil {
				return err
			}
		}
	default:
		return moerr.NewNotSupportedNoCtx("python udf return type: %v", retType.String())
	}

	return nil
}

var (
	t2DataType = map[types.T]pythonservice.DataType{
		types.T_bool:       pythonservice.DataType_BOOL,
		types.T_int8:       pythonservice.DataType_INT8,
		types.T_int16:      pythonservice.DataType_INT16,
		types.T_int32:      pythonservice.DataType_INT32,
		types.T_int64:      pythonservice.DataType_INT64,
		types.T_uint8:      pythonservice.DataType_UINT8,
		types.T_uint16:     pythonservice.DataType_UINT16,
		types.T_uint32:     pythonservice.DataType_UINT32,
		types.T_uint64:     pythonservice.DataType_UINT64,
		types.T_float32:    pythonservice.DataType_FLOAT32,
		types.T_float64:    pythonservice.DataType_FLOAT64,
		types.T_char:       pythonservice.DataType_CHAR,
		types.T_varchar:    pythonservice.DataType_VARCHAR,
		types.T_text:       pythonservice.DataType_TEXT,
		types.T_json:       pythonservice.DataType_JSON,
		types.T_uuid:       pythonservice.DataType_UUID,
		types.T_time:       pythonservice.DataType_TIME,
		types.T_date:       pythonservice.DataType_DATE,
		types.T_datetime:   pythonservice.DataType_DATETIME,
		types.T_timestamp:  pythonservice.DataType_TIMESTAMP,
		types.T_decimal64:  pythonservice.DataType_DECIMAL64,
		types.T_decimal128: pythonservice.DataType_DECIMAL128,
		types.T_binary:     pythonservice.DataType_BINARY,
		types.T_varbinary:  pythonservice.DataType_VARBINARY,
		types.T_blob:       pythonservice.DataType_BLOB,
	}

	dataType2T = map[pythonservice.DataType]types.T{
		pythonservice.DataType_BOOL:       types.T_bool,
		pythonservice.DataType_INT8:       types.T_int8,
		pythonservice.DataType_INT16:      types.T_int16,
		pythonservice.DataType_INT32:      types.T_int32,
		pythonservice.DataType_INT64:      types.T_int64,
		pythonservice.DataType_UINT8:      types.T_uint8,
		pythonservice.DataType_UINT16:     types.T_uint16,
		pythonservice.DataType_UINT32:     types.T_uint32,
		pythonservice.DataType_UINT64:     types.T_uint64,
		pythonservice.DataType_FLOAT32:    types.T_float32,
		pythonservice.DataType_FLOAT64:    types.T_float64,
		pythonservice.DataType_CHAR:       types.T_char,
		pythonservice.DataType_VARCHAR:    types.T_varchar,
		pythonservice.DataType_TEXT:       types.T_text,
		pythonservice.DataType_JSON:       types.T_json,
		pythonservice.DataType_UUID:       types.T_uuid,
		pythonservice.DataType_TIME:       types.T_time,
		pythonservice.DataType_DATE:       types.T_date,
		pythonservice.DataType_DATETIME:   types.T_datetime,
		pythonservice.DataType_TIMESTAMP:  types.T_timestamp,
		pythonservice.DataType_DECIMAL64:  types.T_decimal64,
		pythonservice.DataType_DECIMAL128: types.T_decimal128,
		pythonservice.DataType_BINARY:     types.T_binary,
		pythonservice.DataType_VARBINARY:  types.T_varbinary,
		pythonservice.DataType_BLOB:       types.T_blob,
	}
)
