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
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Udf struct {
	Body     string `json:"body"`
	Language string `json:"language"`
	RetType  string `json:"rettype"`
	Args     []*Arg `json:"args"`

	ArgsType []types.Type `json:"-"`
}

// Arg of Udf
type Arg struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (u *Udf) GetPlanExpr() *plan.Expr {
	bytes, _ := json.Marshal(u)
	return &plan.Expr{
		Typ: type2PlanType(types.T_text.ToType()),
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: false,
				Value: &plan.Const_Sval{
					Sval: string(bytes),
				},
			},
		},
	}
}

func (u *Udf) GetArgsPlanType() []*plan.Type {
	typ := u.GetArgsType()
	ptyp := make([]*plan.Type, len(typ))
	for i, t := range typ {
		ptyp[i] = type2PlanType(t)
	}
	return ptyp
}

func (u *Udf) GetRetPlanType() *plan.Type {
	typ := u.GetRetType()
	return type2PlanType(typ)
}

func (u *Udf) GetArgsType() []types.Type {
	return u.ArgsType
}

func (u *Udf) GetRetType() types.Type {
	return types.Types[u.RetType].ToType()
}

func type2PlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

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
	request := &PythonUdfRequest{
		Udf: &PythonUdf{
			Handler: body.Handler,
			AsFun:   body.As,
			RetType: t2DataType[udf.GetRetType().Oid],
		},
		Vectors: make([]*DataVector, len(parameters)-1),
		Length:  int64(length),
	}
	for i := 1; i < len(parameters); i++ {
		dataVector, _ := vector2DataVector(parameters[i])
		request.Vectors[i-1] = dataVector
	}

	// run
	client, err := getPythonUdfClient()
	if err != nil {
		return err
	}
	response, err := client.Run(context.Background(), request)
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

func getDataFromDataVector(v *DataVector, i int) *Data {
	if v == nil {
		return nil
	}
	if v.Const {
		return v.Data[0]
	}
	return v.Data[i]
}

func vector2DataVector(v *vector.Vector) (*DataVector, error) {
	if v == nil {
		return nil, nil
	}
	dv := &DataVector{
		Const:  v.IsConst(),
		Length: int64(v.Length()),
		Type:   t2DataType[v.GetType().Oid],
		Scale:  v.GetType().Scale,
	}
	size := v.Length()
	if dv.Const {
		size = 1
	}
	dv.Data = make([]*Data, size)
	switch v.GetType().Oid {
	case types.T_bool:
		p := vector.GenerateFunctionFixedTypeParameter[bool](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_BoolVal{BoolVal: val}}
			}
		}
	case types.T_int8:
		p := vector.GenerateFunctionFixedTypeParameter[int8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int16:
		p := vector.GenerateFunctionFixedTypeParameter[int16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int32:
		p := vector.GenerateFunctionFixedTypeParameter[int32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_IntVal{IntVal: val}}
			}
		}
	case types.T_int64:
		p := vector.GenerateFunctionFixedTypeParameter[int64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_Int64Val{Int64Val: val}}
			}
		}
	case types.T_uint8:
		p := vector.GenerateFunctionFixedTypeParameter[uint8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint16:
		p := vector.GenerateFunctionFixedTypeParameter[uint16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint32:
		p := vector.GenerateFunctionFixedTypeParameter[uint32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_UintVal{UintVal: val}}
			}
		}
	case types.T_uint64:
		p := vector.GenerateFunctionFixedTypeParameter[uint64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_Uint64Val{Uint64Val: val}}
			}
		}
	case types.T_float32:
		p := vector.GenerateFunctionFixedTypeParameter[float32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_FloatVal{FloatVal: val}}
			}
		}
	case types.T_float64:
		p := vector.GenerateFunctionFixedTypeParameter[float64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_DoubleVal{DoubleVal: val}}
			}
		}
	case types.T_char, types.T_varchar, types.T_text:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: string(val)}}
			}
		}
	case types.T_json:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: types.DecodeJson(val).String()}}
			}
		}
	case types.T_uuid:
		p := vector.GenerateFunctionFixedTypeParameter[types.Uuid](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.ToString()}}
			}
		}
	case types.T_time:
		p := vector.GenerateFunctionFixedTypeParameter[types.Time](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_date:
		p := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.String()}}
			}
		}
	case types.T_datetime:
		p := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_timestamp:
		p := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.String2(time.Local, v.GetType().Scale)}}
			}
		}
	case types.T_decimal64:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_decimal128:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &Data{Val: &Data_BytesVal{BytesVal: val}}
			}
		}
	default:
		return nil, moerr.NewInvalidArgNoCtx("python udf arg type", v.GetType().String())
	}
	return dv, nil
}

func writeResponse(response *PythonUdfResponse, result vector.FunctionResultWrapper) error {
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
	t2DataType = map[types.T]DataType{
		types.T_bool:       DataType_BOOL,
		types.T_int8:       DataType_INT8,
		types.T_int16:      DataType_INT16,
		types.T_int32:      DataType_INT32,
		types.T_int64:      DataType_INT64,
		types.T_uint8:      DataType_UINT8,
		types.T_uint16:     DataType_UINT16,
		types.T_uint32:     DataType_UINT32,
		types.T_uint64:     DataType_UINT64,
		types.T_float32:    DataType_FLOAT32,
		types.T_float64:    DataType_FLOAT64,
		types.T_char:       DataType_CHAR,
		types.T_varchar:    DataType_VARCHAR,
		types.T_text:       DataType_TEXT,
		types.T_json:       DataType_JSON,
		types.T_uuid:       DataType_UUID,
		types.T_time:       DataType_TIME,
		types.T_date:       DataType_DATE,
		types.T_datetime:   DataType_DATETIME,
		types.T_timestamp:  DataType_TIMESTAMP,
		types.T_decimal64:  DataType_DECIMAL64,
		types.T_decimal128: DataType_DECIMAL128,
		types.T_binary:     DataType_BINARY,
		types.T_varbinary:  DataType_VARBINARY,
		types.T_blob:       DataType_BLOB,
	}

	dataType2T = map[DataType]types.T{
		DataType_BOOL:       types.T_bool,
		DataType_INT8:       types.T_int8,
		DataType_INT16:      types.T_int16,
		DataType_INT32:      types.T_int32,
		DataType_INT64:      types.T_int64,
		DataType_UINT8:      types.T_uint8,
		DataType_UINT16:     types.T_uint16,
		DataType_UINT32:     types.T_uint32,
		DataType_UINT64:     types.T_uint64,
		DataType_FLOAT32:    types.T_float32,
		DataType_FLOAT64:    types.T_float64,
		DataType_CHAR:       types.T_char,
		DataType_VARCHAR:    types.T_varchar,
		DataType_TEXT:       types.T_text,
		DataType_JSON:       types.T_json,
		DataType_UUID:       types.T_uuid,
		DataType_TIME:       types.T_time,
		DataType_DATE:       types.T_date,
		DataType_DATETIME:   types.T_datetime,
		DataType_TIMESTAMP:  types.T_timestamp,
		DataType_DECIMAL64:  types.T_decimal64,
		DataType_DECIMAL128: types.T_decimal128,
		DataType_BINARY:     types.T_binary,
		DataType_VARBINARY:  types.T_varbinary,
		DataType_BLOB:       types.T_blob,
	}
)

var (
	pythonUdfClient      PythonUdfServiceClient
	pythonUdfClientMutex sync.Mutex
)

func getPythonUdfClient() (PythonUdfServiceClient, error) {
	if pythonUdfClient == nil {
		pythonUdfClientMutex.Lock()
		if pythonUdfClient == nil {
			conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			pythonUdfClient = NewPythonUdfServiceClient(conn)
		}
		pythonUdfClientMutex.Unlock()
	}
	return pythonUdfClient, nil
}

func UdfArgTypeMatch(from []types.Type, to []types.T) (bool, int) {
	sta, cost := tryToMatch(from, to)
	return sta != matchFailed, cost
}

func UdfArgTypeCast(from []types.Type, to []types.T) []types.Type {
	castType := make([]types.Type, len(from))
	for i := range castType {
		if to[i] == from[i].Oid {
			castType[i] = from[i]
		} else {
			castType[i] = to[i].ToType()
			setTargetScaleFromSource(&from[i], &castType[i])
		}
	}
	return castType
}
