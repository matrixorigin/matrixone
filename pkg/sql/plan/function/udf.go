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
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

type Udf struct {
	// sql string, or json string of NonSqlUdfBody
	Body         string `json:"body"`
	Language     string `json:"language"`
	RetType      string `json:"rettype"`
	Args         []*Arg `json:"args"`
	Db           string `json:"db"`
	ModifiedTime string `json:"modified_time"`

	ArgsType []types.Type `json:"-"`
}

type UdfWithContext struct {
	*Udf    `json:"udf"`
	Context map[string]string `json:"context"` // context map
}

type NonSqlUdfBody struct {
	Handler string `json:"handler"`
	Import  bool   `json:"import"`
	Body    string `json:"body"`
}

// Arg of Udf
type Arg struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (u *Udf) GetPlanExpr() *plan.Expr {
	uid, _ := uuid.NewV7()
	bytes, _ := json.Marshal(&UdfWithContext{
		Udf: u,
		Context: map[string]string{
			"queryId": uid.String(),
		},
	})
	return &plan.Expr{
		Typ: *type2PlanType(types.T_text.ToType()),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value: &plan.Literal_Sval{
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

func getDataFromDataVector(v *udf.DataVector, i int) *udf.Data {
	if v == nil {
		return nil
	}
	if v.Const {
		return v.Data[0]
	}
	return v.Data[i]
}

func vector2DataVector(v *vector.Vector) (*udf.DataVector, error) {
	if v == nil {
		return nil, nil
	}
	dv := &udf.DataVector{
		Const:  v.IsConst(),
		Length: int64(v.Length()),
		Type:   t2DataType[v.GetType().Oid],
		Scale:  v.GetType().Scale,
	}
	size := v.Length()
	if dv.Const {
		size = 1
	}
	dv.Data = make([]*udf.Data, size)
	switch v.GetType().Oid {
	case types.T_bool:
		p := vector.GenerateFunctionFixedTypeParameter[bool](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_BoolVal{BoolVal: val}}
			}
		}
	case types.T_int8:
		p := vector.GenerateFunctionFixedTypeParameter[int8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int16:
		p := vector.GenerateFunctionFixedTypeParameter[int16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_IntVal{IntVal: int32(val)}}
			}
		}
	case types.T_int32:
		p := vector.GenerateFunctionFixedTypeParameter[int32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_IntVal{IntVal: val}}
			}
		}
	case types.T_int64:
		p := vector.GenerateFunctionFixedTypeParameter[int64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_Int64Val{Int64Val: val}}
			}
		}
	case types.T_uint8:
		p := vector.GenerateFunctionFixedTypeParameter[uint8](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint16:
		p := vector.GenerateFunctionFixedTypeParameter[uint16](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_UintVal{UintVal: uint32(val)}}
			}
		}
	case types.T_uint32:
		p := vector.GenerateFunctionFixedTypeParameter[uint32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_UintVal{UintVal: val}}
			}
		}
	case types.T_uint64:
		p := vector.GenerateFunctionFixedTypeParameter[uint64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_Uint64Val{Uint64Val: val}}
			}
		}
	case types.T_float32:
		p := vector.GenerateFunctionFixedTypeParameter[float32](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_FloatVal{FloatVal: val}}
			}
		}
	case types.T_float64:
		p := vector.GenerateFunctionFixedTypeParameter[float64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_DoubleVal{DoubleVal: val}}
			}
		}
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: string(val)}}
			}
		}
	case types.T_json:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: types.DecodeJson(val).String()}}
			}
		}
	case types.T_uuid:
		p := vector.GenerateFunctionFixedTypeParameter[types.Uuid](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.String()}}
			}
		}
	case types.T_time:
		p := vector.GenerateFunctionFixedTypeParameter[types.Time](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_date:
		p := vector.GenerateFunctionFixedTypeParameter[types.Date](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.String()}}
			}
		}
	case types.T_datetime:
		p := vector.GenerateFunctionFixedTypeParameter[types.Datetime](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.String2(v.GetType().Scale)}}
			}
		}
	case types.T_timestamp:
		p := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.String2(time.Local, v.GetType().Scale)}}
			}
		}
	case types.T_decimal64:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_decimal128:
		p := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_StringVal{StringVal: val.Format(v.GetType().Scale)}}
			}
		}
	case types.T_binary, types.T_varbinary, types.T_blob:
		p := vector.GenerateFunctionStrParameter(v)
		for i := 0; i < size; i++ {
			val, isNull := p.GetStrValue(uint64(i))
			if !isNull {
				dv.Data[i] = &udf.Data{Val: &udf.Data_BytesVal{BytesVal: val}}
			}
		}
	default:
		return nil, moerr.NewInvalidArgNoCtx("python udf arg type", v.GetType().String())
	}
	return dv, nil
}

func writeResponse(response *udf.Response, result vector.FunctionResultWrapper) error {
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
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
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
	t2DataType = map[types.T]udf.DataType{
		types.T_bool:       udf.DataType_BOOL,
		types.T_int8:       udf.DataType_INT8,
		types.T_int16:      udf.DataType_INT16,
		types.T_int32:      udf.DataType_INT32,
		types.T_int64:      udf.DataType_INT64,
		types.T_uint8:      udf.DataType_UINT8,
		types.T_uint16:     udf.DataType_UINT16,
		types.T_uint32:     udf.DataType_UINT32,
		types.T_uint64:     udf.DataType_UINT64,
		types.T_float32:    udf.DataType_FLOAT32,
		types.T_float64:    udf.DataType_FLOAT64,
		types.T_char:       udf.DataType_CHAR,
		types.T_varchar:    udf.DataType_VARCHAR,
		types.T_text:       udf.DataType_TEXT,
		types.T_json:       udf.DataType_JSON,
		types.T_uuid:       udf.DataType_UUID,
		types.T_time:       udf.DataType_TIME,
		types.T_date:       udf.DataType_DATE,
		types.T_datetime:   udf.DataType_DATETIME,
		types.T_timestamp:  udf.DataType_TIMESTAMP,
		types.T_decimal64:  udf.DataType_DECIMAL64,
		types.T_decimal128: udf.DataType_DECIMAL128,
		types.T_binary:     udf.DataType_BINARY,
		types.T_varbinary:  udf.DataType_VARBINARY,
		types.T_blob:       udf.DataType_BLOB,
	}

	dataType2T = map[udf.DataType]types.T{
		udf.DataType_BOOL:       types.T_bool,
		udf.DataType_INT8:       types.T_int8,
		udf.DataType_INT16:      types.T_int16,
		udf.DataType_INT32:      types.T_int32,
		udf.DataType_INT64:      types.T_int64,
		udf.DataType_UINT8:      types.T_uint8,
		udf.DataType_UINT16:     types.T_uint16,
		udf.DataType_UINT32:     types.T_uint32,
		udf.DataType_UINT64:     types.T_uint64,
		udf.DataType_FLOAT32:    types.T_float32,
		udf.DataType_FLOAT64:    types.T_float64,
		udf.DataType_CHAR:       types.T_char,
		udf.DataType_VARCHAR:    types.T_varchar,
		udf.DataType_TEXT:       types.T_text,
		udf.DataType_JSON:       types.T_json,
		udf.DataType_UUID:       types.T_uuid,
		udf.DataType_TIME:       types.T_time,
		udf.DataType_DATE:       types.T_date,
		udf.DataType_DATETIME:   types.T_datetime,
		udf.DataType_TIMESTAMP:  types.T_timestamp,
		udf.DataType_DECIMAL64:  types.T_decimal64,
		udf.DataType_DECIMAL128: types.T_decimal128,
		udf.DataType_BINARY:     types.T_binary,
		udf.DataType_VARBINARY:  types.T_varbinary,
		udf.DataType_BLOB:       types.T_blob,
	}
)

type DefaultPkgReader struct {
	Proc *process.Process
}

func (d *DefaultPkgReader) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	reader, writer := io.Pipe()
	var errGroup *errgroup.Group
	defer func() {
		if errGroup == nil {
			writer.Close()
		}
	}()

	ioVector := &fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{
			{
				Offset:        0,
				Size:          -1,
				WriterForRead: writer,
			},
		},
	}

	errGroup = new(errgroup.Group)
	errGroup.Go(func() error {
		defer writer.Close()
		return d.Proc.GetFileService().Read(ctx, ioVector)
	})

	return reader, nil
}
