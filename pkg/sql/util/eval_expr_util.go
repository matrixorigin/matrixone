// Copyright 2021 Matrix Origin
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

package util

import (
	"context"
	"fmt"
	"go/constant"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func getVal(val any) string {
	switch v := val.(type) {
	/*
		case float32:
			return fmt.Sprintf("%e", val)
		case float64:
			return fmt.Sprintf("%e", val)
	*/
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}

func GenVectorByVarValue(proc *process.Process, typ types.Type, val any) (*vector.Vector, error) {
	if val == nil {
		vec := vector.NewConstNull(typ, 1, proc.Mp()) // todo use pool
		return vec, nil
	} else {
		strVal := getVal(val)
		vec := vector.NewConstBytes(typ, []byte(strVal), 1, proc.Mp()) // todo use pool
		return vec, nil
	}
}

func AppendAnyToStringVector(proc *process.Process, val any, vec *vector.Vector) error {
	if val == nil {
		return vector.AppendBytes(vec, []byte{}, true, proc.Mp())
	} else {
		strVal := getVal(val)
		return vector.AppendBytes(vec, []byte(strVal), false, proc.Mp())
	}
}

func SetAnyToStringVector(proc *process.Process, val any, vec *vector.Vector, idx int) error {
	if val == nil {
		vec.GetNulls().Set(uint64(idx))
		return nil
	} else {
		strVal := getVal(val)
		return vector.SetBytesAt(vec, idx, []byte(strVal), proc.Mp())
	}
}

func SetBytesToAnyVector(ctx context.Context, val string, row int,
	isNull bool, vec *vector.Vector, proc *process.Process) error {
	if isNull {
		vec.GetNulls().Set(uint64(row))
		return nil
	} else {
		vec.GetNulls().Unset(uint64(row))
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		v, err := types.ParseBool(val)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_int8:
		v, err := strconv.ParseInt(val, 0, 8)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "int8", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, int8(v))
	case types.T_int16:
		v, err := strconv.ParseInt(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "int16", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, int16(v))
	case types.T_int32:
		v, err := strconv.ParseInt(val, 0, 32)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "int32", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, int32(v))
	case types.T_int64:
		v, err := strconv.ParseInt(val, 0, 64)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "int64", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, int64(v))
	case types.T_uint8:
		v, err := strconv.ParseUint(val, 0, 8)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "uint8", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, uint8(v))
	case types.T_uint16:
		v, err := strconv.ParseUint(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "uint16", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, uint16(v))
	case types.T_uint32:
		v, err := strconv.ParseUint(val, 0, 32)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "uint32", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, uint32(v))
	case types.T_uint64:
		v, err := strconv.ParseUint(val, 0, 64)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "uint64", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, uint64(v))
	case types.T_float32:
		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "float32", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, float32(v))
	case types.T_float64:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "float64", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, float64(v))
	case types.T_decimal64:
		v, err := types.ParseDecimal64(val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_decimal128:
		v, err := types.ParseDecimal128(val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_text:
		return vector.SetBytesAt(vec, row, []byte(val), proc.Mp())
	case types.T_json:
		return vector.SetBytesAt(vec, row, []byte(val), proc.Mp())
	case types.T_time:
		v, err := types.ParseTime(val, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_datetime:
		v, err := types.ParseDatetime(val, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_timestamp:
		v, err := types.ParseTimestamp(time.Local, val, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	default:
		panic(fmt.Sprintf("unsupported type %v", vec.GetType().Oid))
	}
}

func SetInsertValue(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (bool, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return setInsertValueBool(proc, numVal, vec)
	case types.T_int8:
		return setInsertValueNumber[int8](proc, numVal, vec)
	case types.T_int16:
		return setInsertValueNumber[int16](proc, numVal, vec)
	case types.T_int32:
		return setInsertValueNumber[int32](proc, numVal, vec)
	case types.T_int64:
		return setInsertValueNumber[int64](proc, numVal, vec)
	case types.T_uint8:
		return setInsertValueNumber[uint8](proc, numVal, vec)
	case types.T_uint16:
		return setInsertValueNumber[uint16](proc, numVal, vec)
	case types.T_uint32:
		return setInsertValueNumber[uint32](proc, numVal, vec)
	case types.T_uint64:
		return setInsertValueNumber[uint64](proc, numVal, vec)
	case types.T_float32:
		return setInsertValueNumber[float32](proc, numVal, vec)
	case types.T_float64:
		return setInsertValueNumber[float64](proc, numVal, vec)
	case types.T_decimal64:
		return setInsertValueDecimal64(proc, numVal, vec)
	case types.T_decimal128:
		return setInsertValueDecimal128(proc, numVal, vec)
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_text:
		return setInsertValueString(proc, numVal, vec)
	case types.T_json:
		return setInsertValueJSON(proc, numVal, vec)
	case types.T_uuid:
		return setInsertValueUuid(proc, numVal, vec)
	case types.T_time:
		return setInsertValueTime(proc, numVal, vec)
	case types.T_date:
		return setInsertValueDate(proc, numVal, vec)
	case types.T_datetime:
		return setInsertValueDateTime(proc, numVal, vec)
	case types.T_timestamp:
		return setInsertValueTimeStamp(proc, numVal, vec)
	}

	return false, nil
}

func setInsertValueTimeStamp(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	appendIntegerTimeStamp := func(val int64) error {
		if val < 0 || uint64(val) > 32536771199 {
			return vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())
		}
		result := types.UnixToTimestamp(val)
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = appendIntegerTimeStamp(val)

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = appendIntegerTimeStamp(int64(val))

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		var val uint64
		val, err = hexToInt(numVal.OrigString())
		if err != nil {
			return false, err
		}
		err = appendIntegerTimeStamp(int64(val))

	case tree.P_char:
		s := numVal.OrigString()
		if len(s) == 0 {
			err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())
		} else {
			typ := vec.GetType()
			var val types.Timestamp
			zone := time.Local
			if proc != nil {
				zone = proc.SessionInfo.TimeZone
			}
			val, err = types.ParseTimestamp(zone, s, typ.Scale)
			if err != nil {
				return
			}
			err = vector.AppendFixed(vec, val, false, proc.Mp())
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())
	default:
		canInsert = false
	}
	return
}

func setInsertValueDateTime(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())

	case tree.P_int64:
		canInsert = false

	case tree.P_uint64:
		canInsert = false

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		canInsert = false

	case tree.P_char:
		s := numVal.OrigString()
		if len(s) == 0 {
			err = vector.AppendFixed[types.Datetime](vec, 0, true, proc.GetMPool())
		} else {
			typ := vec.GetType()
			var val types.Datetime
			val, err = types.ParseDatetime(s, typ.Scale)
			if err != nil {
				return
			}
			err = vector.AppendFixed(vec, val, false, proc.Mp())
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())
	default:
		canInsert = false
	}
	return
}

func setInsertValueTime(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	appendIntegerTime := func(val int64) error {
		typ := vec.GetType()
		if val < types.MinInputIntTime || val > types.MaxInputIntTime {
			return moerr.NewOutOfRange(proc.Ctx, "time", "value %d", val)
		}
		result, err := types.ParseInt64ToTime(val, typ.Scale)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendFixed[types.Time](vec, 0, true, proc.GetMPool())

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = appendIntegerTime(val)

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = appendIntegerTime(int64(val))

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		var val uint64
		val, err = hexToInt(numVal.OrigString())
		if err != nil {
			return false, err
		}
		err = appendIntegerTime(int64(val))

	case tree.P_char:
		s := numVal.OrigString()
		if len(s) == 0 {
			err = vector.AppendFixed[types.Time](vec, 0, true, proc.GetMPool())
		} else {
			typ := vec.GetType()
			var val types.Time
			val, err = types.ParseTime(s, typ.Scale)
			if err != nil {
				return
			}
			err = vector.AppendFixed(vec, val, false, proc.Mp())
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendFixed[types.Time](vec, 0, true, proc.GetMPool())
	default:
		canInsert = false
	}
	return
}

func setInsertValueDate(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendFixed[types.Time](vec, 0, true, proc.GetMPool())

	case tree.P_int64:
		canInsert = false

	case tree.P_uint64:
		canInsert = false

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		canInsert = false

	case tree.P_char:
		s := numVal.OrigString()
		var val types.Date
		if len(s) == 0 {
			err = vector.AppendFixed[types.Date](vec, 0, true, proc.GetMPool())
		} else {
			val, err = types.ParseDateCast(s)
			if err != nil {
				return
			}
			err = vector.AppendFixed(vec, val, false, proc.Mp())
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendFixed[types.Time](vec, 0, true, proc.GetMPool())
	default:
		canInsert = false
	}
	return
}

func setInsertValueUuid(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())

	case tree.P_int64:
		canInsert = false

	case tree.P_uint64:
		canInsert = false

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		canInsert = false

	case tree.P_char:
		s := numVal.OrigString()
		var val types.Uuid
		val, err = types.ParseUuid(s)
		if err != nil {
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendFixed[types.Timestamp](vec, 0, true, proc.GetMPool())
	default:
		canInsert = false
	}
	return
}

func hexToInt(hex string) (uint64, error) {
	s := hex[2:]
	if len(s)%2 != 0 {
		s = string('0') + s
	}
	return strconv.ParseUint(s, 16, 64)
}

func setInsertValueBool(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true
	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_bool:
		val := constant.BoolVal(numVal.Value)
		err = vector.AppendFixed[bool](vec, val, false, proc.Mp())

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = vector.AppendFixed[bool](vec, val == 1, false, proc.Mp())

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = vector.AppendFixed[bool](vec, val == 1, false, proc.Mp())

	case tree.P_decimal:
		canInsert = false
	case tree.P_float64:
		canInsert = false
	case tree.P_hexnum:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_char:
		originStr := numVal.OrigString()
		if len(originStr) == 4 && strings.ToLower(originStr) == "true" {
			err = vector.AppendFixed[bool](vec, true, false, proc.Mp())
		} else {
			err = vector.AppendFixed[bool](vec, false, false, proc.Mp())
		}

	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		canInsert = false
	}
	return
}

func setInsertValueString(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true

	checkStrLen := func(s string) ([]byte, error) {
		typ := vec.GetType()
		destLen := int(typ.Width)
		if typ.Oid != types.T_text && typ.Oid != types.T_binary && destLen != 0 {
			if utf8.RuneCountInString(s) > destLen {
				return nil, function.FormatCastErrorForInsertValue(proc.Ctx, s, *typ, fmt.Sprintf("Src length %v is larger than Dest length %v", len(s), destLen))
			}
		}
		v := []byte(s)
		if typ.Oid == types.T_binary && len(v) < int(typ.Width) {
			add0 := int(typ.Width) - len(v)
			for ; add0 != 0; add0-- {
				v = append(v, 0)
			}
		}
		return v, nil
	}

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_bool:
		var s string
		if constant.BoolVal(numVal.Value) {
			s = "1"
		} else {
			s = "0"
		}
		var val []byte
		val, err = checkStrLen(s)
		if err != nil {
			return
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())

	case tree.P_int64, tree.P_uint64, tree.P_char, tree.P_decimal, tree.P_float64, tree.P_hexnum:
		s := numVal.OrigString()
		var val []byte
		val, err = checkStrLen(s)
		if err != nil {
			return
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())

	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		canInsert = false
	}
	return
}

func setInsertValueJSON(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true
	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		var json bytejson.ByteJson
		originStr := numVal.OrigString()
		json, err = types.ParseStringToByteJson(originStr)
		if err != nil {
			return false, err
		}
		var val []byte
		val, err = types.EncodeJson(json)
		if err != nil {
			return false, err
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())
	}
	return
}

func checkOverFlow[T1, T2 constraints.Integer | constraints.Float](ctx context.Context, typ *types.Type, val T1, n *nulls.Nulls) error {
	if typ.Scale >= 0 && typ.Width > 0 {
		max_value := math.Pow10(int(typ.Width-typ.Scale)) - 1
		if float64(val) < -max_value || float64(val) > max_value {
			return moerr.NewOutOfRange(ctx, "float", "value '%v'", val)
		}
	} else {
		return function.OverflowForNumericToNumeric[T1, T2](ctx, []T1{val}, n)
	}
	return nil
}

func setInsertValueNumber[T constraints.Integer | constraints.Float](proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true
	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_bool:
		val := constant.BoolVal(numVal.Value)
		if val {
			err = vector.AppendFixed(vec, T(1), false, proc.Mp())
		} else {
			err = vector.AppendFixed(vec, T(0), false, proc.Mp())
		}
		vec.GetType()

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = checkOverFlow[int64, T](proc.Ctx, vec.GetType(), val, vec.GetNulls())
		if err != nil {
			return false, err
		}
		err = vector.AppendFixed(vec, T(val), false, proc.Mp())

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = checkOverFlow[uint64, T](proc.Ctx, vec.GetType(), val, vec.GetNulls())
		if err != nil {
			return false, err
		}
		err = vector.AppendFixed(vec, T(val), false, proc.Mp())

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		var val uint64
		val, err = hexToInt(numVal.OrigString())
		if err != nil {
			return false, err
		}
		err = checkOverFlow[uint64, T](proc.Ctx, vec.GetType(), val, vec.GetNulls())
		if err != nil {
			return false, err
		}
		err = vector.AppendFixed(vec, T(val), false, proc.Mp())

	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_char:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		canInsert = false
	}
	return
}

func setInsertValueDecimal64(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true
	appendWithStr := func(str string) error {
		typ := vec.GetType()
		result, err := types.ParseDecimal64(str, typ.Width, typ.Scale)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}
	appendWithUnSigned := func(v uint64) error {
		typ := vec.GetType()
		result, _ := types.Decimal64(v).Scale(typ.Scale)
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = appendWithUnSigned(uint64(val))

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = appendWithUnSigned(uint64(val))

	case tree.P_decimal, tree.P_char, tree.P_float64:
		originStr := numVal.OrigString()
		err = appendWithStr(originStr)

	case tree.P_hexnum:
		var val uint64
		val, err = hexToInt(numVal.OrigString())
		if err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		canInsert = false
	}
	return
}

func setInsertValueDecimal128(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	canInsert = true
	appendWithStr := func(str string) error {
		typ := vec.GetType()
		result, err := types.ParseDecimal128(str, typ.Width, typ.Scale)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}
	appendWithUnSigned := func(v uint64) error {
		typ := vec.GetType()
		result := types.Decimal128{B0_63: v, B64_127: 0}
		result, _ = result.Scale(typ.Scale)
		return vector.AppendFixed(vec, result, false, proc.Mp())
	}

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_int64:
		val, ok := constant.Int64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
		}
		err = appendWithUnSigned(uint64(val))

	case tree.P_uint64:
		val, ok := constant.Uint64Val(numVal.Value)
		if !ok {
			return false, moerr.NewInvalidInput(proc.Ctx, "invalid uint value '%s'", numVal.Value.String())
		}
		err = appendWithUnSigned(uint64(val))

	case tree.P_decimal, tree.P_char, tree.P_float64:
		originStr := numVal.OrigString()
		err = appendWithStr(originStr)

	case tree.P_hexnum:
		var val uint64
		val, err = hexToInt(numVal.OrigString())
		if err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
	default:
		canInsert = false
	}
	return
}
