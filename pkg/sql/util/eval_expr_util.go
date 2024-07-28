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
	"encoding/hex"
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
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}

func HexToInt(hex string) (uint64, error) {
	s := hex[2:]
	return strconv.ParseUint(s, 16, 64)
}

func BinaryToInt(b string) (uint64, error) {
	s := b[2:]
	return strconv.ParseUint(s, 2, 64)
}

func ScoreBinaryToInt(s string) (uint64, error) {
	if len(s) > 8 {
		return 0, moerr.NewInvalidInputNoCtx("s is too long, len(s) =  %v", len(s))
	}

	v := uint64(0)
	for i := 0; i < len(s); i++ {
		v = v<<8 | uint64(s[i])
	}
	return v, nil
}

func DecodeBinaryString(s string) ([]byte, error) {
	b := make([]byte, (len(s)+7)/8)
	padding := strings.Repeat("0", len(b)*8-len(s))
	s = padding + s
	for i, j := 0, 0; i < len(s); i, j = i+8, j+1 {
		val, err := strconv.ParseUint(s[i:i+8], 2, 8)
		if err != nil {
			return b, err
		}
		b[j] = byte(val)
	}
	return b, nil
}

func GenVectorByVarValue(proc *process.Process, typ types.Type, val any) (*vector.Vector, error) {
	if val == nil {
		vec := vector.NewConstNull(typ, 1, proc.Mp())
		return vec, nil
	} else {
		strVal := getVal(val)
		return vector.NewConstBytes(typ, []byte(strVal), 1, proc.Mp())
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
	case types.T_bit:
		width := int(vec.GetType().Width)
		v, err := strconv.ParseUint(val, 0, width)
		if err != nil {
			return moerr.NewOutOfRange(ctx, fmt.Sprintf("bit(%d)", width), "value '%v'", val)
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
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_text, types.T_datalink:
		return vector.SetBytesAt(vec, row, []byte(val), proc.Mp())
	case types.T_array_float32:
		v, err := types.StringToArrayToBytes[float32](val)
		if err != nil {
			return err
		}
		return vector.SetBytesAt(vec, row, v, proc.Mp())
	case types.T_array_float64:
		v, err := types.StringToArrayToBytes[float64](val)
		if err != nil {
			return err
		}
		return vector.SetBytesAt(vec, row, v, proc.Mp())
	case types.T_json:
		val, err := function.ConvertJsonBytes([]byte(val))
		if err != nil {
			return err
		}
		return vector.SetBytesAt(vec, row, val, proc.Mp())
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
	case types.T_date:
		v, err := types.ParseDateCast(val)
		if err != nil {
			return err
		}
		return vector.SetFixedAt(vec, row, v)
	case types.T_enum:
		v, err := strconv.ParseUint(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRange(ctx, "enum", "value '%v'", val)
		}
		return vector.SetFixedAt(vec, row, types.Enum(v))
	default:
		panic(fmt.Sprintf("unsupported type %v", vec.GetType().Oid))
	}
}

func SetInsertValue(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (bool, error) {
	switch vec.GetType().Oid {
	case types.T_bool:
		return setInsertValueBool(proc, numVal, vec)
	case types.T_bit:
		return setInsertValueBit(proc, numVal, vec)
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
	case types.T_char, types.T_varchar, types.T_blob, types.T_binary, types.T_varbinary, types.T_text, types.T_datalink,
		types.T_array_float32, types.T_array_float64:
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
	case types.T_enum:
		return false, nil
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
		if val, err = HexToInt(numVal.OrigString()); err != nil {
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
				zone = proc.GetSessionInfo().TimeZone
			}
			val, err = types.ParseTimestamp(zone, s, typ.Scale)
			if err != nil {
				return
			}
			err = vector.AppendFixed(vec, val, false, proc.Mp())
		}

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendIntegerTimeStamp(int64(val))

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
		err = vector.AppendFixed[types.Datetime](vec, 0, true, proc.GetMPool())

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
		if val, err = HexToInt(numVal.OrigString()); err != nil {
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

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendIntegerTime(int64(val))

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
		err = vector.AppendFixed[types.Date](vec, 0, true, proc.GetMPool())

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
		err = vector.AppendFixed[types.Uuid](vec, types.Uuid{}, true, proc.GetMPool())

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
		if typ.Oid != types.T_text && typ.Oid != types.T_datalink && typ.Oid != types.T_binary && destLen != 0 && !typ.Oid.IsArrayRelate() {
			if utf8.RuneCountInString(s) > destLen {
				return nil, function.FormatCastErrorForInsertValue(proc.Ctx, s, *typ, fmt.Sprintf("Src length %v is larger than Dest length %v", len(s), destLen))
			}
		}
		if typ.Oid.IsDatalink() {
			_, _, _, err2 := types.ParseDatalink(s)
			if err2 != nil {
				return nil, err2
			}
		}

		var v []byte
		if typ.Oid.IsArrayRelate() {
			// Assuming that input s is of type "[1,2,3]"

			switch typ.Oid {
			case types.T_array_float32:
				_v, err := types.StringToArray[float32](s)
				if err != nil {
					return nil, err
				}

				if len(_v) != destLen {
					return nil, moerr.NewArrayDefMismatchNoCtx(int(typ.Width), len(_v))
				}

				v = types.ArrayToBytes[float32](_v)

			case types.T_array_float64:
				_v, err := types.StringToArray[float64](s)
				if err != nil {
					return nil, err
				}

				if len(_v) != destLen {
					return nil, moerr.NewArrayDefMismatchNoCtx(int(typ.Width), len(_v))
				}

				v = types.ArrayToBytes[float64](_v)
			default:
				return nil, moerr.NewInternalErrorNoCtx("%s is not supported array type", typ.String())

			}

		} else {
			v = []byte(s)
		}

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

	case tree.P_int64, tree.P_uint64, tree.P_char, tree.P_decimal, tree.P_float64:
		s := numVal.OrigString()
		var val []byte
		val, err = checkStrLen(s)
		if err != nil {
			return
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())

	case tree.P_hexnum:
		s := numVal.OrigString()[2:]
		var val []byte
		if val, err = hex.DecodeString(s); err != nil {
			return
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())

	case tree.P_bit:
		s := numVal.OrigString()[2:]
		var val []byte
		if val, err = DecodeBinaryString(s); err != nil {
			return
		}
		err = vector.AppendBytes(vec, val, false, proc.Mp())

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
		var max_value float64
		if typ.Oid == types.T_float32 || typ.Oid == types.T_float64 {
			pow := math.Pow10(int(typ.Scale))
			max_value = math.Pow10(int(typ.Width - typ.Scale))
			max_value -= 1.0 / pow
		} else {
			max_value = math.Pow10(int(typ.Width-typ.Scale)) - 1
		}
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

	case tree.P_float64:
		val, ok := constant.Float64Val(numVal.Value)
		if canInsert = ok; canInsert {
			var v T
			if err = checkOverFlow[float64, T](proc.Ctx, vec.GetType(), val,
				vec.GetNulls()); err != nil {
				return false, err
			}
			if vec.GetType().Scale < 0 || vec.GetType().Width == 0 {
				v = T(val)
			} else {
				v, err = floatNumToFixFloat[T](val, numVal.OrigString(), vec.GetType())
				if err != nil {
					return false, err
				}
			}
			if err = vector.AppendFixed(vec, v, false, proc.Mp()); err != nil {
				return false, err
			}
		}

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		if err = checkOverFlow[uint64, T](proc.Ctx, vec.GetType(), val, vec.GetNulls()); err != nil {
			return false, err
		}
		err = vector.AppendFixed(vec, T(val), false, proc.Mp())

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		if err = checkOverFlow[uint64, T](proc.Ctx, vec.GetType(), val, vec.GetNulls()); err != nil {
			return false, err
		}
		err = vector.AppendFixed(vec, T(val), false, proc.Mp())

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
		if val, err = HexToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

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
		if val, err = HexToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return false, err
		}
		err = appendWithUnSigned(val)

	case tree.P_nulltext:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	default:
		canInsert = false
	}
	return
}

func floatNumToFixFloat[T constraints.Float | constraints.Integer](
	from float64, originStr string, typ *types.Type) (T, error) {

	pow := math.Pow10(int(typ.Scale))
	max_value := math.Pow10(int(typ.Width - typ.Scale))
	max_value -= 1.0 / pow

	tmp := math.Round((from-math.Floor(from))*pow) / pow
	v := math.Floor(from) + tmp
	if v < -max_value || v > max_value {
		if originStr == "" {
			return 0, moerr.NewOutOfRange(context.TODO(), "float", "value '%v'", from)
		} else {
			return 0, moerr.NewOutOfRange(context.TODO(), "float", "value '%s'", originStr)
		}
	}
	return T(v), nil
}

func setInsertValueBit(proc *process.Process, numVal *tree.NumVal, vec *vector.Vector) (canInsert bool, err error) {
	var ok bool
	canInsert = true
	width := vec.GetType().Width

	switch numVal.ValType {
	case tree.P_null:
		err = vector.AppendBytes(vec, nil, true, proc.Mp())

	case tree.P_bool:
		var val uint64
		if constant.BoolVal(numVal.Value) {
			val = 1
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_char:
		s := numVal.OrigString()
		if len(s) > 8 {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long")
			return
		}

		var val uint64
		for i := 0; i < len(s); i++ {
			val = (val << 8) | uint64(s[i])
		}
		if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_float64:
		var val float64
		if val, ok = constant.Float64Val(numVal.Value); !ok {
			err = moerr.NewInvalidInput(proc.Ctx, "invalid float value '%s'", numVal.Value.String())
			return
		} else if val < 0 {
			err = moerr.NewInvalidInput(proc.Ctx, "unsupported negative value %v", val)
			return
		} else if uint64(math.Round(val)) > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, uint64(math.Round(val)), false, proc.Mp())

	case tree.P_int64:
		var val int64
		if val, ok = constant.Int64Val(numVal.Value); !ok {
			err = moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
			return
		} else if val < 0 {
			err = moerr.NewInvalidInput(proc.Ctx, "unsupported negative value %d", val)
			return
		} else if uint64(val) > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, uint64(val), false, proc.Mp())

	case tree.P_uint64:
		var val uint64
		if val, ok = constant.Uint64Val(numVal.Value); !ok {
			err = moerr.NewInvalidInput(proc.Ctx, "invalid int value '%s'", numVal.Value.String())
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.OrigString()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.OrigString()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	case tree.P_ScoreBinary:
		var val uint64
		if val, err = ScoreBinaryToInt(numVal.OrigString()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		err = vector.AppendFixed(vec, val, false, proc.Mp())

	default:
		canInsert = false
	}
	return
}
