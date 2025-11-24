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
	"github.com/matrixorigin/matrixone/pkg/datalink"
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
		return 0, moerr.NewInvalidInputNoCtxf("s is too long, len(s) =  %v", len(s))
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
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_bit:
		width := int(vec.GetType().Width)
		v, err := strconv.ParseUint(val, 0, width)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, fmt.Sprintf("bit(%d)", width), "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_int8:
		v, err := strconv.ParseInt(val, 0, 8)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "int8", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, int8(v))
	case types.T_int16:
		v, err := strconv.ParseInt(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "int16", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, int16(v))
	case types.T_int32:
		v, err := strconv.ParseInt(val, 0, 32)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "int32", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, int32(v))
	case types.T_int64:
		v, err := strconv.ParseInt(val, 0, 64)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "int64", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, int64(v))
	case types.T_uint8:
		v, err := strconv.ParseUint(val, 0, 8)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "uint8", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, uint8(v))
	case types.T_uint16:
		v, err := strconv.ParseUint(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "uint16", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, uint16(v))
	case types.T_uint32:
		v, err := strconv.ParseUint(val, 0, 32)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "uint32", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, uint32(v))
	case types.T_uint64:
		v, err := strconv.ParseUint(val, 0, 64)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "uint64", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, uint64(v))
	case types.T_float32:
		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "float32", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, float32(v))
	case types.T_float64:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "float64", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, float64(v))
	case types.T_decimal64:
		v, err := types.ParseDecimal64(val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_decimal128:
		v, err := types.ParseDecimal128(val, vec.GetType().Width, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
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
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_datetime:
		v, err := types.ParseDatetime(val, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_timestamp:
		v, err := types.ParseTimestamp(time.Local, val, vec.GetType().Scale)
		if err != nil {
			return err
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_date:
		v, err := types.ParseDateCast(val)
		if err != nil {
			return err
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, v)
	case types.T_enum:
		v, err := strconv.ParseUint(val, 0, 16)
		if err != nil {
			return moerr.NewOutOfRangef(ctx, "enum", "value '%v'", val)
		}
		return vector.SetFixedAtNoTypeCheck(vec, row, types.Enum(v))
	default:
		panic(fmt.Sprintf("unsupported type %v", vec.GetType().Oid))
	}
}

func SetInsertValueTimeStamp(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, isnull bool, res types.Timestamp, err error) {
	canInsert = true
	isnull = false

	appendIntegerTimeStamp := func(val int64) (types.Timestamp, error) {
		if val < 0 || uint64(val) > 32536771199 {
			return 0, nil
		}
		result := types.UnixToTimestamp(val)
		return result, nil
	}

	switch numVal.ValType {
	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		if res, err = appendIntegerTimeStamp(val); err != nil {
			return false, false, res, err
		}
		return

	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		if res, err = appendIntegerTimeStamp(int64(val)); err != nil {
			return false, false, res, err
		}
		return

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.String()); err != nil {
			return false, false, res, err
		}
		if res, err = appendIntegerTimeStamp(int64(val)); err != nil {
			return false, false, res, err
		}
		return

	case tree.P_char:
		s := numVal.String()
		if len(s) == 0 {
			return true, true, res, err
		} else {
			zone := time.Local
			if proc != nil {
				zone = proc.GetSessionInfo().TimeZone
			}
			res, err = types.ParseTimestamp(zone, s, typ.Scale)
			if err != nil {
				return false, false, res, err
			}
			// Validate TIMESTAMP minimum value: '1970-01-01 00:00:01.000000' (MySQL behavior)
			// Note: We don't enforce maximum value limit to allow values beyond MySQL's 2038 limit
			if res < types.TimestampMinValue {
				// MySQL error format: "Incorrect datetime value: 'value' for column 'column' at row 1"
				// Use row 1 as default since we don't have row number in this context
				return false, false, res, moerr.NewTruncatedValueForField(proc.Ctx, "datetime", s, "ts_min", 1)
			}
			return true, false, res, err
		}

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return false, false, res, err
		}
		if res, err = appendIntegerTimeStamp(int64(val)); err != nil {
			return false, false, res, err
		}
		return

	default:
		canInsert = false
	}
	return
}

func SetInsertValueDateTime(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, isnull bool, res types.Datetime, err error) {
	canInsert = true
	switch numVal.ValType {
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
		s := numVal.String()
		if len(s) == 0 {
			isnull = true
		} else {
			res, err = types.ParseDatetime(s, typ.Scale)
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	default:
		canInsert = false
	}
	return
}

func SetInsertValueTime(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, isnull bool, res types.Time, err error) {
	canInsert = true
	isnull = false

	appendIntegerTime := func(val int64) (types.Time, error) {
		var res types.Time
		if val < types.MinInputIntTime || val > types.MaxInputIntTime {
			return res, moerr.NewOutOfRangef(proc.Ctx, "time", "value %d", val)
		}
		return types.ParseInt64ToTime(val, typ.Scale)
	}

	switch numVal.ValType {
	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, isnull, res, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		res, err = appendIntegerTime(val)
		return

	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		res, err = appendIntegerTime(int64(val))
		return

	case tree.P_decimal:
		canInsert = false

	case tree.P_float64:
		canInsert = false

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.String()); err != nil {
			return false, false, res, err
		}
		res, err = appendIntegerTime(int64(val))
		return

	case tree.P_char:
		s := numVal.String()
		if len(s) == 0 {
			isnull = true
		} else {
			res, err = types.ParseTime(s, typ.Scale)
			if err != nil {
				return false, false, res, err
			}
		}
		return

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return false, false, res, err
		}
		res, err = appendIntegerTime(int64(val))
		return

	default:
		canInsert = false
	}
	return
}

func SetInsertValueDate(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, isnull bool, res types.Date, err error) {
	canInsert = true
	isnull = false

	switch numVal.ValType {
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
		s := numVal.String()
		if len(s) == 0 {
			isnull = true
		} else {
			res, err = types.ParseDateCast(s)
			if err != nil {
				canInsert = false
			}
		}

	case tree.P_bool:
		canInsert = false
	case tree.P_ScoreBinary:
		canInsert = false
	case tree.P_bit:
		canInsert = false
	default:
		canInsert = false
	}
	return
}

func SetInsertValueBool(proc *process.Process, numVal *tree.NumVal) (canInsert bool, num bool, err error) {
	canInsert = true
	switch numVal.ValType {
	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, false, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		num = val == 1

	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, false, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		num = val == 1

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
		originStr := numVal.String()
		bval, err := types.ParseBool(originStr)
		if err != nil {
			return false, false, err
		}
		num = bval
	default:
		canInsert = false
	}
	return
}

func SetInsertValueString(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, val []byte, err error) {
	canInsert = true

	checkStrLen := func(s string) ([]byte, error) {
		destLen := int(typ.Width)
		if typ.Oid != types.T_text && typ.Oid != types.T_datalink && typ.Oid != types.T_binary && destLen != 0 && !typ.Oid.IsArrayRelate() {
			if utf8.RuneCountInString(s) > destLen {
				return nil, function.FormatCastErrorForInsertValue(proc.Ctx, s, *typ, fmt.Sprintf("Src length %v is larger than Dest length %v", len(s), destLen))
			}
		}
		if typ.Oid.IsDatalink() {
			_, _, err2 := datalink.ParseDatalink(s, proc)
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
				return nil, moerr.NewInternalErrorNoCtxf("%s is not supported array type", typ.String())

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
	case tree.P_bool:
		var s string
		if numVal.Bool() {
			s = "1"
		} else {
			s = "0"
		}
		if val, err = checkStrLen(s); err != nil {
			canInsert = false
		}
		return

	case tree.P_int64, tree.P_uint64, tree.P_char, tree.P_decimal, tree.P_float64:
		s := numVal.String()
		if val, err = checkStrLen(s); err != nil {
			canInsert = false
		}
		return

	case tree.P_hexnum:
		s := numVal.String()[2:]
		if val, err = hex.DecodeString(s); err != nil {
			canInsert = false
		}
		return

	case tree.P_bit:
		s := numVal.String()[2:]
		if val, err = DecodeBinaryString(s); err != nil {
			canInsert = false
		}
		return

	default:
		canInsert = false
	}
	return
}

func SetInsertValueJSON(proc *process.Process, numVal *tree.NumVal) (canInsert bool, val []byte, err error) {
	canInsert = true
	switch numVal.ValType {
	default:
		var json bytejson.ByteJson
		originStr := numVal.String()
		json, err = types.ParseStringToByteJson(originStr)
		if err != nil {
			return false, nil, err
		}
		val, err = types.EncodeJson(json)
		if err != nil {
			return false, nil, err
		}
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
			return moerr.NewOutOfRangef(ctx, "float", "value '%v'", val)
		}
	} else {
		return function.OverflowForNumericToNumeric[T1, T2](ctx, []T1{val}, n)
	}
	return nil
}

func SetInsertValueNumber[T constraints.Integer | constraints.Float](proc *process.Process, numVal *tree.NumVal, colType *types.Type) (canInsert bool, num T, err error) {
	canInsert = true
	var n nulls.Nulls
	switch numVal.ValType {
	case tree.P_bool:
		val := numVal.Bool()
		if val {
			num = T(1)
		} else {
			num = T(0)
		}

	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, num, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		err = checkOverFlow[int64, T](proc.Ctx, colType, val, &n)
		if err != nil {
			return false, num, err
		}
		num = T(val)

	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, num, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		err = checkOverFlow[uint64, T](proc.Ctx, colType, val, &n)
		if err != nil {
			return false, num, err
		}
		num = T(val)

	case tree.P_float64:
		val, ok := numVal.Float64()
		if canInsert = ok; canInsert {
			if err = checkOverFlow[float64, T](proc.Ctx, colType, val,
				&n); err != nil {
				return false, num, err
			}
			if colType.Scale < 0 || colType.Width == 0 {
				num = T(val)
			} else {
				num, err = floatNumToFixFloat[T](val, numVal.String(), colType)
				if err != nil {
					return false, num, err
				}
			}
		}

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.String()); err != nil {
			return false, num, err
		}
		if err = checkOverFlow[uint64, T](proc.Ctx, colType, val, &n); err != nil {
			return false, num, err
		}
		num = T(val)

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return false, num, err
		}
		if err = checkOverFlow[uint64, T](proc.Ctx, colType, val, &n); err != nil {
			return false, num, err
		}
		num = T(val)

	default:
		canInsert = false
	}
	return
}

func SetInsertValueDecimal64(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, res types.Decimal64, err error) {
	canInsert = true
	appendWithStr := func(str string) (types.Decimal64, error) {
		return types.ParseDecimal64(str, typ.Width, typ.Scale)
	}
	appendWithUnSigned := func(v uint64) (types.Decimal64, error) {
		return types.Decimal64(v).Scale(typ.Scale)
	}

	switch numVal.ValType {
	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		res, err = appendWithUnSigned(uint64(val))
		return

	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		res, err = appendWithUnSigned(uint64(val))
		return

	case tree.P_decimal, tree.P_char, tree.P_float64:
		originStr := numVal.String()
		res, err = appendWithStr(originStr)
		return

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.String()); err != nil {
			return false, res, err
		}
		res, err = appendWithUnSigned(val)
		return

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return false, res, err
		}
		res, err = appendWithUnSigned(val)
		return

	default:
		canInsert = false
	}
	return
}

func SetInsertValueDecimal128(proc *process.Process, numVal *tree.NumVal, typ *types.Type) (canInsert bool, res types.Decimal128, err error) {
	canInsert = true
	appendWithStr := func(str string) (types.Decimal128, error) {
		return types.ParseDecimal128(str, typ.Width, typ.Scale)
	}
	appendWithUnSigned := func(v uint64) (types.Decimal128, error) {
		result := types.Decimal128{B0_63: v, B64_127: 0}
		return result.Scale(typ.Scale)
	}

	switch numVal.ValType {
	case tree.P_int64:
		val, ok := numVal.Int64()
		if !ok {
			return false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
		}
		res, err = appendWithUnSigned(uint64(val))
		return
	case tree.P_uint64:
		val, ok := numVal.Uint64()
		if !ok {
			return false, res, moerr.NewInvalidInputf(proc.Ctx, "invalid uint value '%s'", numVal.String())
		}
		res, err = appendWithUnSigned(uint64(val))
		return

	case tree.P_decimal, tree.P_char, tree.P_float64:
		originStr := numVal.String()
		res, err = appendWithStr(originStr)
		return

	case tree.P_hexnum:
		var val uint64
		if val, err = HexToInt(numVal.String()); err != nil {
			return false, res, err
		}
		res, err = appendWithUnSigned(val)
		return

	case tree.P_bit:
		var val uint64
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return false, res, err
		}
		res, err = appendWithUnSigned(val)
		return

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
			return 0, moerr.NewOutOfRangef(context.TODO(), "float", "value '%v'", from)
		} else {
			return 0, moerr.NewOutOfRangef(context.TODO(), "float", "value '%s'", originStr)
		}
	}
	return T(v), nil
}

func SetInsertValueBit(proc *process.Process, numVal *tree.NumVal, colType *types.Type) (canInsert bool, val uint64, err error) {
	var ok bool
	canInsert = true
	width := colType.Width

	switch numVal.ValType {
	case tree.P_bool:
		if numVal.Bool() {
			val = 1
		}

	case tree.P_char:
		s := numVal.String()
		if len(s) > 8 {
			err = moerr.NewInvalidInput(proc.Ctx, "data too long")
			return
		}

		for i := 0; i < len(s); i++ {
			val = (val << 8) | uint64(s[i])
		}
		if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}

	case tree.P_float64:
		var num float64
		if num, ok = numVal.Float64(); !ok {
			err = moerr.NewInvalidInputf(proc.Ctx, "invalid float value '%s'", numVal.String())
			return
		} else if num < 0 {
			err = moerr.NewInvalidInputf(proc.Ctx, "unsupported negative value %v", val)
			return
		} else if uint64(math.Round(num)) > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}
		val = uint64(math.Round(num))

	case tree.P_int64:
		var tempVal int64
		if tempVal, ok = numVal.Int64(); !ok {
			err = moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
			return
		} else if tempVal < 0 {
			err = moerr.NewInvalidInputf(proc.Ctx, "unsupported negative value %d", tempVal)
			return
		} else if uint64(tempVal) > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, tempVal)
			return
		}
		val = uint64(tempVal)

	case tree.P_uint64:
		if val, ok = numVal.Uint64(); !ok {
			err = moerr.NewInvalidInputf(proc.Ctx, "invalid int value '%s'", numVal.String())
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}

	case tree.P_hexnum:
		if val, err = HexToInt(numVal.String()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}

	case tree.P_bit:
		if val, err = BinaryToInt(numVal.String()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}

	case tree.P_ScoreBinary:
		if val, err = ScoreBinaryToInt(numVal.String()); err != nil {
			return
		} else if val > uint64(1<<width-1) {
			err = moerr.NewInvalidInputf(proc.Ctx, "data too long, type width = %d, val = %b", width, val)
			return
		}

	default:
		canInsert = false
	}
	return
}
