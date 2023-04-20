// Copyright 2021 - 2022 Matrix Origin
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

package function2

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_quote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func AbsUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(ivec.GetValue(i)); err != nil {
			return err
		}
	}
	return nil
}

func absSigned[T constraints.Signed | constraints.Float](v T) T {
	if v < 0 {
		v = -v
	}
	if v < 0 {
		panic(moerr.NewOutOfRangeNoCtx("int", "'%v'", v))
	}
	return v
}

func AbsInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(absSigned(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func AbsFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[float64](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(absSigned(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func absDecimal128(v types.Decimal128) types.Decimal128 {
	if v.Sign() {
		v = v.Minus()
	}
	return v
}

func AbsDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Decimal128](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(absDecimal128(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func StringSingle(val []byte) uint8 {
	if len(val) == 0 {
		return 0
	}
	return val[0]
}

func AsciiString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(StringSingle(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	intStartMap = map[types.T]int{
		types.T_int8:  3,
		types.T_int16: 2,
		types.T_int32: 1,
		types.T_int64: 0,
	}
	uintStartMap = map[types.T]int{
		types.T_uint8:  3,
		types.T_uint16: 2,
		types.T_uint32: 1,
		types.T_uint64: 0,
	}
	ints  = []int64{1e16, 1e8, 1e4, 1e2, 1e1}
	uints = []uint64{1e16, 1e8, 1e4, 1e2, 1e1}
)

func IntSingle[T types.Ints](val T, start int) uint8 {
	if val < 0 {
		return '-'
	}
	i64Val := int64(val)
	for _, v := range ints[start:] {
		if i64Val >= v {
			i64Val /= v
		}
	}
	return uint8(i64Val) + '0'
}

func AsciiInt[T types.Ints](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	start := intStartMap[ivecs[0].GetType().Oid]
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(IntSingle(v, start), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func UintSingle[T types.UInts](val T, start int) uint8 {
	u64Val := uint64(val)
	for _, v := range uints[start:] {
		if u64Val >= v {
			u64Val /= v
		}
	}
	return uint8(u64Val) + '0'
}

func AsciiUint[T types.UInts](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	start := intStartMap[ivecs[0].GetType().Oid]
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(UintSingle(v, start), false); err != nil {
				return err
			}
		}
	}
	return nil
}

type binT interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

type binFun[T binT] func(v T, proc *process.Process) (string, error)

func uintToBinary(x uint64) string {
	if x == 0 {
		return "0"
	}
	b, i := [64]byte{}, 63
	for x > 0 {
		if x&1 == 1 {
			b[i] = '1'
		} else {
			b[i] = '0'
		}
		x >>= 1
		i -= 1
	}

	return string(b[i+1:])
}

func binInteger[T constraints.Unsigned | constraints.Signed](v T, proc *process.Process) (string, error) {
	return uintToBinary(uint64(v)), nil
}

func binFloat[T constraints.Float](v T, proc *process.Process) (string, error) {
	if err := binary.NumericToNumericOverflow(proc.Ctx, []T{v}, []int64{}); err != nil {
		return "", err
	}
	return uintToBinary(uint64(int64(v))), nil
}

func Bin[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return generalBin(ivecs, result, proc, length, binInteger[T])
}

func BinFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return generalBin(ivecs, result, proc, length, binFloat[T])
}

func generalBin[T binT](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, cb binFun[T]) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := cb(v, proc)
			if err != nil {
				return err
			}
			if err := rs.AppendBytes([]byte(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func BitLengthFunc(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(len(v)*8), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func CurrentDate(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	var err error
	for i := uint64(0); i < uint64(length); i++ {

		loc := proc.SessionInfo.TimeZone
		if loc == nil {
			logutil.Warn("missing timezone in session info")
			loc = time.Local
		}
		ts := types.UnixNanoToTimestamp(proc.UnixTime)
		dateTimes := make([]types.Datetime, 1)
		dateTimes, err = types.TimestampToDatetime(loc, []types.Timestamp{ts}, dateTimes)
		if err != nil {
			return err
		}
		if err = rs.Append(dateTimes[0].ToDate(), false); err != nil {
			return err
		}

	}
	return nil
}

func DateToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateStringToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDatetime(string(v), 6)
			if e != nil {
				return moerr.NewOutOfRangeNoCtx("date", "'%s'", v)
			}
			if err := rs.Append(d.ToDate(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Day(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Day(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DayOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint16](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.DayOfYear(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Empty(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[bool](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(len(v) == 0, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func JsonQuote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := json_quote.Single(string(v))
			if err != nil {
				return err
			}
			if err := rs.AppendBytes(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func HexString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := hexEncodeString(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func HexInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := hexEncodeInt64(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func hexEncodeString(xs []byte) string {
	return hex.EncodeToString(xs)
}

func hexEncodeInt64(xs int64) string {
	return fmt.Sprintf("%X", xs)
}

func Length(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := strLength(function2Util.QuickBytesToStr(v))
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strLength(xs string) int64 {
	return int64(len(xs))
}

func LengthUTF8(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			res := strLengthUTF8(v)
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func strLengthUTF8(xs []byte) uint64 {
	return lengthutf8.CountUTF8CodePoints(xs)
}

func Ltrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/8cc47db01615a6b6504822d2e63f7085c41f3a47/pkg/sql/plan/function/builtin/unary/ltrim.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := ltrim(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func ltrim(xs string) string {
	return strings.TrimLeft(xs, " ")
}

func Rtrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/3751d33a42435b21bc0416fe47df9d6f4b1060bc/pkg/sql/plan/function/builtin/unary/rtrim.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := rtrim(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func rtrim(xs string) string {
	return strings.TrimRight(xs, " ")
}

func Reverse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])

	//TODO: Need help in handling T_blob case. Original Code: https://github.com/m-schen/matrixone/blob/a8360197d569920c66b295d957fb1213b0dd1828/pkg/sql/plan/function/builtin/unary/reverse.go#L28
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := reverse(function2Util.QuickBytesToStr(v))
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func reverse(str string) string {
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func Oct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			var nilVal types.Decimal128
			if err := rs.Append(nilVal, true); err != nil {
				return err
			}
		} else {
			res, err := oct(v)
			if err != nil {
				return err
			}
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func oct[T constraints.Unsigned | constraints.Signed](val T) (types.Decimal128, error) {
	_val := uint64(val)
	return types.ParseDecimal128(fmt.Sprintf("%o", _val), 38, 0)
}

func OctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			var nilVal types.Decimal128
			if err := rs.Append(nilVal, true); err != nil {
				return err
			}
		} else {
			res, err := octFloat(v)
			if err != nil {
				return err
			}
			if err := rs.Append(res, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func octFloat[T constraints.Float](xs T) (types.Decimal128, error) {
	var res types.Decimal128

	if xs < 0 {
		val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, err
		}
		res, err = oct(uint64(val))
		if err != nil {
			return res, err
		}
	} else {
		val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, err
		}
		res, err = oct(val)
		if err != nil {
			return res, err
		}
	}
	return res, nil

}
