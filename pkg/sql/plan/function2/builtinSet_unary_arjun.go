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

package function2

import (
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/get_timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"strconv"
	"strings"
)

// Hex

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

// Length

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

// LengthUTF8

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

// Ltrim

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

// Rtrim

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

// Reverse

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

// Oct

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

// Month

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DateStringToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(function2Util.QuickBytesToStr(v))
			if e != nil {
				return e
			}
			if err := rs.Append(d.Month(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Year

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DateStringToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(function2Util.QuickBytesToStr(v))
			if e != nil {
				return e
			}
			if err := rs.Append(int64(d.Year()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Week

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.WeekOfYear2(), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {

	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToDate().WeekOfYear2(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Weekday

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.DayOfWeek2()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v.ToDate().DayOfWeek2()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// Info functions

func FoundRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L170
		if err := rs.Append(0, false); err != nil {
			return err
		}
	}
	return nil
}

func ICULIBVersion(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L192
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(""), false); err != nil {
			return err
		}
	}
	return nil
}

func LastInsertID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L206
		if err := rs.Append(proc.SessionInfo.LastInsertID, false); err != nil {
			return err
		}
	}
	return nil
}

func LastQueryIDWithoutParam(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.SessionInfo.QueryId))
		if cnt == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		// LAST_QUERY_ID(-1) returns the most recently-executed query (equivalent to LAST_QUERY_ID()).
		var idx int
		idx, err = makeQueryIdIdx(-1, cnt, proc)
		if err != nil {
			//TODO: Validate: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L223
			return err
		}

		if err = rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.QueryId[idx]), false); err != nil {
			return err
		}
	}
	return nil
}

func makeQueryIdIdx(loc, cnt int64, proc *process.Process) (int, error) {
	// https://docs.snowflake.com/en/sql-reference/functions/last_query_id.html
	var idx int
	if loc < 0 {
		if loc < -cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc + cnt)
	} else {
		if loc > cnt {
			return 0, moerr.NewInvalidInput(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc)
	}
	return idx, nil
}

func LastQueryID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	//TODO: Not at all sure about this. Should we do null check
	// Validate: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L245
	loc, _ := ivec.GetValue(0)
	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.SessionInfo.QueryId))
		if cnt == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		var idx int
		idx, err = makeQueryIdIdx(loc, cnt, proc)
		if err != nil {
			return err
		}

		if err = rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.QueryId[idx]), false); err != nil {
			return err
		}
	}
	return nil
}

func RolesGraphml(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L281
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(""), false); err != nil {
			return err
		}
	}
	return nil
}

func RowCount(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code:https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L295
		if err := rs.Append(0, false); err != nil {
			return err
		}
	}
	return nil
}

func User(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		//TODO: Validate: Code: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L94
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.GetUserHost()), false); err != nil {
			return err
		}
	}
	return nil
}

// ********* Multi from here on *******

// PI

func Pi(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(pi.GetPi(), false); err != nil {
			return err
		}
	}
	return nil
}

// DISABLE_FAULT_INJECTION

func DisableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	//TODO: Validate. Should this be called inside the for loop
	fault.Disable()
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(true, false); err != nil {
			return err
		}
	}
	return nil
}

// ENABLE_FAULT_INJECTION

func EnableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	//TODO: Validate. Should this be called inside the for loop
	fault.Enable()
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(true, false); err != nil {
			return err
		}
	}
	return nil
}

func RemoveFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if ivecs[0].IsConst() || !ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[bool](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.Append(false, true); err != nil {
				return err
			}
		} else {

			//TODO: Need validation. Original code: https://github.com/m-schen/matrixone/blob/9a29d4656c2c6be66885270a2a50664d3ba2a203/pkg/sql/plan/function/builtin/multi/faultinj.go#L61
			if err = fault.RemoveFaultPoint(proc.Ctx, function2Util.QuickBytesToStr(v)); err != nil {
				return err
			} else {
				if err = rs.Append(true, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func TriggerFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) (err error) {
	if ivecs[0].IsConst() || !ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {

			iv, _, ok := fault.TriggerFault(function2Util.QuickBytesToStr(v))
			if !ok {

				//TODO: Need validation. Original code: https://github.com/m-schen/matrixone/blob/9a29d4656c2c6be66885270a2a50664d3ba2a203/pkg/sql/plan/function/builtin/multi/faultinj.go#L73
				// Original code: 		return vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp()), nil
				if err = rs.Append(0, true); err != nil {
					return err
				}
			} else {
				if err = rs.Append(iv, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func UTCTimestamp(_ []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Datetime](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(get_timestamp.GetUTCTimestamp(), false); err != nil {
			return err
		}
	}
	return nil
}
