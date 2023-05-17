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
	"context"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/get_timestamp"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_quote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_unquote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vectorize/pi"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"io"
	"strconv"
	"strings"
	"time"
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
		types.T_int8:   3,
		types.T_uint8:  3,
		types.T_int16:  2,
		types.T_uint16: 2,
		types.T_int32:  1,
		types.T_uint32: 1,
		types.T_int64:  0,
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
				return moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
			}
			if err := rs.AppendBytes([]byte(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func BitLengthFunc(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2ForStr2[int64](ivecs, result, proc, length, func(v string) int64 {
		return int64(len(v) * 8)
	})
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

func DateToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, types.Date](ivecs, result, proc, length, func(v types.Date) types.Date {
		return v
	})
}

func DatetimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, types.Date](ivecs, result, proc, length, func(v types.Datetime) types.Date {
		return v.ToDate()
	})
}

func TimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Time, types.Date](ivecs, result, proc, length, func(v types.Time) types.Date {
		return v.ToDate()
	})
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
			d, e := types.ParseDatetime(function2Util.QuickBytesToStr(v), 6)
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

func DateToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Day()
	})
}

func DatetimeToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Day()
	})
}

func DayOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, uint16](ivecs, result, proc, length, func(v types.Date) uint16 {
		return v.DayOfYear()
	})
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

func JsonUnquote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])

	fSingle := json_unquote.JsonSingle
	if ivecs[0].GetType().Oid.IsMySQLString() {
		fSingle = json_unquote.StringSingle
	}

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := fSingle(v)
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

const (
	blobsize = 65536 // 2^16-1
)

func ReadFromFile(Filepath string, fs fileservice.FileService) (io.ReadCloser, error) {
	fs, readPath, err := fileservice.GetForETL(fs, Filepath)
	if fs == nil || err != nil {
		return nil, err
	}
	var r io.ReadCloser
	ctx := context.TODO()
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func LoadFile(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	Filepath, null := ivec.GetStrValue(0)
	if null {
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}
	fs := proc.FileService
	r, err := ReadFromFile(string(Filepath), fs)
	if err != nil {
		return err
	}
	defer r.Close()
	ctx, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(ctx) > blobsize {
		return moerr.NewInternalError(proc.Ctx, "Data too long for blob")
	}
	if len(ctx) == 0 {
		if err = rs.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}

	if err = rs.AppendBytes(ctx, false); err != nil {
		return err
	}
	return nil
}

func MoMemUsage(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	v, null := ivec.GetStrValue(0)
	if null {
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	} else {
		memUsage := mpool.ReportMemUsage(string(v))
		if err := rs.AppendBytes([]byte(memUsage), false); err != nil {
			return err
		}
	}
	return nil
}

func moMemUsageCmd(cmd string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	v, null := ivec.GetStrValue(0)
	if null {
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	} else {
		ok := mpool.MPoolControl(string(v), cmd)
		if err := rs.AppendBytes([]byte(ok), false); err != nil {
			return err
		}
	}
	return nil
}

func MoEnableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moMemUsageCmd("enable_detail", ivecs, result, proc, length)
}

func MoDisableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return moMemUsageCmd("disable_detail", ivecs, result, proc, length)
}

const (
	MaxAllowedValue = 8000
)

func FillSpaceNumber[T types.BuiltinNumber](v T) (string, error) {
	var ilen int
	if v < 0 {
		ilen = 0
	} else {
		ilen = int(v)
		if ilen > MaxAllowedValue || ilen < 0 {
			return "", moerr.NewInvalidInputNoCtx("the space count is greater than max allowed value %d", MaxAllowedValue)
		}
	}
	return strings.Repeat(" ", ilen), nil
}

func SpaceNumber[T types.BuiltinNumber](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			val, err := FillSpaceNumber(v)
			if err != nil {
				return err
			}
			if err = rs.AppendBytes([]byte(val), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Time](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(ivec.GetValue(i)); err != nil {
			return err
		}
	}
	return nil
}

func DateToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToTime(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	scale := ivecs[0].GetType().Scale
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToTime(scale), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Int64ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			t, e := types.ParseInt64ToTime(v, 0)
			if e != nil {
				return moerr.NewOutOfRangeNoCtx("time", "'%d'", v)
			}
			if err := rs.Append(t, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateStringToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			t, e := types.ParseTime(string(v), 6)
			if e != nil {
				return moerr.NewOutOfRangeNoCtx("time", "'%s'", string(v))
			}
			if err := rs.Append(t, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Decimal128ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Time](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](ivecs[0])
	scale := ivecs[0].GetType().Scale
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			t, e := types.ParseDecimal128ToTime(v, scale, 6)
			if e != nil {
				return moerr.NewOutOfRangeNoCtx("time", "'%s'", v.Format(0))
			}
			if err := rs.Append(t, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DateToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, types.Timestamp](ivecs, result, proc, length, func(v types.Date) types.Timestamp {
		return v.ToTimestamp(proc.SessionInfo.TimeZone)
	})
}

func DatetimeToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, types.Timestamp](ivecs, result, proc, length, func(v types.Datetime) types.Timestamp {
		return v.ToTimestamp(proc.SessionInfo.TimeZone)
	})
}

func TimestampToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(ivec.GetValue(i)); err != nil {
			return err
		}
	}
	return nil
}

func DateStringToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val, err := types.ParseTimestamp(proc.SessionInfo.TimeZone, string(v), 6)
			if err != nil {
				return err
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Values(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	fromVec := parameters[0]
	toVec := result.GetResultVector()
	toVec.Reset(*toVec.GetType())

	sels := make([]int32, fromVec.Length())
	for j := 0; j < len(sels); j++ {
		sels[j] = int32(j)
	}

	err := toVec.Union(fromVec, sels, proc.GetMPool())
	return err
}

func TimestampToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Hour())
	})
}

func DatetimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Hour())
	})
}

func TimestampToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Minute())
	})
}

func DatetimeToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Minute())
	})
}

func TimestampToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Sec())
	})
}

func DatetimeToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Sec())
	})
}

func doBinary(orig []byte) []byte {
	if len(orig) > types.MaxBinaryLen {
		return orig[:types.MaxBinaryLen]
	} else {
		return orig
	}
}

func Binary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if err := rs.AppendBytes(doBinary(v), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func Charset(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		r := proc.SessionInfo.GetCharset()
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(r), false); err != nil {
			return err
		}
	}
	return nil
}

func Collation(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		r := proc.SessionInfo.GetCollation()
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(r), false); err != nil {
			return err
		}
	}
	return nil
}

func ConnectionID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		r := proc.SessionInfo.ConnectionID
		if err := rs.Append(r, false); err != nil {
			return err
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

func Length(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2ForStr2[int64](ivecs, result, proc, length, strLength)
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

func OctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
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
				return moerr.NewInternalError(proc.Ctx, "the input value is out of integer range")
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

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Month()
	})
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Month()
	})
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
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				if err := rs.Append(d.Month(), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.Year())
	})
}

func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.Year())
	})
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

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.WeekOfYear2()
	})
}

func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.ToDate().WeekOfYear2()
	})
}

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.DayOfWeek2())
	})
}

func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return optimizedTypeTemplate2[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.ToDate().DayOfWeek2())
	})
}

func FoundRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(0, false); err != nil {
			return err
		}
	}
	return nil
}

func ICULIBVersion(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(""), false); err != nil {
			return err
		}
	}
	return nil
}

func LastInsertID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
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
		var idx int
		idx, err = makeQueryIdIdx(-1, cnt, proc)
		if err != nil {
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
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(""), false); err != nil {
			return err
		}
	}
	return nil
}

func RowCount(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(0, false); err != nil {
			return err
		}
	}
	return nil
}

func User(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(proc.SessionInfo.GetUserHost()), false); err != nil {
			return err
		}
	}
	return nil
}

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
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
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
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "TriggerFaultPoint", "not scalar")
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

func sleepSeconds(proc *process.Process, sec float64) (uint8, error) {
	if sec < 0 {
		return 0, moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains negative")
	}

	sleepNano := time.Nanosecond * time.Duration(sec*1e9)
	select {
	case <-time.After(sleepNano):
		return 0, nil
	case <-proc.Ctx.Done(): //query aborted
		return 1, nil
	}
}

func Sleep[T uint64 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[T](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			return moerr.NewInvalidArg(proc.Ctx, "sleep", "input contains null")
		} else {
			res, err := sleepSeconds(proc, float64(v))
			if err == nil {
				err = rs.Append(res, false)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func Version(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	var err error
	versionStr := proc.SessionInfo.GetVersion()

	for i := uint64(0); i < uint64(length); i++ {
		if err = rs.AppendBytes([]byte(versionStr), false); err != nil {
			return err
		}
	}
	return nil
}

func GitVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	var err error

	s := "unknown"
	if version.CommitID != "" {
		s = version.CommitID
	}

	for i := uint64(0); i < uint64(length); i++ {
		if err = rs.AppendBytes([]byte(s), false); err != nil {
			return err
		}
	}
	return nil
}

func BuildVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	var err error

	t, err := strconv.ParseInt(version.BuildTime, 10, 64)
	if err != nil {
		return err
	}
	buildT := types.UnixToTimestamp(t)

	for i := uint64(0); i < uint64(length); i++ {
		if err = rs.Append(buildT, false); err != nil {
			return err
		}
	}
	return nil
}
