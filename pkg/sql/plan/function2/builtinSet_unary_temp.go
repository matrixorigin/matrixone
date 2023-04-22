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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_quote"
	"github.com/matrixorigin/matrixone/pkg/vectorize/json_unquote"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"io"
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
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(v, true); err != nil {
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
	rs := vector.MustFunctionResult[types.Timestamp](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToTimestamp(proc.SessionInfo.TimeZone), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(v.ToTimestamp(proc.SessionInfo.TimeZone), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimestampToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
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

	sels := make([]int32, fromVec.Length())
	for j := 0; j < len(sels); j++ {
		sels[j] = int32(j)
	}
	toVec.Union(fromVec, sels, proc.GetMPool())
	return nil
}

func TimestampToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Hour()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.Hour()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimestampToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Minute()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.Minute()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func TimestampToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.ToDatetime(proc.SessionInfo.TimeZone).Sec()), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func DatetimeToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[uint8](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(uint8(v.Sec()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
