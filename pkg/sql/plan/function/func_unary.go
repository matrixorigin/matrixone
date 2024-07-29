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

package function

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"io"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func AbsUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[uint64, uint64](ivecs, result, proc, length, func(v uint64) uint64 {
		return v
	}, selectList)
}

func AbsInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[int64, int64](ivecs, result, proc, length, func(v int64) (int64, error) {
		return momath.AbsSigned[int64](v)
	}, selectList)
}

func AbsFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[float64, float64](ivecs, result, proc, length, func(v float64) (float64, error) {
		return momath.AbsSigned[float64](v)
	}, selectList)
}

func absDecimal64(v types.Decimal64) types.Decimal64 {
	if v.Sign() {
		v = v.Minus()
	}
	return v
}

func AbsDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal64, types.Decimal64](ivecs, result, proc, length, func(v types.Decimal64) types.Decimal64 {
		return absDecimal64(v)
	}, selectList)
}

func absDecimal128(v types.Decimal128) types.Decimal128 {
	if v.Sign() {
		v = v.Minus()
	}
	return v
}

func AbsDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal128, types.Decimal128](ivecs, result, proc, length, func(v types.Decimal128) types.Decimal128 {
		return absDecimal128(v)
	}, selectList)
}

func AbsArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(in []byte) ([]byte, error) {
		_in := types.BytesToArray[T](in)
		_out, err := moarray.Abs(_in)
		if err != nil {
			return nil, err
		}
		return types.ArrayToBytes[T](_out), nil
	}, selectList)
}

var (
	arrayF32Pool = sync.Pool{
		New: func() interface{} {
			s := make([]float32, 128)
			return &s
		},
	}

	arrayF64Pool = sync.Pool{
		New: func() interface{} {
			s := make([]float64, 128)
			return &s
		},
	}
)

func NormalizeL2Array[T types.RealNumbers](parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)

	var inArrayF32 []float32
	var outArrayF32Ptr *[]float32
	var outArrayF32 []float32

	var inArrayF64 []float64
	var outArrayF64Ptr *[]float64
	var outArrayF64 []float64

	var data []byte
	var null bool

	for i := uint64(0); i < rowCount; i++ {
		data, null = source.GetStrValue(i)
		if null {
			_ = rs.AppendMustNullForBytesResult()
			continue
		}

		switch t := parameters[0].GetType().Oid; t {
		case types.T_array_float32:
			inArrayF32 = types.BytesToArray[float32](data)

			outArrayF32Ptr = arrayF32Pool.Get().(*[]float32)
			outArrayF32 = *outArrayF32Ptr

			if cap(outArrayF32) < len(inArrayF32) {
				outArrayF32 = make([]float32, len(inArrayF32))
			} else {
				outArrayF32 = outArrayF32[:len(inArrayF32)]
			}
			_ = moarray.NormalizeL2(inArrayF32, outArrayF32)
			_ = rs.AppendBytes(types.ArrayToBytes[float32](outArrayF32), false)

			*outArrayF32Ptr = outArrayF32
			arrayF32Pool.Put(outArrayF32Ptr)
		case types.T_array_float64:
			inArrayF64 = types.BytesToArray[float64](data)

			outArrayF64Ptr = arrayF64Pool.Get().(*[]float64)
			outArrayF64 = *outArrayF64Ptr

			if cap(outArrayF64) < len(inArrayF64) {
				outArrayF64 = make([]float64, len(inArrayF64))
			} else {
				outArrayF64 = outArrayF64[:len(inArrayF64)]
			}
			_ = moarray.NormalizeL2(inArrayF64, outArrayF64)
			_ = rs.AppendBytes(types.ArrayToBytes[float64](outArrayF64), false)

			*outArrayF64Ptr = outArrayF64
			arrayF64Pool.Put(outArrayF64Ptr)
		}

	}

	return nil
}

func L1NormArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(in []byte) (float64, error) {
		_in := types.BytesToArray[T](in)
		return moarray.L1Norm(_in)
	}, selectList)
}

func L2NormArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(in []byte) (out float64, err error) {
		_in := types.BytesToArray[T](in)
		return moarray.L2Norm(_in)
	}, selectList)
}

func VectorDimsArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixed[int64](ivecs, result, proc, length, func(in []byte) (out int64) {
		_in := types.BytesToArray[T](in)
		return int64(len(_in))
	}, selectList)
}

func SummationArray[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(in []byte) (out float64, err error) {
		_in := types.BytesToArray[T](in)

		return moarray.Summation[T](_in)
	}, selectList)
}

func SubVectorWith2Args[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		v, null1 := vs.GetStrValue(i)
		s, null2 := starts.GetValue(i)

		if null1 || null2 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var r []T
			if s > 0 {
				r = moarray.SubArrayFromLeft[T](types.BytesToArray[T](v), s-1)
			} else if s < 0 {
				r = moarray.SubArrayFromRight[T](types.BytesToArray[T](v), -s)
			} else {
				r = []T{}
			}
			if err = rs.AppendBytes(types.ArrayToBytes[T](r), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func SubVectorWith3Args[T types.RealNumbers](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	vs := vector.GenerateFunctionStrParameter(ivecs[0])
	starts := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	lens := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[2])

	for i := uint64(0); i < uint64(length); i++ {
		in, null1 := vs.GetStrValue(i)
		s, null2 := starts.GetValue(i)
		l, null3 := lens.GetValue(i)

		if null1 || null2 || null3 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			var r []T
			if s > 0 {
				r = moarray.SubArrayFromLeftWithLength[T](types.BytesToArray[T](in), s-1, l)
			} else if s < 0 {
				r = moarray.SubArrayFromRightWithLength[T](types.BytesToArray[T](in), -s, l)
			} else {
				r = []T{}
			}
			if err = rs.AppendBytes(types.ArrayToBytes[T](r), false); err != nil {
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

func AsciiString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opUnaryBytesToFixed[uint8](ivecs, result, proc, length, func(v []byte) uint8 {
		return StringSingle(v)
	}, selectList)
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
		types.T_bit:    0,
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

func AsciiInt[T types.Ints](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	start := intStartMap[ivecs[0].GetType().Oid]

	return opUnaryFixedToFixed[T, uint8](ivecs, result, proc, length, func(v T) uint8 {
		return IntSingle[T](v, start)
	}, selectList)
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

func AsciiUint[T types.UInts](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	start := intStartMap[ivecs[0].GetType().Oid]

	return opUnaryFixedToFixed[T, uint8](ivecs, result, proc, length, func(v T) uint8 {
		return UintSingle[T](v, start)
	}, selectList)
}

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
	if err := overflowForNumericToNumeric[T, int64](proc.Ctx, []T{v}, nil); err != nil {
		return "", err
	}
	return uintToBinary(uint64(int64(v))), nil
}

func Bin[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, func(v T) (string, error) {
		val, err := binInteger[T](v, proc)
		if err != nil {
			return "", moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return val, err
	}, selectList)
}

func BinFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, func(v T) (string, error) {
		val, err := binFloat[T](v, proc)
		if err != nil {
			return "", moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return val, err
	}, selectList)
}

func BitLengthFunc(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToFixed[int64](ivecs, result, proc, length, func(v string) int64 {
		return int64(len(v) * 8)
	}, selectList)
}

func CurrentDate(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error

	loc := proc.GetSessionInfo().TimeZone
	if loc == nil {
		logutil.Warn("missing timezone in session info")
		loc = time.Local
	}
	ts := types.UnixNanoToTimestamp(proc.GetUnixTime())
	dateTimes := make([]types.Datetime, 1)
	dateTimes, err = types.TimestampToDatetime(loc, []types.Timestamp{ts}, dateTimes)
	if err != nil {
		return err
	}
	r := dateTimes[0].ToDate()

	return opNoneParamToFixed[types.Date](result, proc, length, func() types.Date {
		return r
	})
}

func DateToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, types.Date](ivecs, result, proc, length, func(v types.Date) types.Date {
		return v
	}, selectList)
}

func DatetimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, types.Date](ivecs, result, proc, length, func(v types.Datetime) types.Date {
		return v.ToDate()
	}, selectList)
}

func TimeToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, types.Date](ivecs, result, proc, length, func(v types.Time) types.Date {
		return v.ToDate()
	}, selectList)
}

// DateStringToDate can still speed up if vec is const. but we will do the constant fold. so it does not matter.
func DateStringToDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Date](ivecs, result, proc, length, func(v []byte) (types.Date, error) {
		d, e := types.ParseDatetime(functionUtil.QuickBytesToStr(v), 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("date", "'%s'", v)
		}
		return d.ToDate(), nil
	}, selectList)
}

func DateToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Day()
	}, selectList)
}

func DatetimeToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Day()
	}, selectList)
}

func DayOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, uint16](ivecs, result, proc, length, func(v types.Date) uint16 {
		return v.DayOfYear()
	}, selectList)
}

func Empty(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixed[bool](ivecs, result, proc, length, func(v []byte) bool {
		return len(v) == 0
	}, selectList)
}

func JsonQuote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	single := func(str string) ([]byte, error) {
		bj, err := types.ParseStringToByteJson(strconv.Quote(str))
		if err != nil {
			return nil, err
		}
		return bj.Marshal()
	}

	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, single, selectList)
}

func JsonUnquote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	jsonSingle := func(v []byte) (string, error) {
		bj := types.DecodeJson(v)
		return bj.Unquote()
	}

	stringSingle := func(v []byte) (string, error) {
		bj, err := types.ParseSliceToByteJson(v)
		if err != nil {
			return "", err
		}
		return bj.Unquote()
	}

	fSingle := jsonSingle
	if ivecs[0].GetType().Oid.IsMySQLString() {
		fSingle = stringSingle
	}

	return opUnaryBytesToStrWithErrorCheck(ivecs, result, proc, length, fSingle, selectList)
}

func ReadFromFile(Filepath string, fs fileservice.FileService) (io.ReadCloser, error) {
	return ReadFromFileOffsetSize(Filepath, fs, 0, -1)
}

func ReadFromFileOffsetSize(Filepath string, fs fileservice.FileService, offset, size int64) (io.ReadCloser, error) {
	fs, readPath, err := fileservice.GetForETL(context.TODO(), fs, Filepath)
	if fs == nil || err != nil {
		return nil, err
	}
	var r io.ReadCloser
	ctx := context.TODO()
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            offset, //0 - default
				Size:              size,   //-1 - default
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

// Too confused.
func LoadFile(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	Filepath, null := ivec.GetStrValue(0)
	if null {
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
		return nil
	}
	fs := proc.GetFileService()
	r, err := ReadFromFile(string(Filepath), fs)
	if err != nil {
		return err
	}
	defer r.Close()
	ctx, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(ctx) > 65536 /*blob size*/ {
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

// LoadFileDatalink reads a file from the file service and returns the content as a blob.
func LoadFileDatalink(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	filePathVec := vector.GenerateFunctionStrParameter(ivecs[0])

	for i := uint64(0); i < uint64(length); i++ {
		_filePath, null1 := filePathVec.GetStrValue(i)
		if null1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		filePath := util.UnsafeBytesToString(_filePath)
		fs := proc.GetFileService()

		moUrl, offsetSize, ext, err := types.ParseDatalink(filePath)
		if err != nil {
			return err
		}
		r, err := ReadFromFileOffsetSize(moUrl, fs, int64(offsetSize[0]), int64(offsetSize[1]))
		if err != nil {
			return err
		}

		defer r.Close()

		fileBytes, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		var contentBytes []byte
		switch ext {
		case ".csv", ".txt":
			contentBytes = fileBytes
			//nothing to do.
		default:
			return moerr.NewInvalidInput(proc.Ctx, "unsupported file extension: %s", ext)
		}

		if len(fileBytes) > 65536 /*blob size*/ {
			return moerr.NewInternalError(proc.Ctx, "Data too long for blob")
		}
		if len(fileBytes) == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		}

		if err = rs.AppendBytes(contentBytes, false); err != nil {
			return err
		}
	}
	return nil
}

func MoMemUsage(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}

	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, func(v string) ([]byte, error) {
		memUsage := mpool.ReportMemUsage(v)
		return functionUtil.QuickStrToBytes(memUsage), nil
	}, selectList)
}

func moMemUsageCmd(cmd string, ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input")
	}

	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, func(v string) ([]byte, error) {
		ok := mpool.MPoolControl(v, cmd)
		return functionUtil.QuickStrToBytes(ok), nil
	}, selectList)
}

func MoEnableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moMemUsageCmd("enable_detail", ivecs, result, proc, length, selectList)
}

func MoDisableMemUsageDetail(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moMemUsageCmd("disable_detail", ivecs, result, proc, length, selectList)
}

func MoMemory(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no memory command name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo memory can only take scalar input")
	}
	return opUnaryStrToFixedWithErrorCheck(ivecs, result, proc, length, func(v string) (int64, error) {
		switch v {
		case "go":
			return int64(system.MemoryGolang()), nil
		case "total":
			return int64(system.MemoryTotal()), nil
		case "used":
			return int64(system.MemoryUsed()), nil
		case "available":
			return int64(system.MemoryAvailable()), nil
		default:
			return -1, moerr.NewInvalidInput(proc.Ctx, "unsupported memory command: %s", v)
		}
	}, selectList)
}

func MoCPU(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no cpu command name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo cpu can only take scalar input")
	}
	return opUnaryStrToFixedWithErrorCheck(ivecs, result, proc, length, func(v string) (int64, error) {
		switch v {
		case "goroutine":
			return int64(system.GoRoutines()), nil
		case "total":
			return int64(system.NumCPU()), nil
		case "available":
			return int64(system.CPUAvailable()), nil
		default:
			return -1, moerr.NewInvalidInput(proc.Ctx, "no cpu command name")
		}
	}, selectList)
}

const (
	DefaultStackSize = 10 << 20 // 10MB
)

func MoCPUDump(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(ivecs) != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "no cpu dump command name")
	}
	if !ivecs[0].IsConst() {
		return moerr.NewInvalidInput(proc.Ctx, "mo cpu dump can only take scalar input")
	}
	return opUnaryStrToBytesWithErrorCheck(ivecs, result, proc, length, func(v string) ([]byte, error) {
		switch v {
		case "goroutine":
			buf := make([]byte, DefaultStackSize)
			n := runtime.Stack(buf, true)
			return buf[:n], nil
		default:
			return nil, moerr.NewInvalidInput(proc.Ctx, "no cpu dump command name")
		}
	}, selectList)
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

func SpaceNumber[T types.BuiltinNumber](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStrWithErrorCheck[T](ivecs, result, proc, length, FillSpaceNumber[T], selectList)
}

func TimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, types.Time](ivecs, result, proc, length, func(v types.Time) types.Time {
		return v
	}, selectList)
}

func DateToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, types.Time](ivecs, result, proc, length, func(v types.Date) types.Time {
		return v.ToTime()
	}, selectList)
}

func DatetimeToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixed[types.Datetime, types.Time](ivecs, result, proc, length, func(v types.Datetime) types.Time {
		return v.ToTime(scale)
	}, selectList)
}

func Int64ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[int64, types.Time](ivecs, result, proc, length, func(v int64) (types.Time, error) {
		t, e := types.ParseInt64ToTime(v, 0)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%d'", v)
		}
		return t, nil
	}, selectList)
}

func DateStringToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Time](ivecs, result, proc, length, func(v []byte) (types.Time, error) {
		t, e := types.ParseTime(string(v), 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%s'", string(v))
		}
		return t, nil
	}, selectList)
}

func Decimal128ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, types.Time](ivecs, result, proc, length, func(v types.Decimal128) (types.Time, error) {
		t, e := types.ParseDecimal128ToTime(v, scale, 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtx("time", "'%s'", v.Format(0))
		}
		return t, nil
	}, selectList)
}

func DateToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, types.Timestamp](ivecs, result, proc, length, func(v types.Date) types.Timestamp {
		return v.ToTimestamp(proc.GetSessionInfo().TimeZone)
	}, selectList)
}

func DatetimeToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, types.Timestamp](ivecs, result, proc, length, func(v types.Datetime) types.Timestamp {
		return v.ToTimestamp(proc.GetSessionInfo().TimeZone)
	}, selectList)
}

func TimestampToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, types.Timestamp](ivecs, result, proc, length, func(v types.Timestamp) types.Timestamp {
		return v
	}, selectList)
}

func DateStringToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToFixedWithErrorCheck[types.Timestamp](ivecs, result, proc, length, func(v string) (types.Timestamp, error) {
		val, err := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, v, 6)
		if err != nil {
			return 0, err
		}
		return val, nil
	}, selectList)
}

func Values(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
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

func TimestampToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.GetSessionInfo().TimeZone).Hour())
	}, selectList)
}

func DatetimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Hour())
	}, selectList)
}

func TimestampToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.GetSessionInfo().TimeZone).Minute())
	}, selectList)
}

func DatetimeToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Minute())
	}, selectList)
}

func TimestampToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		return uint8(v.ToDatetime(proc.GetSessionInfo().TimeZone).Sec())
	}, selectList)
}

func DatetimeToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.Sec())
	}, selectList)
}

func doBinary(orig []byte) []byte {
	if len(orig) > types.MaxBinaryLen {
		return orig[:types.MaxBinaryLen]
	} else {
		return orig
	}
}

func Binary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(ivecs, result, proc, length, doBinary, selectList)
}

func Charset(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	r := proc.GetSessionInfo().GetCharset()
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes(r)
	})
}

func Collation(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	r := proc.GetSessionInfo().GetCollation()
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes(r)
	})
}

func ConnectionID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	r := proc.GetSessionInfo().ConnectionID
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return r
	})
}

// HexString returns a hexadecimal string representation of a string.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func HexString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToStr(ivecs, result, proc, length, hexEncodeString, selectList)
}

func HexInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[int64](ivecs, result, proc, length, hexEncodeInt64, selectList)
}

func HexUint64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[uint64](ivecs, result, proc, length, hexEncodeUint64, selectList)
}

func HexFloat32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[float32](ivecs, result, proc, length, func(v float32) string {
		// round is used to handle select hex(456.789); which should return 1C9 and not 1C8
		return fmt.Sprintf("%X", uint64(math.Round(float64(v))))
	}, selectList)
}

func HexFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[float64](ivecs, result, proc, length, func(v float64) string {
		// round is used to handle select hex(456.789); which should return 1C9 and not 1C8
		return fmt.Sprintf("%X", uint64(math.Round(v)))
	}, selectList)
}

func HexArray(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(data []byte) ([]byte, error) {
		buf := make([]byte, hex.EncodedLen(len(functionUtil.QuickBytesToStr(data))))
		hex.Encode(buf, data)
		return buf, nil
	}, selectList)
}

func hexEncodeString(xs []byte) string {
	return strings.ToUpper(hex.EncodeToString(xs))
}

func hexEncodeInt64(xs int64) string {
	return fmt.Sprintf("%X", uint64(xs))
}

func hexEncodeUint64(xs uint64) string {
	return fmt.Sprintf("%X", xs)
}

// UnhexString returns a string representation of a hexadecimal value.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func unhexToBytes(data []byte, null bool, rs *vector.FunctionResult[types.Varlena]) error {
	if null {
		return rs.AppendMustNullForBytesResult()
	}

	// Add a '0' to the front, if the length is not the multiple of 2
	str := functionUtil.QuickBytesToStr(data)
	if len(str)%2 != 0 {
		str = "0" + str
	}

	bs, err := hex.DecodeString(str)
	if err != nil {
		return rs.AppendMustNullForBytesResult()
	}
	return rs.AppendMustBytesValue(bs)
}

func Unhex(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		data, null := source.GetStrValue(i)
		if err := unhexToBytes(data, null, rs); err != nil {
			return err
		}
	}

	return nil
}

func Md5(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(parameters, result, proc, length, func(data []byte) []byte {
		sum := md5.Sum(data)
		return []byte(hex.EncodeToString(sum[:]))
	}, selectList)

}

func ToBase64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(data []byte) ([]byte, error) {
		buf := make([]byte, base64.StdEncoding.EncodedLen(len(functionUtil.QuickBytesToStr(data))))
		base64.StdEncoding.Encode(buf, data)
		return buf, nil
	}, selectList)
}

func FromBase64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		data, null := source.GetStrValue(i)
		if null {
			return rs.AppendMustNullForBytesResult()
		}

		buf := make([]byte, base64.StdEncoding.DecodedLen(len(functionUtil.QuickBytesToStr(data))))
		_, err := base64.StdEncoding.Decode(buf, data)
		if err != nil {
			return rs.AppendMustNullForBytesResult()
		}
		_ = rs.AppendMustBytesValue(buf)
	}

	return nil
}

func Length(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToFixed[int64](ivecs, result, proc, length, strLength, selectList)
}

func strLength(xs string) int64 {
	return int64(len(xs))
}

func LengthUTF8(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixed[uint64](ivecs, result, proc, length, strLengthUTF8, selectList)
}

func strLengthUTF8(xs []byte) uint64 {
	return lengthutf8.CountUTF8CodePoints(xs)
}

func Ltrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToStr(ivecs, result, proc, length, ltrim, selectList)
}

func ltrim(xs string) string {
	return strings.TrimLeft(xs, " ")
}

func Rtrim(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToStr(ivecs, result, proc, length, rtrim, selectList)
}

func rtrim(xs string) string {
	return strings.TrimRight(xs, " ")
}

func Reverse(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToStr(ivecs, result, proc, length, reverse, selectList)
}

func reverse(str string) string {
	runes := []rune(str)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func Oct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, oct[T], selectList)
}

func oct[T constraints.Unsigned | constraints.Signed](val T) (types.Decimal128, error) {
	_val := uint64(val)
	return types.ParseDecimal128(fmt.Sprintf("%o", _val), 38, 0)
}

func OctFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[T, types.Decimal128](ivecs, result, proc, length, octFloat[T], selectList)
}

func octFloat[T constraints.Float](xs T) (types.Decimal128, error) {
	var res types.Decimal128

	if xs < 0 {
		val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = oct(uint64(val))
		if err != nil {
			return res, err
		}
	} else {
		val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs), 10, 64)
		if err != nil {
			return res, moerr.NewInternalErrorNoCtx("the input value is out of integer range")
		}
		res, err = oct(val)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func DateToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.Month()
	}, selectList)
}

func DatetimeToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.Month()
	}, selectList)
}

// TODO: I will support template soon.
func DateStringToMonth(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	//return opUnaryStrToFixedWithErrorCheck[uint8](ivecs, result, proc, length, func(v string) (uint8, error) {
	//	d, e := types.ParseDateCast(v)
	//	if e != nil {
	//		return 0, e
	//	}
	//	return d.Month(), nil
	//})

	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint8](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			d, e := types.ParseDateCast(functionUtil.QuickBytesToStr(v))
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

func DateToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.Year())
	}, selectList)
}

func DatetimeToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.Year())
	}, selectList)
}

func DateStringToYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryStrToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v string) (int64, error) {
		d, e := types.ParseDateCast(v)
		if e != nil {
			return 0, e
		}
		return int64(d.Year()), nil
	}, selectList)
}

func DateToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return v.WeekOfYear2()
	}, selectList)
}

func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return v.ToDate().WeekOfYear2()
	}, selectList)
}

func DateToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		return int64(v.DayOfWeek2())
	}, selectList)
}

func DatetimeToWeekday(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return int64(v.ToDate().DayOfWeek2())
	}, selectList)
}

func FoundRows(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return 0
	})
}

func ICULIBVersion(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes("")
	})
}

func LastInsertID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return proc.GetSessionInfo().LastInsertID
	})
}

// TODO: may support soon.
func LastQueryIDWithoutParam(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.GetSessionInfo().QueryId))
		if cnt == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		var idx int
		idx, err = makeQueryIdIdx(-1, cnt, proc)
		if err != nil {
			return err
		}

		if err = rs.AppendBytes(functionUtil.QuickStrToBytes(proc.GetSessionInfo().QueryId[idx]), false); err != nil {
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

func LastQueryID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	//TODO: Not at all sure about this. Should we do null check
	// Validate: https://github.com/m-schen/matrixone/blob/9e8ef37e2a6f34873ceeb3c101ec9bb14a82a8a7/pkg/sql/plan/function/builtin/unary/infomation_function.go#L245
	loc, _ := ivec.GetValue(0)
	for i := uint64(0); i < uint64(length); i++ {
		cnt := int64(len(proc.GetSessionInfo().QueryId))
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

		if err = rs.AppendBytes(functionUtil.QuickStrToBytes(proc.GetSessionInfo().QueryId[idx]), false); err != nil {
			return err
		}
	}
	return nil
}

func RolesGraphml(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes("")
	})
}

func RowCount(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToFixed[uint64](result, proc, length, func() uint64 {
		return 0
	})
}

func User(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes(proc.GetSessionInfo().GetUserHost())
	})
}

func Pi(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	r := math.Pi

	return opNoneParamToFixed[float64](result, proc, length, func() float64 {
		return r
	})
}

func DisableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	fault.Disable()

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return true
	})
}

func EnableFaultInjection(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	fault.Enable()

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return true
	})
}

func RemoveFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	return opUnaryStrToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v string) (bool, error) {
		err = fault.RemoveFaultPoint(proc.Ctx, v)
		return true, err
	}, selectList)
}

func TriggerFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
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
			iv, _, ok := fault.TriggerFault(functionUtil.QuickBytesToStr(v))
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

func UTCTimestamp(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opNoneParamToFixed[types.Datetime](result, proc, length, func() types.Datetime {
		return types.UTC()
	})
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

func Sleep[T uint64 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
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

func Version(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	versionStr := proc.GetSessionInfo().GetVersion()

	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes(versionStr)
	})
}

func GitVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	s := "unknown"
	if version.CommitID != "" {
		s = version.CommitID
	}

	return opNoneParamToBytes(result, proc, length, func() []byte {
		return functionUtil.QuickStrToBytes(s)
	})
}

func BuildVersion(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	t, err := strconv.ParseInt(version.BuildTime, 10, 64)
	if err != nil {
		return err
	}
	buildT := types.UnixToTimestamp(t)

	return opNoneParamToFixed[types.Timestamp](result, proc, length, func() types.Timestamp {
		return buildT
	})
}

func bitCastBinaryToFixed[T types.FixedSizeTExceptStrType](
	ctx context.Context,
	from vector.FunctionParameterWrapper[types.Varlena],
	to *vector.FunctionResult[T],
	byteLen int,
	length int,
) error {
	var i uint64
	var l = uint64(length)
	var result, emptyT T
	resultBytes := unsafe.Slice((*byte)(unsafe.Pointer(&result)), byteLen)

	for i = 0; i < l; i++ {
		v, null := from.GetStrValue(i)
		if null {
			if err := to.Append(result, true); err != nil {
				return err
			}
		} else {
			if len(v) > byteLen {
				return moerr.NewOutOfRange(ctx, fmt.Sprintf("%d-byte fixed-length type", byteLen), "binary value '0x%s'", hex.EncodeToString(v))
			}

			if len(v) < byteLen {
				result = emptyT
			}
			copy(resultBytes, v)
			if err := to.Append(result, false); err != nil {
				return err
			}
		}
	}

	return nil
}

func BitCast(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int, selectList *FunctionSelectList,
) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	toType := parameters[1].GetType()
	ctx := proc.Ctx

	switch toType.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return bitCastBinaryToFixed(ctx, source, rs, 1, length)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return bitCastBinaryToFixed(ctx, source, rs, 2, length)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return bitCastBinaryToFixed(ctx, source, rs, 4, length)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return bitCastBinaryToFixed(ctx, source, rs, 1, length)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return bitCastBinaryToFixed(ctx, source, rs, 2, length)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return bitCastBinaryToFixed(ctx, source, rs, 4, length)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return bitCastBinaryToFixed(ctx, source, rs, 4, length)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return bitCastBinaryToFixed(ctx, source, rs, 16, length)
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return bitCastBinaryToFixed(ctx, source, rs, 1, length)
	case types.T_uuid:
		rs := vector.MustFunctionResult[types.Uuid](result)
		return bitCastBinaryToFixed(ctx, source, rs, 16, length)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return bitCastBinaryToFixed(ctx, source, rs, 4, length)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return bitCastBinaryToFixed(ctx, source, rs, 8, length)
	}

	return moerr.NewInternalError(ctx, fmt.Sprintf("unsupported cast from %s to %s", source.GetType(), toType))
}

func BitmapBitPosition(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[uint64, uint64](parameters, result, proc, length, func(v uint64) uint64 {
		// low 15 bits
		return v & 0x7fff
	}, selectList)
}

func BitmapBucketNumber(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[uint64, uint64](parameters, result, proc, length, func(v uint64) uint64 {
		return v >> 15
	}, selectList)
}

func BitmapCount(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixed[uint64](parameters, result, proc, length, func(v []byte) (cnt uint64) {
		bmp := roaring.New()
		if err := bmp.UnmarshalBinary(v); err != nil {
			return 0
		}
		return bmp.GetCardinality()
	}, selectList)
}

func SHA1Func(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	return opUnaryBytesToBytes(parameters, result, proc, length, func(v []byte) []byte {
		sum := sha1.Sum(v)
		return []byte(hex.EncodeToString(sum[:]))
	}, selectList)
}

func LastDay(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	p1 := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			day := functionUtil.QuickBytesToStr(v1)
			var dt types.Date
			var err error
			var dtt types.Datetime
			if len(day) < 14 {
				dt, err = types.ParseDateCast(day)
				if err != nil {
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
					continue
				}
			} else {
				dtt, err = types.ParseDatetime(day, 6)
				if err != nil {
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
					continue
				}
				dt = dtt.ToDate()
			}

			year := dt.Year()
			month := dt.Month()

			lastDay := types.LastDay(int32(year), month)
			resDt := types.DateFromCalendar(int32(year), month, lastDay)
			if err := rs.AppendBytes([]byte(resDt.String()), false); err != nil {
				return err
			}
		}
	}
	return nil
}
