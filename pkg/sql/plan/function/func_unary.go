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
	"bytes"
	"compress/flate"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/datalink"

	"github.com/RoaringBitmap/roaring"
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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

func absDecimal256(v types.Decimal256) types.Decimal256 {
	if v.Sign() {
		v = v.Minus()
	}
	return v
}

func AbsDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal256, types.Decimal256](ivecs, result, proc, length, func(v types.Decimal256) types.Decimal256 {
		return absDecimal256(v)
	}, selectList)
}

// SIGN function implementations
// SIGN returns 1 for positive numbers, 0 for zero, -1 for negative numbers
func SignInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[int64, int64](ivecs, result, proc, length, func(v int64) int64 {
		if v > 0 {
			return 1
		} else if v < 0 {
			return -1
		}
		return 0
	}, selectList)
}

func SignUInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[uint64, int64](ivecs, result, proc, length, func(v uint64) int64 {
		if v > 0 {
			return 1
		}
		return 0
	}, selectList)
}

func SignFloat64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[float64, int64](ivecs, result, proc, length, func(v float64) int64 {
		if v > 0 {
			return 1
		} else if v < 0 {
			return -1
		}
		return 0
	}, selectList)
}

func signDecimal64(v types.Decimal64) int64 {
	zero := types.Decimal64(0)
	if v.Compare(zero) == 0 {
		return 0
	}
	if v.Sign() {
		return -1
	}
	return 1
}

func SignDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal64, int64](ivecs, result, proc, length, signDecimal64, selectList)
}

func signDecimal128(v types.Decimal128) int64 {
	zero := types.Decimal128{B0_63: 0, B64_127: 0}
	if v.Compare(zero) == 0 {
		return 0
	}
	if v.Sign() {
		return -1
	}
	return 1
}

func SignDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal128, int64](ivecs, result, proc, length, signDecimal128, selectList)
}

func signDecimal256(v types.Decimal256) int64 {
	zero := types.Decimal256{}
	if v.Compare(zero) == 0 {
		return 0
	}
	if v.Sign() {
		return -1
	}
	return 1
}

func SignDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Decimal256, int64](ivecs, result, proc, length, signDecimal256, selectList)
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

// OrdString calculates the ORD value for a string
// For single-byte characters: returns the byte value (same as ASCII)
// For multibyte characters: returns (byte1) + (byte2 * 256) + (byte3 * 256²) + ...
func OrdString(val []byte) int64 {
	if len(val) == 0 {
		return 0
	}

	// Get the first character (rune) to determine its byte size
	_, runeSize := utf8.DecodeRune(val)
	if runeSize == 0 {
		return 0
	}

	// If it's a single-byte character (ASCII), return the byte value
	if runeSize == 1 {
		return int64(val[0])
	}

	// For multibyte characters, calculate using the formula:
	// (byte1) + (byte2 * 256) + (byte3 * 256²) + ...
	var result int64
	for i := 0; i < runeSize && i < len(val); i++ {
		result += int64(val[i]) * int64(1<<(8*i)) // 256^i = 2^(8*i)
	}

	return result
}

func Ord(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	return opUnaryBytesToFixed[int64](ivecs, result, proc, length, func(v []byte) int64 {
		return OrdString(v)
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

func UtcDate(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var err error

	// Use UTC timezone instead of session timezone
	loc := time.UTC
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
			return 0, moerr.NewOutOfRangeNoCtxf("date", "'%s'", v)
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

func TimestampToDay(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, uint8](ivecs, result, proc, length, func(v types.Timestamp) uint8 {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		return dt.Day()
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

// QuoteString quotes a string for use in SQL statements
// Escapes single quotes by doubling them, backslashes, and control characters
func QuoteString(str string) string {
	var result strings.Builder
	result.WriteByte('\'')

	for _, r := range str {
		switch r {
		case '\'':
			// Escape single quote by doubling it
			result.WriteString("''")
		case '\\':
			// Escape backslash
			result.WriteString("\\\\")
		case '\n':
			// Escape newline
			result.WriteString("\\n")
		case '\r':
			// Escape carriage return
			result.WriteString("\\r")
		case '\t':
			// Escape tab
			result.WriteString("\\t")
		case '\x00':
			// Escape null byte
			result.WriteString("\\0")
		case '\x1a':
			// Escape Ctrl+Z (EOF in Windows)
			result.WriteString("\\Z")
		default:
			// Write the character as-is
			result.WriteRune(r)
		}
	}

	result.WriteByte('\'')
	return result.String()
}

func Quote(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(ivecs, result, proc, length, func(v []byte) []byte {
		str := functionUtil.QuickBytesToStr(v)
		quoted := QuoteString(str)
		return functionUtil.QuickStrToBytes(quoted)
	}, selectList)
}

func StAsText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		wkt, _, _, err := decodeGeometryPayload(v)
		if err != nil {
			return nil, err
		}
		return functionUtil.QuickStrToBytes(wkt), nil
	}, selectList)
}

func StGeomFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		wkt := strings.TrimSpace(functionUtil.QuickBytesToStr(v))
		if len(wkt) == 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if _, err := geometryTypeNameFromText(wkt); err != nil {
			return nil, err
		}
		return encodeGeometryPayload(wkt, 0, false), nil
	}, selectList)
}

func StGeomFromTextWithSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	srids := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	if selectList != nil && selectList.IgnoreAllRow() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		v, null1 := source.GetStrValue(i)
		sridValue, null2 := srids.GetValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if sridValue < 0 {
			return moerr.NewInvalidInputNoCtx("SRID should be between 0 and 4294967295")
		}

		wkt := strings.TrimSpace(functionUtil.QuickBytesToStr(v))
		if len(wkt) == 0 {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if _, err := geometryTypeNameFromText(wkt); err != nil {
			return err
		}
		if err := rs.AppendBytes(encodeGeometryPayload(wkt, uint32(sridValue), true), false); err != nil {
			return err
		}
	}
	return nil
}

func StSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[uint32](ivecs, result, proc, length, func(v []byte) (uint32, error) {
		_, srid, sridDefined, err := decodeGeometryPayload(v)
		if err != nil {
			return 0, err
		}
		if !sridDefined {
			return 0, nil
		}
		return srid, nil
	}, selectList)
}

func encodeGeometryPayload(wkt string, srid uint32, sridDefined bool) []byte {
	wkt = strings.TrimSpace(wkt)
	if !sridDefined {
		return functionUtil.QuickStrToBytes(wkt)
	}
	return functionUtil.QuickStrToBytes(fmt.Sprintf("SRID=%d;%s", srid, wkt))
}

func geometryTypeNameFromText(wkt string) (string, error) {
	s := strings.TrimSpace(wkt)
	if len(s) == 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	typeName := strings.ToUpper(strings.TrimSpace(s[:openIdx]))
	switch typeName {
	case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION EMPTY":
		return strings.TrimSuffix(typeName, " EMPTY"), nil
	case "GEOMETRY":
		return typeName, nil
	default:
		return "", moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func decodeGeometryPayload(payload []byte) (wkt string, srid uint32, sridDefined bool, err error) {
	s := strings.TrimSpace(functionUtil.QuickBytesToStr(payload))
	if len(s) == 0 {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "SRID=") {
		return s, 0, false, nil
	}

	sepIdx := strings.IndexByte(s, ';')
	if sepIdx <= len("SRID=") || sepIdx == len(s)-1 {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	value := strings.TrimSpace(s[len("SRID="):sepIdx])
	parsed, parseErr := strconv.ParseUint(value, 10, 32)
	if parseErr != nil {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	wkt = strings.TrimSpace(s[sepIdx+1:])
	if wkt == "" {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return wkt, uint32(parsed), true, nil
}

func geometryTypeNameFromPayload(payload []byte) (string, error) {
	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return "", err
	}
	return geometryTypeNameFromText(s)
}

func parsePointXYFromPayload(payload []byte) (float64, float64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, 0, err
	}
	if typeName != "POINT" {
		return 0, 0, moerr.NewInvalidInputNoCtx("geometry is not a POINT")
	}

	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, 0, err
	}
	openIdx := strings.IndexByte(s, '(')
	closeIdx := strings.LastIndexByte(s, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid point payload")
	}
	coords := strings.Fields(strings.TrimSpace(s[openIdx+1 : closeIdx]))
	if len(coords) != 2 {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid point payload")
	}
	x, err := strconv.ParseFloat(coords[0], 64)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid point payload")
	}
	y, err := strconv.ParseFloat(coords[1], 64)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid point payload")
	}
	return x, y, nil
}

func splitTopLevelGeometryItems(content string) []string {
	var items []string
	depth := 0
	start := 0
	for i, r := range content {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				item := strings.TrimSpace(content[start:i])
				if item != "" {
					items = append(items, item)
				}
				start = i + 1
			}
		}
	}
	last := strings.TrimSpace(content[start:])
	if last != "" {
		items = append(items, last)
	}
	return items
}

func geometryCountFromPayload(payload []byte) (int64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, err
	}
	switch typeName {
	case "POINT", "LINESTRING", "POLYGON":
		return 1, nil
	}

	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}
	openIdx := strings.IndexByte(s, '(')
	closeIdx := strings.LastIndexByte(s, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	content := strings.TrimSpace(s[openIdx+1 : closeIdx])
	if content == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	switch typeName {
	case "MULTIPOINT":
		return int64(len(splitTopLevelGeometryItems(content))), nil
	case "MULTILINESTRING", "MULTIPOLYGON":
		return int64(len(splitTopLevelGeometryItems(content))), nil
	case "GEOMETRYCOLLECTION":
		return int64(len(splitTopLevelGeometryItems(content))), nil
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func geometryNFromPayload(payload []byte, n int64) (string, error) {
	if n <= 0 {
		return "", moerr.NewInvalidInputNoCtx("geometry index must be greater than 0")
	}
	_, srid, sridDefined, err := decodeGeometryPayload(payload)
	if err != nil {
		return "", err
	}
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return "", err
	}

	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return "", err
	}
	openIdx := strings.IndexByte(s, '(')
	closeIdx := strings.LastIndexByte(s, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	content := strings.TrimSpace(s[openIdx+1 : closeIdx])
	if content == "" {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	var items []string
	switch typeName {
	case "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		items = splitTopLevelGeometryItems(content)
	default:
		return "", moerr.NewInvalidInputNoCtx("geometry is not a collection")
	}
	if int64(len(items)) < n {
		return "", moerr.NewInvalidInputNoCtx("geometry index out of range")
	}
	item := strings.TrimSpace(items[n-1])
	switch typeName {
	case "MULTIPOINT":
		if strings.HasPrefix(strings.ToUpper(item), "POINT") {
			return functionUtil.QuickBytesToStr(encodeGeometryPayload(item, srid, sridDefined)), nil
		}
		if strings.HasPrefix(item, "(") {
			return functionUtil.QuickBytesToStr(encodeGeometryPayload("POINT"+item, srid, sridDefined)), nil
		}
		return functionUtil.QuickBytesToStr(encodeGeometryPayload("POINT("+item+")", srid, sridDefined)), nil
	case "MULTILINESTRING":
		return functionUtil.QuickBytesToStr(encodeGeometryPayload("LINESTRING"+item, srid, sridDefined)), nil
	case "MULTIPOLYGON":
		return functionUtil.QuickBytesToStr(encodeGeometryPayload("POLYGON"+item, srid, sridDefined)), nil
	case "GEOMETRYCOLLECTION":
		if _, err := geometryTypeNameFromText(item); err != nil {
			return "", err
		}
		return functionUtil.QuickBytesToStr(encodeGeometryPayload(item, srid, sridDefined)), nil
	default:
		return "", moerr.NewInvalidInputNoCtx("geometry is not a collection")
	}
}

func geometryIsEmpty(payload []byte) (bool, error) {
	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return false, err
	}
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, "EMPTY") {
		prefix := strings.TrimSpace(strings.TrimSuffix(upper, "EMPTY"))
		switch prefix {
		case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
			return true, nil
		default:
			return false, moerr.NewInvalidInputNoCtx("invalid geometry type")
		}
	}

	if _, err := geometryTypeNameFromPayload(payload); err != nil {
		return false, err
	}

	if upper == "GEOMETRYCOLLECTION()" || upper == "MULTIPOINT()" || upper == "MULTILINESTRING()" || upper == "MULTIPOLYGON()" {
		return true, nil
	}

	openIdx := strings.IndexByte(s, '(')
	closeIdx := strings.LastIndexByte(s, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	content := strings.TrimSpace(s[openIdx+1 : closeIdx])
	return len(content) == 0, nil
}

func StGeometryType(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		typeName, err := geometryTypeNameFromPayload(v)
		if err != nil {
			return nil, err
		}
		return functionUtil.QuickStrToBytes(typeName), nil
	}, selectList)
}

func StX(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		x, _, err := parsePointXYFromPayload(v)
		return x, err
	}, selectList)
}

func StY(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		_, y, err := parsePointXYFromPayload(v)
		return y, err
	}, selectList)
}

func StNumGeometries(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v []byte) (int64, error) {
		return geometryCountFromPayload(v)
	}, selectList)
}

func StGeometryN(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	indexes := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	if selectList != nil && selectList.IgnoreAllRow() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		payload, null1 := source.GetStrValue(i)
		n, null2 := indexes.GetValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		item, err := geometryNFromPayload(payload, n)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(functionUtil.QuickStrToBytes(item), false); err != nil {
			return err
		}
	}
	return nil
}

func StPointN(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	indexes := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	if selectList != nil && selectList.IgnoreAllRow() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		payload, null1 := source.GetStrValue(i)
		n, null2 := indexes.GetValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		point, err := lineStringPointNFromPayload(payload, n)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(point, false); err != nil {
			return err
		}
	}
	return nil
}

func StExteriorRing(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return polygonExteriorRingFromPayload(v)
	}, selectList)
}

func StNumInteriorRings(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v []byte) (int64, error) {
		return numInteriorRingsFromPayload(v)
	}, selectList)
}

func StInteriorRingN(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	indexes := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	if selectList != nil && selectList.IgnoreAllRow() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		payload, null1 := source.GetStrValue(i)
		n, null2 := indexes.GetValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		ring, err := polygonInteriorRingNFromPayload(payload, n)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(ring, false); err != nil {
			return err
		}
	}
	return nil
}

func StNumPoints(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v []byte) (int64, error) {
		return numPointsFromPayload(v)
	}, selectList)
}

func StIsClosed(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isClosedFromPayload(v)
	}, selectList)
}

func StIsCollection(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isCollectionFromPayload(v)
	}, selectList)
}

func StDimension(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(v []byte) (int64, error) {
		return dimensionFromPayload(v)
	}, selectList)
}

func StIsSimple(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isSimpleFromPayload(v)
	}, selectList)
}

func StIsRing(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isRingFromPayload(v)
	}, selectList)
}

func StEnvelope(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return envelopeFromPayload(v)
	}, selectList)
}

func StCentroid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return centroidFromPayload(v)
	}, selectList)
}

func StBoundary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return boundaryFromPayload(v)
	}, selectList)
}

func StIsValid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isValidFromPayload(v)
	}, selectList)
}

func StPointOnSurface(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return pointOnSurfaceFromPayload(v)
	}, selectList)
}

func StStartPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return lineStringTerminalPointFromPayload(v, true)
	}, selectList)
}

func StEndPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		return lineStringTerminalPointFromPayload(v, false)
	}, selectList)
}

func lineStringTerminalPointFromPayload(payload []byte, first bool) ([]byte, error) {
	points, srid, sridDefined, err := lineStringPointsFromPayload(payload)
	if err != nil {
		return nil, err
	}

	point := points[0]
	if !first {
		point = points[len(points)-1]
	}
	return encodeGeometryPayload("POINT("+point+")", srid, sridDefined), nil
}

func polygonExteriorRingFromPayload(payload []byte) ([]byte, error) {
	rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
	if err != nil {
		return nil, err
	}
	return encodeGeometryPayload("LINESTRING"+rings[0], srid, sridDefined), nil
}

func numInteriorRingsFromPayload(payload []byte) (int64, error) {
	rings, _, _, err := polygonRingsFromPayload(payload)
	if err != nil {
		return 0, err
	}
	return int64(len(rings) - 1), nil
}

func polygonInteriorRingNFromPayload(payload []byte, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, moerr.NewInvalidInputNoCtx("ring index must be greater than 0")
	}

	rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
	if err != nil {
		return nil, err
	}
	if int64(len(rings)-1) < n {
		return nil, moerr.NewInvalidInputNoCtx("ring index out of range")
	}
	return encodeGeometryPayload("LINESTRING"+rings[n], srid, sridDefined), nil
}

func polygonRingsFromPayload(payload []byte) ([]string, uint32, bool, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, 0, false, err
	}
	if typeName != "POLYGON" {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("geometry is not a POLYGON")
	}

	wkt, srid, sridDefined, err := decodeGeometryPayload(payload)
	if err != nil {
		return nil, 0, false, err
	}
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
	rings := splitTopLevelGeometryItems(content)
	if len(rings) == 0 {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	for i, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return nil, 0, false, moerr.NewInvalidInputNoCtx("invalid polygon payload")
		}
		if _, err := parsePolygonRingPoints(ring[1 : len(ring)-1]); err != nil {
			return nil, 0, false, err
		}
		rings[i] = ring
	}
	return rings, srid, sridDefined, nil
}

func numPointsFromPayload(payload []byte) (int64, error) {
	points, _, _, err := lineStringPointsFromPayload(payload)
	if err != nil {
		return 0, err
	}
	return int64(len(points)), nil
}

func isClosedFromPayload(payload []byte) (bool, error) {
	points, _, _, err := lineStringPointsFromPayload(payload)
	if err != nil {
		return false, err
	}
	return points[0] == points[len(points)-1], nil
}

func isCollectionFromPayload(payload []byte) (bool, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return false, err
	}
	return strings.HasPrefix(typeName, "MULTI") || typeName == "GEOMETRYCOLLECTION", nil
}

func dimensionFromPayload(payload []byte) (int64, error) {
	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}
	return geometryDimensionFromText(wkt)
}

func geometryDimensionFromText(wkt string) (int64, error) {
	typeName, err := geometryTypeNameFromText(wkt)
	if err != nil {
		return 0, err
	}

	switch typeName {
	case "POINT", "MULTIPOINT":
		return 0, nil
	case "LINESTRING", "MULTILINESTRING":
		return 1, nil
	case "POLYGON", "MULTIPOLYGON":
		return 2, nil
	case "GEOMETRYCOLLECTION":
		openIdx := strings.IndexByte(wkt, '(')
		closeIdx := strings.LastIndexByte(wkt, ')')
		if openIdx < 0 || closeIdx <= openIdx {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry collection payload")
		}
		content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
		items := splitTopLevelGeometryItems(content)
		if len(items) == 0 {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry collection payload")
		}
		maxDimension := int64(-1)
		for _, item := range items {
			dimension, err := geometryDimensionFromText(strings.TrimSpace(item))
			if err != nil {
				return 0, err
			}
			if dimension > maxDimension {
				maxDimension = dimension
			}
		}
		if maxDimension < 0 {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry collection payload")
		}
		return maxDimension, nil
	default:
		return 0, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_Dimension")
	}
}

func isSimpleFromPayload(payload []byte) (bool, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return false, err
	}

	switch typeName {
	case "POINT":
		if _, _, err := parsePointXYFromPayload(payload); err != nil {
			return false, err
		}
		return true, nil
	case "LINESTRING":
		return lineStringIsSimpleFromPayload(payload)
	default:
		return false, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_IsSimple")
	}
}

func lineStringIsSimpleFromPayload(payload []byte) (bool, error) {
	points, err := lineStringGeometryPointsFromPayload(payload)
	if err != nil {
		return false, err
	}
	return lineStringPointsAreSimple(points), nil
}

func isRingFromPayload(payload []byte) (bool, error) {
	points, err := lineStringGeometryPointsFromPayload(payload)
	if err != nil {
		return false, err
	}
	if !sameGeometryPoint(points[0], points[len(points)-1]) {
		return false, nil
	}
	return lineStringPointsAreSimple(points), nil
}

func envelopeFromPayload(payload []byte) ([]byte, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, err
	}

	switch typeName {
	case "POINT":
		x, y, err := parsePointXYFromPayload(payload)
		if err != nil {
			return nil, err
		}
		_, srid, sridDefined, err := decodeGeometryPayload(payload)
		if err != nil {
			return nil, err
		}
		return envelopeGeometryFromBounds(x, x, y, y, srid, sridDefined), nil
	case "LINESTRING":
		pointTexts, srid, sridDefined, err := lineStringPointsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		minX, maxX, minY, maxY, err := geometryBoundsFromCoordinateTexts(pointTexts, "invalid linestring payload")
		if err != nil {
			return nil, err
		}
		return envelopeGeometryFromBounds(minX, maxX, minY, maxY, srid, sridDefined), nil
	case "POLYGON":
		rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		minX, maxX, minY, maxY, err := polygonBoundsFromRings(rings)
		if err != nil {
			return nil, err
		}
		return envelopeGeometryFromBounds(minX, maxX, minY, maxY, srid, sridDefined), nil
	default:
		return nil, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_Envelope")
	}
}

func centroidFromPayload(payload []byte) ([]byte, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, err
	}

	switch typeName {
	case "POINT":
		x, y, err := parsePointXYFromPayload(payload)
		if err != nil {
			return nil, err
		}
		_, srid, sridDefined, err := decodeGeometryPayload(payload)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	case "LINESTRING":
		points, err := lineStringGeometryPointsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		x, y, err := lineStringCentroid(points)
		if err != nil {
			return nil, err
		}
		_, srid, sridDefined, err := decodeGeometryPayload(payload)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	case "POLYGON":
		rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		x, y, err := polygonCentroid(rings)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	default:
		return nil, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_Centroid")
	}
}

func boundaryFromPayload(payload []byte) ([]byte, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, err
	}

	switch typeName {
	case "LINESTRING":
		points, srid, sridDefined, err := lineStringPointsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		if points[0] == points[len(points)-1] {
			return encodeGeometryPayload("MULTIPOINT()", srid, sridDefined), nil
		}
		return encodeGeometryPayload("MULTIPOINT(("+points[0]+"),("+points[len(points)-1]+"))", srid, sridDefined), nil
	case "POLYGON":
		rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		return encodeGeometryPayload("MULTILINESTRING("+strings.Join(rings, ",")+")", srid, sridDefined), nil
	default:
		return nil, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_Boundary")
	}
}

type geometryInterval struct {
	start float64
	end   float64
}

func pointOnSurfaceFromPayload(payload []byte) ([]byte, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, err
	}

	switch typeName {
	case "POINT":
		x, y, err := parsePointXYFromPayload(payload)
		if err != nil {
			return nil, err
		}
		_, srid, sridDefined, err := decodeGeometryPayload(payload)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	case "LINESTRING":
		points, err := lineStringGeometryPointsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		x, y, err := lineStringPointOnSurface(points)
		if err != nil {
			return nil, err
		}
		_, srid, sridDefined, err := decodeGeometryPayload(payload)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	case "POLYGON":
		rings, srid, sridDefined, err := polygonRingsFromPayload(payload)
		if err != nil {
			return nil, err
		}
		x, y, err := polygonPointOnSurface(rings)
		if err != nil {
			return nil, err
		}
		return pointGeometryPayload(x, y, srid, sridDefined), nil
	default:
		return nil, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_PointOnSurface")
	}
}

func lineStringPointOnSurface(points []geometryPoint2D) (float64, float64, error) {
	if len(points) == 0 {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}

	totalLength := 0.0
	for i := 0; i < len(points)-1; i++ {
		totalLength += math.Hypot(points[i+1].x-points[i].x, points[i+1].y-points[i].y)
	}
	if sameGeometryCoordinate(totalLength, 0) {
		return points[0].x, points[0].y, nil
	}

	target := totalLength / 2
	traversed := 0.0
	for i := 0; i < len(points)-1; i++ {
		dx := points[i+1].x - points[i].x
		dy := points[i+1].y - points[i].y
		segmentLength := math.Hypot(dx, dy)
		if sameGeometryCoordinate(segmentLength, 0) {
			continue
		}
		if target < traversed+segmentLength || sameGeometryCoordinate(target, traversed+segmentLength) {
			ratio := (target - traversed) / segmentLength
			return points[i].x + dx*ratio, points[i].y + dy*ratio, nil
		}
		traversed += segmentLength
	}
	return points[len(points)-1].x, points[len(points)-1].y, nil
}

func polygonPointOnSurface(rings []string) (float64, float64, error) {
	parsedRings := make([][]geometryPoint2D, 0, len(rings))
	for _, ring := range rings {
		points, err := parsePolygonRingPoints(ring[1 : len(ring)-1])
		if err != nil {
			return 0, 0, err
		}
		parsedRings = append(parsedRings, points)
	}

	centroidX, centroidY, err := polygonCentroid(rings)
	if err == nil && pointInsidePolygonRings(parsedRings, centroidX, centroidY) {
		return centroidX, centroidY, nil
	}

	bestWidth := 0.0
	bestX := 0.0
	bestY := 0.0
	found := false
	for _, candidateY := range polygonPointOnSurfaceCandidateYs(parsedRings, centroidY) {
		intervals := polygonInteriorIntervalsAtY(parsedRings, candidateY)
		for _, interval := range intervals {
			width := interval.end - interval.start
			if width <= bestWidth || sameGeometryCoordinate(width, bestWidth) {
				continue
			}
			candidateX := (interval.start + interval.end) / 2
			if !pointInsidePolygonRings(parsedRings, candidateX, candidateY) {
				continue
			}
			bestWidth = width
			bestX = candidateX
			bestY = candidateY
			found = true
		}
	}
	if found {
		return bestX, bestY, nil
	}
	return 0, 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
}

func pointInsidePolygonRings(rings [][]geometryPoint2D, x, y float64) bool {
	if len(rings) == 0 || !pointInPolygon(rings[0], x, y) {
		return false
	}
	for _, hole := range rings[1:] {
		if pointOnPolygonBoundary(hole, x, y) || pointInPolygon(hole, x, y) {
			return false
		}
	}
	return true
}

func polygonPointOnSurfaceCandidateYs(rings [][]geometryPoint2D, centroidY float64) []float64 {
	candidates := make([]float64, 0, len(rings)+1)
	candidates = appendUniqueGeometryCoordinate(candidates, centroidY)

	allY := make([]float64, 0)
	for _, ring := range rings {
		for _, point := range ring {
			allY = append(allY, point.y)
		}
	}
	sort.Float64s(allY)

	uniqueY := make([]float64, 0, len(allY))
	for _, y := range allY {
		uniqueY = appendUniqueGeometryCoordinate(uniqueY, y)
	}
	for i := 0; i < len(uniqueY)-1; i++ {
		if sameGeometryCoordinate(uniqueY[i], uniqueY[i+1]) {
			continue
		}
		candidates = appendUniqueGeometryCoordinate(candidates, (uniqueY[i]+uniqueY[i+1])/2)
	}
	return candidates
}

func appendUniqueGeometryCoordinate(values []float64, value float64) []float64 {
	for _, existing := range values {
		if sameGeometryCoordinate(existing, value) {
			return values
		}
	}
	return append(values, value)
}

func polygonInteriorIntervalsAtY(rings [][]geometryPoint2D, y float64) []geometryInterval {
	if len(rings) == 0 {
		return nil
	}

	intervals := polygonScanlineInteriorIntervals(rings[0], y)
	for _, hole := range rings[1:] {
		intervals = subtractGeometryIntervals(intervals, polygonScanlineInteriorIntervals(hole, y))
		if len(intervals) == 0 {
			return nil
		}
	}
	filtered := make([]geometryInterval, 0, len(intervals))
	for _, interval := range intervals {
		if interval.end-interval.start <= 1e-9 {
			continue
		}
		filtered = append(filtered, interval)
	}
	return filtered
}

func polygonScanlineInteriorIntervals(ring []geometryPoint2D, y float64) []geometryInterval {
	intersections := make([]float64, 0, len(ring))
	j := len(ring) - 1
	for i := 0; i < len(ring); i++ {
		yi := ring[i].y
		yj := ring[j].y
		if sameGeometryCoordinate(yi, yj) {
			j = i
			continue
		}
		lowerY := math.Min(yi, yj)
		upperY := math.Max(yi, yj)
		if y < lowerY || y >= upperY {
			j = i
			continue
		}
		x := ring[j].x + (y-yj)*(ring[i].x-ring[j].x)/(yi-yj)
		intersections = append(intersections, x)
		j = i
	}
	sort.Float64s(intersections)

	intervals := make([]geometryInterval, 0, len(intersections)/2)
	for i := 0; i+1 < len(intersections); i += 2 {
		start := intersections[i]
		end := intersections[i+1]
		if end-start <= 1e-9 {
			continue
		}
		intervals = append(intervals, geometryInterval{start: start, end: end})
	}
	return intervals
}

func subtractGeometryIntervals(base, cuts []geometryInterval) []geometryInterval {
	result := base
	for _, cut := range cuts {
		result = subtractSingleGeometryInterval(result, cut)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

func subtractSingleGeometryInterval(base []geometryInterval, cut geometryInterval) []geometryInterval {
	result := make([]geometryInterval, 0, len(base)+1)
	for _, interval := range base {
		if cut.end <= interval.start+1e-9 || cut.start >= interval.end-1e-9 {
			result = append(result, interval)
			continue
		}
		if cut.start > interval.start+1e-9 {
			result = append(result, geometryInterval{start: interval.start, end: cut.start})
		}
		if cut.end < interval.end-1e-9 {
			result = append(result, geometryInterval{start: cut.end, end: interval.end})
		}
	}
	return result
}

func isValidFromPayload(payload []byte) (bool, error) {
	raw := strings.TrimSpace(functionUtil.QuickBytesToStr(payload))
	upper := strings.ToUpper(raw)
	switch upper {
	case "GEOMETRYCOLLECTION()":
		return true, nil
	case "MULTIPOINT()", "MULTILINESTRING()", "MULTIPOLYGON()":
		return false, nil
	}
	if strings.HasSuffix(upper, " EMPTY") {
		return false, nil
	}

	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return false, err
	}

	switch typeName {
	case "POINT":
		_, _, err := parsePointXYFromPayload(payload)
		if err != nil {
			if isGeometryInvalidError(err, "invalid point payload") {
				return false, nil
			}
			return false, err
		}
		return true, nil
	case "LINESTRING":
		return lineStringIsValidFromPayload(payload)
	case "POLYGON":
		return polygonIsValidFromPayload(payload)
	default:
		return false, moerr.NewInvalidInputNoCtx("geometry type is not supported by ST_IsValid")
	}
}

func lineStringIsValidFromPayload(payload []byte) (bool, error) {
	points, err := lineStringGeometryPointsFromPayload(payload)
	if err != nil {
		if isGeometryInvalidError(err, "invalid linestring payload") {
			return false, nil
		}
		return false, err
	}
	for i := 0; i < len(points)-1; i++ {
		if sameGeometryPoint(points[i], points[i+1]) {
			return false, nil
		}
	}
	return true, nil
}

func polygonIsValidFromPayload(payload []byte) (bool, error) {
	rings, _, _, err := polygonRingsFromPayload(payload)
	if err != nil {
		if isGeometryInvalidError(err, "invalid polygon payload") {
			return false, nil
		}
		return false, err
	}

	parsedRings := make([][]geometryPoint2D, 0, len(rings))
	for _, ring := range rings {
		points, err := parsePolygonRingPoints(ring[1 : len(ring)-1])
		if err != nil {
			if isGeometryInvalidError(err, "invalid polygon payload") {
				return false, nil
			}
			return false, err
		}
		if !polygonRingIsValid(points) {
			return false, nil
		}
		parsedRings = append(parsedRings, points)
	}

	exterior := parsedRings[0]
	for i := 1; i < len(parsedRings); i++ {
		hole := parsedRings[i]
		if !pointInPolygon(exterior, hole[0].x, hole[0].y) {
			return false, nil
		}
		if ringsIntersect(exterior, hole) {
			return false, nil
		}
		for j := i + 1; j < len(parsedRings); j++ {
			otherHole := parsedRings[j]
			if ringsIntersect(hole, otherHole) {
				return false, nil
			}
			if pointInPolygon(hole, otherHole[0].x, otherHole[0].y) || pointInPolygon(otherHole, hole[0].x, hole[0].y) {
				return false, nil
			}
		}
	}
	return true, nil
}

func polygonRingIsValid(points []geometryPoint2D) bool {
	closedPoints := make([]geometryPoint2D, 0, len(points)+1)
	closedPoints = append(closedPoints, points...)
	closedPoints = append(closedPoints, points[0])
	if !lineStringPointsAreSimple(closedPoints) {
		return false
	}
	_, _, _, err := polygonRingAreaAndCentroid(points)
	return err == nil
}

func ringsIntersect(a, b []geometryPoint2D) bool {
	for i := 0; i < len(a); i++ {
		aNext := (i + 1) % len(a)
		for j := 0; j < len(b); j++ {
			bNext := (j + 1) % len(b)
			if lineSegmentsIntersect(a[i], a[aNext], b[j], b[bNext]) {
				return true
			}
		}
	}
	return false
}

func isGeometryInvalidError(err error, fragment string) bool {
	return err != nil && strings.Contains(err.Error(), fragment)
}

func lineStringPointsAreSimple(points []geometryPoint2D) bool {
	segmentCount := len(points) - 1
	closed := sameGeometryPoint(points[0], points[len(points)-1])
	for i := 0; i < segmentCount; i++ {
		if sameGeometryPoint(points[i], points[i+1]) {
			return false
		}
		for j := i + 1; j < segmentCount; j++ {
			if !lineSegmentsIntersect(points[i], points[i+1], points[j], points[j+1]) {
				continue
			}
			if adjacentLineSegmentsMeetSimply(points, i, j, segmentCount, closed) {
				continue
			}
			return false
		}
	}
	return true
}

func adjacentLineSegmentsMeetSimply(points []geometryPoint2D, i, j, segmentCount int, closed bool) bool {
	if j == i+1 {
		return segmentsMeetSimplyAtSharedEndpoint(points[i+1], points[i], points[j+1])
	}
	if closed && i == 0 && j == segmentCount-1 {
		return segmentsMeetSimplyAtSharedEndpoint(points[0], points[1], points[segmentCount-1])
	}
	return false
}

func segmentsMeetSimplyAtSharedEndpoint(shared, other1, other2 geometryPoint2D) bool {
	if collinearGeometryPoints(other1, shared, other2) {
		dot := (other1.x-shared.x)*(other2.x-shared.x) + (other1.y-shared.y)*(other2.y-shared.y)
		return dot < 0
	}
	return true
}

func lineSegmentsIntersect(a, b, c, d geometryPoint2D) bool {
	o1 := geometryOrientation(a, b, c)
	o2 := geometryOrientation(a, b, d)
	o3 := geometryOrientation(c, d, a)
	o4 := geometryOrientation(c, d, b)

	if o1 != o2 && o3 != o4 {
		return true
	}
	if o1 == 0 && pointOnSegment(c.x, c.y, a, b) {
		return true
	}
	if o2 == 0 && pointOnSegment(d.x, d.y, a, b) {
		return true
	}
	if o3 == 0 && pointOnSegment(a.x, a.y, c, d) {
		return true
	}
	if o4 == 0 && pointOnSegment(b.x, b.y, c, d) {
		return true
	}
	return false
}

func geometryOrientation(a, b, c geometryPoint2D) int {
	const epsilon = 1e-9

	cross := (b.x-a.x)*(c.y-a.y) - (b.y-a.y)*(c.x-a.x)
	if math.Abs(cross) <= epsilon {
		return 0
	}
	if cross > 0 {
		return 1
	}
	return -1
}

func collinearGeometryPoints(a, b, c geometryPoint2D) bool {
	return geometryOrientation(a, b, c) == 0
}

func lineStringPointNFromPayload(payload []byte, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, moerr.NewInvalidInputNoCtx("point index must be greater than 0")
	}

	points, srid, sridDefined, err := lineStringPointsFromPayload(payload)
	if err != nil {
		return nil, err
	}
	if int64(len(points)) < n {
		return nil, moerr.NewInvalidInputNoCtx("point index out of range")
	}
	return encodeGeometryPayload("POINT("+points[n-1]+")", srid, sridDefined), nil
}

func lineStringPointsFromPayload(payload []byte) ([]string, uint32, bool, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return nil, 0, false, err
	}
	if typeName != "LINESTRING" {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("geometry is not a LINESTRING")
	}

	wkt, srid, sridDefined, err := decodeGeometryPayload(payload)
	if err != nil {
		return nil, 0, false, err
	}
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}

	points := splitTopLevelGeometryItems(wkt[openIdx+1 : closeIdx])
	if len(points) < 2 {
		return nil, 0, false, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	for i, point := range points {
		point = strings.TrimSpace(point)
		if _, _, err := parseCoordinatePairWithError(point, "invalid linestring payload"); err != nil {
			return nil, 0, false, err
		}
		points[i] = point
	}
	return points, srid, sridDefined, nil
}

func lineStringGeometryPointsFromPayload(payload []byte) ([]geometryPoint2D, error) {
	pointTexts, _, _, err := lineStringPointsFromPayload(payload)
	if err != nil {
		return nil, err
	}

	points := make([]geometryPoint2D, 0, len(pointTexts))
	for _, pointText := range pointTexts {
		x, y, err := parseCoordinatePairWithError(pointText, "invalid linestring payload")
		if err != nil {
			return nil, err
		}
		points = append(points, geometryPoint2D{x: x, y: y})
	}
	return points, nil
}

func polygonBoundsFromRings(rings []string) (float64, float64, float64, float64, error) {
	hasBounds := false
	var minX, maxX, minY, maxY float64
	for _, ring := range rings {
		pointTexts := splitTopLevelGeometryItems(ring[1 : len(ring)-1])
		ringMinX, ringMaxX, ringMinY, ringMaxY, err := geometryBoundsFromCoordinateTexts(pointTexts, "invalid polygon payload")
		if err != nil {
			return 0, 0, 0, 0, err
		}
		if !hasBounds {
			minX, maxX, minY, maxY = ringMinX, ringMaxX, ringMinY, ringMaxY
			hasBounds = true
			continue
		}
		minX = math.Min(minX, ringMinX)
		maxX = math.Max(maxX, ringMaxX)
		minY = math.Min(minY, ringMinY)
		maxY = math.Max(maxY, ringMaxY)
	}
	if !hasBounds {
		return 0, 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	return minX, maxX, minY, maxY, nil
}

func geometryBoundsFromCoordinateTexts(pointTexts []string, invalidMessage string) (float64, float64, float64, float64, error) {
	if len(pointTexts) == 0 {
		return 0, 0, 0, 0, moerr.NewInvalidInputNoCtx(invalidMessage)
	}

	firstX, firstY, err := parseCoordinatePairWithError(pointTexts[0], invalidMessage)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	minX, maxX, minY, maxY := firstX, firstX, firstY, firstY
	for _, pointText := range pointTexts[1:] {
		x, y, err := parseCoordinatePairWithError(pointText, invalidMessage)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		minX = math.Min(minX, x)
		maxX = math.Max(maxX, x)
		minY = math.Min(minY, y)
		maxY = math.Max(maxY, y)
	}
	return minX, maxX, minY, maxY, nil
}

func pointGeometryPayload(x, y float64, srid uint32, sridDefined bool) []byte {
	xText := strconv.FormatFloat(x, 'f', -1, 64)
	yText := strconv.FormatFloat(y, 'f', -1, 64)
	return encodeGeometryPayload("POINT("+xText+" "+yText+")", srid, sridDefined)
}

func envelopeGeometryFromBounds(minX, maxX, minY, maxY float64, srid uint32, sridDefined bool) []byte {
	minXText := strconv.FormatFloat(minX, 'f', -1, 64)
	maxXText := strconv.FormatFloat(maxX, 'f', -1, 64)
	minYText := strconv.FormatFloat(minY, 'f', -1, 64)
	maxYText := strconv.FormatFloat(maxY, 'f', -1, 64)

	switch {
	case sameGeometryCoordinate(minX, maxX) && sameGeometryCoordinate(minY, maxY):
		return pointGeometryPayload(minX, minY, srid, sridDefined)
	case sameGeometryCoordinate(minX, maxX):
		return encodeGeometryPayload("LINESTRING("+minXText+" "+minYText+","+minXText+" "+maxYText+")", srid, sridDefined)
	case sameGeometryCoordinate(minY, maxY):
		return encodeGeometryPayload("LINESTRING("+minXText+" "+minYText+","+maxXText+" "+minYText+")", srid, sridDefined)
	default:
		return encodeGeometryPayload(
			"POLYGON(("+minXText+" "+minYText+","+maxXText+" "+minYText+","+maxXText+" "+maxYText+","+minXText+" "+maxYText+","+minXText+" "+minYText+"))",
			srid,
			sridDefined,
		)
	}
}

func sameGeometryCoordinate(a, b float64) bool {
	const epsilon = 1e-9
	return math.Abs(a-b) <= epsilon
}

func lineStringCentroid(points []geometryPoint2D) (float64, float64, error) {
	totalLength := 0.0
	sumX := 0.0
	sumY := 0.0
	for i := 0; i < len(points)-1; i++ {
		dx := points[i+1].x - points[i].x
		dy := points[i+1].y - points[i].y
		segmentLength := math.Hypot(dx, dy)
		if sameGeometryCoordinate(segmentLength, 0) {
			continue
		}
		midX := (points[i].x + points[i+1].x) / 2
		midY := (points[i].y + points[i+1].y) / 2
		totalLength += segmentLength
		sumX += midX * segmentLength
		sumY += midY * segmentLength
	}
	if sameGeometryCoordinate(totalLength, 0) {
		return points[0].x, points[0].y, nil
	}
	return sumX / totalLength, sumY / totalLength, nil
}

func polygonCentroid(rings []string) (float64, float64, error) {
	totalArea := 0.0
	sumX := 0.0
	sumY := 0.0
	for i, ring := range rings {
		points, err := parsePolygonRingPoints(ring[1 : len(ring)-1])
		if err != nil {
			return 0, 0, err
		}
		area, centroidX, centroidY, err := polygonRingAreaAndCentroid(points)
		if err != nil {
			return 0, 0, err
		}
		weight := math.Abs(area)
		if i > 0 {
			weight = -weight
		}
		totalArea += weight
		sumX += centroidX * weight
		sumY += centroidY * weight
	}
	if sameGeometryCoordinate(totalArea, 0) {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	return sumX / totalArea, sumY / totalArea, nil
}

func polygonRingAreaAndCentroid(points []geometryPoint2D) (float64, float64, float64, error) {
	crossSum := 0.0
	centroidFactorX := 0.0
	centroidFactorY := 0.0
	for i := 0; i < len(points); i++ {
		j := (i + 1) % len(points)
		cross := points[i].x*points[j].y - points[j].x*points[i].y
		crossSum += cross
		centroidFactorX += (points[i].x + points[j].x) * cross
		centroidFactorY += (points[i].y + points[j].y) * cross
	}
	area := crossSum / 2
	if sameGeometryCoordinate(area, 0) {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}
	return area, centroidFactorX / (6 * area), centroidFactorY / (6 * area), nil
}

func StIsEmpty(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return geometryIsEmpty(v)
	}, selectList)
}

func StLength(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		return geometryLength(v)
	}, selectList)
}

func StArea(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		return geometryArea(v)
	}, selectList)
}

func geometryLength(payload []byte) (float64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, err
	}
	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}

	switch typeName {
	case "LINESTRING":
		return lineStringLengthFromText(wkt)
	case "MULTILINESTRING":
		return multiLineStringLengthFromText(wkt)
	default:
		return 0, moerr.NewInvalidInputNoCtx("geometry is not a LINESTRING or MULTILINESTRING")
	}
}

func geometryArea(payload []byte) (float64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, err
	}

	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}

	switch typeName {
	case "POLYGON":
		return polygonAreaFromText(wkt)
	case "MULTIPOLYGON":
		return multiPolygonAreaFromText(wkt)
	default:
		return 0, moerr.NewInvalidInputNoCtx("geometry is not a POLYGON or MULTIPOLYGON")
	}
}

func lineStringLengthFromText(wkt string) (float64, error) {
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}
	return lineStringLengthFromContent(wkt[openIdx+1 : closeIdx])
}

func multiLineStringLengthFromText(wkt string) (float64, error) {
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
	if content == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	items := splitTopLevelGeometryItems(content)
	total := 0.0
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		length, err := lineStringLengthFromContent(item[1 : len(item)-1])
		if err != nil {
			return 0, err
		}
		total += length
	}
	return total, nil
}

func lineStringLengthFromContent(content string) (float64, error) {
	points := splitTopLevelGeometryItems(content)
	if len(points) < 2 {
		return 0, moerr.NewInvalidInputNoCtx("invalid linestring payload")
	}

	total := 0.0
	prevX, prevY, err := parseCoordinatePair(points[0])
	if err != nil {
		return 0, err
	}
	for _, point := range points[1:] {
		x, y, err := parseCoordinatePair(point)
		if err != nil {
			return 0, err
		}
		total += math.Hypot(x-prevX, y-prevY)
		prevX, prevY = x, y
	}
	return total, nil
}

func polygonAreaFromText(wkt string) (float64, error) {
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
	return polygonAreaFromPolygonContent(content)
}

func multiPolygonAreaFromText(wkt string) (float64, error) {
	openIdx := strings.IndexByte(wkt, '(')
	closeIdx := strings.LastIndexByte(wkt, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return 0, moerr.NewInvalidInputNoCtx("invalid multipolygon payload")
	}

	content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
	if content == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid multipolygon payload")
	}

	items := splitTopLevelGeometryItems(content)
	total := 0.0
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid multipolygon payload")
		}
		area, err := polygonAreaFromPolygonContent(item[1 : len(item)-1])
		if err != nil {
			return 0, err
		}
		total += area
	}
	return total, nil
}

func polygonAreaFromPolygonContent(content string) (float64, error) {
	if content == "" {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	rings := splitTopLevelGeometryItems(content)
	if len(rings) == 0 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	total := 0.0
	for i, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
		}

		area, err := polygonAreaFromRingContent(ring[1 : len(ring)-1])
		if err != nil {
			return 0, err
		}
		if i == 0 {
			total = area
		} else {
			total -= area
		}
	}
	return total, nil
}

func polygonAreaFromRingContent(content string) (float64, error) {
	points := splitTopLevelGeometryItems(content)
	if len(points) < 3 {
		return 0, moerr.NewInvalidInputNoCtx("invalid polygon payload")
	}

	prevX, prevY, err := parseCoordinatePairWithError(points[len(points)-1], "invalid polygon payload")
	if err != nil {
		return 0, err
	}

	area := 0.0
	for _, point := range points {
		x, y, err := parseCoordinatePairWithError(point, "invalid polygon payload")
		if err != nil {
			return 0, err
		}
		area += prevX*y - x*prevY
		prevX, prevY = x, y
	}
	return math.Abs(area) / 2, nil
}

func parseCoordinatePair(point string) (float64, float64, error) {
	return parseCoordinatePairWithError(point, "invalid linestring payload")
}

func parseCoordinatePairWithError(point string, errMsg string) (float64, float64, error) {
	coords := strings.Fields(strings.TrimSpace(point))
	if len(coords) != 2 {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}

	x, err := strconv.ParseFloat(coords[0], 64)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	y, err := strconv.ParseFloat(coords[1], 64)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	return x, y, nil
}

// SoundexString implements the SOUNDEX algorithm
// Returns a phonetic code representing how a string sounds
func SoundexString(str string) string {
	if len(str) == 0 {
		return "0000"
	}

	// Convert to uppercase and process only alphabetic characters
	upper := strings.ToUpper(str)

	// Find the first alphabetic character
	firstChar := byte(0)
	firstIdx := -1
	for i := 0; i < len(upper); i++ {
		if upper[i] >= 'A' && upper[i] <= 'Z' {
			firstChar = upper[i]
			firstIdx = i
			break
		}
	}

	// If no alphabetic character found, return "0000"
	if firstChar == 0 {
		return "0000"
	}

	// Build the soundex code
	var code strings.Builder
	code.WriteByte(firstChar)

	// Soundex mapping: B, F, P, V → 1; C, G, J, K, Q, S, X, Z → 2; D, T → 3; L → 4; M, N → 5; R → 6
	// Index: A=0, B=1, C=2, ..., Z=25
	soundexMap := [26]byte{
		0,   // A
		'1', // B
		'2', // C
		'3', // D
		0,   // E
		'1', // F
		'2', // G
		0,   // H
		0,   // I
		'2', // J
		'2', // K
		'4', // L
		'5', // M
		'5', // N
		0,   // O
		'1', // P
		'2', // Q
		'6', // R
		'2', // S
		'3', // T
		0,   // U
		'1', // V
		0,   // W
		'2', // X
		0,   // Y
		'2', // Z
	}

	lastCode := byte(0)
	for i := firstIdx + 1; i < len(upper) && code.Len() < 4; i++ {
		c := upper[i]
		if c < 'A' || c > 'Z' {
			continue
		}

		codeChar := soundexMap[c-'A']
		// Skip vowels, H, W (codeChar == 0)
		if codeChar == 0 {
			continue
		}

		// Skip consecutive duplicate codes
		if codeChar == lastCode {
			continue
		}

		code.WriteByte(codeChar)
		lastCode = codeChar
	}

	// Pad with zeros to make it 4 characters
	result := code.String()
	for len(result) < 4 {
		result += "0"
	}

	return result
}

func Soundex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(ivecs, result, proc, length, func(v []byte) []byte {
		str := functionUtil.QuickBytesToStr(v)
		soundex := SoundexString(str)
		return functionUtil.QuickStrToBytes(soundex)
	}, selectList)
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

	if len(ctx) > types.MaxBlobLen /*blob size*/ {
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

		dl, err := datalink.NewDatalink(filePath, proc)
		if err != nil {
			return err
		}
		size := dl.Size
		if size < 0 {
			etlFS, readPath, err := fileservice.GetForETL(proc.Ctx, proc.GetFileService(), dl.MoPath)
			if err != nil {
				return err
			}
			entry, err := etlFS.StatFile(proc.Ctx, readPath)
			if err != nil {
				return err
			}
			if dl.Offset > entry.Size {
				return moerr.NewInternalError(proc.Ctx, "offset exceeds file size")
			}
			size = entry.Size - dl.Offset
		}
		if size > int64(types.MaxBlobLen) {
			return moerr.NewInternalError(proc.Ctx, "Data too long for blob")
		}
		fileBytes, err := dl.GetBytes(proc)
		if err != nil {
			return err
		}

		if len(fileBytes) == 0 {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			return nil
		}

		if err = rs.AppendBytes(fileBytes, false); err != nil {
			return err
		}
	}
	return nil
}

// WriteFileDatalink write content to file service and return number of byte written
func WriteFileDatalink(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	filePathVec := vector.GenerateFunctionStrParameter(ivecs[0])
	contentVec := vector.GenerateFunctionStrParameter(ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		_filePath, null1 := filePathVec.GetStrValue(i)
		if null1 {
			if err := rs.Append(int64(0), true); err != nil {
				return err
			}
			continue
		}
		filePath := util.UnsafeBytesToString(_filePath)

		dl, err := datalink.NewDatalink(filePath, proc)
		if err != nil {
			return err
		}

		_content, null2 := contentVec.GetStrValue(i)
		if null2 {
			if err := rs.Append(int64(0), true); err != nil {
				return err
			}
			continue
		}
		content := util.UnsafeBytesToString(_content)

		err = func() error {
			writer, err := dl.NewWriter(proc)
			if err != nil {
				return err
			}

			defer writer.Close()

			n, err := writer.Write([]byte(content))
			if err != nil {
				return err
			}

			if err = rs.Append(int64(n), false); err != nil {
				return err
			}

			return nil
		}()

		if err != nil {
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
			return -1, moerr.NewInvalidInputf(proc.Ctx, "unsupported memory command: %s", v)
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
			return "", moerr.NewInvalidInputNoCtxf("the space count is greater than max allowed value %d", MaxAllowedValue)
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

func TimestampToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixed[types.Timestamp, types.Time](ivecs, result, proc, length, func(v types.Timestamp) types.Time {
		return v.ToDatetime(time.Local).ToTime(scale)
	}, selectList)
}

func Int64ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[int64, types.Time](ivecs, result, proc, length, func(v int64) (types.Time, error) {
		t, e := types.ParseInt64ToTime(v, 0)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtxf("time", "'%d'", v)
		}
		return t, nil
	}, selectList)
}

func DateStringToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Time](ivecs, result, proc, length, func(v []byte) (types.Time, error) {
		t, e := types.ParseTime(string(v), 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtxf("time", "'%s'", string(v))
		}
		return t, nil
	}, selectList)
}

func Decimal128ToTime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, types.Time](ivecs, result, proc, length, func(v types.Decimal128) (types.Time, error) {
		t, e := types.ParseDecimal128ToTime(v, scale, 6)
		if e != nil {
			return 0, moerr.NewOutOfRangeNoCtxf("time", "'%s'", v.Format(0))
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
	targetScale := result.GetResultVector().GetType().Scale
	return opUnaryFixedToFixed[types.Datetime, types.Timestamp](ivecs, result, proc, length, func(v types.Datetime) types.Timestamp {
		ts := v.ToTimestamp(proc.GetSessionInfo().TimeZone)
		return ts.TruncateToScale(targetScale)
	}, selectList)
}

func TimestampToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	targetScale := result.GetResultVector().GetType().Scale
	return opUnaryFixedToFixed[types.Timestamp, types.Timestamp](ivecs, result, proc, length, func(v types.Timestamp) types.Timestamp {
		return v.TruncateToScale(targetScale)
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

	sels := make([]int64, fromVec.Length())
	for j := 0; j < len(sels); j++ {
		sels[j] = int64(j)
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

func TimeToHour(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, uint8](ivecs, result, proc, length, func(v types.Time) uint8 {
		hour, _, _, _, _ := v.ClockFormat()
		// HOUR function returns 0-23, so we need to take modulo 24
		return uint8(hour % 24)
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

// TimeToMinute returns the minute from time (0-59)
func TimeToMinute(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, uint8](ivecs, result, proc, length, func(v types.Time) uint8 {
		_, minute, _, _, _ := v.ClockFormat()
		return uint8(minute)
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

// TimeToSecond returns the second from time (0-59)
func TimeToSecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, uint8](ivecs, result, proc, length, func(v types.Time) uint8 {
		_, _, sec, _, _ := v.ClockFormat()
		return uint8(sec)
	}, selectList)
}

// TimeToSec returns the time argument, converted to seconds (total seconds)
func TimeToSec(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, int64](ivecs, result, proc, length, func(v types.Time) int64 {
		// Time is stored in microseconds, so divide by MicroSecsPerSec to get seconds
		// This gives total seconds (hours*3600 + minutes*60 + seconds)
		return int64(v) / types.MicroSecsPerSec
	}, selectList)
}

// TimestampToMicrosecond returns the microseconds from timestamp (0-999999)
func TimestampToMicrosecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, int64](ivecs, result, proc, length, func(v types.Timestamp) int64 {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		return dt.MicroSec()
	}, selectList)
}

// DatetimeToMicrosecond returns the microseconds from datetime (0-999999)
func DatetimeToMicrosecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return v.MicroSec()
	}, selectList)
}

// TimeToMicrosecond returns the microseconds from time (0-999999)
func TimeToMicrosecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Time, int64](ivecs, result, proc, length, func(v types.Time) int64 {
		return v.MicroSec()
	}, selectList)
}

// StringToMicrosecond returns the microseconds from a string input
// Tries to parse as TIME, DATETIME, or TIMESTAMP. Returns NULL if parsing fails (MySQL behavior)
func StringToMicrosecond(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	strParam := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)
	scale := int32(6) // Use max scale for parsing

	zone := time.Local
	if proc != nil && proc.GetSessionInfo() != nil {
		zone = proc.GetSessionInfo().TimeZone
		if zone == nil {
			zone = time.Local
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		strVal, null := strParam.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		str := functionUtil.QuickBytesToStr(strVal)

		// Empty string should return NULL (MySQL behavior)
		// Note: ParseTime("", scale) returns Time(0) successfully, but MySQL returns NULL
		if len(str) == 0 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Try parsing as TIME first (most common format for microsecond)
		timeVal, err1 := types.ParseTime(str, scale)
		if err1 == nil {
			if err := rs.Append(timeVal.MicroSec(), false); err != nil {
				return err
			}
			continue
		}

		// Try parsing as DATETIME
		dtVal, err2 := types.ParseDatetime(str, scale)
		if err2 == nil {
			if err := rs.Append(dtVal.MicroSec(), false); err != nil {
				return err
			}
			continue
		}

		// Try parsing as TIMESTAMP
		tsVal, err3 := types.ParseTimestamp(zone, str, scale)
		if err3 == nil {
			dt := tsVal.ToDatetime(zone)
			if err := rs.Append(dt.MicroSec(), false); err != nil {
				return err
			}
			continue
		}

		// All parsing attempts failed, return NULL (MySQL behavior)
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}

	return nil
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

// Inet6Aton converts an IPv6 or IPv4 address string to a binary representation.
// IPv4 addresses return 4 bytes, IPv6 addresses return 16 bytes.
// Invalid addresses return NULL.
func Inet6Aton(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip == nil {
				// Invalid IP: return NULL for all rows
				nulls.AddRange(rsNull, 0, uint64(length))
			} else {
				var resultBytes []byte
				if ip4 := ip.To4(); ip4 != nil {
					// IPv4: return 4 bytes
					resultBytes = ip4
				} else {
					// IPv6: return 16 bytes
					resultBytes = ip
				}
				rowCount := uint64(length)
				for i := uint64(0); i < rowCount; i++ {
					if err := rs.AppendMustBytesValue(resultBytes); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
				continue
			}
			v1, _ := p1.GetStrValue(i)
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip == nil {
				// Invalid IP: return NULL
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				var resultBytes []byte
				if ip4 := ip.To4(); ip4 != nil {
					resultBytes = ip4
				} else {
					resultBytes = ip
				}
				if err := rs.AppendMustBytesValue(resultBytes); err != nil {
					return err
				}
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		ipStr := functionUtil.QuickBytesToStr(v1)
		ip := net.ParseIP(ipStr)
		if ip == nil {
			// Invalid IP: return NULL
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
		} else {
			var resultBytes []byte
			if ip4 := ip.To4(); ip4 != nil {
				resultBytes = ip4
			} else {
				resultBytes = ip
			}
			if err := rs.AppendMustBytesValue(resultBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

// Inet6Ntoa converts a binary representation of an IPv6 or IPv4 address to a string.
// Input can be 4 bytes (IPv4) or 16 bytes (IPv6).
// Invalid input returns NULL.
func Inet6Ntoa(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[types.Varlena](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			var resultStr string
			if len(v1) == 4 {
				// IPv4: 4 bytes
				ip := net.IP(v1)
				resultStr = ip.String()
			} else if len(v1) == 16 {
				// IPv6: 16 bytes
				ip := net.IP(v1)
				// Check if it's an IPv4-mapped IPv6 address (::ffff:x.x.x.x)
				if ip4 := ip.To4(); ip4 != nil && isIPv4Mapped(ip) {
					resultStr = ip4.String()
				} else {
					resultStr = ip.String()
				}
			} else {
				// Invalid length: return NULL for all rows
				nulls.AddRange(rsNull, 0, uint64(length))
				return nil
			}
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(resultStr)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
				continue
			}
			v1, _ := p1.GetStrValue(i)
			var resultStr string
			if len(v1) == 4 {
				ip := net.IP(v1)
				resultStr = ip.String()
			} else if len(v1) == 16 {
				ip := net.IP(v1)
				if ip4 := ip.To4(); ip4 != nil && isIPv4Mapped(ip) {
					resultStr = ip4.String()
				} else {
					resultStr = ip.String()
				}
			} else {
				// Invalid length: return NULL
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
				continue
			}
			if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(resultStr)); err != nil {
				return err
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		var resultStr string
		if len(v1) == 4 {
			ip := net.IP(v1)
			resultStr = ip.String()
		} else if len(v1) == 16 {
			ip := net.IP(v1)
			if ip4 := ip.To4(); ip4 != nil && isIPv4Mapped(ip) {
				resultStr = ip4.String()
			} else {
				resultStr = ip.String()
			}
		} else {
			// Invalid length: return NULL
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(resultStr)); err != nil {
			return err
		}
	}
	return nil
}

// isIPv4Mapped checks if an IPv6 address is IPv4-mapped (::ffff:x.x.x.x)
func isIPv4Mapped(ip net.IP) bool {
	if len(ip) != 16 {
		return false
	}
	// Check for ::ffff: prefix
	return ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 &&
		ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
		ip[8] == 0 && ip[9] == 0 && ip[10] == 0xff && ip[11] == 0xff
}

// InetAton converts an IPv4 address string to an unsigned integer.
// Returns NULL for invalid addresses.
// Formula: a.b.c.d = a*256^3 + b*256^2 + c*256 + d
func InetAton(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[uint64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[uint64](rsVec)
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip == nil {
				// Invalid IP: return NULL for all rows
				nulls.AddRange(rsNull, 0, uint64(length))
			} else {
				ip4 := ip.To4()
				if ip4 == nil {
					// Not IPv4: return NULL
					nulls.AddRange(rsNull, 0, uint64(length))
				} else {
					// Convert to uint32: a.b.c.d = a*256^3 + b*256^2 + c*256 + d
					resultVal := uint64(ip4[0])<<24 | uint64(ip4[1])<<16 | uint64(ip4[2])<<8 | uint64(ip4[3])
					rowCount := uint64(length)
					for i := uint64(0); i < rowCount; i++ {
						rss[i] = resultVal
					}
				}
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				continue
			}
			v1, _ := p1.GetStrValue(i)
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip == nil {
				// Invalid IP: return NULL
				rsNull.Add(i)
			} else {
				ip4 := ip.To4()
				if ip4 == nil {
					// Not IPv4: return NULL
					rsNull.Add(i)
				} else {
					// Convert to uint32
					rss[i] = uint64(ip4[0])<<24 | uint64(ip4[1])<<16 | uint64(ip4[2])<<8 | uint64(ip4[3])
				}
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		ipStr := functionUtil.QuickBytesToStr(v1)
		ip := net.ParseIP(ipStr)
		if ip == nil {
			// Invalid IP: return NULL
			rsNull.Add(i)
		} else {
			ip4 := ip.To4()
			if ip4 == nil {
				// Not IPv4: return NULL
				rsNull.Add(i)
			} else {
				// Convert to uint32
				rss[i] = uint64(ip4[0])<<24 | uint64(ip4[1])<<16 | uint64(ip4[2])<<8 | uint64(ip4[3])
			}
		}
	}
	return nil
}

// InetNtoa converts an unsigned integer to an IPv4 address string.
// Returns NULL for invalid input.
// Supports uint64, uint32, int64, int32 types.
func InetNtoa(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	argType := ivecs[0].GetType()

	switch argType.Oid {
	case types.T_uint64:
		return opUnaryFixedToStr[uint64](ivecs, result, proc, length, func(val uint64) string {
			ipVal := uint32(val & 0xFFFFFFFF)
			ip := net.IPv4(
				byte(ipVal>>24),
				byte(ipVal>>16&0xFF),
				byte(ipVal>>8&0xFF),
				byte(ipVal&0xFF),
			)
			return ip.String()
		}, selectList)
	case types.T_uint32:
		return opUnaryFixedToStr[uint32](ivecs, result, proc, length, func(val uint32) string {
			ip := net.IPv4(
				byte(val>>24),
				byte(val>>16&0xFF),
				byte(val>>8&0xFF),
				byte(val&0xFF),
			)
			return ip.String()
		}, selectList)
	case types.T_int64:
		return opUnaryFixedToStr[int64](ivecs, result, proc, length, func(val int64) string {
			// Treat as unsigned
			ipVal := uint32(uint64(val) & 0xFFFFFFFF)
			ip := net.IPv4(
				byte(ipVal>>24),
				byte(ipVal>>16&0xFF),
				byte(ipVal>>8&0xFF),
				byte(ipVal&0xFF),
			)
			return ip.String()
		}, selectList)
	case types.T_int32:
		return opUnaryFixedToStr[int32](ivecs, result, proc, length, func(val int32) string {
			// Treat as unsigned
			ipVal := uint32(val)
			ip := net.IPv4(
				byte(ipVal>>24),
				byte(ipVal>>16&0xFF),
				byte(ipVal>>8&0xFF),
				byte(ipVal&0xFF),
			)
			return ip.String()
		}, selectList)
	default:
		// Fallback to uint64
		return opUnaryFixedToStr[uint64](ivecs, result, proc, length, func(val uint64) string {
			ipVal := uint32(val & 0xFFFFFFFF)
			ip := net.IPv4(
				byte(ipVal>>24),
				byte(ipVal>>16&0xFF),
				byte(ipVal>>8&0xFF),
				byte(ipVal&0xFF),
			)
			return ip.String()
		}, selectList)
	}
}

// IsIPv4 returns 1 if the argument is a valid IPv4 address, 0 otherwise.
// Returns NULL if the input is NULL.
func IsIPv4(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[int64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			var resultVal int64
			if ip != nil && ip.To4() != nil {
				// Valid IPv4 address
				resultVal = 1
			} else {
				// Not a valid IPv4 address
				resultVal = 0
			}
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = resultVal
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				continue
			}
			v1, _ := p1.GetStrValue(i)
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip != nil && ip.To4() != nil {
				rss[i] = 1
			} else {
				rss[i] = 0
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		ipStr := functionUtil.QuickBytesToStr(v1)
		ip := net.ParseIP(ipStr)
		if ip != nil && ip.To4() != nil {
			rss[i] = 1
		} else {
			rss[i] = 0
		}
	}
	return nil
}

// IsIPv6 returns 1 if the argument is a valid IPv6 address, 0 otherwise.
// Returns NULL if the input is NULL.
func IsIPv6(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[int64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			var resultVal int64
			if ip != nil && ip.To4() == nil {
				// Valid IPv6 address (not IPv4)
				resultVal = 1
			} else {
				// Not a valid IPv6 address (could be IPv4 or invalid)
				resultVal = 0
			}
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = resultVal
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				continue
			}
			v1, _ := p1.GetStrValue(i)
			ipStr := functionUtil.QuickBytesToStr(v1)
			ip := net.ParseIP(ipStr)
			if ip != nil && ip.To4() == nil {
				rss[i] = 1
			} else {
				rss[i] = 0
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		ipStr := functionUtil.QuickBytesToStr(v1)
		ip := net.ParseIP(ipStr)
		if ip != nil && ip.To4() == nil {
			rss[i] = 1
		} else {
			rss[i] = 0
		}
	}
	return nil
}

// isIPv4Compat checks if an IPv6 address is IPv4-compatible (::a.b.c.d)
// IPv4-compatible addresses have the first 12 bytes as zeros
func isIPv4Compat(ip net.IP) bool {
	if len(ip) != 16 {
		return false
	}
	// Check for :: prefix (first 12 bytes are zeros)
	return ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0 &&
		ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
		ip[8] == 0 && ip[9] == 0 && ip[10] == 0 && ip[11] == 0 &&
		// Last 4 bytes should not be all zeros (0.0.0.0 is not considered IPv4-compatible)
		!(ip[12] == 0 && ip[13] == 0 && ip[14] == 0 && ip[15] == 0)
}

// IsIPv4Compat returns 1 if the argument is a valid IPv4-compatible IPv6 address, 0 otherwise.
// Returns NULL if the input is NULL.
// The input should be a binary representation from INET6_ATON (16 bytes for IPv6).
func IsIPv4Compat(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[int64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			var resultVal int64
			if len(v1) == 16 {
				ip := net.IP(v1)
				if isIPv4Compat(ip) {
					resultVal = 1
				} else {
					resultVal = 0
				}
			} else {
				// Invalid length: not a valid IPv6 binary representation
				resultVal = 0
			}
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = resultVal
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				continue
			}
			v1, _ := p1.GetStrValue(i)
			if len(v1) == 16 {
				ip := net.IP(v1)
				if isIPv4Compat(ip) {
					rss[i] = 1
				} else {
					rss[i] = 0
				}
			} else {
				rss[i] = 0
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		if len(v1) == 16 {
			ip := net.IP(v1)
			if isIPv4Compat(ip) {
				rss[i] = 1
			} else {
				rss[i] = 0
			}
		} else {
			rss[i] = 0
		}
	}
	return nil
}

// IsIPv4Mapped returns 1 if the argument is a valid IPv4-mapped IPv6 address, 0 otherwise.
// Returns NULL if the input is NULL.
// The input should be a binary representation from INET6_ATON (16 bytes for IPv6).
// IPv4-mapped addresses have the format ::ffff:a.b.c.d
func IsIPv4Mapped(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	result.UseOptFunctionParamFrame(1)
	rs := vector.MustFunctionResult[int64](result)
	p1 := vector.OptGetBytesParamFromWrapper(rs, 0, ivecs[0])
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)
	rsNull := rsVec.GetNulls()

	c1 := ivecs[0].IsConst()
	rsAnyNull := false

	if selectList != nil {
		if selectList.IgnoreAllRow() {
			nulls.AddRange(rsNull, 0, uint64(length))
			return nil
		}
		if !selectList.ShouldEvalAllRow() {
			rsAnyNull = true
			for i := range selectList.SelectList {
				if selectList.Contains(uint64(i)) {
					rsNull.Add(uint64(i))
				}
			}
		}
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsNull, 0, uint64(length))
		} else {
			var resultVal int64
			if len(v1) == 16 {
				ip := net.IP(v1)
				if isIPv4Mapped(ip) {
					resultVal = 1
				} else {
					resultVal = 0
				}
			} else {
				// Invalid length: not a valid IPv6 binary representation
				resultVal = 0
			}
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = resultVal
			}
		}
		return nil
	}

	// basic case
	if p1.WithAnyNullValue() || rsAnyNull {
		nulls.Or(rsNull, ivecs[0].GetNulls(), rsNull)
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			if rsNull.Contains(i) {
				continue
			}
			v1, _ := p1.GetStrValue(i)
			if len(v1) == 16 {
				ip := net.IP(v1)
				if isIPv4Mapped(ip) {
					rss[i] = 1
				} else {
					rss[i] = 0
				}
			} else {
				rss[i] = 0
			}
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, _ := p1.GetStrValue(i)
		if len(v1) == 16 {
			ip := net.IP(v1)
			if isIPv4Mapped(ip) {
				rss[i] = 1
			} else {
				rss[i] = 0
			}
		} else {
			rss[i] = 0
		}
	}
	return nil
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

type crc32ExecContext struct {
	hah hash.Hash32
}

func newCrc32ExecContext() *crc32ExecContext {
	return &crc32ExecContext{
		hah: crc32.NewIEEE(),
	}
}

func (content *crc32ExecContext) builtInCrc32(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[uint32](
		parameters,
		result, proc, length, func(v []byte) (uint32, error) {
			content.hah.Reset()
			_, err := content.hah.Write(v)
			if err != nil {
				return 0, err
			}
			return content.hah.Sum32(), nil
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

// Compress: COMPRESS(string) - Compresses a string using zlib compression
// MySQL format: 4-byte length (little-endian) + compressed data
func Compress(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		data, null := source.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Compress using zlib (flate)
		var buf bytes.Buffer
		writer, err := flate.NewWriter(&buf, flate.DefaultCompression)
		if err != nil {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		_, err = writer.Write(data)
		if err != nil {
			writer.Close()
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		err = writer.Close()
		if err != nil {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		compressed := buf.Bytes()

		// MySQL format: 4-byte length (little-endian) + compressed data
		originalLen := uint32(len(data))
		result := make([]byte, 4+len(compressed))
		binary.LittleEndian.PutUint32(result[0:4], originalLen)
		copy(result[4:], compressed)

		if err := rs.AppendBytes(result, false); err != nil {
			return err
		}
	}

	return nil
}

// Uncompress: UNCOMPRESS(string) - Uncompresses a string compressed by COMPRESS()
// Reads 4-byte length, then decompresses the rest
func Uncompress(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		data, null := source.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Check minimum length (4 bytes for length + at least 1 byte for compressed data)
		if len(data) < 5 {
			// Not a valid compressed string, return NULL
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Read 4-byte length (little-endian)
		originalLen := binary.LittleEndian.Uint32(data[0:4])
		compressed := data[4:]

		// Decompress using zlib (flate)
		reader := flate.NewReader(bytes.NewReader(compressed))
		decompressed := make([]byte, originalLen)
		n, err := reader.Read(decompressed)
		reader.Close()

		if err != nil && err != io.EOF {
			// Decompression failed, return NULL
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Check if we got the expected length
		if uint32(n) != originalLen {
			// Length mismatch, return NULL
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := rs.AppendBytes(decompressed, false); err != nil {
			return err
		}
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

func OctDate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[types.Date, types.Decimal128](ivecs, result, proc, length, func(v types.Date) (types.Decimal128, error) {
		// MySQL behavior: OCT(DATE) returns octal of the year, not days since epoch
		// Extract year from DATE and convert to octal
		year, _, _, _ := v.Calendar(true)
		val := int64(year)
		return oct[int64](val)
	}, selectList)
}

func OctDatetime(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[types.Datetime, types.Decimal128](ivecs, result, proc, length, func(v types.Datetime) (types.Decimal128, error) {
		// MySQL behavior: OCT(DATETIME) returns octal of the year, not days since epoch or microseconds
		// Extract year from DATETIME and convert to octal
		year, _, _, _ := v.ToDate().Calendar(true)
		val := int64(year)
		return oct[int64](val)
	}, selectList)
}

// OctString handles OCT function for string types (varchar, char, text)
// It tries to parse the string as DATE or DATETIME, then converts to octal
func OctString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[types.Decimal128](ivecs, result, proc, length, func(v []byte) (types.Decimal128, error) {
		s := string(v)
		// Try to parse as DATETIME first (more common for date_add/sub results)
		dt, err := types.ParseDatetime(s, 6)
		if err == nil {
			// MySQL behavior: OCT(DATETIME string) returns octal of the year, not days since epoch or microseconds
			// Extract year from DATETIME and convert to octal
			year, _, _, _ := dt.ToDate().Calendar(true)
			val := int64(year)
			return oct[int64](val)
		}
		// Try to parse as DATE
		d, err2 := types.ParseDateCast(s)
		if err2 == nil {
			// MySQL behavior: OCT(DATE string) returns octal of the year, not days since epoch
			// Extract year from DATE and convert to octal
			year, _, _, _ := d.Calendar(true)
			val := int64(year)
			return oct[int64](val)
		}
		// If both parsing fail, try to parse as integer directly
		// This handles cases where the string is already a number
		val, err3 := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err3 == nil {
			return oct[int64](val)
		}
		// If all parsing fails, return error (MySQL behavior: invalid input returns error)
		return types.Decimal128{}, moerr.NewInvalidArgNoCtx("function oct", s)
	}, selectList)
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

func generateSHAKey(key []byte) []byte {
	// return 32 bytes SHA256 checksum of the key
	hash := sha256.Sum256(key)
	return hash[:]
}

func generateInitializationVector(key []byte, length int) []byte {
	data := append(key, byte(length))
	hash := sha256.Sum256(data)
	return hash[:aes.BlockSize]
}

// encode function encrypts a string, returns a binary string of the same length of the original string.
// https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func encodeByAES(plaintext []byte, key []byte, null bool, rs *vector.FunctionResult[types.Varlena]) error {
	if null {
		return rs.AppendMustNullForBytesResult()
	}
	fixedKey := generateSHAKey(key)
	block, err := aes.NewCipher(fixedKey)
	if err != nil {
		return err
	}
	initializationVector := generateInitializationVector(key, len(plaintext))
	ciphertext := make([]byte, len(plaintext))
	stream := cipher.NewCTR(block, initializationVector)
	stream.XORKeyStream(ciphertext, plaintext)
	return rs.AppendMustBytesValue(ciphertext)
}

func Encode(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	key := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		data, nullData := source.GetStrValue(i)
		keyData, nullKey := key.GetStrValue(i)
		if err := encodeByAES(data, keyData, nullData || nullKey, rs); err != nil {
			return err
		}
	}

	return nil
}

// decode function decodes an encoded string and returns the original string
// https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func decodeByAES(ciphertext []byte, key []byte, null bool, rs *vector.FunctionResult[types.Varlena]) error {
	if null {
		return rs.AppendMustNullForBytesResult()
	}
	fixedKey := generateSHAKey(key)
	block, err := aes.NewCipher(fixedKey)
	if err != nil {
		return err
	}
	iv := generateInitializationVector(key, len(ciphertext))
	plaintext := make([]byte, len(ciphertext))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(plaintext, ciphertext)
	return rs.AppendMustBytesValue(plaintext)
}

func Decode(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	key := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		data, nullData := source.GetStrValue(i)
		keyData, nullKey := key.GetStrValue(i)
		if err := decodeByAES(data, keyData, nullData || nullKey, rs); err != nil {
			return err
		}
	}

	return nil
}

// UncompressedLength: UNCOMPRESSED_LENGTH(compressed_string) - Returns the length that the compressed string had before being compressed
// Reads the first 4 bytes (little-endian) from the compressed string
func UncompressedLength(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		data, null := source.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Check minimum length (at least 4 bytes for the length field)
		if len(data) < 4 {
			// Not a valid compressed string, return NULL
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		// Read 4-byte length (little-endian)
		originalLen := binary.LittleEndian.Uint32(data[0:4])
		if err := rs.Append(int64(originalLen), false); err != nil {
			return err
		}
	}

	return nil
}

// RandomBytes: RANDOM_BYTES(len) - Returns a binary string of len random bytes
// Uses crypto/rand for cryptographically secure random bytes
// Handles both int64 and uint64 parameter types
func RandomBytes(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	paramType := parameters[0].GetType().Oid

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		var lenVal int64
		var null bool

		// Handle different numeric types
		switch paramType {
		case types.T_int64:
			lenParam := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
			val, nullVal := lenParam.GetValue(i)
			lenVal = val
			null = nullVal
		case types.T_uint64:
			lenParam := vector.GenerateFunctionFixedTypeParameter[uint64](parameters[0])
			val, nullVal := lenParam.GetValue(i)
			if val > uint64(9223372036854775807) { // Max int64
				lenVal = 1025 // Force > 1024 to return NULL
			} else {
				lenVal = int64(val)
			}
			null = nullVal
		default:
			// Fallback to int64
			lenParam := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
			val, nullVal := lenParam.GetValue(i)
			lenVal = val
			null = nullVal
		}

		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Validate length (must be positive, MySQL allows 1 to 1024)
		if lenVal < 1 {
			// Return NULL for invalid length (MySQL behavior)
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// MySQL limits RANDOM_BYTES to max 1024 bytes
		if lenVal > 1024 {
			// Return NULL for length > 1024 (MySQL behavior)
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		// Generate random bytes using crypto/rand
		randomBytes := make([]byte, lenVal)
		_, err := rand.Read(randomBytes)
		if err != nil {
			// On error, return NULL
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := rs.AppendBytes(randomBytes, false); err != nil {
			return err
		}
	}

	return nil
}

// validatePasswordStrength calculates password strength score (0-100)
// Scoring based on:
// - Length (longer is better)
// - Presence of uppercase letters
// - Presence of lowercase letters
// - Presence of numbers
// - Presence of special characters
// - Mix of different character types
// Returns 0, 25, 50, 75, or 100 (MySQL behavior)
func validatePasswordStrength(password string) int64 {
	if len(password) == 0 {
		return 0
	}

	hasUpper := false
	hasLower := false
	hasDigit := false
	hasSpecial := false

	for _, r := range password {
		switch {
		case r >= 'A' && r <= 'Z':
			hasUpper = true
		case r >= 'a' && r <= 'z':
			hasLower = true
		case r >= '0' && r <= '9':
			hasDigit = true
		default:
			hasSpecial = true
		}
	}

	// Count character types
	typeCount := 0
	if hasUpper {
		typeCount++
	}
	if hasLower {
		typeCount++
	}
	if hasDigit {
		typeCount++
	}
	if hasSpecial {
		typeCount++
	}

	length := len(password)
	score := int64(0)

	// Length scoring
	if length >= 16 {
		score += 30
	} else if length >= 12 {
		score += 20
	} else if length >= 8 {
		score += 10
	} else if length < 4 {
		// Very short passwords get minimal score
		score = 0
	}

	// Character type scoring - more types = higher score
	if typeCount >= 4 {
		score += 50 // All 4 types
	} else if typeCount >= 3 {
		score += 30 // 3 types
	} else if typeCount >= 2 {
		score += 15 // 2 types
	} else if typeCount >= 1 {
		score += 5 // 1 type only
	}

	// Additional bonus for length when combined with multiple types
	if length >= 8 && typeCount >= 3 {
		score += 10
	}
	if length >= 12 && typeCount >= 4 {
		score += 10
	}

	// Cap at 100
	if score > 100 {
		score = 100
	}

	// Round to nearest 25 (MySQL behavior: returns 0, 25, 50, 75, or 100)
	if score < 12 {
		return 0
	} else if score < 37 {
		return 25
	} else if score < 62 {
		return 50
	} else if score < 87 {
		return 75
	}
	return 100
}

// ValidatePasswordStrength: VALIDATE_PASSWORD_STRENGTH(str) - Returns an integer to indicate how strong the password is
// Returns 0 (weak) to 100 (strong), typically in increments of 25
func ValidatePasswordStrength(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		data, null := source.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		password := string(data)
		strength := validatePasswordStrength(password)
		if err := rs.Append(strength, false); err != nil {
			return err
		}
	}

	return nil
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

// DateToQuarter returns the quarter of the year for date (1-4)
func DateToQuarter(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, uint8](ivecs, result, proc, length, func(v types.Date) uint8 {
		return uint8(v.Quarter())
	}, selectList)
}

// DatetimeToQuarter returns the quarter of the year for datetime (1-4)
func DatetimeToQuarter(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, uint8](ivecs, result, proc, length, func(v types.Datetime) uint8 {
		return uint8(v.ToDate().Quarter())
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
	rs := vector.MustFunctionResult[uint8](result)
	dates := vector.GenerateFunctionFixedTypeParameter[types.Date](ivecs[0])

	// Get mode (default 0 if not provided)
	// MySQL uses mode % 8 for out-of-range values
	mode := 0
	if len(ivecs) > 1 && !ivecs[1].IsConstNull() {
		mode = int(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
		mode = ((mode % 8) + 8) % 8
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		date, null := dates.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		week := date.Week(mode)
		if err := rs.Append(uint8(week), false); err != nil {
			return err
		}
	}
	return nil
}

func DatetimeToWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint8](result)
	datetimes := vector.GenerateFunctionFixedTypeParameter[types.Datetime](ivecs[0])

	// Get mode (default 0 if not provided)
	// MySQL uses mode % 8 for out-of-range values
	mode := 0
	if len(ivecs) > 1 && !ivecs[1].IsConstNull() {
		mode = int(vector.MustFixedColWithTypeCheck[int64](ivecs[1])[0])
		mode = ((mode % 8) + 8) % 8
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		dt, null := datetimes.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		week := dt.ToDate().Week(mode)
		if err := rs.Append(uint8(week), false); err != nil {
			return err
		}
	}
	return nil
}

// weekOfYearHelper calculates the week of year for a given date.
// WEEKOFYEAR always returns the week number for the year that the date belongs to.
func weekOfYearHelper(d types.Date) int64 {
	// Get the year of the input date
	dateYear := int32(d.Year())

	// Find the Thursday of the calendar week containing this date
	delta := 4 - int32(d.DayOfWeek())
	if delta == 4 {
		delta = -3 // Sunday
	}
	thursdayDate := types.Date(int32(d) + delta)
	thursdayYear, _, _, thursdayYday := thursdayDate.Calendar(false)

	// If Thursday is in a different year than the date, we need special handling
	if thursdayYear != dateYear {
		if thursdayYear > dateYear {
			// Thursday is in the next year, so this date is at the end of the current year
			// Count how many days of this week are in the current year
			daysInCurrentYear := 0
			weekStart := types.Date(int32(d) - delta) // Monday of the week
			for i := int32(0); i < 7; i++ {
				checkDate := types.Date(int32(weekStart) + i)
				if checkDate.Year() == uint16(dateYear) {
					daysInCurrentYear++
				}
			}
			// If at least 4 days are in the current year, it's week 53 of current year
			// Otherwise, it's week 1 of next year, but WEEKOFYEAR returns week for date's year
			if daysInCurrentYear >= 4 {
				return 53
			}
			// If less than 4 days, it's actually week 1 of next year,
			// but WEEKOFYEAR should return week 53 for the date's year
			return 53
		} else {
			// Thursday is in the previous year, so this date is at the start of the current year
			// This should be week 1 of current year
			return 1
		}
	}

	// Thursday is in the same year as the date, calculate week normally
	return int64((thursdayYday-1)/7 + 1)
}

// DateToWeekOfYear returns the calendar week of the date as a number in the range from 1 to 53.
// WEEKOFYEAR(date) is equivalent to WEEK(date, 3) which uses ISO 8601 week calculation.
// WEEKOFYEAR always returns the week number for the year that the date belongs to.
func DateToWeekOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, weekOfYearHelper, selectList)
}

// DatetimeToWeekOfYear returns the calendar week of the datetime as a number in the range from 1 to 53.
func DatetimeToWeekOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		return weekOfYearHelper(v.ToDate())
	}, selectList)
}

// TimestampToWeekOfYear returns the calendar week of the timestamp as a number in the range from 1 to 53.
func TimestampToWeekOfYear(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, int64](ivecs, result, proc, length, func(v types.Timestamp) int64 {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		return weekOfYearHelper(dt.ToDate())
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

// DateToDayOfWeek returns the weekday index for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
func DateToDayOfWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Date, int64](ivecs, result, proc, length, func(v types.Date) int64 {
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// DAYOFWEEK needs: 1=Sunday, 2=Monday, ..., 7=Saturday
		return int64(v.DayOfWeek()) + 1
	}, selectList)
}

// DatetimeToDayOfWeek returns the weekday index for datetime (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
func DatetimeToDayOfWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Datetime, int64](ivecs, result, proc, length, func(v types.Datetime) int64 {
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// DAYOFWEEK needs: 1=Sunday, 2=Monday, ..., 7=Saturday
		return int64(v.DayOfWeek()) + 1
	}, selectList)
}

// TimestampToDayOfWeek returns the weekday index for timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
func TimestampToDayOfWeek(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[types.Timestamp, int64](ivecs, result, proc, length, func(v types.Timestamp) int64 {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// DAYOFWEEK needs: 1=Sunday, 2=Monday, ..., 7=Saturday
		return int64(dt.DayOfWeek()) + 1
	}, selectList)
}

// DateToDayName returns the weekday name for date (e.g., "Sunday", "Monday", ...)
func DateToDayName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Date](ivecs, result, proc, length, func(v types.Date) string {
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// Use String() method to get the weekday name
		return v.DayOfWeek().String()
	}, selectList)
}

// DatetimeToDayName returns the weekday name for datetime (e.g., "Sunday", "Monday", ...)
func DatetimeToDayName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Datetime](ivecs, result, proc, length, func(v types.Datetime) string {
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// Use String() method to get the weekday name
		return v.DayOfWeek().String()
	}, selectList)
}

// TimestampToDayName returns the weekday name for timestamp (e.g., "Sunday", "Monday", ...)
func TimestampToDayName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Timestamp](ivecs, result, proc, length, func(v types.Timestamp) string {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		// DayOfWeek() returns 0=Sunday, 1=Monday, ..., 6=Saturday
		// Use String() method to get the weekday name
		return dt.DayOfWeek().String()
	}, selectList)
}

// DateToMonthName returns the month name for date (e.g., "January", "February", ...)
func DateToMonthName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Date](ivecs, result, proc, length, func(v types.Date) string {
		// Month() returns 1-12
		month := v.Month()
		if month >= 1 && month <= 12 {
			return MonthNames[month-1]
		}
		return ""
	}, selectList)
}

// DatetimeToMonthName returns the month name for datetime (e.g., "January", "February", ...)
func DatetimeToMonthName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Datetime](ivecs, result, proc, length, func(v types.Datetime) string {
		// Month() returns 1-12
		month := v.Month()
		if month >= 1 && month <= 12 {
			return MonthNames[month-1]
		}
		return ""
	}, selectList)
}

// TimestampToMonthName returns the month name for timestamp (e.g., "January", "February", ...)
func TimestampToMonthName(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToStr[types.Timestamp](ivecs, result, proc, length, func(v types.Timestamp) string {
		loc := proc.GetSessionInfo().TimeZone
		if loc == nil {
			loc = time.Local
		}
		dt := v.ToDatetime(loc)
		// Month() returns 1-12
		month := dt.Month()
		if month >= 1 && month <= 12 {
			return MonthNames[month-1]
		}
		return ""
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
		return proc.GetLastInsertID()
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
			return 0, moerr.NewInvalidInputf(proc.Ctx, "index out of range: %d", loc)
		}
		idx = int(loc + cnt)
	} else {
		if loc > cnt {
			return 0, moerr.NewInvalidInputf(proc.Ctx, "index out of range: %d", loc)
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

func DisableFaultInjection(
	_ []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {

	var (
		err      error
		finalVal bool
	)

	// this call may come from UT
	if proc.GetSessionInfo() == nil || proc.GetSessionInfo().SqlHelper == nil {
		fault.Disable()
		finalVal = true
	} else {
		sql := "select fault_inject('all.','disable_fault_injection','');"

		if finalVal, err = doFaultPoint(proc, sql); err != nil {
			return err
		}
	}

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return finalVal
	})
}

func EnableFaultInjection(
	_ []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {

	var (
		err      error
		finalVal bool
	)

	// this call may come from UT
	if proc.GetSessionInfo() == nil || proc.GetSessionInfo().SqlHelper == nil {
		fault.Enable()
		finalVal = true
	} else {
		sql := "select fault_inject('all.','enable_fault_injection','');"

		if finalVal, err = doFaultPoint(proc, sql); err != nil {
			return err
		}
	}

	return opNoneParamToFixed[bool](result, proc, length, func() bool {
		return finalVal
	})
}

func RemoveFaultPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	return opUnaryStrToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v string) (bool, error) {
		_, err = fault.RemoveFaultPoint(proc.Ctx, v)
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

func UTCTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Datetime](result)

	// Get scale from parameter, default to 0 if not provided (matching MySQL behavior)
	scale := int32(0)
	if len(ivecs) == 1 && !ivecs[0].IsConstNull() && ivecs[0].Length() > 0 {
		scale = int32(vector.MustFixedColWithTypeCheck[int64](ivecs[0])[0])
		// Validate scale range: 0-6 (matching MySQL behavior)
		if scale < 0 {
			return moerr.NewInvalidArg(proc.Ctx, "utc_timestamp", fmt.Sprintf("negative precision %d specified", scale))
		}
		if scale > 6 {
			return moerr.NewErrTooBigPrecision(proc.Ctx, scale, "utc_timestamp", 6)
		}
	}
	rs.TempSetType(types.New(types.T_datetime, 0, scale))

	resultValue := types.UTC().TruncateToScale(scale)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(resultValue, false); err != nil {
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

func Version(
	_ []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {

	var (
		err error

		versionAny interface{}
		versionStr string
	)

	resolveVariableFunc := proc.GetResolveVariableFunc()

	if versionAny, err = resolveVariableFunc(
		"version", true, true,
	); err != nil {
		return err
	}

	versionStr = versionAny.(string)
	retBytes := functionUtil.QuickStrToBytes(versionStr)

	return opNoneParamToBytes(
		result, proc, length, func() []byte {
			return retBytes
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
				return moerr.NewOutOfRangef(ctx, fmt.Sprintf("%d-byte fixed-length type", byteLen), "binary value '0x%s'", hex.EncodeToString(v))
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

func GroupingFunc(parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)

	for i := 0; i < length; i++ {
		var ans int64 = 0
		power := 0
		for j := len(parameters) - 1; j >= 0; j-- {
			rollup := parameters[j].GetGrouping()
			isRollup := rollup.Contains(uint64(i))
			if isRollup {
				ans += 1 << power
			}
			power++
		}
		rs.AppendMustValue(ans)
	}
	return nil
}
