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
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"net"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	hll "github.com/axiomhq/hyperloglog"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/version"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
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

func bitCountFromUint64(v uint64) uint64 {
	return uint64(bits.OnesCount64(v))
}

const minInt64BitPattern = uint64(1) << 63

func bitCountFromSignedInt64Pattern(v int64) uint64 {
	return bitCountFromUint64(uint64(v))
}

func bitCountFromMysqlIntegerString(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	if strings.HasPrefix(s, "-") {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return bitCountFromUint64(minInt64BitPattern), nil
			}
			return 0, err
		}
		return bitCountFromUint64(uint64(val)), nil
	}

	s = strings.TrimPrefix(s, "+")
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return bitCountFromUint64(math.MaxUint64), nil
		}
		return 0, err
	}
	return bitCountFromUint64(val), nil
}

func bitCountFromDecimalIntegerString(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	if strings.HasPrefix(s, "-") {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return bitCountFromUint64(minInt64BitPattern), nil
			}
			return 0, err
		}
		return bitCountFromUint64(uint64(val)), nil
	}

	s = strings.TrimPrefix(s, "+")
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, moerr.NewInvalidInputNoCtx("The input value is out of range")
		}
		return 0, err
	}
	return bitCountFromUint64(val), nil
}

func bitCountFromFloat[T constraints.Float](v T, proc *process.Process) (uint64, error) {
	val := float64(v)
	if math.IsNaN(val) {
		return 0, moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
	}
	if val <= float64(math.MinInt64) {
		return bitCountFromUint64(minInt64BitPattern), nil
	}
	//2^63 - 1 = 9223372036854775807
	//2^64 - 1 = 18446744073709551615
	// The largest longlong that will fix into a double (LLONG_MAX is not
	// exactly convertible to double, so for large double x, the test
	// x <= LLONG_MAX does not guarantee x will fit in a longlong,
	// and may give a compiler warning). LLONG_MIN is exact.

	// Similar, for ulonglong.
	const ULLONG_MAX_DOUBLE = 18446744073709549568.0

	rounded := math.RoundToEven(val)
	if rounded <= float64(math.MinInt64) {
		return bitCountFromUint64(minInt64BitPattern), nil
	}
	if rounded < 0 {
		return bitCountFromUint64(uint64(int64(rounded))), nil
	}
	if rounded >= ULLONG_MAX_DOUBLE {
		return bitCountFromUint64(uint64(math.MaxUint64)), nil
	}
	return bitCountFromUint64(uint64(rounded)), nil
}

func bitCountFromDecimal64(v types.Decimal64, scale int32) (uint64, error) {
	v = v.Round(scale, 0, true)
	if v.Less(types.Decimal64Min) {
		return bitCountFromUint64(minInt64BitPattern), nil
	}
	if types.Decimal64Max.Less(v) {
		return bitCountFromUint64(uint64(math.MaxInt64)), nil
	}
	return bitCountFromSignedInt64Pattern(int64(v)), nil
}

func bitCountFromDecimal128(v types.Decimal128, scale int32) (uint64, error) {
	v = v.Round(scale, 0, true)
	return bitCountFromDecimalIntegerString(v.Format(0))
}

func bitCountFromDecimal256(v types.Decimal256, scale int32) (uint64, error) {
	v = v.Round(scale, 0, true)
	return bitCountFromDecimalIntegerString(v.Format(0))
}

func bitCountFromNonBinaryString(v []byte) (uint64, error) {
	return bitCountFromMysqlIntegerString(convertByteSliceToString(v))
}

func bitCountFromBinaryString(v []byte) uint64 {
	var cnt uint64
	for _, b := range v {
		cnt += uint64(bits.OnesCount8(b))
	}
	return cnt
}

func BitCountInteger[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixed[T, uint64](ivecs, result, proc, length, func(v T) uint64 {
		return bitCountFromUint64(uint64(v))
	}, selectList)
}

func BitCountFloat[T constraints.Float](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[T, uint64](ivecs, result, proc, length, func(v T) (uint64, error) {
		return bitCountFromFloat(v, proc)
	}, selectList)
}

func BitCountDecimal64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal64, uint64](ivecs, result, proc, length, func(v types.Decimal64) (uint64, error) {
		return bitCountFromDecimal64(v, scale)
	}, selectList)
}

func BitCountDecimal128(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal128, uint64](ivecs, result, proc, length, func(v types.Decimal128) (uint64, error) {
		return bitCountFromDecimal128(v, scale)
	}, selectList)
}

func BitCountDecimal256(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	scale := ivecs[0].GetType().Scale
	return opUnaryFixedToFixedWithErrorCheck[types.Decimal256, uint64](ivecs, result, proc, length, func(v types.Decimal256) (uint64, error) {
		return bitCountFromDecimal256(v, scale)
	}, selectList)
}

func BitCountNonBinaryString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if ivecs[0].GetIsBin() {
		return opUnaryBytesToFixed[uint64](ivecs, result, proc, length, bitCountFromBinaryString, selectList)
	}
	return opUnaryBytesToFixedWithErrorCheck[uint64](ivecs, result, proc, length, bitCountFromNonBinaryString, selectList)
}

func BitCountBinaryString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixed[uint64](ivecs, result, proc, length, bitCountFromBinaryString, selectList)
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

// StAsWKB returns the standard (float64) Well-Known Binary of a geometry,
// up-converting a GEOMETRY32 value to float64 so the output is always
// interoperable standard WKB.
func StAsWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		wkt, _, _, err := decodeGeometryPayload(v)
		if err != nil {
			return nil, err
		}
		g, perr := geo.ParseWKT(wkt)
		if perr != nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		return geo.WriteWKB(g), nil
	}, selectList)
}

// StAsGeoJSON renders a geometry as an RFC 7946 GeoJSON geometry object
// (full coordinate precision).
func StAsGeoJSON(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		g, err := decodeGeoGeometry(v)
		if err != nil {
			return nil, err
		}
		return functionUtil.QuickStrToBytes(geo.WriteGeoJSON(g, -1)), nil
	}, selectList)
}

// StGeomFromGeoJSON builds a geometry from a GeoJSON geometry object. Per
// MySQL, the default SRID is 4326 (recorded in the result type's Width by the
// binder/overload).
func StGeomFromGeoJSON(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	maxPoints := maxPointsInGeometryLimit(proc)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		g, err := geo.ParseGeoJSON(v)
		if err != nil {
			return nil, err
		}
		if err := validateGeometryTextForStorage(geo.WriteWKT(g), maxPoints); err != nil {
			return nil, err
		}
		return geo.WriteWKB(g), nil
	}, selectList)
}

// StConvexHull returns the convex hull of a geometry (planar, monotone chain).
func StConvexHull(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		g, err := decodeGeoGeometry(v)
		if err != nil {
			return nil, err
		}
		return geoEncodeWKB(geo.ConvexHull(g), f32), nil
	}, selectList)
}

// StGeomFromWKB builds a geometry from standard Well-Known Binary.
func StGeomFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	maxPoints := maxPointsInGeometryLimit(proc)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		g, err := geo.ReadWKB(v)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		wkt := geo.WriteWKT(g)
		if err := validateGeometryTextForStorage(wkt, maxPoints); err != nil {
			return nil, err
		}
		return geo.WriteWKB(g), nil
	}, selectList)
}

func StGeomFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	maxPoints := maxPointsInGeometryLimit(proc)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		wkt := strings.TrimSpace(functionUtil.QuickBytesToStr(v))
		if len(wkt) == 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validateGeometryTextForStorage(wkt, maxPoints); err != nil {
			return nil, err
		}
		return encodeGeometryPayload(wkt, 0, false), nil
	}, selectList)
}

func StGeomFromTextWithSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	srids := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	maxPoints := maxPointsInGeometryLimit(proc)

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
		if sridValue < 0 || sridValue > int64(geo.MaxSRID) {
			return moerr.NewInvalidInputNoCtxf("SRID should be between 0 and %d", geo.MaxSRID)
		}

		wkt := strings.TrimSpace(functionUtil.QuickBytesToStr(v))
		if len(wkt) == 0 {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validateGeometryTextForStorage(wkt, maxPoints); err != nil {
			return err
		}
		if err := rs.AppendBytes(encodeGeometryPayload(wkt, uint32(sridValue), true), false); err != nil {
			return err
		}
	}
	return nil
}

// geomFromTextSubtype is the shared implementation of the typed text
// constructors (ST_PointFromText, ST_LineFromText, ...): parse WKT, assert the
// expected subtype, and return bare WKB. The optional SRID argument of the
// +SRID overloads is ignored here (it lands in the result type via the binder);
// the eval only reads the WKT parameter.
func geomFromTextSubtype(payload []byte, maxPoints int64, want string) ([]byte, error) {
	wkt := strings.TrimSpace(functionUtil.QuickBytesToStr(payload))
	if len(wkt) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if err := validateGeometryTextForStorage(wkt, maxPoints); err != nil {
		return nil, err
	}
	typeName, err := geometryTypeNameFromText(wkt)
	if err != nil {
		return nil, err
	}
	if typeName != want {
		return nil, moerr.NewInvalidInputNoCtxf("geometry is not a %s", want)
	}
	return encodeGeometryPayload(wkt, 0, false), nil
}

func stFromTextSubtype(want string) func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error {
	return func(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
		maxPoints := maxPointsInGeometryLimit(proc)
		return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
			return geomFromTextSubtype(v, maxPoints, want)
		}, selectList)
	}
}

func StPointFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("POINT")(ivecs, result, proc, length, selectList)
}

func StLineFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("LINESTRING")(ivecs, result, proc, length, selectList)
}

func StPolyFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("POLYGON")(ivecs, result, proc, length, selectList)
}

func StMPointFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("MULTIPOINT")(ivecs, result, proc, length, selectList)
}

func StMLineFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("MULTILINESTRING")(ivecs, result, proc, length, selectList)
}

func StMPolyFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("MULTIPOLYGON")(ivecs, result, proc, length, selectList)
}

func StGeomCollFromText(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromTextSubtype("GEOMETRYCOLLECTION")(ivecs, result, proc, length, selectList)
}

// geomFromWKBSubtype is the shared implementation of the typed WKB constructors
// (ST_PointFromWKB, ...): read standard WKB, assert the expected subtype, and
// re-emit bare WKB.
func geomFromWKBSubtype(payload []byte, maxPoints int64, want string) ([]byte, error) {
	g, err := geo.ReadWKB(payload)
	if err != nil {
		g, err = geo.ReadWKBFloat32(payload)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
	}
	wkt := geo.WriteWKT(g)
	if err := validateGeometryTextForStorage(wkt, maxPoints); err != nil {
		return nil, err
	}
	typeName, err := geometryTypeNameFromText(wkt)
	if err != nil {
		return nil, err
	}
	if typeName != want {
		return nil, moerr.NewInvalidInputNoCtxf("geometry is not a %s", want)
	}
	return geo.WriteWKB(g), nil
}

func stFromWKBSubtype(want string) fEvalFn {
	return func(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
		maxPoints := maxPointsInGeometryLimit(proc)
		return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
			return geomFromWKBSubtype(v, maxPoints, want)
		}, selectList)
	}
}

func StPointFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("POINT")(ivecs, result, proc, length, selectList)
}

func StLineFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("LINESTRING")(ivecs, result, proc, length, selectList)
}

func StPolyFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("POLYGON")(ivecs, result, proc, length, selectList)
}

func StMPointFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("MULTIPOINT")(ivecs, result, proc, length, selectList)
}

func StMLineFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("MULTILINESTRING")(ivecs, result, proc, length, selectList)
}

func StMPolyFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("MULTIPOLYGON")(ivecs, result, proc, length, selectList)
}

func StGeomCollFromWKB(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stFromWKBSubtype("GEOMETRYCOLLECTION")(ivecs, result, proc, length, selectList)
}

// StLongitude returns the X (longitude) ordinate of a point (ST_Longitude).
func StLongitude(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float64](ivecs, result, proc, length, selectList, true)
}

// StLongitude32 is the GEOMETRY32 overload of ST_Longitude (returns float32).
func StLongitude32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float32](ivecs, result, proc, length, selectList, true)
}

// StLatitude returns the Y (latitude) ordinate of a point (ST_Latitude).
func StLatitude(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float64](ivecs, result, proc, length, selectList, false)
}

// StLatitude32 is the GEOMETRY32 overload of ST_Latitude (returns float32).
func StLatitude32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float32](ivecs, result, proc, length, selectList, false)
}

// stPointOrdinate returns the X (wantX) or Y ordinate of a POINT as type T, so
// the same logic serves the float64 (GEOMETRY) and float32 (GEOMETRY32) forms.
func stPointOrdinate[T float32 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, wantX bool) error {
	return opUnaryBytesToFixedWithErrorCheck[T](ivecs, result, proc, length, func(v []byte) (T, error) {
		x, y, err := parsePointXYFromPayload(v)
		if wantX {
			return T(x), err
		}
		return T(y), err
	}, selectList)
}

// StSwapXY swaps the X and Y of every coordinate of a geometry (ST_SwapXY).
func StSwapXY(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		g, err := decodeGeoGeometry(v)
		if err != nil {
			return nil, err
		}
		return geoEncodeWKB(geo.SwapXY(g), f32), nil
	}, selectList)
}

// StLatFromGeoHash returns the latitude of the center of a geohash cell
// (ST_LatFromGeoHash).
func StLatFromGeoHash(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		_, lat, err := geo.DecodeGeoHash(functionUtil.QuickBytesToStr(v))
		if err != nil {
			return 0, moerr.NewInvalidInputNoCtx("invalid geohash")
		}
		return lat, nil
	}, selectList)
}

// StLongFromGeoHash returns the longitude of the center of a geohash cell
// (ST_LongFromGeoHash).
func StLongFromGeoHash(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v []byte) (float64, error) {
		lon, _, err := geo.DecodeGeoHash(functionUtil.QuickBytesToStr(v))
		if err != nil {
			return 0, moerr.NewInvalidInputNoCtx("invalid geohash")
		}
		return lon, nil
	}, selectList)
}

// StValidate returns the geometry if it is structurally valid, otherwise NULL
// (ST_Validate).
func StValidate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	src := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && (selectList.IgnoreAllRow() ||
			(!selectList.ShouldEvalAllRow() && selectList.Contains(i))) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		valid, err := isValidFromPayload(v)
		if err != nil || !valid {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendBytes(v, false); err != nil {
			return err
		}
	}
	return nil
}

func StSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// SRID is carried by the column/expression type (Width = srid+1 when a SRID
	// is defined, 0 otherwise), not by the bare-WKB payload.
	srid := sridFromTypeWidth(ivecs[0].GetType().Width)
	rs := vector.MustFunctionResult[uint32](result)
	src := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && (selectList.IgnoreAllRow() ||
			(!selectList.ShouldEvalAllRow() && selectList.Contains(i))) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if _, null := src.GetStrValue(i); null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.Append(srid, false); err != nil {
			return err
		}
	}
	return nil
}

// sridFromTypeWidth decodes the SRID stored in a geometry type's Width
// (srid+1 when defined, 0 when undefined → SRID 0).
func sridFromTypeWidth(width int32) uint32 {
	if width > 0 {
		return uint32(width - 1)
	}
	return 0
}

// sridFromInt64Arg validates an explicit SRID argument (e.g. the trailing srid
// of ST_Distance/ST_Length/ST_Area) before narrowing it to uint32. A bare
// uint32(srid) cast would silently wrap a negative or oversized value (-1 →
// 4294967295) and then pick the wrong coordinate-system kernel, so reject it.
func sridFromInt64Arg(srid int64) (uint32, error) {
	if srid < 0 || srid > math.MaxUint32 {
		return 0, moerr.NewInvalidInputNoCtxf("SRID value %d is out of range [0, 4294967295]", srid)
	}
	return uint32(srid), nil
}

// validateComputationSRID rejects SRIDs the measurement kernels cannot compute
// in. Only SRID 0 (unitless Cartesian) and 4326 (WGS 84 geodetic) are dispatched;
// any other SRID — including projected systems such as 3857 — would otherwise
// silently fall back to Cartesian math and return a semantically wrong, unitless
// result. This is the single gate for every ST_Length/ST_Area/ST_Distance path,
// whether the SRID comes from the operand type's Width or an explicit argument.
func validateComputationSRID(srid uint32) error {
	if !geo.SupportedSRID(srid) {
		return moerr.NewInvalidInputNoCtxf("unsupported SRID %d for spatial computation; only 0 and %d are supported", srid, geo.SRIDWGS84)
	}
	return nil
}

// geometryResultType is the retType for geometry-producing spatial functions.
// SRID lives in the type (Width), so a derived geometry inherits its source
// geometry's SRID. For constructors whose first argument is text (e.g.
// ST_GeomFromText), there is no source geometry and the SRID is supplied by the
// binder from a constant argument (Width stays 0 here).
func geometryResultType(parameters []types.Type) types.Type {
	// A GEOMETRY32 input yields a GEOMETRY32 (float32) result; everything else
	// stays GEOMETRY (float64). The OID is always available from the argument
	// type, so geometry-returning functions preserve the input precision.
	oid := types.T_geometry
	if len(parameters) > 0 && parameters[0].Oid == types.T_geometry32 {
		oid = types.T_geometry32
	}
	t := oid.ToType()
	if len(parameters) > 0 && (parameters[0].Oid == types.T_geometry || parameters[0].Oid == types.T_geometry32) {
		t.Width = parameters[0].Width
	}
	return t
}

// geometryArgIsFloat32 reports whether the i-th argument vector is a GEOMETRY32
// (float32-coordinate) value, so an eval function can emit matching output.
func geometryArgIsFloat32(ivecs []*vector.Vector, i int) bool {
	return i < len(ivecs) && ivecs[i].GetType().Oid == types.T_geometry32
}

// geoEncodeWKB writes g as float32 WKB when f32 is set, else standard float64 WKB.
func geoEncodeWKB(g geo.Geometry, f32 bool) []byte {
	if f32 {
		return geo.WriteWKBFloat32(g)
	}
	return geo.WriteWKB(g)
}

// reencodeGeom32 transcodes an already-encoded WKB payload to float32 WKB when
// f32 is set; otherwise it returns the payload unchanged. Used by
// geometry-returning functions that build their output through helpers that
// always emit standard float64 WKB.
func reencodeGeom32(out []byte, f32 bool) []byte {
	if !f32 || len(out) == 0 {
		return out
	}
	g, err := geo.ReadWKB(out)
	if err != nil {
		if g, err = geo.ReadWKBFloat32(out); err != nil {
			return out
		}
	}
	return geo.WriteWKBFloat32(g)
}

// encodeGeometryPayload parses a WKT (or EWKT "SRID=n;WKT") string and returns
// the bare standard WKB bytes that a geometry varlena stores. SRID is NOT
// stored in the cell (it lives in the column type), so the srid arguments are
// ignored and any EWKT SRID prefix is stripped. See docs/design/gisimpl.md.
func encodeGeometryPayload(wkt string, _ uint32, _ bool) []byte {
	wkt, _, _ = stripEWKTSRID(strings.TrimSpace(wkt))
	g, err := geo.ParseWKT(wkt)
	if err != nil {
		// Internal callers pass well-formed WKT and external input is
		// validated before storage; on an unexpected parse error keep the raw
		// text (decode tolerates legacy text) so the error surfaces downstream
		// rather than silently corrupting data.
		return functionUtil.QuickStrToBytes(wkt)
	}
	return geo.WriteWKB(g)
}

// decodeGeoGeometry decodes a stored geometry payload (WKB float64/float32, or
// legacy WKT/EWKT text) into the geo.Geometry model used by the Cartesian and
// geodetic computation kernels.
func decodeGeoGeometry(payload []byte) (geo.Geometry, error) {
	if len(payload) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if payloadIsWKB(payload) {
		g, err := geo.ReadWKB(payload)
		if err != nil {
			g, err = geo.ReadWKBFloat32(payload)
			if err != nil {
				return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
		}
		return g, nil
	}
	s, _, _ := stripEWKTSRID(strings.TrimSpace(functionUtil.QuickBytesToStr(payload)))
	g, err := geo.ParseWKT(s)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return g, nil
}

// geodeticArea returns the WGS 84 geodesic area (square meters) of a polygon or
// multipolygon.
func geodeticArea(payload []byte) (float64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, err
	}
	if typeName != "POLYGON" && typeName != "MULTIPOLYGON" {
		return 0, moerr.NewInvalidInputNoCtx("geometry is not a POLYGON or MULTIPOLYGON")
	}
	g, err := decodeGeoGeometry(payload)
	if err != nil {
		return 0, err
	}
	return geo.AreaSquareMeters(g), nil
}

// geodeticLength returns the WGS 84 geodesic length (meters) of a linestring or
// multilinestring.
func geodeticLength(payload []byte) (float64, error) {
	typeName, err := geometryTypeNameFromPayload(payload)
	if err != nil {
		return 0, err
	}
	if typeName != "LINESTRING" && typeName != "MULTILINESTRING" {
		return 0, moerr.NewInvalidInputNoCtx("geometry is not a LINESTRING or MULTILINESTRING")
	}
	g, err := decodeGeoGeometry(payload)
	if err != nil {
		return 0, err
	}
	return geo.LengthMeters(g), nil
}

// encodeGeometryPayloadFloat32 is the GEOMETRY32 counterpart of
// encodeGeometryPayload: it parses WKT and returns float32-coordinate WKB.
func encodeGeometryPayloadFloat32(wkt string) []byte {
	wkt, _, _ = stripEWKTSRID(strings.TrimSpace(wkt))
	g, err := geo.ParseWKT(wkt)
	if err != nil {
		return functionUtil.QuickStrToBytes(wkt)
	}
	return geo.WriteWKBFloat32(g)
}

// stripEWKTSRID removes a leading "SRID=n;" prefix from an (E)WKT string,
// returning the bare WKT, the parsed SRID, and whether a prefix was present.
func stripEWKTSRID(s string) (wkt string, srid uint32, hasSRID bool) {
	if !strings.HasPrefix(strings.ToUpper(s), "SRID=") {
		return s, 0, false
	}
	sep := strings.IndexByte(s, ';')
	if sep <= len("SRID=") {
		return s, 0, false
	}
	parsed, err := strconv.ParseUint(strings.TrimSpace(s[len("SRID="):sep]), 10, 32)
	if err != nil {
		return s, 0, false
	}
	return strings.TrimSpace(s[sep+1:]), uint32(parsed), true
}

// payloadIsWKB reports whether a geometry payload is WKB (the stored form)
// rather than legacy WKT text. Standard WKB begins with a byte-order marker
// (0x00 big-endian or 0x01 little-endian); WKT begins with a type-keyword
// letter.
func payloadIsWKB(payload []byte) bool {
	return len(payload) > 0 && (payload[0] == 0 || payload[0] == 1)
}

func geometryTypeNameFromText(wkt string) (string, error) {
	s := strings.TrimSpace(wkt)
	if len(s) == 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	upper := strings.ToUpper(s)
	if strings.HasSuffix(upper, " EMPTY") {
		typeName := strings.TrimSpace(strings.TrimSuffix(upper, " EMPTY"))
		switch typeName {
		case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
			return typeName, nil
		default:
			return "", moerr.NewInvalidInputNoCtx("invalid geometry type")
		}
	}
	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 {
		return "", moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	typeName := strings.TrimSpace(upper[:openIdx])
	switch typeName {
	case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return typeName, nil
	case "GEOMETRY":
		return typeName, nil
	default:
		return "", moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

// decodeGeometryPayload returns the WKT text of a stored geometry. The stored
// form is bare WKB; for backward/test compatibility legacy WKT (or EWKT) text
// payloads are also accepted. SRID is never carried in the payload, so the srid
// return values are always (0, false) — callers that need the SRID read it from
// the column/vector type instead.
func decodeGeometryPayload(payload []byte) (wkt string, srid uint32, sridDefined bool, err error) {
	if len(payload) == 0 {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if payloadIsWKB(payload) {
		g, rerr := geo.ReadWKB(payload)
		if rerr != nil {
			// A float32-coordinate WKB (GEOMETRY32) carries 4 coordinate bytes
			// where standard WKB carries 8, so the float64 reader runs out of
			// bytes on it; fall back to the float32 reader. The two encodings
			// are unambiguous by length, so this also serves columns whose OID
			// is GEOMETRY32.
			g, rerr = geo.ReadWKBFloat32(payload)
			if rerr != nil {
				return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
		}
		return geo.WriteWKT(g), 0, false, nil
	}
	// Legacy WKT/EWKT text.
	s, _, _ := stripEWKTSRID(strings.TrimSpace(functionUtil.QuickBytesToStr(payload)))
	if s == "" {
		return "", 0, false, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return s, 0, false, nil
}

func GeometryPayloadToText(payload []byte) (string, error) {
	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return "", err
	}
	return wkt, nil
}

const defaultMaxPointsInGeometry = int64(65536)

func validateGeometryPayload(payload []byte, maxPoints int64) (wkt string, typeName string, srid uint32, sridDefined bool, err error) {
	wkt, srid, sridDefined, err = decodeGeometryPayload(payload)
	if err != nil {
		return "", "", 0, false, err
	}
	typeName, err = geometryTypeNameFromText(wkt)
	if err != nil {
		return "", "", 0, false, err
	}
	if err = validateGeometryTextForStorage(wkt, maxPoints); err != nil {
		return "", "", 0, false, err
	}
	return wkt, typeName, srid, sridDefined, nil
}

func maxPointsInGeometryLimit(proc *process.Process) int64 {
	if proc != nil && proc.GetResolveVariableFunc() != nil {
		if v, err := proc.GetResolveVariableFunc()("max_points_in_geometry", true, false); err == nil && v != nil {
			switch val := v.(type) {
			case int64:
				return val
			case int32:
				return int64(val)
			case uint64:
				return int64(val)
			case uint32:
				return int64(val)
			case int:
				return int64(val)
			case uint:
				return int64(val)
			}
		}
	}
	return defaultMaxPointsInGeometry
}

func validateGeometryTextForStorage(wkt string, maxPoints int64) error {
	if err := validateGeometryTextStructure(wkt); err != nil {
		return err
	}
	if err := validateFiniteCoordinatesInGeometryText(wkt); err != nil {
		return err
	}
	if maxPoints <= 0 {
		return nil
	}
	pointCount, err := geometryPointCountFromText(wkt)
	if err != nil {
		return err
	}
	if pointCount > maxPoints {
		return moerr.NewInvalidInputNoCtxf("geometry has %d points, which exceeds max_points_in_geometry=%d", pointCount, maxPoints)
	}
	return nil
}

func validateGeometryTextStructure(wkt string) error {
	return validateGeometryTextStructureWithDepth(wkt, 0)
}

func validateGeometryTextStructureWithDepth(wkt string, depth int) error {
	s := strings.TrimSpace(wkt)
	typeName, err := geometryTypeNameFromText(s)
	if err != nil {
		return err
	}

	if strings.EqualFold(strings.TrimSpace(s), typeName+" EMPTY") {
		return nil
	}

	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 || s[len(s)-1] != ')' {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if strings.TrimSpace(strings.ToUpper(s[:openIdx])) != typeName {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	content := strings.TrimSpace(s[openIdx+1 : len(s)-1])
	switch typeName {
	case "POINT":
		return validatePointGeometryTextContent(content)
	case "LINESTRING":
		return validateLineStringGeometryTextContent(content)
	case "POLYGON":
		return validatePolygonGeometryTextContent(content)
	case "MULTIPOINT":
		return validateMultiPointGeometryTextContent(content, depth)
	case "MULTILINESTRING":
		return validateMultiLineStringGeometryTextContent(content)
	case "MULTIPOLYGON":
		return validateMultiPolygonGeometryTextContent(content)
	case "GEOMETRYCOLLECTION":
		return validateGeometryCollectionTextContent(content, depth)
	case "GEOMETRY":
		return validateGenericGeometryTextContent(content, depth)
	default:
		return moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func validatePointGeometryTextContent(content string) error {
	if content == "" {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	_, _, err := parseCoordinatePairWithError(content, "invalid geometry payload")
	return err
}

func validateLineStringGeometryTextContent(content string) error {
	points, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(points) < 2 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	for _, point := range points {
		if _, _, err := parseCoordinatePairWithError(point, "invalid geometry payload"); err != nil {
			return err
		}
	}
	return nil
}

func validatePolygonGeometryTextContent(content string) error {
	rings, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validatePolygonRingTextContent(ring[1 : len(ring)-1]); err != nil {
			return err
		}
	}
	return nil
}

func validatePolygonRingTextContent(content string) error {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(items) < 3 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	points := make([]geometryPoint2D, 0, len(items))
	for _, item := range items {
		x, y, err := parseCoordinatePairWithError(item, "invalid geometry payload")
		if err != nil {
			return err
		}
		points = append(points, geometryPoint2D{x: x, y: y})
	}

	if len(points) > 1 && sameGeometryPoint(points[0], points[len(points)-1]) {
		points = points[:len(points)-1]
	}
	if len(points) < 3 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return nil
}

func validateMultiPointGeometryTextContent(content string, depth int) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if itemType, err := geometryTypeNameFromText(item); err == nil {
			if itemType != "POINT" {
				return moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			if err := validateGeometryTextStructureWithDepth(item, depth+1); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(item, "(") {
			if len(item) < 2 || item[len(item)-1] != ')' {
				return moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			if err := validatePointGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
				return err
			}
			continue
		}
		if err := validatePointGeometryTextContent(item); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiLineStringGeometryTextContent(content string) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validateLineStringGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
			return err
		}
	}
	return nil
}

func validateMultiPolygonGeometryTextContent(content string) error {
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		if err := validatePolygonGeometryTextContent(strings.TrimSpace(item[1 : len(item)-1])); err != nil {
			return err
		}
	}
	return nil
}

func validateGeometryCollectionTextContent(content string, depth int) error {
	if depth >= maxGeometryCollectionNestingDepth {
		return moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryCollectionNestingDepth)
	}
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := validateGeometryTextStructureWithDepth(item, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateGenericGeometryTextContent(content string, depth int) error {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	if len(items) != 1 {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return validateGeometryTextStructureWithDepth(items[0], depth+1)
}

func geometryPointCountFromText(wkt string) (int64, error) {
	return geometryPointCountFromTextWithDepth(wkt, 0)
}

func geometryPointCountFromTextWithDepth(wkt string, depth int) (int64, error) {
	s := strings.TrimSpace(wkt)
	typeName, err := geometryTypeNameFromText(s)
	if err != nil {
		return 0, err
	}
	if strings.EqualFold(strings.TrimSpace(s), typeName+" EMPTY") {
		return 0, nil
	}

	openIdx := strings.IndexByte(s, '(')
	if openIdx <= 0 || s[len(s)-1] != ')' {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if strings.TrimSpace(strings.ToUpper(s[:openIdx])) != typeName {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}

	content := strings.TrimSpace(s[openIdx+1 : len(s)-1])
	switch typeName {
	case "POINT":
		if content == "" {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		return 1, nil
	case "LINESTRING":
		return coordinateListPointCount(content)
	case "POLYGON":
		return polygonPointCountFromTextContent(content)
	case "MULTIPOINT":
		return multiPointCountFromTextContent(content, depth)
	case "MULTILINESTRING":
		return multiLineStringPointCountFromTextContent(content)
	case "MULTIPOLYGON":
		return multiPolygonPointCountFromTextContent(content)
	case "GEOMETRYCOLLECTION":
		return geometryCollectionPointCountFromTextContent(content, depth)
	case "GEOMETRY":
		return genericGeometryPointCountFromTextContent(content, depth)
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry type")
	}
}

func coordinateListPointCount(content string) (int64, error) {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

func polygonPointCountFromTextContent(content string) (int64, error) {
	rings, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, ring := range rings {
		ring = strings.TrimSpace(ring)
		if len(ring) < 2 || ring[0] != '(' || ring[len(ring)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := coordinateListPointCount(strings.TrimSpace(ring[1 : len(ring)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func multiPointCountFromTextContent(content string, depth int) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if itemType, err := geometryTypeNameFromText(item); err == nil {
			if itemType != "POINT" {
				return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
			}
			count, err := geometryPointCountFromTextWithDepth(item, depth+1)
			if err != nil {
				return 0, err
			}
			total += count
			continue
		}
		total++
	}
	return total, nil
}

func multiLineStringPointCountFromTextContent(content string) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := coordinateListPointCount(strings.TrimSpace(item[1 : len(item)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func multiPolygonPointCountFromTextContent(content string) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) < 2 || item[0] != '(' || item[len(item)-1] != ')' {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
		count, err := polygonPointCountFromTextContent(strings.TrimSpace(item[1 : len(item)-1]))
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func geometryCollectionPointCountFromTextContent(content string, depth int) (int64, error) {
	if content == "" {
		return 0, nil
	}
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	total := int64(0)
	for _, item := range items {
		count, err := geometryPointCountFromTextWithDepth(item, depth+1)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func genericGeometryPointCountFromTextContent(content string, depth int) (int64, error) {
	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return 0, err
	}
	if len(items) != 1 {
		return 0, moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return geometryPointCountFromTextWithDepth(items[0], depth+1)
}

func geometryTypeNameFromPayload(payload []byte) (string, error) {
	s, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return "", err
	}
	return geometryTypeNameFromText(s)
}

func validateFiniteCoordinatesInGeometryText(wkt string) error {
	tokens := strings.FieldsFunc(wkt, func(r rune) bool {
		switch r {
		case '(', ')', ',', ' ', '\t', '\n', '\r':
			return true
		default:
			return false
		}
	})
	for _, token := range tokens {
		if token == "" {
			continue
		}
		value, err := strconv.ParseFloat(token, 64)
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			continue
		}
		if err == nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return moerr.NewInvalidInputNoCtx("invalid geometry payload")
		}
	}
	return nil
}

func parseFiniteCoordinate(token string, errMsg string) (float64, error) {
	value, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	return value, nil
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
	x, err := parseFiniteCoordinate(coords[0], "invalid point payload")
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx("invalid point payload")
	}
	y, err := parseFiniteCoordinate(coords[1], "invalid point payload")
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

func splitTopLevelGeometryItemsStrict(content string, errMsg string) ([]string, error) {
	items := make([]string, 0)
	depth := 0
	start := 0
	for i, r := range content {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, moerr.NewInvalidInputNoCtx(errMsg)
			}
		case ',':
			if depth == 0 {
				item := strings.TrimSpace(content[start:i])
				if item == "" {
					return nil, moerr.NewInvalidInputNoCtx(errMsg)
				}
				items = append(items, item)
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, moerr.NewInvalidInputNoCtx(errMsg)
	}
	last := strings.TrimSpace(content[start:])
	if last == "" {
		return nil, moerr.NewInvalidInputNoCtx(errMsg)
	}
	items = append(items, last)
	return items, nil
}

const maxGeometryCollectionNestingDepth = 64

func validateGeometryCollectionNestingDepthFromText(wkt string) error {
	return validateGeometryCollectionNestingDepthFromTextWithDepth(wkt, 0)
}

func validateGeometryCollectionNestingDepthFromTextWithDepth(wkt string, depth int) error {
	s := strings.TrimSpace(wkt)
	typeName, err := geometryTypeNameFromText(s)
	if err != nil {
		return err
	}
	if typeName != "GEOMETRYCOLLECTION" {
		return nil
	}
	if depth >= maxGeometryCollectionNestingDepth {
		return moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryCollectionNestingDepth)
	}

	openIdx := strings.IndexByte(s, '(')
	closeIdx := strings.LastIndexByte(s, ')')
	if openIdx < 0 || closeIdx <= openIdx {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	content := strings.TrimSpace(s[openIdx+1 : closeIdx])
	if content == "" {
		return nil
	}

	items, err := splitTopLevelGeometryItemsStrict(content, "invalid geometry payload")
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := validateGeometryCollectionNestingDepthFromTextWithDepth(item, depth+1); err != nil {
			return err
		}
	}
	return nil
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
	if typeName == "GEOMETRYCOLLECTION" {
		if err := validateGeometryCollectionNestingDepthFromText(s); err != nil {
			return 0, err
		}
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
	if typeName == "GEOMETRYCOLLECTION" {
		if err := validateGeometryCollectionNestingDepthFromText(s); err != nil {
			return "", err
		}
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
	return stPointOrdinate[float64](ivecs, result, proc, length, selectList, true)
}

// StX32 is the GEOMETRY32 overload of ST_X. It returns float32, matching the
// stored coordinate width of a GEOMETRY32 value. This is a deliberate MatrixOne
// extension: MySQL's ST_X always returns DOUBLE; for the float32 GEOMETRY32
// family the coordinate accessors stay float32 to avoid implying precision the
// value does not carry. Plain GEOMETRY still returns float64 via StX.
func StX32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float32](ivecs, result, proc, length, selectList, true)
}

func StY(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float64](ivecs, result, proc, length, selectList, false)
}

// StY32 is the GEOMETRY32 overload of ST_Y (returns float32).
func StY32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stPointOrdinate[float32](ivecs, result, proc, length, selectList, false)
}

func StNumGeometries(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)

	if selectList != nil && selectList.IgnoreAllRow() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		v, null := source.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		isEmpty, err := geometryIsEmpty(v)
		if err != nil {
			return err
		}
		if isEmpty {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		count, err := geometryCountFromPayload(v)
		if err != nil {
			return err
		}
		if err := rs.Append(count, false); err != nil {
			return err
		}
	}
	return nil
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
		out := reencodeGeom32(functionUtil.QuickStrToBytes(item), geometryArgIsFloat32(ivecs, 0))
		if err := rs.AppendBytes(out, false); err != nil {
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
		if err := rs.AppendBytes(reencodeGeom32(point, geometryArgIsFloat32(ivecs, 0)), false); err != nil {
			return err
		}
	}
	return nil
}

func StExteriorRing(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := polygonExteriorRingFromPayload(v)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
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
		if err := rs.AppendBytes(reencodeGeom32(ring, geometryArgIsFloat32(ivecs, 0)), false); err != nil {
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
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := envelopeFromPayload(v)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
	}, selectList)
}

func StCentroid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := centroidFromPayload(v)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
	}, selectList)
}

func StBoundary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := boundaryFromPayload(v)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
	}, selectList)
}

func StIsValid(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[bool](ivecs, result, proc, length, func(v []byte) (bool, error) {
		return isValidFromPayload(v)
	}, selectList)
}

func StPointOnSurface(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := pointOnSurfaceFromPayload(v)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
	}, selectList)
}

func StStartPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := lineStringTerminalPointFromPayload(v, true)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
	}, selectList)
}

func StEndPoint(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	f32 := geometryArgIsFloat32(ivecs, 0)
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		out, err := lineStringTerminalPointFromPayload(v, false)
		if err != nil {
			return nil, err
		}
		return reencodeGeom32(out, f32), nil
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
	return geometryDimensionFromTextWithDepth(wkt, 0)
}

func geometryDimensionFromTextWithDepth(wkt string, depth int) (int64, error) {
	isEmpty, err := geometryIsEmpty(functionUtil.QuickStrToBytes(wkt))
	if err != nil {
		return 0, err
	}
	if isEmpty {
		return -1, nil
	}

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
		if depth >= maxGeometryCollectionNestingDepth {
			return 0, moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryCollectionNestingDepth)
		}
		openIdx := strings.IndexByte(wkt, '(')
		closeIdx := strings.LastIndexByte(wkt, ')')
		if openIdx < 0 || closeIdx <= openIdx {
			return 0, moerr.NewInvalidInputNoCtx("invalid geometry collection payload")
		}
		content := strings.TrimSpace(wkt[openIdx+1 : closeIdx])
		items := splitTopLevelGeometryItems(content)
		if len(items) == 0 {
			return -1, nil
		}
		maxDimension := int64(-1)
		for _, item := range items {
			dimension, err := geometryDimensionFromTextWithDepth(strings.TrimSpace(item), depth+1)
			if err != nil {
				return 0, err
			}
			if dimension > maxDimension {
				maxDimension = dimension
			}
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

// geometryLengthBySRID computes the length of a geometry in the coordinate
// system selected by srid: geodesic meters for SRID 4326, Cartesian otherwise.
func geometryLengthBySRID(payload []byte, srid uint32) (float64, error) {
	if err := validateComputationSRID(srid); err != nil {
		return 0, err
	}
	if srid == geo.SRIDWGS84 {
		return geodeticLength(payload)
	}
	return geometryLength(payload)
}

func StLength(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stLength[float64](ivecs, result, proc, length, selectList)
}

// StLength32 is the GEOMETRY32 overload of ST_Length (returns float32).
func StLength32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stLength[float32](ivecs, result, proc, length, selectList)
}

func stLength[T float32 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	srid := sridFromTypeWidth(ivecs[0].GetType().Width)
	return opUnaryBytesToFixedWithErrorCheck[T](ivecs, result, proc, length, func(v []byte) (T, error) {
		l, err := geometryLengthBySRID(v, srid)
		return T(l), err
	}, selectList)
}

// StLengthWithSRID is the ST_Length(geom, srid) overload.
func StLengthWithSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stLengthWithSRID[float64](ivecs, result, proc, length, selectList)
}

// StLengthWithSRID32 is the GEOMETRY32 overload of ST_Length(geom, srid).
func StLengthWithSRID32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stLengthWithSRID[float32](ivecs, result, proc, length, selectList)
}

func stLengthWithSRID[T float32 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryStrFixedToFixedWithErrorCheck[int64, T](ivecs, result, proc, length, func(v string, srid int64) (T, error) {
		su, err := sridFromInt64Arg(srid)
		if err != nil {
			return 0, err
		}
		l, err := geometryLengthBySRID(functionUtil.QuickStrToBytes(v), su)
		return T(l), err
	}, selectList)
}

// geometryAreaBySRID computes the area of a geometry in the coordinate system
// selected by srid: geodesic square meters for SRID 4326, Cartesian otherwise.
func geometryAreaBySRID(payload []byte, srid uint32) (float64, error) {
	if err := validateComputationSRID(srid); err != nil {
		return 0, err
	}
	// An empty areal geometry has zero area. ST_Intersection of disjoint
	// polygons yields POLYGON EMPTY, whose coordinate-less WKT both area
	// kernels would otherwise fail to parse ("invalid polygon payload").
	wkt, _, _, err := decodeGeometryPayload(payload)
	if err != nil {
		return 0, err
	}
	if isEmptyGeometryWKT(wkt) {
		return 0, nil
	}
	if srid == geo.SRIDWGS84 {
		return geodeticArea(payload)
	}
	return geometryArea(payload)
}

// isEmptyGeometryWKT reports whether wkt is an empty geometry such as
// "POLYGON EMPTY" or "MULTIPOLYGON EMPTY".
func isEmptyGeometryWKT(wkt string) bool {
	return strings.HasSuffix(strings.ToUpper(strings.TrimSpace(wkt)), " EMPTY")
}

func StArea(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stArea[float64](ivecs, result, proc, length, selectList)
}

// StArea32 is the GEOMETRY32 overload of ST_Area (returns float32).
func StArea32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stArea[float32](ivecs, result, proc, length, selectList)
}

func stArea[T float32 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	srid := sridFromTypeWidth(ivecs[0].GetType().Width)
	return opUnaryBytesToFixedWithErrorCheck[T](ivecs, result, proc, length, func(v []byte) (T, error) {
		a, err := geometryAreaBySRID(v, srid)
		return T(a), err
	}, selectList)
}

// StAreaWithSRID is the ST_Area(geom, srid) overload: the explicit SRID
// overrides the geometry's type SRID for the computation.
func StAreaWithSRID(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stAreaWithSRID[float64](ivecs, result, proc, length, selectList)
}

// StAreaWithSRID32 is the GEOMETRY32 overload of ST_Area(geom, srid).
func StAreaWithSRID32(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return stAreaWithSRID[float32](ivecs, result, proc, length, selectList)
}

func stAreaWithSRID[T float32 | float64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryStrFixedToFixedWithErrorCheck[int64, T](ivecs, result, proc, length, func(v string, srid int64) (T, error) {
		su, err := sridFromInt64Arg(srid)
		if err != nil {
			return 0, err
		}
		a, err := geometryAreaBySRID(functionUtil.QuickStrToBytes(v), su)
		return T(a), err
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

	x, err := parseFiniteCoordinate(coords[0], errMsg)
	if err != nil {
		return 0, 0, moerr.NewInvalidInputNoCtx(errMsg)
	}
	y, err := parseFiniteCoordinate(coords[1], errMsg)
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

func builtInNameConst(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	fromVec := parameters[1]
	toVec := result.GetResultVector()
	toVec.Reset(*toVec.GetType())

	for i := 0; i < length; i++ {
		if err := toVec.UnionOne(fromVec, int64(i), proc.Mp()); err != nil {
			return err
		}
	}
	return nil
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

// VecFromBase64 decodes a base64-encoded string into a vector (vecf32 or vecf64).
// The base64 payload must be the raw little-endian bytes of the vector elements,
// as produced by to_base64(vecf32_col) or to_base64(vecf64_col).
func VecFromBase64[T types.RealNumbers](parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	source := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	var elemSize int
	switch any((*new(T))).(type) {
	case float32:
		elemSize = 4
	case float64:
		elemSize = 8
	}

	// Pre-extend area: peek at the first non-null input to estimate per-row decoded size.
	if length > 0 {
		for j := uint64(0); j < uint64(length); j++ {
			data, null := source.GetStrValue(j)
			if !null {
				decodedSize := base64.StdEncoding.DecodedLen(len(data))
				_ = rs.GetResultVector().PreExtendWithArea(length, decodedSize*length, proc.Mp())
				break
			}
		}
	}

	var buf []byte
	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		data, null := source.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		need := base64.StdEncoding.DecodedLen(len(data))
		if cap(buf) < need {
			buf = make([]byte, need)
		} else {
			buf = buf[:need]
		}
		n, err := base64.StdEncoding.Decode(buf, data)
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("vecf%d_from_base64: invalid base64 input", elemSize*8)
		}

		if n%elemSize != 0 {
			return moerr.NewInternalErrorNoCtxf("vecf%d_from_base64: decoded length %d is not a multiple of %d bytes", elemSize*8, n, elemSize)
		}

		if err = rs.AppendBytes(buf[:n], false); err != nil {
			return err
		}
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

const userLevelLockTableID uint64 = 1 << 62

type userLevelLockKey struct {
	owner string
	name  string
}

var userLevelLocks = struct {
	sync.Mutex
	// userLevelLocks is process-local state for the current CN. It tracks only
	// refcounts and lock names seen by sessions running in this process.
	//owner, lockname -> count
	counts map[userLevelLockKey]uint64
	//owner -> lockername
	byOwner map[string]map[string]struct{}
}{
	counts:  make(map[userLevelLockKey]uint64),
	byOwner: make(map[string]map[string]struct{}),
}

func userLevelLockOwner(proc *process.Process) string {
	if proc == nil || proc.GetSessionInfo() == nil {
		return ""
	}
	si := proc.GetSessionInfo()
	if si.SessionId != uuid.Nil {
		return si.SessionId.String()
	}
	if proc.GetLockService() != nil {
		return fmt.Sprintf("%s:%d", proc.GetLockService().GetServiceID(), si.GetConnectionID())
	}
	return fmt.Sprintf("%d", si.GetConnectionID())
}

func userLevelLockConnectionID(proc *process.Process) uint64 {
	if proc == nil || proc.GetSessionInfo() == nil {
		return 0
	}
	return proc.GetSessionInfo().GetConnectionID()
}

// maxUserLevelLockNameLength is the MySQL-compatible maximum length for
// user-level lock names (GET_LOCK, RELEASE_LOCK, IS_FREE_LOCK, IS_USED_LOCK).
const maxUserLevelLockNameLength = 64

// normalizeUserLevelLockName validates and normalizes a MySQL user-level lock name.
// It returns the normalized (lowercased) name, or an error if the name is empty or
// exceeds maxUserLevelLockNameLength characters (MySQL enforces 64 characters, not bytes).
func normalizeUserLevelLockName(name string) (string, error) {
	if len(name) == 0 {
		return "", moerr.NewInternalErrorNoCtx("user-level lock name must not be empty")
	}
	if strings.IndexByte(name, 0) >= 0 {
		return "", moerr.NewInternalErrorNoCtx("user-level lock name must not contain NUL bytes")
	}
	normalized := strings.ToLower(name)
	if utf8.RuneCountInString(normalized) > maxUserLevelLockNameLength {
		return "", moerr.NewInternalErrorNoCtxf(
			"user-level lock name exceeds maximum length of %d characters",
			maxUserLevelLockNameLength,
		)
	}
	return normalized, nil
}

func userLevelLockTxnID(owner string, connID uint64, name string) []byte {
	return []byte(fmt.Sprintf("mo-user-level-lock\x00%s\x00%s\x00%d", owner, name, connID))
}

func userLevelLockTxnIDOld(owner, name string) []byte {
	return []byte("mo-user-level-lock\x00" + owner + "\x00" + name)
}

func userLevelLockProbeTxnID(owner string, connID uint64, name, probeType string) []byte {
	return []byte(fmt.Sprintf("mo-user-level-lock-probe\x00%s\x00%s\x00%s\x00%d", probeType, owner, name, connID))
}

func userLevelLockConnectionIDFromTxnID(txnID []byte) (uint64, bool) {
	// The user-level lock txnID format uses NUL as a field separator. Lock names
	// are validated to reject NUL bytes, and owners are derived from session IDs
	// / connection IDs, so splitting here is safe for supported inputs.
	parts := strings.Split(string(txnID), "\x00")
	if len(parts) < 3 || len(parts) > 4 || parts[0] != "mo-user-level-lock" {
		return 0, false
	}
	if len(parts) == 3 {
		return 0, false
	}
	connID, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return 0, false
	}
	return connID, true
}

func userLevelLockRow(proc *process.Process, name string) []byte {
	account := ""
	if proc != nil && proc.GetSessionInfo() != nil {
		account = proc.GetSessionInfo().Account
	}
	return []byte(account + "\x00" + name)
}

func userLevelLockService(proc *process.Process) (lockservice.LockService, error) {
	if proc == nil || proc.GetLockService() == nil {
		return nil, moerr.NewInternalErrorNoCtx("GET_LOCK requires lock service")
	}
	return proc.GetLockService(), nil
}

func unlockUserLevelLockTxnIDs(ctx context.Context, ls lockservice.LockService, owner string, connID uint64, name string) error {
	if err := ls.Unlock(ctx, userLevelLockTxnID(owner, connID, name), timestamp.Timestamp{}); err != nil {
		return err
	}
	return ls.Unlock(ctx, userLevelLockTxnIDOld(owner, name), timestamp.Timestamp{})
}

func userLevelLockOptions(policy lockpb.WaitPolicy) lockpb.LockOptions {
	return lockpb.LockOptions{
		Granularity: lockpb.Granularity_Row,
		Mode:        lockpb.LockMode_Exclusive,
		Policy:      policy,
	}
}

func userLevelLockContext(proc *process.Process, timeout float64) (context.Context, context.CancelFunc, lockpb.WaitPolicy) {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	if timeout == 0 {
		return ctx, func() {}, lockpb.WaitPolicy_FastFail
	}
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout*float64(time.Second)))
		return ctx, cancel, lockpb.WaitPolicy_Wait
	}
	return ctx, func() {}, lockpb.WaitPolicy_Wait
}

func userLevelLockConflictOrTimeout(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrLockConflict) ||
		errors.Is(err, lockservice.ErrLockConflict) ||
		errors.Is(err, context.DeadlineExceeded)
}

func trackUserLevelLock(owner, name string) {
	key := userLevelLockKey{owner: owner, name: name}
	userLevelLocks.Lock()
	defer userLevelLocks.Unlock()

	userLevelLocks.counts[key]++
	if userLevelLocks.byOwner[owner] == nil {
		userLevelLocks.byOwner[owner] = make(map[string]struct{})
	}
	userLevelLocks.byOwner[owner][name] = struct{}{}
}

func userLevelLockRefCount(owner, name string) uint64 {
	userLevelLocks.Lock()
	defer userLevelLocks.Unlock()
	return userLevelLocks.counts[userLevelLockKey{owner: owner, name: name}]
}

func untrackUserLevelLock(owner, name string) (uint64, bool) {
	key := userLevelLockKey{owner: owner, name: name}
	userLevelLocks.Lock()
	defer userLevelLocks.Unlock()

	count := userLevelLocks.counts[key]
	if count == 0 {
		return 0, false
	}
	if count > 1 {
		userLevelLocks.counts[key] = count - 1
		return count - 1, true
	}
	delete(userLevelLocks.counts, key)
	if names := userLevelLocks.byOwner[owner]; names != nil {
		delete(names, name)
		if len(names) == 0 {
			delete(userLevelLocks.byOwner, owner)
		}
	}
	return 0, true
}

func untrackAllUserLevelLock(owner, name string) (uint64, bool) {
	key := userLevelLockKey{owner: owner, name: name}
	userLevelLocks.Lock()
	defer userLevelLocks.Unlock()

	count := userLevelLocks.counts[key]
	if count == 0 {
		return 0, false
	}
	delete(userLevelLocks.counts, key)
	if names := userLevelLocks.byOwner[owner]; names != nil {
		delete(names, name)
		if len(names) == 0 {
			delete(userLevelLocks.byOwner, owner)
		}
	}
	return count, true
}

func userLevelLocksForOwner(owner string) []string {
	userLevelLocks.Lock()
	defer userLevelLocks.Unlock()

	names := userLevelLocks.byOwner[owner]
	if len(names) == 0 {
		return nil
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func getUserLevelLock(name string, timeout float64, proc *process.Process) (int64, error) {
	name, err := normalizeUserLevelLockName(name)
	if err != nil {
		return 0, err
	}
	ls, err := userLevelLockService(proc)
	if err != nil {
		return 0, err
	}
	//owner = sessionid or serviceid:connectionid  or  connectionid
	owner := userLevelLockOwner(proc)
	//connectionid
	connID := userLevelLockConnectionID(proc)
	//owner+lockname -> count
	//serviceid:connectionid + lockname -> count
	if userLevelLockRefCount(owner, name) > 0 {
		//count++
		//owner -> lockname
		trackUserLevelLock(owner, name)
		return 1, nil
	}

	ctx, cancel, policy := userLevelLockContext(proc, timeout)
	defer cancel()

	_, err = ls.Lock(
		ctx,
		userLevelLockTableID,
		[][]byte{userLevelLockRow(proc, name)},
		userLevelLockTxnID(owner, connID, name),
		userLevelLockOptions(policy))
	if err != nil {
		if userLevelLockConflictOrTimeout(err) {
			return 0, nil
		}
		return 0, err
	}
	trackUserLevelLock(owner, name)
	return 1, nil
}

// releaseUserLevelLock releases a user-level lock.
// It returns (value, isNull, error):
//   - (1, false, nil): lock was released by this call.
//   - (0, false, nil): lock exists but is held by another session.
//   - (0, true, nil):  lock does not exist (MySQL returns NULL).
func releaseUserLevelLock(name string, proc *process.Process) (int64, bool, error) {
	name, err := normalizeUserLevelLockName(name)
	if err != nil {
		return 0, false, err
	}
	ls, err := userLevelLockService(proc)
	if err != nil {
		return 0, false, err
	}
	owner := userLevelLockOwner(proc)
	connID := userLevelLockConnectionID(proc)
	count := userLevelLockRefCount(owner, name)
	if count == 0 {
		// Probe the lockservice to distinguish "lock does not exist" (NULL)
		// from "lock exists but held by another session" (0).
		_, probeErr := ls.Lock(
			proc.Ctx,
			userLevelLockTableID,
			[][]byte{userLevelLockRow(proc, name)},
			userLevelLockProbeTxnID(owner, connID, name, "release"),
			userLevelLockOptions(lockpb.WaitPolicy_FastFail))
		if probeErr != nil {
			if userLevelLockConflictOrTimeout(probeErr) {
				// Lock exists but is held by another session.
				return 0, false, nil
			}
			return 0, false, probeErr
		}
		// Lock did not exist — we acquired it via the probe. Release it and
		// return NULL to signal the lock was already free.
		if err := ls.Unlock(proc.Ctx, userLevelLockProbeTxnID(owner, connID, name, "release"), timestamp.Timestamp{}); err != nil {
			return 0, false, err
		}
		return 0, true, nil
	}
	if count > 1 {
		untrackUserLevelLock(owner, name)
		return 1, false, nil
	}
	if err := unlockUserLevelLockTxnIDs(proc.Ctx, ls, owner, connID, name); err != nil {
		return 0, false, err
	}
	untrackUserLevelLock(owner, name)
	return 1, false, nil
}

func isUserLevelLockFree(name string, proc *process.Process) (int64, error) {
	name, err := normalizeUserLevelLockName(name)
	if err != nil {
		return 0, err
	}
	ls, err := userLevelLockService(proc)
	if err != nil {
		return 0, err
	}
	owner := userLevelLockOwner(proc)
	connID := userLevelLockConnectionID(proc)
	_, err = ls.Lock(
		proc.Ctx,
		userLevelLockTableID,
		[][]byte{userLevelLockRow(proc, name)},
		userLevelLockProbeTxnID(owner, connID, name, "is_free"),
		userLevelLockOptions(lockpb.WaitPolicy_FastFail))
	if err != nil {
		if userLevelLockConflictOrTimeout(err) {
			return 0, nil
		}
		return 0, err
	}
	if err := ls.Unlock(proc.Ctx, userLevelLockProbeTxnID(owner, connID, name, "is_free"), timestamp.Timestamp{}); err != nil {
		return 0, err
	}
	return 1, nil
}

func isUserLevelLockUsed(name string, proc *process.Process) (uint64, bool, error) {
	name, err := normalizeUserLevelLockName(name)
	if err != nil {
		return 0, false, err
	}
	ls, err := userLevelLockService(proc)
	if err != nil {
		return 0, false, err
	}

	holder, ok, err := ls.GetLockHolder(
		proc.Ctx,
		userLevelLockTableID,
		userLevelLockRow(proc, name),
		userLevelLockOptions(lockpb.WaitPolicy_FastFail))
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			return 0, true, nil
		}
		return 0, false, err
	}
	if !ok {
		return 0, true, nil
	}
	holderConnID, ok := userLevelLockConnectionIDFromTxnID(holder.TxnID)
	if !ok {
		return 0, true, nil
	}
	return holderConnID, false, nil
}

func releaseAllUserLevelLocks(proc *process.Process) (int64, error) {
	if proc == nil || proc.GetLockService() == nil {
		return 0, nil
	}
	owner := userLevelLockOwner(proc)
	connID := userLevelLockConnectionID(proc)
	var released int64
	for _, name := range userLevelLocksForOwner(owner) {
		if userLevelLockRefCount(owner, name) == 0 {
			continue
		}
		if err := unlockUserLevelLockTxnIDs(context.Background(), proc.GetLockService(), owner, connID, name); err != nil {
			logutil.Warn(fmt.Sprintf("releaseAllUserLevelLocks unlock failed: owner=%s lock=%s err=%v", owner, name, err))
			return released, err
		}
		if count, held := untrackAllUserLevelLock(owner, name); held {
			released += int64(count)
		}
	}
	return released, nil
}

func ReleaseUserLevelLocks(proc *process.Process) {
	_, _ = releaseAllUserLevelLocks(proc)
}

func GetLock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	names := vector.GenerateFunctionStrParameter(ivecs[0])
	timeouts := vector.GenerateFunctionFixedTypeParameter[float64](ivecs[1])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		name, nameNull := names.GetStrValue(i)
		timeout, timeoutNull := timeouts.GetValue(i)
		if nameNull || timeoutNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, err := getUserLevelLock(string(name), timeout, proc)
		if err != nil {
			return err
		}
		if err := rs.Append(value, false); err != nil {
			return err
		}
	}
	return nil
}

func ReleaseLock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	names := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		name, null := names.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, isNull, err := releaseUserLevelLock(string(name), proc)
		if err != nil {
			return err
		}
		if err := rs.Append(value, isNull); err != nil {
			return err
		}
	}
	return nil
}

func IsFreeLock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	names := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		name, null := names.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, err := isUserLevelLockFree(string(name), proc)
		if err != nil {
			return err
		}
		if err := rs.Append(value, false); err != nil {
			return err
		}
	}
	return nil
}

func IsUsedLock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	names := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint64](result)

	for i := uint64(0); i < uint64(length); i++ {
		name, null := names.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, isNull, err := isUserLevelLockUsed(string(name), proc)
		if err != nil {
			return err
		}
		if err := rs.Append(value, isNull); err != nil {
			return err
		}
	}
	return nil
}

func ReleaseAllLocks(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[int64](rsVec)

	for i := 0; i < length; i++ {
		released, err := releaseAllUserLevelLocks(proc)
		if err != nil {
			return err
		}
		rss[i] = released
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

func HllCardinality(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[uint64](parameters, result, proc, length, func(v []byte) (uint64, error) {
		sketch := hll.NewNoSparse()
		if err := sketch.UnmarshalBinary(v); err != nil {
			return 0, moerr.NewInvalidInputf(proc.Ctx, "invalid HLL sketch: %v", err)
		}
		return sketch.Estimate(), nil
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
