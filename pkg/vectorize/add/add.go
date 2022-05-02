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

package add

/*
#include <stdint.h>

// The test checks
// 	 	if z[i] and tmpx have different sign, and z[i] and tmpy have different sign
//		therefore tmpx and tmpy has same sign but the result has different sign.
// 		therefore it is an overflow.
// Then we set the sign bit of flag.
//
// Do not use if (flag < 0) return in the loop, which gcc cannot vectorize.
// Do not use __builtin_add_overflow, which gcc cannot vectorize.

#define ADDINT_T(ZT)                     			\
	ZT flag = 0; 									\
	for (int32_t i = 0; i < len; i++) {             \
		ZT tmpx = x[i]; 							\
		ZT tmpy = y[i]; 							\
		z[i] = tmpx + tmpy; 						\
		flag |= (z[i] ^ tmpx) & (z[i] ^ tmpy); 		\
	}                                              	\
	return flag < 0 ? -1 : len

static int32_t cInt8Add(int8_t *x, int8_t *y, int8_t *z, int32_t len) {
	ADDINT_T(int8_t);
}

static int32_t cInt16Add(int16_t *x, int16_t *y, int16_t *z, int32_t len) {
	ADDINT_T(int16_t);
}

static int32_t cInt32Add(int32_t *x, int32_t *y, int32_t *z, int32_t len) {
	ADDINT_T(int32_t);
}

static int32_t cInt64Add(int64_t *x, int64_t *y, int64_t *z, int32_t len) {
	ADDINT_T(int64_t);
}

#define ADDUINT_T(ZT)                     			\
	ZT flag = 0; 									\
	for (int32_t i = 0; i < len; i++) {             \
		ZT tmpx = x[i]; 							\
		ZT tmpy = y[i]; 							\
		z[i] = tmpx + tmpy; 						\
		if (z[i] < tmpx) { 							\
			flag = 1; 								\
		}                                           \
	}                                              	\
	return flag == 0 ? len : -1

static int32_t cUint8Add(uint8_t *x, uint8_t *y, uint8_t *z, int32_t len) {
	ADDUINT_T(uint8_t);
}
static int32_t cUint16Add(uint16_t *x, uint16_t *y, uint16_t *z, int32_t len) {
	ADDUINT_T(uint16_t);
}
static int32_t cUint32Add(uint32_t *x, uint32_t *y, uint32_t *z, int32_t len) {
	ADDUINT_T(uint32_t);
}
static int32_t cUint64Add(uint64_t *x, uint64_t *y, uint64_t *z, int32_t len) {
	ADDUINT_T(uint64_t);
}
*/
import "C"

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

// Exported functions.
var (
	Int8Add              = cInt8Add
	Int8AddScalar        = numericAddScalar[int8]
	Int16Add             = cInt16Add
	Int16AddScalar       = numericAddScalar[int16]
	Int32Add             = cInt32Add
	Int32AddScalar       = numericAddScalar[int32]
	Int64Add             = cInt64Add
	Int64AddScalar       = numericAddScalar[int64]
	Uint8Add             = cUint8Add
	Uint8AddScalar       = numericAddScalar[uint8]
	Uint16Add            = cUint16Add
	Uint16AddScalar      = numericAddScalar[uint16]
	Uint32Add            = cUint32Add
	Uint32AddScalar      = numericAddScalar[uint32]
	Uint64Add            = cUint64Add
	Uint64AddScalar      = numericAddScalar[uint64]
	Float32Add           = numericAdd[float32]
	Float32AddScalar     = numericAddScalar[float32]
	Float64Add           = numericAdd[float64]
	Float64AddScalar     = numericAddScalar[float64]
	Int8AddSels          = numericAddSels[int8]
	Int8AddScalarSels    = numericAddScalarSels[int8]
	Int16AddSels         = numericAddSels[int16]
	Int16AddScalarSels   = numericAddScalarSels[int16]
	Int32AddSels         = numericAddSels[int32]
	Int32AddScalarSels   = numericAddScalarSels[int32]
	Int64AddSels         = numericAddSels[int64]
	Int64AddScalarSels   = numericAddScalarSels[int64]
	Uint8AddSels         = numericAddSels[uint8]
	Uint8AddScalarSels   = numericAddScalarSels[uint8]
	Uint16AddSels        = numericAddSels[uint16]
	Uint16AddScalarSels  = numericAddScalarSels[uint16]
	Uint32AddSels        = numericAddSels[uint32]
	Uint32AddScalarSels  = numericAddScalarSels[uint32]
	Uint64AddSels        = numericAddSels[uint64]
	Uint64AddScalarSels  = numericAddScalarSels[uint64]
	Float32AddSels       = numericAddSels[float32]
	Float32AddScalarSels = numericAddScalarSels[float32]
	Float64AddSels       = numericAddSels[float64]
	Float64AddScalarSels = numericAddScalarSels[float64]

	Int32Int64Add               = numericAdd2[int32, int64]
	Int32Int64AddScalar         = numericAddScalar2[int32, int64]
	Int32Int64AddSels           = numericAddSels2[int32, int64]
	Int32Int64AddScalarSels     = numericAddScalarSels2[int32, int64]
	Int16Int64Add               = numericAdd2[int16, int64]
	Int16Int64AddScalar         = numericAddScalar2[int16, int64]
	Int16Int64AddSels           = numericAddSels2[int16, int64]
	Int16Int64AddScalarSels     = numericAddScalarSels2[int16, int64]
	Int8Int64Add                = numericAdd2[int8, int64]
	Int8Int64AddScalar          = numericAddScalar2[int8, int64]
	Int8Int64AddSels            = numericAddSels2[int8, int64]
	Int8Int64AddScalarSels      = numericAddScalarSels2[int8, int64]
	Int16Int32Add               = numericAdd2[int16, int32]
	Int16Int32AddScalar         = numericAddScalar2[int16, int32]
	Int16Int32AddSels           = numericAddSels2[int16, int32]
	Int16Int32AddScalarSels     = numericAddScalarSels2[int16, int32]
	Int8Int32Add                = numericAdd2[int8, int32]
	Int8Int32AddScalar          = numericAddScalar2[int8, int32]
	Int8Int32AddSels            = numericAddSels2[int8, int32]
	Int8Int32AddScalarSels      = numericAddScalarSels2[int8, int32]
	Int8Int16Add                = numericAdd2[int8, int16]
	Int8Int16AddScalar          = numericAddScalar2[int8, int16]
	Int8Int16AddSels            = numericAddSels2[int8, int16]
	Int8Int16AddScalarSels      = numericAddScalarSels2[int8, int16]
	Float32Float64Add           = numericAdd2[float32, float64]
	Float32Float64AddScalar     = numericAddScalar2[float32, float64]
	Float32Float64AddSels       = numericAddSels2[float32, float64]
	Float32Float64AddScalarSels = numericAddScalarSels2[float32, float64]
	Uint32Uint64Add             = numericAdd2[uint32, uint64]
	Uint32Uint64AddScalar       = numericAddScalar2[uint32, uint64]
	Uint32Uint64AddSels         = numericAddSels2[uint32, uint64]
	Uint32Uint64AddScalarSels   = numericAddScalarSels2[uint32, uint64]
	Uint16Uint64Add             = numericAdd2[uint16, uint64]
	Uint16Uint64AddScalar       = numericAddScalar2[uint16, uint64]
	Uint16Uint64AddSels         = numericAddSels2[uint16, uint64]
	Uint16Uint64AddScalarSels   = numericAddScalarSels2[uint16, uint64]
	Uint8Uint64Add              = numericAdd2[uint8, uint64]
	Uint8Uint64AddScalar        = numericAddScalar2[uint8, uint64]
	Uint8Uint64AddSels          = numericAddSels2[uint8, uint64]
	Uint8Uint64AddScalarSels    = numericAddScalarSels2[uint8, uint64]
	Uint16Uint32Add             = numericAdd2[uint16, uint32]
	Uint16Uint32AddScalar       = numericAddScalar2[uint16, uint32]
	Uint16Uint32AddSels         = numericAddSels2[uint16, uint32]
	Uint16Uint32AddScalarSels   = numericAddScalarSels2[uint16, uint32]
	Uint8Uint32Add              = numericAdd2[uint8, uint32]
	Uint8Uint32AddScalar        = numericAddScalar2[uint8, uint32]
	Uint8Uint32AddSels          = numericAddSels2[uint8, uint32]
	Uint8Uint32AddScalarSels    = numericAddScalarSels2[uint8, uint32]
	Uint8Uint16Add              = numericAdd2[uint8, uint16]
	Uint8Uint16AddScalar        = numericAddScalar2[uint8, uint16]
	Uint8Uint16AddSels          = numericAddSels2[uint8, uint16]
	Uint8Uint16AddScalarSels    = numericAddScalarSels2[uint8, uint16]

	Decimal64Add            = decimal64Add
	Decimal64AddSels        = decimal64AddSels
	Decimal64AddScalar      = decimal64AddScalar
	Decimal64AddScalarSels  = decimal64AddScalarSels
	Decimal128Add           = decimal128Add
	Decimal128AddSels       = decimal128AddSels
	Decimal128AddScalar     = decimal128AddScalar
	Decimal128AddScalarSels = decimal128AddScalarSels
)

func cInt8Add(x, y, z []int8) []int8 {
	ret := C.cInt8Add((*C.int8_t)(unsafe.Pointer(&x[0])), (*C.int8_t)(unsafe.Pointer(&y[0])), (*C.int8_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
	}
	return z
}

func cInt16Add(x, y, z []int16) []int16 {
	ret := C.cInt16Add((*C.int16_t)(unsafe.Pointer(&x[0])), (*C.int16_t)(unsafe.Pointer(&y[0])), (*C.int16_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
	}
	return z
}

func cInt32Add(x, y, z []int32) []int32 {
	ret := C.cInt32Add((*C.int32_t)(unsafe.Pointer(&x[0])), (*C.int32_t)(unsafe.Pointer(&y[0])), (*C.int32_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
	}
	return z
}

func cInt64Add(x, y, z []int64) []int64 {
	ret := C.cInt64Add((*C.int64_t)(unsafe.Pointer(&x[0])), (*C.int64_t)(unsafe.Pointer(&y[0])), (*C.int64_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "int add overflow"))
	}
	return z
}

func cUint8Add(x, y, z []uint8) []uint8 {
	ret := C.cUint8Add((*C.uint8_t)(unsafe.Pointer(&x[0])), (*C.uint8_t)(unsafe.Pointer(&y[0])), (*C.uint8_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow"))
	}
	return z
}

func cUint16Add(x, y, z []uint16) []uint16 {
	ret := C.cUint16Add((*C.uint16_t)(unsafe.Pointer(&x[0])), (*C.uint16_t)(unsafe.Pointer(&y[0])), (*C.uint16_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow"))
	}
	return z
}

func cUint32Add(x, y, z []uint32) []uint32 {
	ret := C.cUint32Add((*C.uint32_t)(unsafe.Pointer(&x[0])), (*C.uint32_t)(unsafe.Pointer(&y[0])), (*C.uint32_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow"))
	}
	return z
}

func cUint64Add(x, y, z []uint64) []uint64 {
	ret := C.cUint64Add((*C.uint64_t)(unsafe.Pointer(&x[0])), (*C.uint64_t)(unsafe.Pointer(&y[0])), (*C.uint64_t)(unsafe.Pointer(&z[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "uint add overflow"))
	}
	return z
}

func numericAdd[T constraints.Integer | constraints.Float](xs, ys, rs []T) []T {
	for i, x := range xs {
		rs[i] = x + ys[i]
	}
	return rs
}

func numericAddSels[T constraints.Integer | constraints.Float](xs, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = xs[sel] + ys[sel]
	}
	return rs
}

func numericAddScalar[T constraints.Integer | constraints.Float](x T, ys, rs []T) []T {
	for i, y := range ys {
		rs[i] = x + y
	}
	return rs
}

func numericAddScalarSels[T constraints.Integer | constraints.Float](x T, ys, rs []T, sels []int64) []T {
	for i, sel := range sels {
		rs[i] = x + ys[sel]
	}
	return rs
}

func numericAdd2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig) []TBig {
	for i, x := range xs {
		rs[i] = TBig(x) + ys[i]
	}
	return rs
}

func numericAddSels2[TSmall, TBig constraints.Integer | constraints.Float](xs []TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(xs[sel]) + ys[sel]
	}
	return rs
}

func numericAddScalar2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig) []TBig {
	for i, y := range ys {
		rs[i] = TBig(x) + y
	}
	return rs
}

func numericAddScalarSels2[TSmall, TBig constraints.Integer | constraints.Float](x TSmall, ys, rs []TBig, sels []int64) []TBig {
	for i, sel := range sels {
		rs[i] = TBig(x) + ys[sel]
	}
	return rs
}

func decimal64Add(xs []types.Decimal64, ys []types.Decimal64, xsScale int32, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	/* to add two decimal64 value, first we need to align them to the same scale(the maximum of the two)
																	Decimal(10, 5), Decimal(10, 6)
	value																321.4			123.5
	representation														32,140,000		123,500,000
	align to the same scale	by scale 32,140,000 by 10 					321,400,000		123,500,000
	add
	*/
	if xsScale > ysScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xsScale - ysScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, x := range xs {
			rs[i] = types.Decimal64AddAligned(x, ysScaled[i])
		}
		return rs
	} else if xsScale < ysScale {
		xsScaled := make([]types.Decimal64, len(xs))
		scaleDiff := ysScale - xsScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, x := range xs {
			xsScaled[i] = types.ScaleDecimal64(x, scale)
		}
		for i, y := range ys {
			rs[i] = types.Decimal64AddAligned(xsScaled[i], y)
		}
		return rs
	} else {
		for i, x := range xs {
			rs[i] = types.Decimal64AddAligned(x, ys[i])
		}
		return rs
	}
}

func decimal64AddSels(xs, ys []types.Decimal64, xsScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Add(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal64AddScalar(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64) []types.Decimal64 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal64, len(ys))
		scaleDiff := xScale - ysScale
		scale := int64(math.Pow10(int(scaleDiff)))
		for i, y := range ys {
			ysScaled[i] = types.ScaleDecimal64(y, scale)
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal64AddAligned(x, yScaled)
		}
		return rs
	} else if xScale < ysScale {
		scaleDiff := ysScale - xScale
		scale := int64(math.Pow10(int(scaleDiff)))
		xScaled := types.ScaleDecimal64(x, scale)
		for i, y := range ys {
			rs[i] = types.Decimal64AddAligned(xScaled, y)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal64AddAligned(x, y)
		}
		return rs
	}
}

func decimal64AddScalarSels(x types.Decimal64, ys []types.Decimal64, xScale, ysScale int32, rs []types.Decimal64, sels []int64) []types.Decimal64 {
	for i, sel := range sels {
		rs[i] = types.Decimal64Add(x, ys[sel], xScale, ysScale)
	}
	return rs
}

func decimal128Add(xs []types.Decimal128, ys []types.Decimal128, xsScale int32, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	/* to add two decimal128 value, first we need to align them to the same scale(the maximum of the two)
																	Decimal(20, 5), Decimal(20, 6)
	value																321.4			123.5
	representation														32,140,000		123,500,000
	align to the same scale	by scale 12340000 by 10 321400000			321,400,000		123,500,000
	add

	*/
	if xsScale > ysScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xsScale - ysScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, x := range xs {
			rs[i] = types.Decimal128AddAligned(x, ysScaled[i])
		}
		return rs
	} else if xsScale < ysScale {
		xsScaled := make([]types.Decimal128, len(xs))
		scaleDiff := ysScale - xsScale
		for i, x := range xs {
			xsScaled[i] = x
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				xsScaled[i] = types.ScaleDecimal128By10(xsScaled[i])
			}
		}
		for i, y := range ys {
			rs[i] = types.Decimal128AddAligned(xsScaled[i], y)
		}
		return rs
	} else {
		for i, x := range xs {
			rs[i] = types.Decimal128AddAligned(x, ys[i])
		}
		return rs
	}
}

func decimal128AddSels(xs, ys []types.Decimal128, xsScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Add(xs[sel], ys[sel], xsScale, ysScale)
	}
	return rs
}

func decimal128AddScalar(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128) []types.Decimal128 {
	if xScale > ysScale {
		ysScaled := make([]types.Decimal128, len(ys))
		scaleDiff := xScale - ysScale
		for i, y := range ys {
			ysScaled[i] = y
			// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
			for j := 0; j < int(scaleDiff); j++ {
				ysScaled[i] = types.ScaleDecimal128By10(ysScaled[i])
			}
		}
		for i, yScaled := range ysScaled {
			rs[i] = types.Decimal128AddAligned(x, yScaled)
		}
		return rs
	} else if xScale < ysScale {
		xScaled := x
		scaleDiff := ysScale - xScale
		// since the possible scale difference is (0, 38], and 10**38 can not fit in a int64, double loop is necessary
		for i := 0; i < int(scaleDiff); i++ {
			xScaled = types.ScaleDecimal128By10(xScaled)
		}
		for i, y := range ys {
			rs[i] = types.Decimal128AddAligned(xScaled, y)
		}
		return rs
	} else {
		for i, y := range ys {
			rs[i] = types.Decimal128AddAligned(x, y)
		}
		return rs
	}
}

func decimal128AddScalarSels(x types.Decimal128, ys []types.Decimal128, xScale, ysScale int32, rs []types.Decimal128, sels []int64) []types.Decimal128 {
	for i, sel := range sels {
		rs[i] = types.Decimal128Add(x, ys[sel], xScale, ysScale)
	}
	return rs
}
