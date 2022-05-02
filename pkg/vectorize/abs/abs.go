// Copyright 2022 Matrix Origin
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

package abs

/*
#include <stdint.h>
#include <stdlib.h>

#define ABS_IT(T)     					\
	T offlag = 0; 						\
	for (int32_t i = 0; i < len; i++) { \
		y[i] = x[i] < 0 ? -x[i] : x[i]; \
		offlag |= y[i]; 				\
	} 									\
	return offlag < 0 ? -1 : len 		\

static int32_t CAbsInt64(int64_t *x, int64_t *y, int32_t len) {
	ABS_IT(int64_t);
}

static int32_t CAbsInt32(int32_t *x, int32_t *y, int32_t len) {
	ABS_IT(int32_t);
}

static int32_t CAbsInt16(int16_t *x, int16_t *y, int32_t len) {
	ABS_IT(int16_t);
}

static int32_t CAbsInt8(int8_t *x, int8_t *y, int32_t len) {
	ABS_IT(int8_t);
}

#define ABS_FT							\
	for (int32_t i = 0; i < len; i++) { \
		y[i] = x[i] < 0 ? -x[i] : x[i]; \
	}                                   \
	return len                          \


static int32_t CAbsFloat64(double *x, double *y, int32_t len) {
	ABS_FT;
}

static int32_t CAbsFloat32(float *x, float *y, int32_t len) {
	ABS_FT;
}
*/
import "C"
import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"golang.org/x/exp/constraints"
)

// Exported functions.
var (
	AbsUint8   func([]uint8, []uint8) []uint8
	AbsUint16  func([]uint16, []uint16) []uint16
	AbsUint32  func([]uint32, []uint32) []uint32
	AbsUint64  func([]uint64, []uint64) []uint64
	AbsInt8    func([]int8, []int8) []int8
	AbsInt16   func([]int16, []int16) []int16
	AbsInt32   func([]int32, []int32) []int32
	AbsInt64   func([]int64, []int64) []int64
	AbsFloat32 func([]float32, []float32) []float32
	AbsFloat64 func([]float64, []float64) []float64
)

func init() {
	AbsUint8 = absUnsigned[uint8]
	AbsUint16 = absUnsigned[uint16]
	AbsUint32 = absUnsigned[uint32]
	AbsUint64 = absUnsigned[uint64]
	AbsInt8 = absInt8
	AbsInt16 = absInt16
	AbsInt32 = absInt32
	AbsInt64 = absInt64
	AbsFloat32 = absFloat32
	AbsFloat64 = absFloat64
}

// Unsigned simply return
func absUnsigned[T constraints.Unsigned](xs, rs []T) []T {
	return xs
}

func absInt64(x, y []int64) []int64 {
	ret := C.CAbsInt64((*C.int64_t)(unsafe.Pointer(&x[0])), (*C.int64_t)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int64 out of range"))
	}
	return y
}

func absInt32(x, y []int32) []int32 {
	ret := C.CAbsInt32((*C.int32_t)(unsafe.Pointer(&x[0])), (*C.int32_t)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int32 out of range"))
	}
	return y
}

func absInt16(x, y []int16) []int16 {
	ret := C.CAbsInt16((*C.int16_t)(unsafe.Pointer(&x[0])), (*C.int16_t)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int16 out of range"))
	}
	return y
}

func absInt8(x, y []int8) []int8 {
	ret := C.CAbsInt8((*C.int8_t)(unsafe.Pointer(&x[0])), (*C.int8_t)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	if ret < 0 {
		panic(moerr.NewError(moerr.OUT_OF_RANGE, "abs int8 out of range"))
	}
	return y
}

func absFloat64(x, y []float64) []float64 {
	C.CAbsFloat64((*C.double)(unsafe.Pointer(&x[0])), (*C.double)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	return y
}

func absFloat32(x, y []float32) []float32 {
	C.CAbsFloat32((*C.float)(unsafe.Pointer(&x[0])), (*C.float)(unsafe.Pointer(&y[0])), C.int32_t(len(x)))
	return y
}
