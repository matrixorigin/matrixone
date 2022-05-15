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
#ifdef __ARM_NEON

#include "arm_neon.h"
typedef signed char GoInt8;
typedef unsigned char GoUint8;
typedef short GoInt16;
typedef unsigned short GoUint16;
typedef int GoInt32;
typedef unsigned int GoUint32;
typedef long long GoInt64;
typedef unsigned long long GoUint64;
typedef GoInt64 GoInt;
typedef GoUint64 GoUint;
typedef float GoFloat32;
typedef double GoFloat64;

void int8AddArm(GoInt8* x, GoInt8* y, GoInt8* r, int len) {
	int i = 0;
	for (; i <= len - 16; i += 16)
	{
		*(int8x16_t*)(r + i) = vaddq_s8(*(int8x16_t*)(x + i), *(int8x16_t*)(y + i));
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *(y + i);
}

void int16AddArm(GoInt16* x, GoInt16* y, GoInt16* r, int len) {
	int i = 0;
	for (; i <= len - 8; i += 8) {
		*(int16x8_t*)(r + i) = vaddq_s16(*(int16x8_t*)(x + i), *(int16x8_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void int32AddArm(GoInt32* x, GoInt32* y, GoInt32* r, int len) {
	int i = 0;
	for (; i <= len - 4; i += 4)
	{
		*(int32x4_t*)(r + i) = vaddq_s32(*(int32x4_t*)(x + i), *(int32x4_t*)(y + i));
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *(y + i);
}

void int64AddArm(GoInt64* x, GoInt64* y, GoInt64* r, int len) {
	int i = 0;
	for (; i <= len - 2; i += 2) {
		*(int64x2_t*)(r + i) = vaddq_s64(*(int64x2_t*)(x + i), *(int64x2_t*)(y + i));
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *(y + i);
}

void uint8AddArm(GoUint8* x, GoUint8* y, GoUint8* r, int len)
{
	int i = 0;
	for (; i <= len - 16; i += 16) {
		*(uint8x16_t*)(r + i) = vaddq_u8(*(uint8x16_t*)(x + i), *(uint8x16_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void uint16AddArm(GoUint16* x, GoUint16* y, GoUint16* r, int len) {
	int i = 0;
	for (; i <= len - 8; i += 8) {
		*(uint16x8_t*)(r + i) = vaddq_u16(*(uint16x8_t*)(x + i), *(uint16x8_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void uint32AddArm(GoUint32* x, GoUint32* y, GoUint32* r, int len) {
	int i = 0;
	for (; i <= len - 4; i += 4) {
		*(uint32x4_t*)(r + i) = vaddq_u32(*(uint32x4_t*)(x + i), *(uint32x4_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);

}

void uint64AddArm(GoUint64* x, GoUint64* y, GoUint64* r, int len) {
	int i = 0;
	for (; i <= len - 2; i += 2) {
		*(uint64x2_t*)(r + i) = vaddq_u64(*(uint64x2_t*)(x + i), *(uint64x2_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void float32AddArm(GoFloat32* x, GoFloat32* y, GoFloat32* r, int len) {
	int i = 0;
	for (; i <= len - 4; i += 4) {
		*(float32x4_t*)(r + i) = vaddq_f32(*(float32x4_t*)(x + i), *(float32x4_t*)(y + i));
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void float64AddArm(GoFloat64* x, GoFloat64* y, GoFloat64* r, int len) {
	int i = 0;
	for (; i < len; ++i)  *(r + i) = *(x + i) + *(y + i);
}

void int8AddScalarArm(GoInt8* x, GoInt8* y, GoInt8* r, int len) {
	int i = 0;
	int8x16_t dup = vdupq_n_s8(*(int8_t*)y);
	for (; i <= len - 16; i += 16) {
		*(int8x16_t*)(r + i) = vaddq_s8(*(int8x16_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void int16AddScalarArm(GoInt16* x, GoInt16* y, GoInt16* r, int len) {
	int i = 0; int16x8_t dup = vdupq_n_s16(*(int16_t*)y);
	for (; i <= len - 8; i += 8) {
		*(int16x8_t*)(r + i) = vaddq_s16(*(int16x8_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void int32AddScalarArm(GoInt32* x, GoInt32* y, GoInt32* r, int len) {
	int i = 0;
	int32x4_t dup = vdupq_n_s32(*(int32_t*)y);
	for (; i <= len - 4; i += 4) {
		*(int32x4_t*)(r + i) = vaddq_s32(*(int32x4_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void int64AddScalarArm(GoInt64* x, GoInt64* y, GoInt64* r, int len) {
	int i = 0;
	int64x2_t dup = vdupq_n_s64(*(int64_t*)y);
	for (; i <= len - 2; i += 2) {
		*(int64x2_t*)(r + i) = vaddq_s64(*(int64x2_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void uint8AddScalarArm(GoUint8* x, GoUint8* y, GoUint8* r, int len) {
	int i = 0;
	uint8x16_t dup = vdupq_n_u8(*(uint8_t*)y);
	for (; i <= len - 16; i += 16) {
		*(uint8x16_t*)(r + i) = vaddq_u8(*(uint8x16_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void uint16AddScalarArm(GoUint16* x, GoUint16* y, GoUint16* r, int len) {
	int i = 0;
	uint16x8_t dup = vdupq_n_u16(*(uint16_t*)y);
	for (; i <= len - 8; i += 8) {
		*(uint16x8_t*)(r + i) = vaddq_u16(*(uint16x8_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void uint32AddScalarArm(GoUint32* x, GoUint32* y, GoUint32* r, int len) {
	int i = 0;
	uint32x4_t dup = vdupq_n_u32(*(uint32_t*)y);
	for (; i <= len - 4; i += 4) {
		*(uint32x4_t*)(r + i) = vaddq_u32(*(uint32x4_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void uint64AddScalarArm(GoUint64* x, GoUint64* y, GoUint64* r, int len) {
	int i = 0;
	uint64x2_t dup = vdupq_n_u64(*(uint64_t*)y);
	for (; i <= len - 2; i += 2) {
		*(uint64x2_t*)(r + i) = vaddq_u64(*(uint64x2_t*)(x + i), dup);
	}
	for (; i < len; ++i) *(r + i) = *(x + i) + *y;
}

void float32AddScalarArm(GoFloat32* x, GoFloat32* y, GoFloat32* r, int len) {
	int i = 0;
	float32x4_t dup = vdupq_n_f32(*(float32_t*)y);
	for (; i <= len - 4; i += 4) {
		*(float32x4_t*)(r + i) = vaddq_f32(*(float32x4_t*)(x + i), dup);
	}
	for (; i < len; ++i)  *(r + i) = *(x + i) + *y;
}

void float64AddScalarArm(GoFloat64* x, GoFloat64* y, GoFloat64* r, int len) {
	int i = 0;
	for (; i < len; ++i)  *(r + i) = *(x + i) + *y;
}
#endif
*/
import "C"

int8AddArm(x[]int8, y[]int8, r[]int8)
int16AddArm(x[]int16, y[]int16, r[]int16)
int32AddArm(x[]int32, y[]int32, r[]int32)
int64AddArm(x[]int64, y[]int64, r[]int64)
Uint8AddArm(x[]Uint8, y[]Uint8, r[]Uint8)
Uint16AddArm(x[]Uint16, y[]Uint16, r[]Uint16)
Uint32AddArm(x[]Uint32, y[]Uint32, r[]Uint32)
Uint64AddArm(x[]Uint64, y[]Uint64, r[]Uint64)
float32AddArm(x[]float32, y[]float32, r[]float32)
float64AddArm(x[]float64, y[]float64, r[]float64)
int8AddScalarArm(x int8, y[]int8, r[]int8)
int16AddScalarArm(x int16, y[]int16, r[]int16)
int32AddScalarArm(x int32, y[]int32, r[]int32)
int64AddScalarArm(x int64, y[]int64, r[]int64)
uint8AddScalarArm(x uint8, y[]uint8, r[]uint8)
uint16AddScalarArm(x uint16, y[]uint16, r[]uint16)
uint32AddScalarArm(x uint32, y[]uint32, r[]uint32)
uint64AddScalarArm(x uint64, y[]uint64, r[]uint64)
float32AddScalarArm(x float32, y[]float32, r[]float32)
float64AddScalarArm(x float64, y[]float64, r[]float64)


Int8Add = int8AddArm
Int8AddScalar = int8AddScalarArm
Int16Add = int16AddArm
Int16AddScalar = int16AddScalarArm
Int32Add = int32AddArm
Int32AddScalar = int32AddScalarArm
Int64Add = int64AddArm
Int64AddScalar = int64AddScalarArm
Uint8Add = uint8AddArm
Uint8AddScalar = uint8AddScalarArm
Uint16Add = uint16AddArm
Uint16AddScalar = uint16AddScalarArm
Uint32Add = uint32AddArm
Uint32AddScalar = uint32AddScalarArm
Uint64Add = uint64AddArm
Uint64AddScalar = uint64AddScalarArm
Float32Add = float32AddArm
Float32AddScalar = float32AddScalarArm
Float64Add = float64AddArm
Float64AddScalar = float64AddScalarArm

func int8AddArm(x, y, r []int8)[]int8{ 
    C.int8AddArm(&x[0], &y[0], &r[0], C.int(len(r)))
    return r 
}
func int16AddArm(x, y, r []int16)[]int16{ 
    C.int16AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func int32AddArm(x, y, r []int32)[]int32{ 
    C.int32AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func int64AddArm(x, y, r []int64)[]int64{ 
    C.int64AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func uint8AddArm(x, y, r[]uint8)[]uint8{ 
    C.uint8AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func uint16AddArm(x, y, r[]uint16)[]uint16{ 
    C.uint16AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func uint32AddArm(x, y, r[]uint32)[]uint32{ 
    C.uint32AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}
func uint64AddArm(x, y, r[]uint64)[]uint64{ 
    C.uint64AddArm(&x[0], &y[0], &r[0], C.int(len(r))) 
    return r 
}

func int8AddScalarArm(x int8, y, r[]int8)[]int8{ 
	C.int8AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func int16AddScalarArm(x int16, y, r[]int16)[]int16{ 
	C.int16AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func int32AddScalarArm(x int32, y, r[]int32)[]int32{ 
	C.int32AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func int64AddScalarArm(x int64, y, r[]int64)[]int64{ 
	C.int64AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func uint8AddScalarArm(x uint8, y, r[]uint8)[]uint8{ 
	C.uint8AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func uint16AddScalarArm(x uint16, y, r[]uint16)[]uint16{ 
	C.uint16AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func uint32AddScalarArm(x uint32, y, r[]uint32)[]uint32{ 
	C.uint32AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func uint64AddScalarArm(x uint64, y, r[]uint64)[]uint64{
	C.uint64AddScalarArm(&y[0], &x, &r[0], C.int(len(r))) 
	return r
}
func float32AddArm(x, y, r []float32)[]float32{
	C.float32AddArm(&x[0],&y[0], &r[0], C.int(len(r))) 
	return r
}
func float64AddArm(x, y, r []float64)[]float64{
	C.float64AddArm(&x[0],&y[0], &r[0], C.int(len(r))) 
	return r
}
func float32AddScalarArm(x float32, y, r []float32)[]float32{
	C.float32AddScalarArm(&y[0],&x, &r[0], C.int(len(r))) 
	return r
}
func float64AddScalarArm(x float64, y, r []float64)[]float64{
	C.float64AddScalarArm(&y[0],&x, &r[0], C.int(len(r))) 
	return r
}
