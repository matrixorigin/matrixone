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

package sign

/*
#include<stdio.h>
void sign_int8(void *a,void * res){
	signed char tmp = *(signed char *)a;
	*(signed char*)res = (tmp > 0) - (tmp < 0);
}
void sign_uint8(void *a,void * res){
	*(signed char*)res = *(signed char *)a > 0;
}
void sign_int16(void *a,void * res){
	short tmp = *(short *)a;
	*(signed char*)res = (tmp > 0) - (tmp < 0);
}
void sign_uint16(void *a,void * res){
	*(signed char*)res = *(unsigned short*)a>0;
}
void sign_int32(void *a,void * res){
	int tmp = *(int*)a;
	*(signed char*)res = (tmp > 0) - (tmp < 0);
}
void sign_uint32(void *a,void * res){
	*(signed char*)res = *(unsigned int*)a > 0;
}
void sign_int64(void *a,void * res){
	long long int tmp = *(long long int *)a;
	*(signed char*)res = (tmp > 0) - (tmp<0);
}
void sign_uint64(void *a,void * res){
	*(signed char*)res = *(unsigned long long int*)a > 0;
}
void sign_float32(void *a,void * res){
	float tmp = *(float*)a;
	*(signed char*)res = (tmp > 0) - (tmp < 0);
}
void sign_float64(void *a,void *res){
	double tmp = *(double *)a;
	*(signed char*)res = (tmp > 0) - (tmp < 0);
}
*/
import "C"
import "unsafe"

var (
	SignUint8   func([]uint8, []int8) []int8
	SignUint16  func([]uint16, []int8) []int8
	SignUint32  func([]uint32, []int8) []int8
	SignUint64  func([]uint64, []int8) []int8
	SignInt8    func([]int8, []int8) []int8
	SignInt16   func([]int16, []int8) []int8
	SignInt32   func([]int32, []int8) []int8
	SignInt64   func([]int64, []int8) []int8
	SignFloat32 func([]float32, []int8) []int8
	SignFloat64 func([]float64, []int8) []int8
)

func init() {
	SignUint8 = signUint8
	SignUint16 = signUint16
	SignUint32 = signUint32
	SignUint64 = signUint64
	SignInt8 = signInt8
	SignInt16 = signInt16
	SignInt32 = signInt32
	SignInt64 = signInt64
	SignFloat32 = signFloat32
	SignFloat64 = signFloat64
}

// Sign function
func signUint8(xs []uint8, rs []int8) []int8 {
	for i := range xs {
		C.sign_uint8(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}

func signUint16(xs []uint16, rs []int8) []int8 {
	for i := range xs {
		C.sign_uint16(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}

func signUint32(xs []uint32, rs []int8) []int8 {
	for i := range xs {
		C.sign_uint32(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}

func signUint64(xs []uint64, rs []int8) []int8 {
	for i := range xs {
		C.sign_uint64(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}

func signInt8(xs []int8, rs []int8) []int8 {
	for i := range xs {
		C.sign_int8(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
func signInt16(xs []int16, rs []int8) []int8 {
	for i := range xs {
		C.sign_int16(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
func signInt32(xs []int32, rs []int8) []int8 {
	for i := range xs {
		C.sign_int32(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
func signInt64(xs []int64, rs []int8) []int8 {
	for i := range xs {
		C.sign_int64(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
func signFloat32(xs []float32, rs []int8) []int8 {
	for i := range xs {
		C.sign_float32(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
func signFloat64(xs []float64, rs []int8) []int8 {
	for i := range xs {
		C.sign_float64(unsafe.Pointer(&xs[i]), unsafe.Pointer(&rs[i]))
	}
	return rs
}
