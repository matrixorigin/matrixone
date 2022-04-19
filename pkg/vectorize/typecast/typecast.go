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

package typecast

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"golang.org/x/sys/cpu"
)

var (
	int16ToInt8   func([]int16, []int8) ([]int8, error)
	int32ToInt8   func([]int32, []int8) ([]int8, error)
	int64ToInt8   func([]int64, []int8) ([]int8, error)
	uint8ToInt8   func([]uint8, []int8) ([]int8, error)
	uint16ToInt8  func([]uint16, []int8) ([]int8, error)
	uint32ToInt8  func([]uint32, []int8) ([]int8, error)
	uint64ToInt8  func([]uint64, []int8) ([]int8, error)
	float32ToInt8 func([]float32, []int8) ([]int8, error)
	float64ToInt8 func([]float64, []int8) ([]int8, error)

	int8ToInt16    func([]int8, []int16) ([]int16, error)
	int32ToInt16   func([]int32, []int16) ([]int16, error)
	int64ToInt16   func([]int64, []int16) ([]int16, error)
	uint8ToInt16   func([]uint8, []int16) ([]int16, error)
	uint16ToInt16  func([]uint16, []int16) ([]int16, error)
	uint32ToInt16  func([]uint32, []int16) ([]int16, error)
	uint64ToInt16  func([]uint64, []int16) ([]int16, error)
	float32ToInt16 func([]float32, []int16) ([]int16, error)
	float64ToInt16 func([]float64, []int16) ([]int16, error)

	int8ToInt32    func([]int8, []int32) ([]int32, error)
	int16ToInt32   func([]int16, []int32) ([]int32, error)
	int64ToInt32   func([]int64, []int32) ([]int32, error)
	uint8ToInt32   func([]uint8, []int32) ([]int32, error)
	uint16ToInt32  func([]uint16, []int32) ([]int32, error)
	uint32ToInt32  func([]uint32, []int32) ([]int32, error)
	uint64ToInt32  func([]uint64, []int32) ([]int32, error)
	float32ToInt32 func([]float32, []int32) ([]int32, error)
	float64ToInt32 func([]float64, []int32) ([]int32, error)

	int8ToInt64    func([]int8, []int64) ([]int64, error)
	int16ToInt64   func([]int16, []int64) ([]int64, error)
	int32ToInt64   func([]int32, []int64) ([]int64, error)
	uint8ToInt64   func([]uint8, []int64) ([]int64, error)
	uint16ToInt64  func([]uint16, []int64) ([]int64, error)
	uint32ToInt64  func([]uint32, []int64) ([]int64, error)
	uint64ToInt64  func([]uint64, []int64) ([]int64, error)
	float32ToInt64 func([]float32, []int64) ([]int64, error)
	float64ToInt64 func([]float64, []int64) ([]int64, error)

	int8ToUint8    func([]int8, []uint8) ([]uint8, error)
	int16ToUint8   func([]int16, []uint8) ([]uint8, error)
	int32ToUint8   func([]int32, []uint8) ([]uint8, error)
	int64ToUint8   func([]int64, []uint8) ([]uint8, error)
	uint16ToUint8  func([]uint16, []uint8) ([]uint8, error)
	uint32ToUint8  func([]uint32, []uint8) ([]uint8, error)
	uint64ToUint8  func([]uint64, []uint8) ([]uint8, error)
	float32ToUint8 func([]float32, []uint8) ([]uint8, error)
	float64ToUint8 func([]float64, []uint8) ([]uint8, error)

	int8ToUint16    func([]int8, []uint16) ([]uint16, error)
	int16ToUint16   func([]int16, []uint16) ([]uint16, error)
	int32ToUint16   func([]int32, []uint16) ([]uint16, error)
	int64ToUint16   func([]int64, []uint16) ([]uint16, error)
	uint8ToUint16   func([]uint8, []uint16) ([]uint16, error)
	uint32ToUint16  func([]uint32, []uint16) ([]uint16, error)
	uint64ToUint16  func([]uint64, []uint16) ([]uint16, error)
	float32ToUint16 func([]float32, []uint16) ([]uint16, error)
	float64ToUint16 func([]float64, []uint16) ([]uint16, error)

	int8ToUint32    func([]int8, []uint32) ([]uint32, error)
	int16ToUint32   func([]int16, []uint32) ([]uint32, error)
	int32ToUint32   func([]int32, []uint32) ([]uint32, error)
	int64ToUint32   func([]int64, []uint32) ([]uint32, error)
	uint8ToUint32   func([]uint8, []uint32) ([]uint32, error)
	uint16ToUint32  func([]uint16, []uint32) ([]uint32, error)
	uint64ToUint32  func([]uint64, []uint32) ([]uint32, error)
	float32ToUint32 func([]float32, []uint32) ([]uint32, error)
	float64ToUint32 func([]float64, []uint32) ([]uint32, error)

	int8ToUint64    func([]int8, []uint64) ([]uint64, error)
	int16ToUint64   func([]int16, []uint64) ([]uint64, error)
	int32ToUint64   func([]int32, []uint64) ([]uint64, error)
	int64ToUint64   func([]int64, []uint64) ([]uint64, error)
	uint8ToUint64   func([]uint8, []uint64) ([]uint64, error)
	uint16ToUint64  func([]uint16, []uint64) ([]uint64, error)
	uint32ToUint64  func([]uint32, []uint64) ([]uint64, error)
	float32ToUint64 func([]float32, []uint64) ([]uint64, error)
	float64ToUint64 func([]float64, []uint64) ([]uint64, error)

	int8ToFloat32    func([]int8, []float32) ([]float32, error)
	int16ToFloat32   func([]int16, []float32) ([]float32, error)
	int32ToFloat32   func([]int32, []float32) ([]float32, error)
	int64ToFloat32   func([]int64, []float32) ([]float32, error)
	uint8ToFloat32   func([]uint8, []float32) ([]float32, error)
	uint16ToFloat32  func([]uint16, []float32) ([]float32, error)
	uint32ToFloat32  func([]uint32, []float32) ([]float32, error)
	uint64ToFloat32  func([]uint64, []float32) ([]float32, error)
	float64ToFloat32 func([]float64, []float32) ([]float32, error)

	int8ToFloat64    func([]int8, []float64) ([]float64, error)
	int16ToFloat64   func([]int16, []float64) ([]float64, error)
	int32ToFloat64   func([]int32, []float64) ([]float64, error)
	int64ToFloat64   func([]int64, []float64) ([]float64, error)
	uint8ToFloat64   func([]uint8, []float64) ([]float64, error)
	uint16ToFloat64  func([]uint16, []float64) ([]float64, error)
	uint32ToFloat64  func([]uint32, []float64) ([]float64, error)
	uint64ToFloat64  func([]uint64, []float64) ([]float64, error)
	float32ToFloat64 func([]float32, []float64) ([]float64, error)

	bytesToInt8    func(*types.Bytes, []int8) ([]int8, error)
	int8ToBytes    func([]int8, *types.Bytes) (*types.Bytes, error)
	bytesToInt16   func(*types.Bytes, []int16) ([]int16, error)
	int16ToBytes   func([]int16, *types.Bytes) (*types.Bytes, error)
	bytesToInt32   func(*types.Bytes, []int32) ([]int32, error)
	int32ToBytes   func([]int32, *types.Bytes) (*types.Bytes, error)
	bytesToInt64   func(*types.Bytes, []int64) ([]int64, error)
	int64ToBytes   func([]int64, *types.Bytes) (*types.Bytes, error)
	bytesToUint8   func(*types.Bytes, []uint8) ([]uint8, error)
	uint8ToBytes   func([]uint8, *types.Bytes) (*types.Bytes, error)
	bytesToUint16  func(*types.Bytes, []uint16) ([]uint16, error)
	uint16ToBytes  func([]uint16, *types.Bytes) (*types.Bytes, error)
	bytesToUint32  func(*types.Bytes, []uint32) ([]uint32, error)
	uint32ToBytes  func([]uint32, *types.Bytes) (*types.Bytes, error)
	bytesToUint64  func(*types.Bytes, []uint64) ([]uint64, error)
	uint64ToBytes  func([]uint64, *types.Bytes) (*types.Bytes, error)
	bytesToFloat32 func(*types.Bytes, []float32) ([]float32, error)
	float32ToBytes func([]float32, *types.Bytes) (*types.Bytes, error)
	bytesToFloat64 func(*types.Bytes, []float64) ([]float64, error)
	float64ToBytes func([]float64, *types.Bytes) (*types.Bytes, error)
)

func init() {
	if cpu.X86.HasAVX512 {
		int16ToInt8 = int16ToInt8Pure
		int32ToInt8 = int32ToInt8Pure
		int64ToInt8 = int64ToInt8Pure
		uint8ToInt8 = uint8ToInt8Pure
		uint16ToInt8 = uint16ToInt8Pure
		uint32ToInt8 = uint32ToInt8Pure
		uint64ToInt8 = uint64ToInt8Pure
		float32ToInt8 = float32ToInt8Pure
		float64ToInt8 = float64ToInt8Pure

		int8ToInt16 = int8ToInt16Pure
		int32ToInt16 = int32ToInt16Pure
		int64ToInt16 = int64ToInt16Pure
		uint8ToInt16 = uint8ToInt16Pure
		uint16ToInt16 = uint16ToInt16Pure
		uint32ToInt16 = uint32ToInt16Pure
		uint64ToInt16 = uint64ToInt16Pure
		float32ToInt16 = float32ToInt16Pure
		float64ToInt16 = float64ToInt16Pure

		int8ToInt32 = int8ToInt32Pure
		int16ToInt32 = int16ToInt32Pure
		int64ToInt32 = int64ToInt32Pure
		uint8ToInt32 = uint8ToInt32Pure
		uint16ToInt32 = uint16ToInt32Pure
		uint32ToInt32 = uint32ToInt32Pure
		uint64ToInt32 = uint64ToInt32Pure
		float32ToInt32 = float32ToInt32Pure
		float64ToInt32 = float64ToInt32Pure

		int8ToInt64 = int8ToInt64Pure
		int16ToInt64 = int16ToInt64Pure
		int32ToInt64 = int32ToInt64Pure
		uint8ToInt64 = uint8ToInt64Pure
		uint16ToInt64 = uint16ToInt64Pure
		uint32ToInt64 = uint32ToInt64Pure
		uint64ToInt64 = uint64ToInt64Pure
		float32ToInt64 = float32ToInt64Pure
		float64ToInt64 = float64ToInt64Pure

		int8ToUint8 = int8ToUint8Pure
		int16ToUint8 = int16ToUint8Pure
		int32ToUint8 = int32ToUint8Pure
		int64ToUint8 = int64ToUint8Pure
		uint16ToUint8 = uint16ToUint8Pure
		uint32ToUint8 = uint32ToUint8Pure
		uint64ToUint8 = uint64ToUint8Pure
		float32ToUint8 = float32ToUint8Pure
		float64ToUint8 = float64ToUint8Pure

		int8ToUint16 = int8ToUint16Pure
		int16ToUint16 = int16ToUint16Pure
		int32ToUint16 = int32ToUint16Pure
		int64ToUint16 = int64ToUint16Pure
		uint8ToUint16 = uint8ToUint16Pure
		uint32ToUint16 = uint32ToUint16Pure
		uint64ToUint16 = uint64ToUint16Pure
		float32ToUint16 = float32ToUint16Pure
		float64ToUint16 = float64ToUint16Pure

		int8ToUint32 = int8ToUint32Pure
		int16ToUint32 = int16ToUint32Pure
		int32ToUint32 = int32ToUint32Pure
		int64ToUint32 = int64ToUint32Pure
		uint8ToUint32 = uint8ToUint32Pure
		uint16ToUint32 = uint16ToUint32Pure
		uint64ToUint32 = uint64ToUint32Pure
		float32ToUint32 = float32ToUint32Pure
		float64ToUint32 = float64ToUint32Pure

		int8ToUint64 = int8ToUint64Pure
		int16ToUint64 = int16ToUint64Pure
		int32ToUint64 = int32ToUint64Pure
		int64ToUint64 = int64ToUint64Pure
		uint8ToUint64 = uint8ToUint64Pure
		uint16ToUint64 = uint16ToUint64Pure
		uint32ToUint64 = uint32ToUint64Pure
		float32ToUint64 = float32ToUint64Pure
		float64ToUint64 = float64ToUint64Pure

		int8ToFloat32 = int8ToFloat32Pure
		int16ToFloat32 = int16ToFloat32Pure
		int32ToFloat32 = int32ToFloat32Pure
		int64ToFloat32 = int64ToFloat32Pure
		uint8ToFloat32 = uint8ToFloat32Pure
		uint16ToFloat32 = uint16ToFloat32Pure
		uint32ToFloat32 = uint32ToFloat32Pure
		uint64ToFloat32 = uint64ToFloat32Pure
		float64ToFloat32 = float64ToFloat32Pure

		int8ToFloat64 = int8ToFloat64Pure
		int16ToFloat64 = int16ToFloat64Pure
		int32ToFloat64 = int32ToFloat64Pure
		int64ToFloat64 = int64ToFloat64Pure
		uint8ToFloat64 = uint8ToFloat64Pure
		uint16ToFloat64 = uint16ToFloat64Pure
		uint32ToFloat64 = uint32ToFloat64Pure
		uint64ToFloat64 = uint64ToFloat64Pure
		float32ToFloat64 = float32ToFloat64Pure

	} else if cpu.X86.HasAVX2 {
		int16ToInt8 = int16ToInt8Pure
		int32ToInt8 = int32ToInt8Pure
		int64ToInt8 = int64ToInt8Pure
		uint8ToInt8 = uint8ToInt8Pure
		uint16ToInt8 = uint16ToInt8Pure
		uint32ToInt8 = uint32ToInt8Pure
		uint64ToInt8 = uint64ToInt8Pure
		float32ToInt8 = float32ToInt8Pure
		float64ToInt8 = float64ToInt8Pure

		int8ToInt16 = int8ToInt16Pure
		int32ToInt16 = int32ToInt16Pure
		int64ToInt16 = int64ToInt16Pure
		uint8ToInt16 = uint8ToInt16Pure
		uint16ToInt16 = uint16ToInt16Pure
		uint32ToInt16 = uint32ToInt16Pure
		uint64ToInt16 = uint64ToInt16Pure
		float32ToInt16 = float32ToInt16Pure
		float64ToInt16 = float64ToInt16Pure

		int8ToInt32 = int8ToInt32Pure
		int16ToInt32 = int16ToInt32Pure
		int64ToInt32 = int64ToInt32Pure
		uint8ToInt32 = uint8ToInt32Pure
		uint16ToInt32 = uint16ToInt32Pure
		uint32ToInt32 = uint32ToInt32Pure
		uint64ToInt32 = uint64ToInt32Pure
		float32ToInt32 = float32ToInt32Pure
		float64ToInt32 = float64ToInt32Pure

		int8ToInt64 = int8ToInt64Pure
		int16ToInt64 = int16ToInt64Pure
		int32ToInt64 = int32ToInt64Pure
		uint8ToInt64 = uint8ToInt64Pure
		uint16ToInt64 = uint16ToInt64Pure
		uint32ToInt64 = uint32ToInt64Pure
		uint64ToInt64 = uint64ToInt64Pure
		float32ToInt64 = float32ToInt64Pure
		float64ToInt64 = float64ToInt64Pure

		int8ToUint8 = int8ToUint8Pure
		int16ToUint8 = int16ToUint8Pure
		int32ToUint8 = int32ToUint8Pure
		int64ToUint8 = int64ToUint8Pure
		uint16ToUint8 = uint16ToUint8Pure
		uint32ToUint8 = uint32ToUint8Pure
		uint64ToUint8 = uint64ToUint8Pure
		float32ToUint8 = float32ToUint8Pure
		float64ToUint8 = float64ToUint8Pure

		int8ToUint16 = int8ToUint16Pure
		int16ToUint16 = int16ToUint16Pure
		int32ToUint16 = int32ToUint16Pure
		int64ToUint16 = int64ToUint16Pure
		uint8ToUint16 = uint8ToUint16Pure
		uint32ToUint16 = uint32ToUint16Pure
		uint64ToUint16 = uint64ToUint16Pure
		float32ToUint16 = float32ToUint16Pure
		float64ToUint16 = float64ToUint16Pure

		int8ToUint32 = int8ToUint32Pure
		int16ToUint32 = int16ToUint32Pure
		int32ToUint32 = int32ToUint32Pure
		int64ToUint32 = int64ToUint32Pure
		uint8ToUint32 = uint8ToUint32Pure
		uint16ToUint32 = uint16ToUint32Pure
		uint64ToUint32 = uint64ToUint32Pure
		float32ToUint32 = float32ToUint32Pure
		float64ToUint32 = float64ToUint32Pure

		int8ToUint64 = int8ToUint64Pure
		int16ToUint64 = int16ToUint64Pure
		int32ToUint64 = int32ToUint64Pure
		int64ToUint64 = int64ToUint64Pure
		uint8ToUint64 = uint8ToUint64Pure
		uint16ToUint64 = uint16ToUint64Pure
		uint32ToUint64 = uint32ToUint64Pure
		float32ToUint64 = float32ToUint64Pure
		float64ToUint64 = float64ToUint64Pure

		int8ToFloat32 = int8ToFloat32Pure
		int16ToFloat32 = int16ToFloat32Pure
		int32ToFloat32 = int32ToFloat32Pure
		int64ToFloat32 = int64ToFloat32Pure
		uint8ToFloat32 = uint8ToFloat32Pure
		uint16ToFloat32 = uint16ToFloat32Pure
		uint32ToFloat32 = uint32ToFloat32Pure
		uint64ToFloat32 = uint64ToFloat32Pure
		float64ToFloat32 = float64ToFloat32Pure

		int8ToFloat64 = int8ToFloat64Pure
		int16ToFloat64 = int16ToFloat64Pure
		int32ToFloat64 = int32ToFloat64Pure
		int64ToFloat64 = int64ToFloat64Pure
		uint8ToFloat64 = uint8ToFloat64Pure
		uint16ToFloat64 = uint16ToFloat64Pure
		uint32ToFloat64 = uint32ToFloat64Pure
		uint64ToFloat64 = uint64ToFloat64Pure
		float32ToFloat64 = float32ToFloat64Pure

	} else {
		int16ToInt8 = int16ToInt8Pure
		int32ToInt8 = int32ToInt8Pure
		int64ToInt8 = int64ToInt8Pure
		uint8ToInt8 = uint8ToInt8Pure
		uint16ToInt8 = uint16ToInt8Pure
		uint32ToInt8 = uint32ToInt8Pure
		uint64ToInt8 = uint64ToInt8Pure
		float32ToInt8 = float32ToInt8Pure
		float64ToInt8 = float64ToInt8Pure

		int8ToInt16 = int8ToInt16Pure
		int32ToInt16 = int32ToInt16Pure
		int64ToInt16 = int64ToInt16Pure
		uint8ToInt16 = uint8ToInt16Pure
		uint16ToInt16 = uint16ToInt16Pure
		uint32ToInt16 = uint32ToInt16Pure
		uint64ToInt16 = uint64ToInt16Pure
		float32ToInt16 = float32ToInt16Pure
		float64ToInt16 = float64ToInt16Pure

		int8ToInt32 = int8ToInt32Pure
		int16ToInt32 = int16ToInt32Pure
		int64ToInt32 = int64ToInt32Pure
		uint8ToInt32 = uint8ToInt32Pure
		uint16ToInt32 = uint16ToInt32Pure
		uint32ToInt32 = uint32ToInt32Pure
		uint64ToInt32 = uint64ToInt32Pure
		float32ToInt32 = float32ToInt32Pure
		float64ToInt32 = float64ToInt32Pure

		int8ToInt64 = int8ToInt64Pure
		int16ToInt64 = int16ToInt64Pure
		int32ToInt64 = int32ToInt64Pure
		uint8ToInt64 = uint8ToInt64Pure
		uint16ToInt64 = uint16ToInt64Pure
		uint32ToInt64 = uint32ToInt64Pure
		uint64ToInt64 = uint64ToInt64Pure
		float32ToInt64 = float32ToInt64Pure
		float64ToInt64 = float64ToInt64Pure

		int8ToUint8 = int8ToUint8Pure
		int16ToUint8 = int16ToUint8Pure
		int32ToUint8 = int32ToUint8Pure
		int64ToUint8 = int64ToUint8Pure
		uint16ToUint8 = uint16ToUint8Pure
		uint32ToUint8 = uint32ToUint8Pure
		uint64ToUint8 = uint64ToUint8Pure
		float32ToUint8 = float32ToUint8Pure
		float64ToUint8 = float64ToUint8Pure

		int8ToUint16 = int8ToUint16Pure
		int16ToUint16 = int16ToUint16Pure
		int32ToUint16 = int32ToUint16Pure
		int64ToUint16 = int64ToUint16Pure
		uint8ToUint16 = uint8ToUint16Pure
		uint32ToUint16 = uint32ToUint16Pure
		uint64ToUint16 = uint64ToUint16Pure
		float32ToUint16 = float32ToUint16Pure
		float64ToUint16 = float64ToUint16Pure

		int8ToUint32 = int8ToUint32Pure
		int16ToUint32 = int16ToUint32Pure
		int32ToUint32 = int32ToUint32Pure
		int64ToUint32 = int64ToUint32Pure
		uint8ToUint32 = uint8ToUint32Pure
		uint16ToUint32 = uint16ToUint32Pure
		uint64ToUint32 = uint64ToUint32Pure
		float32ToUint32 = float32ToUint32Pure
		float64ToUint32 = float64ToUint32Pure

		int8ToUint64 = int8ToUint64Pure
		int16ToUint64 = int16ToUint64Pure
		int32ToUint64 = int32ToUint64Pure
		int64ToUint64 = int64ToUint64Pure
		uint8ToUint64 = uint8ToUint64Pure
		uint16ToUint64 = uint16ToUint64Pure
		uint32ToUint64 = uint32ToUint64Pure
		float32ToUint64 = float32ToUint64Pure
		float64ToUint64 = float64ToUint64Pure

		int8ToFloat32 = int8ToFloat32Pure
		int16ToFloat32 = int16ToFloat32Pure
		int32ToFloat32 = int32ToFloat32Pure
		int64ToFloat32 = int64ToFloat32Pure
		uint8ToFloat32 = uint8ToFloat32Pure
		uint16ToFloat32 = uint16ToFloat32Pure
		uint32ToFloat32 = uint32ToFloat32Pure
		uint64ToFloat32 = uint64ToFloat32Pure
		float64ToFloat32 = float64ToFloat32Pure

		int8ToFloat64 = int8ToFloat64Pure
		int16ToFloat64 = int16ToFloat64Pure
		int32ToFloat64 = int32ToFloat64Pure
		int64ToFloat64 = int64ToFloat64Pure
		uint8ToFloat64 = uint8ToFloat64Pure
		uint16ToFloat64 = uint16ToFloat64Pure
		uint32ToFloat64 = uint32ToFloat64Pure
		uint64ToFloat64 = uint64ToFloat64Pure
		float32ToFloat64 = float32ToFloat64Pure

	}

	bytesToInt8 = bytesToInt8Pure
	int8ToBytes = int8ToBytesPure
	bytesToInt16 = bytesToInt16Pure
	int16ToBytes = int16ToBytesPure
	bytesToInt32 = bytesToInt32Pure
	int32ToBytes = int32ToBytesPure
	bytesToInt64 = bytesToInt64Pure
	int64ToBytes = int64ToBytesPure
	bytesToUint8 = bytesToUint8Pure
	uint8ToBytes = uint8ToBytesPure
	bytesToUint16 = bytesToUint16Pure
	uint16ToBytes = uint16ToBytesPure
	bytesToUint32 = bytesToUint32Pure
	uint32ToBytes = uint32ToBytesPure
	bytesToUint64 = bytesToUint64Pure
	uint64ToBytes = uint64ToBytesPure
	bytesToFloat32 = bytesToFloat32Pure
	float32ToBytes = float32ToBytesPure
	bytesToFloat64 = bytesToFloat64Pure
	float64ToBytes = float64ToBytesPure
}

func Int16ToInt8(xs []int16, rs []int8) ([]int8, error) {
	return int16ToInt8(xs, rs)
}

func int16ToInt8Pure(xs []int16, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func int16ToInt8Avx512(xs []int16, rs []int8) ([]int8, error) {
	n := len(xs) / 8
	int16ToInt8Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Int32ToInt8(xs []int32, rs []int8) ([]int8, error) {
	return int32ToInt8(xs, rs)
}

func int32ToInt8Pure(xs []int32, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func int32ToInt8Avx512(xs []int32, rs []int8) ([]int8, error) {
	n := len(xs) / 4
	int32ToInt8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Int64ToInt8(xs []int64, rs []int8) ([]int8, error) {
	return int64ToInt8(xs, rs)
}

func int64ToInt8Pure(xs []int64, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func int64ToInt8Avx512(xs []int64, rs []int8) ([]int8, error) {
	n := len(xs) / 2
	int64ToInt8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToInt8(xs []uint8, rs []int8) ([]int8, error) {
	return uint8ToInt8(xs, rs)
}

func uint8ToInt8Pure(xs []uint8, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func uint8ToInt8Avx2(xs []uint8, rs []int8) ([]int8, error) {
	n := len(xs) / 16
	uint8ToInt8Avx2Asm(xs[:n*16], rs)
	for i, j := n * 16, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}

func uint8ToInt8Avx512(xs []uint8, rs []int8) ([]int8, error) {
	n := len(xs) / 16
	uint8ToInt8Avx512Asm(xs[:n*16], rs)
	for i, j := n * 16, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToInt8(xs []uint16, rs []int8) ([]int8, error) {
	return uint16ToInt8(xs, rs)
}

func uint16ToInt8Pure(xs []uint16, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func uint16ToInt8Avx512(xs []uint16, rs []int8) ([]int8, error) {
	n := len(xs) / 8
	uint16ToInt8Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToInt8(xs []uint32, rs []int8) ([]int8, error) {
	return uint32ToInt8(xs, rs)
}

func uint32ToInt8Pure(xs []uint32, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func uint32ToInt8Avx512(xs []uint32, rs []int8) ([]int8, error) {
	n := len(xs) / 4
	uint32ToInt8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToInt8(xs []uint64, rs []int8) ([]int8, error) {
	return uint64ToInt8(xs, rs)
}

func uint64ToInt8Pure(xs []uint64, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func uint64ToInt8Avx512(xs []uint64, rs []int8) ([]int8, error) {
	n := len(xs) / 2
	uint64ToInt8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Float32ToInt8(xs []float32, rs []int8) ([]int8, error) {
	return float32ToInt8(xs, rs)
}

func float32ToInt8Pure(xs []float32, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func float32ToInt8Avx512(xs []float32, rs []int8) ([]int8, error) {
	n := len(xs) / 4
	float32ToInt8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Float64ToInt8(xs []float64, rs []int8) ([]int8, error) {
	return float64ToInt8(xs, rs)
}

func float64ToInt8Pure(xs []float64, rs []int8) ([]int8, error) {
	for i, x := range xs {
		rs[i] = int8(x)
	}
	return rs, nil
}

/*
func float64ToInt8Avx512(xs []float64, rs []int8) ([]int8, error) {
	n := len(xs) / 2
	float64ToInt8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int8(xs[i])
	}
	return rs, nil
}
*/

func Int8ToInt16(xs []int8, rs []int16) ([]int16, error) {
	return int8ToInt16(xs, rs)
}

func int8ToInt16Pure(xs []int8, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func int8ToInt16Avx2(xs []int8, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	int8ToInt16Avx2Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}

func int8ToInt16Avx512(xs []int8, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	int8ToInt16Avx512Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Int32ToInt16(xs []int32, rs []int16) ([]int16, error) {
	return int32ToInt16(xs, rs)
}

func int32ToInt16Pure(xs []int32, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func int32ToInt16Avx512(xs []int32, rs []int16) ([]int16, error) {
	n := len(xs) / 4
	int32ToInt16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Int64ToInt16(xs []int64, rs []int16) ([]int16, error) {
	return int64ToInt16(xs, rs)
}

func int64ToInt16Pure(xs []int64, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func int64ToInt16Avx512(xs []int64, rs []int16) ([]int16, error) {
	n := len(xs) / 2
	int64ToInt16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToInt16(xs []uint8, rs []int16) ([]int16, error) {
	return uint8ToInt16(xs, rs)
}

func uint8ToInt16Pure(xs []uint8, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func uint8ToInt16Avx2(xs []uint8, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	uint8ToInt16Avx2Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}

func uint8ToInt16Avx512(xs []uint8, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	uint8ToInt16Avx512Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToInt16(xs []uint16, rs []int16) ([]int16, error) {
	return uint16ToInt16(xs, rs)
}

func uint16ToInt16Pure(xs []uint16, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func uint16ToInt16Avx2(xs []uint16, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	uint16ToInt16Avx2Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}

func uint16ToInt16Avx512(xs []uint16, rs []int16) ([]int16, error) {
	n := len(xs) / 8
	uint16ToInt16Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToInt16(xs []uint32, rs []int16) ([]int16, error) {
	return uint32ToInt16(xs, rs)
}

func uint32ToInt16Pure(xs []uint32, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func uint32ToInt16Avx512(xs []uint32, rs []int16) ([]int16, error) {
	n := len(xs) / 4
	uint32ToInt16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToInt16(xs []uint64, rs []int16) ([]int16, error) {
	return uint64ToInt16(xs, rs)
}

func uint64ToInt16Pure(xs []uint64, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func uint64ToInt16Avx512(xs []uint64, rs []int16) ([]int16, error) {
	n := len(xs) / 2
	uint64ToInt16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Float32ToInt16(xs []float32, rs []int16) ([]int16, error) {
	return float32ToInt16(xs, rs)
}

func float32ToInt16Pure(xs []float32, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func float32ToInt16Avx512(xs []float32, rs []int16) ([]int16, error) {
	n := len(xs) / 4
	float32ToInt16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Float64ToInt16(xs []float64, rs []int16) ([]int16, error) {
	return float64ToInt16(xs, rs)
}

func float64ToInt16Pure(xs []float64, rs []int16) ([]int16, error) {
	for i, x := range xs {
		rs[i] = int16(x)
	}
	return rs, nil
}

/*
func float64ToInt16Avx512(xs []float64, rs []int16) ([]int16, error) {
	n := len(xs) / 2
	float64ToInt16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int16(xs[i])
	}
	return rs, nil
}
*/

func Int8ToInt32(xs []int8, rs []int32) ([]int32, error) {
	return int8ToInt32(xs, rs)
}

func int8ToInt32Pure(xs []int8, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func int8ToInt32Avx2(xs []int8, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	int8ToInt32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func int8ToInt32Avx512(xs []int8, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	int8ToInt32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Int16ToInt32(xs []int16, rs []int32) ([]int32, error) {
	return int16ToInt32(xs, rs)
}

func int16ToInt32Pure(xs []int16, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func int16ToInt32Avx2(xs []int16, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	int16ToInt32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func int16ToInt32Avx512(xs []int16, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	int16ToInt32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Int64ToInt32(xs []int64, rs []int32) ([]int32, error) {
	return int64ToInt32(xs, rs)
}

func int64ToInt32Pure(xs []int64, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func int64ToInt32Avx512(xs []int64, rs []int32) ([]int32, error) {
	n := len(xs) / 2
	int64ToInt32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToInt32(xs []uint8, rs []int32) ([]int32, error) {
	return uint8ToInt32(xs, rs)
}

func uint8ToInt32Pure(xs []uint8, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func uint8ToInt32Avx2(xs []uint8, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint8ToInt32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func uint8ToInt32Avx512(xs []uint8, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint8ToInt32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToInt32(xs []uint16, rs []int32) ([]int32, error) {
	return uint16ToInt32(xs, rs)
}

func uint16ToInt32Pure(xs []uint16, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func uint16ToInt32Avx2(xs []uint16, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint16ToInt32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func uint16ToInt32Avx512(xs []uint16, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint16ToInt32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToInt32(xs []uint32, rs []int32) ([]int32, error) {
	return uint32ToInt32(xs, rs)
}

func uint32ToInt32Pure(xs []uint32, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func uint32ToInt32Avx2(xs []uint32, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint32ToInt32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func uint32ToInt32Avx512(xs []uint32, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	uint32ToInt32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToInt32(xs []uint64, rs []int32) ([]int32, error) {
	return uint64ToInt32(xs, rs)
}

func uint64ToInt32Pure(xs []uint64, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func uint64ToInt32Avx512(xs []uint64, rs []int32) ([]int32, error) {
	n := len(xs) / 2
	uint64ToInt32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Float32ToInt32(xs []float32, rs []int32) ([]int32, error) {
	return float32ToInt32(xs, rs)
}

func float32ToInt32Pure(xs []float32, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func float32ToInt32Avx2(xs []float32, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	float32ToInt32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func float32ToInt32Avx512(xs []float32, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	float32ToInt32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Float64ToInt32(xs []float64, rs []int32) ([]int32, error) {
	return float64ToInt32(xs, rs)
}

func float64ToInt32Pure(xs []float64, rs []int32) ([]int32, error) {
	for i, x := range xs {
		rs[i] = int32(x)
	}
	return rs, nil
}

/*
func float64ToInt32Avx2(xs []float64, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	float64ToInt32Avx2Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}

func float64ToInt32Avx512(xs []float64, rs []int32) ([]int32, error) {
	n := len(xs) / 4
	float64ToInt32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = int32(xs[i])
	}
	return rs, nil
}
*/

func Int8ToInt64(xs []int8, rs []int64) ([]int64, error) {
	return int8ToInt64(xs, rs)
}

func int8ToInt64Pure(xs []int8, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func int8ToInt64Avx2(xs []int8, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int8ToInt64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func int8ToInt64Avx512(xs []int8, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int8ToInt64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Int16ToInt64(xs []int16, rs []int64) ([]int64, error) {
	return int16ToInt64(xs, rs)
}

func int16ToInt64Pure(xs []int16, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func int16ToInt64Avx2(xs []int16, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int16ToInt64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func int16ToInt64Avx512(xs []int16, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int16ToInt64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Int32ToInt64(xs []int32, rs []int64) ([]int64, error) {
	return int32ToInt64(xs, rs)
}

func int32ToInt64Pure(xs []int32, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func int32ToInt64Avx2(xs []int32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int32ToInt64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func int32ToInt64Avx512(xs []int32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	int32ToInt64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToInt64(xs []uint8, rs []int64) ([]int64, error) {
	return uint8ToInt64(xs, rs)
}

func uint8ToInt64Pure(xs []uint8, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func uint8ToInt64Avx2(xs []uint8, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint8ToInt64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func uint8ToInt64Avx512(xs []uint8, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint8ToInt64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToInt64(xs []uint16, rs []int64) ([]int64, error) {
	return uint16ToInt64(xs, rs)
}

func uint16ToInt64Pure(xs []uint16, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func uint16ToInt64Avx2(xs []uint16, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint16ToInt64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func uint16ToInt64Avx512(xs []uint16, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint16ToInt64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToInt64(xs []uint32, rs []int64) ([]int64, error) {
	return uint32ToInt64(xs, rs)
}

func uint32ToInt64Pure(xs []uint32, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func uint32ToInt64Avx2(xs []uint32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint32ToInt64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func uint32ToInt64Avx512(xs []uint32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint32ToInt64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToInt64(xs []uint64, rs []int64) ([]int64, error) {
	return uint64ToInt64(xs, rs)
}

func uint64ToInt64Pure(xs []uint64, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func uint64ToInt64Avx2(xs []uint64, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint64ToInt64Avx2Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func uint64ToInt64Avx512(xs []uint64, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	uint64ToInt64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Float32ToInt64(xs []float32, rs []int64) ([]int64, error) {
	return float32ToInt64(xs, rs)
}

func float32ToInt64Pure(xs []float32, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func float32ToInt64Avx2(xs []float32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	float32ToInt64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func float32ToInt64Avx512(xs []float32, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	float32ToInt64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Float64ToInt64(xs []float64, rs []int64) ([]int64, error) {
	return float64ToInt64(xs, rs)
}

func float64ToInt64Pure(xs []float64, rs []int64) ([]int64, error) {
	for i, x := range xs {
		rs[i] = int64(x)
	}
	return rs, nil
}

/*
func float64ToInt64Avx2(xs []float64, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	float64ToInt64Avx2Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}

func float64ToInt64Avx512(xs []float64, rs []int64) ([]int64, error) {
	n := len(xs) / 2
	float64ToInt64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = int64(xs[i])
	}
	return rs, nil
}
*/

func Int8ToUint8(xs []int8, rs []uint8) ([]uint8, error) {
	return int8ToUint8(xs, rs)
}

func int8ToUint8Pure(xs []int8, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func int8ToUint8Avx2(xs []int8, rs []uint8) ([]uint8, error) {
	n := len(xs) / 16
	int8ToUint8Avx2Asm(xs[:n*16], rs)
	for i, j := n * 16, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}

func int8ToUint8Avx512(xs []int8, rs []uint8) ([]uint8, error) {
	n := len(xs) / 16
	int8ToUint8Avx512Asm(xs[:n*16], rs)
	for i, j := n * 16, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Int16ToUint8(xs []int16, rs []uint8) ([]uint8, error) {
	return int16ToUint8(xs, rs)
}

func int16ToUint8Pure(xs []int16, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func int16ToUint8Avx512(xs []int16, rs []uint8) ([]uint8, error) {
	n := len(xs) / 8
	int16ToUint8Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Int32ToUint8(xs []int32, rs []uint8) ([]uint8, error) {
	return int32ToUint8(xs, rs)
}

func int32ToUint8Pure(xs []int32, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func int32ToUint8Avx512(xs []int32, rs []uint8) ([]uint8, error) {
	n := len(xs) / 4
	int32ToUint8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Int64ToUint8(xs []int64, rs []uint8) ([]uint8, error) {
	return int64ToUint8(xs, rs)
}

func int64ToUint8Pure(xs []int64, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func int64ToUint8Avx512(xs []int64, rs []uint8) ([]uint8, error) {
	n := len(xs) / 2
	int64ToUint8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToUint8(xs []uint16, rs []uint8) ([]uint8, error) {
	return uint16ToUint8(xs, rs)
}

func uint16ToUint8Pure(xs []uint16, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func uint16ToUint8Avx512(xs []uint16, rs []uint8) ([]uint8, error) {
	n := len(xs) / 8
	uint16ToUint8Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToUint8(xs []uint32, rs []uint8) ([]uint8, error) {
	return uint32ToUint8(xs, rs)
}

func uint32ToUint8Pure(xs []uint32, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func uint32ToUint8Avx512(xs []uint32, rs []uint8) ([]uint8, error) {
	n := len(xs) / 4
	uint32ToUint8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToUint8(xs []uint64, rs []uint8) ([]uint8, error) {
	return uint64ToUint8(xs, rs)
}

func uint64ToUint8Pure(xs []uint64, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func uint64ToUint8Avx512(xs []uint64, rs []uint8) ([]uint8, error) {
	n := len(xs) / 2
	uint64ToUint8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Float32ToUint8(xs []float32, rs []uint8) ([]uint8, error) {
	return float32ToUint8(xs, rs)
}

func float32ToUint8Pure(xs []float32, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func float32ToUint8Avx512(xs []float32, rs []uint8) ([]uint8, error) {
	n := len(xs) / 4
	float32ToUint8Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Float64ToUint8(xs []float64, rs []uint8) ([]uint8, error) {
	return float64ToUint8(xs, rs)
}

func float64ToUint8Pure(xs []float64, rs []uint8) ([]uint8, error) {
	for i, x := range xs {
		rs[i] = uint8(x)
	}
	return rs, nil
}

/*
func float64ToUint8Avx512(xs []float64, rs []uint8) ([]uint8, error) {
	n := len(xs) / 2
	float64ToUint8Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint8(xs[i])
	}
	return rs, nil
}
*/

func Int8ToUint16(xs []int8, rs []uint16) ([]uint16, error) {
	return int8ToUint16(xs, rs)
}

func int8ToUint16Pure(xs []int8, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func int8ToUint16Avx2(xs []int8, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	int8ToUint16Avx2Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}

func int8ToUint16Avx512(xs []int8, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	int8ToUint16Avx512Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Int16ToUint16(xs []int16, rs []uint16) ([]uint16, error) {
	return int16ToUint16(xs, rs)
}

func int16ToUint16Pure(xs []int16, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func int16ToUint16Avx2(xs []int16, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	int16ToUint16Avx2Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}

func int16ToUint16Avx512(xs []int16, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	int16ToUint16Avx512Asm(xs[:n*8], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Int32ToUint16(xs []int32, rs []uint16) ([]uint16, error) {
	return int32ToUint16(xs, rs)
}

func int32ToUint16Pure(xs []int32, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func int32ToUint16Avx512(xs []int32, rs []uint16) ([]uint16, error) {
	n := len(xs) / 4
	int32ToUint16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Int64ToUint16(xs []int64, rs []uint16) ([]uint16, error) {
	return int64ToUint16(xs, rs)
}

func int64ToUint16Pure(xs []int64, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func int64ToUint16Avx512(xs []int64, rs []uint16) ([]uint16, error) {
	n := len(xs) / 2
	int64ToUint16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToUint16(xs []uint8, rs []uint16) ([]uint16, error) {
	return uint8ToUint16(xs, rs)
}

func uint8ToUint16Pure(xs []uint8, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func uint8ToUint16Avx2(xs []uint8, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	uint8ToUint16Avx2Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}

func uint8ToUint16Avx512(xs []uint8, rs []uint16) ([]uint16, error) {
	n := len(xs) / 8
	uint8ToUint16Avx512Asm(xs[:n*16], rs)
	for i, j := n * 8, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToUint16(xs []uint32, rs []uint16) ([]uint16, error) {
	return uint32ToUint16(xs, rs)
}

func uint32ToUint16Pure(xs []uint32, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func uint32ToUint16Avx512(xs []uint32, rs []uint16) ([]uint16, error) {
	n := len(xs) / 4
	uint32ToUint16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToUint16(xs []uint64, rs []uint16) ([]uint16, error) {
	return uint64ToUint16(xs, rs)
}

func uint64ToUint16Pure(xs []uint64, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func uint64ToUint16Avx512(xs []uint64, rs []uint16) ([]uint16, error) {
	n := len(xs) / 2
	uint64ToUint16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Float32ToUint16(xs []float32, rs []uint16) ([]uint16, error) {
	return float32ToUint16(xs, rs)
}

func float32ToUint16Pure(xs []float32, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func float32ToUint16Avx512(xs []float32, rs []uint16) ([]uint16, error) {
	n := len(xs) / 4
	float32ToUint16Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Float64ToUint16(xs []float64, rs []uint16) ([]uint16, error) {
	return float64ToUint16(xs, rs)
}

func float64ToUint16Pure(xs []float64, rs []uint16) ([]uint16, error) {
	for i, x := range xs {
		rs[i] = uint16(x)
	}
	return rs, nil
}

/*
func float64ToUint16Avx512(xs []float64, rs []uint16) ([]uint16, error) {
	n := len(xs) / 2
	float64ToUint16Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint16(xs[i])
	}
	return rs, nil
}
*/

func Int8ToUint32(xs []int8, rs []uint32) ([]uint32, error) {
	return int8ToUint32(xs, rs)
}

func int8ToUint32Pure(xs []int8, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func int8ToUint32Avx2(xs []int8, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int8ToUint32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func int8ToUint32Avx512(xs []int8, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int8ToUint32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Int16ToUint32(xs []int16, rs []uint32) ([]uint32, error) {
	return int16ToUint32(xs, rs)
}

func int16ToUint32Pure(xs []int16, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func int16ToUint32Avx2(xs []int16, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int16ToUint32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func int16ToUint32Avx512(xs []int16, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int16ToUint32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Int32ToUint32(xs []int32, rs []uint32) ([]uint32, error) {
	return int32ToUint32(xs, rs)
}

func int32ToUint32Pure(xs []int32, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func int32ToUint32Avx2(xs []int32, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int32ToUint32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func int32ToUint32Avx512(xs []int32, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	int32ToUint32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Int64ToUint32(xs []int64, rs []uint32) ([]uint32, error) {
	return int64ToUint32(xs, rs)
}

func int64ToUint32Pure(xs []int64, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func int64ToUint32Avx512(xs []int64, rs []uint32) ([]uint32, error) {
	n := len(xs) / 2
	int64ToUint32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToUint32(xs []uint8, rs []uint32) ([]uint32, error) {
	return uint8ToUint32(xs, rs)
}

func uint8ToUint32Pure(xs []uint8, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func uint8ToUint32Avx2(xs []uint8, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	uint8ToUint32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func uint8ToUint32Avx512(xs []uint8, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	uint8ToUint32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToUint32(xs []uint16, rs []uint32) ([]uint32, error) {
	return uint16ToUint32(xs, rs)
}

func uint16ToUint32Pure(xs []uint16, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func uint16ToUint32Avx2(xs []uint16, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	uint16ToUint32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func uint16ToUint32Avx512(xs []uint16, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	uint16ToUint32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToUint32(xs []uint64, rs []uint32) ([]uint32, error) {
	return uint64ToUint32(xs, rs)
}

func uint64ToUint32Pure(xs []uint64, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func uint64ToUint32Avx512(xs []uint64, rs []uint32) ([]uint32, error) {
	n := len(xs) / 2
	uint64ToUint32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Float32ToUint32(xs []float32, rs []uint32) ([]uint32, error) {
	return float32ToUint32(xs, rs)
}

func float32ToUint32Pure(xs []float32, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func float32ToUint32Avx2(xs []float32, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	float32ToUint32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func float32ToUint32Avx512(xs []float32, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	float32ToUint32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Float64ToUint32(xs []float64, rs []uint32) ([]uint32, error) {
	return float64ToUint32(xs, rs)
}

func float64ToUint32Pure(xs []float64, rs []uint32) ([]uint32, error) {
	for i, x := range xs {
		rs[i] = uint32(x)
	}
	return rs, nil
}

/*
func float64ToUint32Avx2(xs []float64, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	float64ToUint32Avx2Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}

func float64ToUint32Avx512(xs []float64, rs []uint32) ([]uint32, error) {
	n := len(xs) / 4
	float64ToUint32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = uint32(xs[i])
	}
	return rs, nil
}
*/

func Int8ToUint64(xs []int8, rs []uint64) ([]uint64, error) {
	return int8ToUint64(xs, rs)
}

func int8ToUint64Pure(xs []int8, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func int8ToUint64Avx2(xs []int8, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int8ToUint64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func int8ToUint64Avx512(xs []int8, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int8ToUint64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Int16ToUint64(xs []int16, rs []uint64) ([]uint64, error) {
	return int16ToUint64(xs, rs)
}

func int16ToUint64Pure(xs []int16, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func int16ToUint64Avx2(xs []int16, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int16ToUint64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func int16ToUint64Avx512(xs []int16, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int16ToUint64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Int32ToUint64(xs []int32, rs []uint64) ([]uint64, error) {
	return int32ToUint64(xs, rs)
}

func int32ToUint64Pure(xs []int32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func int32ToUint64Avx2(xs []int32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int32ToUint64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func int32ToUint64Avx512(xs []int32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int32ToUint64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Int64ToUint64(xs []int64, rs []uint64) ([]uint64, error) {
	return int64ToUint64(xs, rs)
}

func int64ToUint64Pure(xs []int64, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func int64ToUint64Avx2(xs []int64, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int64ToUint64Avx2Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func int64ToUint64Avx512(xs []int64, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	int64ToUint64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToUint64(xs []uint8, rs []uint64) ([]uint64, error) {
	return uint8ToUint64(xs, rs)
}

func uint8ToUint64Pure(xs []uint8, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func uint8ToUint64Avx2(xs []uint8, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint8ToUint64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func uint8ToUint64Avx512(xs []uint8, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint8ToUint64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToUint64(xs []uint16, rs []uint64) ([]uint64, error) {
	return uint16ToUint64(xs, rs)
}

func uint16ToUint64Pure(xs []uint16, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func uint16ToUint64Avx2(xs []uint16, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint16ToUint64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func uint16ToUint64Avx512(xs []uint16, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint16ToUint64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToUint64(xs []uint32, rs []uint64) ([]uint64, error) {
	return uint32ToUint64(xs, rs)
}

func uint32ToUint64Pure(xs []uint32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func uint32ToUint64Avx2(xs []uint32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint32ToUint64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func uint32ToUint64Avx512(xs []uint32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	uint32ToUint64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Float32ToUint64(xs []float32, rs []uint64) ([]uint64, error) {
	return float32ToUint64(xs, rs)
}

func float32ToUint64Pure(xs []float32, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func float32ToUint64Avx2(xs []float32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	float32ToUint64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func float32ToUint64Avx512(xs []float32, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	float32ToUint64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Float64ToUint64(xs []float64, rs []uint64) ([]uint64, error) {
	return float64ToUint64(xs, rs)
}

func float64ToUint64Pure(xs []float64, rs []uint64) ([]uint64, error) {
	for i, x := range xs {
		rs[i] = uint64(x)
	}
	return rs, nil
}

/*
func float64ToUint64Avx2(xs []float64, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	float64ToUint64Avx2Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}

func float64ToUint64Avx512(xs []float64, rs []uint64) ([]uint64, error) {
	n := len(xs) / 2
	float64ToUint64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = uint64(xs[i])
	}
	return rs, nil
}
*/

func Int8ToFloat32(xs []int8, rs []float32) ([]float32, error) {
	return int8ToFloat32(xs, rs)
}

func int8ToFloat32Pure(xs []int8, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func int8ToFloat32Avx2(xs []int8, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int8ToFloat32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func int8ToFloat32Avx512(xs []int8, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int8ToFloat32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Int16ToFloat32(xs []int16, rs []float32) ([]float32, error) {
	return int16ToFloat32(xs, rs)
}

func int16ToFloat32Pure(xs []int16, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func int16ToFloat32Avx2(xs []int16, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int16ToFloat32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func int16ToFloat32Avx512(xs []int16, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int16ToFloat32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Int32ToFloat32(xs []int32, rs []float32) ([]float32, error) {
	return int32ToFloat32(xs, rs)
}

func int32ToFloat32Pure(xs []int32, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func int32ToFloat32Avx2(xs []int32, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int32ToFloat32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func int32ToFloat32Avx512(xs []int32, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	int32ToFloat32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Int64ToFloat32(xs []int64, rs []float32) ([]float32, error) {
	return int64ToFloat32(xs, rs)
}

func int64ToFloat32Pure(xs []int64, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func int64ToFloat32Avx512(xs []int64, rs []float32) ([]float32, error) {
	n := len(xs) / 2
	int64ToFloat32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToFloat32(xs []uint8, rs []float32) ([]float32, error) {
	return uint8ToFloat32(xs, rs)
}

func uint8ToFloat32Pure(xs []uint8, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func uint8ToFloat32Avx2(xs []uint8, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint8ToFloat32Avx2Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func uint8ToFloat32Avx512(xs []uint8, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint8ToFloat32Avx512Asm(xs[:n*16], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToFloat32(xs []uint16, rs []float32) ([]float32, error) {
	return uint16ToFloat32(xs, rs)
}

func uint16ToFloat32Pure(xs []uint16, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func uint16ToFloat32Avx2(xs []uint16, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint16ToFloat32Avx2Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func uint16ToFloat32Avx512(xs []uint16, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint16ToFloat32Avx512Asm(xs[:n*8], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToFloat32(xs []uint32, rs []float32) ([]float32, error) {
	return uint32ToFloat32(xs, rs)
}

func uint32ToFloat32Pure(xs []uint32, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func uint32ToFloat32Avx2(xs []uint32, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint32ToFloat32Avx2Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func uint32ToFloat32Avx512(xs []uint32, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	uint32ToFloat32Avx512Asm(xs[:n*4], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToFloat32(xs []uint64, rs []float32) ([]float32, error) {
	return uint64ToFloat32(xs, rs)
}

func uint64ToFloat32Pure(xs []uint64, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func uint64ToFloat32Avx512(xs []uint64, rs []float32) ([]float32, error) {
	n := len(xs) / 2
	uint64ToFloat32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Float64ToFloat32(xs []float64, rs []float32) ([]float32, error) {
	return float64ToFloat32(xs, rs)
}

func float64ToFloat32Pure(xs []float64, rs []float32) ([]float32, error) {
	for i, x := range xs {
		rs[i] = float32(x)
	}
	return rs, nil
}

/*
func float64ToFloat32Avx2(xs []float64, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	float64ToFloat32Avx2Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}

func float64ToFloat32Avx512(xs []float64, rs []float32) ([]float32, error) {
	n := len(xs) / 4
	float64ToFloat32Avx512Asm(xs[:n*2], rs)
	for i, j := n * 4, len(xs); i < j; i++ {
		rs[i] = float32(xs[i])
	}
	return rs, nil
}
*/

func Int8ToFloat64(xs []int8, rs []float64) ([]float64, error) {
	return int8ToFloat64(xs, rs)
}

func int8ToFloat64Pure(xs []int8, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func int8ToFloat64Avx2(xs []int8, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int8ToFloat64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func int8ToFloat64Avx512(xs []int8, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int8ToFloat64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Int16ToFloat64(xs []int16, rs []float64) ([]float64, error) {
	return int16ToFloat64(xs, rs)
}

func int16ToFloat64Pure(xs []int16, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func int16ToFloat64Avx2(xs []int16, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int16ToFloat64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func int16ToFloat64Avx512(xs []int16, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int16ToFloat64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Int32ToFloat64(xs []int32, rs []float64) ([]float64, error) {
	return int32ToFloat64(xs, rs)
}

func int32ToFloat64Pure(xs []int32, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func int32ToFloat64Avx2(xs []int32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int32ToFloat64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func int32ToFloat64Avx512(xs []int32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int32ToFloat64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Int64ToFloat64(xs []int64, rs []float64) ([]float64, error) {
	return int64ToFloat64(xs, rs)
}

func int64ToFloat64Pure(xs []int64, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func int64ToFloat64Avx512(xs []int64, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	int64ToFloat64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Uint8ToFloat64(xs []uint8, rs []float64) ([]float64, error) {
	return uint8ToFloat64(xs, rs)
}

func uint8ToFloat64Pure(xs []uint8, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func uint8ToFloat64Avx2(xs []uint8, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint8ToFloat64Avx2Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func uint8ToFloat64Avx512(xs []uint8, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint8ToFloat64Avx512Asm(xs[:n*16], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Uint16ToFloat64(xs []uint16, rs []float64) ([]float64, error) {
	return uint16ToFloat64(xs, rs)
}

func uint16ToFloat64Pure(xs []uint16, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func uint16ToFloat64Avx2(xs []uint16, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint16ToFloat64Avx2Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func uint16ToFloat64Avx512(xs []uint16, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint16ToFloat64Avx512Asm(xs[:n*8], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Uint32ToFloat64(xs []uint32, rs []float64) ([]float64, error) {
	return uint32ToFloat64(xs, rs)
}

func uint32ToFloat64Pure(xs []uint32, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func uint32ToFloat64Avx2(xs []uint32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint32ToFloat64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func uint32ToFloat64Avx512(xs []uint32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint32ToFloat64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Uint64ToFloat64(xs []uint64, rs []float64) ([]float64, error) {
	return uint64ToFloat64(xs, rs)
}

func uint64ToFloat64Pure(xs []uint64, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func uint64ToFloat64Avx512(xs []uint64, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	uint64ToFloat64Avx512Asm(xs[:n*2], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func Float32ToFloat64(xs []float32, rs []float64) ([]float64, error) {
	return float32ToFloat64(xs, rs)
}

func float32ToFloat64Pure(xs []float32, rs []float64) ([]float64, error) {
	for i, x := range xs {
		rs[i] = float64(x)
	}
	return rs, nil
}

/*
func float32ToFloat64Avx2(xs []float32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	float32ToFloat64Avx2Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}

func float32ToFloat64Avx512(xs []float32, rs []float64) ([]float64, error) {
	n := len(xs) / 2
	float32ToFloat64Avx512Asm(xs[:n*4], rs)
	for i, j := n * 2, len(xs); i < j; i++ {
		rs[i] = float64(xs[i])
	}
	return rs, nil
}
*/

func BytesToInt8(xs *types.Bytes, rs []int8) ([]int8, error) {
	return bytesToInt8(xs, rs)
}

func bytesToInt8Pure(xs *types.Bytes, rs []int8) ([]int8, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return nil, err
		}
		rs[i] = int8(val)
	}
	return rs, nil
}

func Int8ToBytes(xs []int8, rs *types.Bytes) (*types.Bytes, error) {
	return int8ToBytes(xs, rs)
}

func int8ToBytesPure(xs []int8, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToInt16(xs *types.Bytes, rs []int16) ([]int16, error) {
	return bytesToInt16(xs, rs)
}

func bytesToInt16Pure(xs *types.Bytes, rs []int16) ([]int16, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return nil, err
		}
		rs[i] = int16(val)
	}
	return rs, nil
}

func Int16ToBytes(xs []int16, rs *types.Bytes) (*types.Bytes, error) {
	return int16ToBytes(xs, rs)
}

func int16ToBytesPure(xs []int16, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToInt32(xs *types.Bytes, rs []int32) ([]int32, error) {
	return bytesToInt32(xs, rs)
}

func bytesToInt32Pure(xs *types.Bytes, rs []int32) ([]int32, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		rs[i] = int32(val)
	}
	return rs, nil
}

func Int32ToBytes(xs []int32, rs *types.Bytes) (*types.Bytes, error) {
	return int32ToBytes(xs, rs)
}

func int32ToBytesPure(xs []int32, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToInt64(xs *types.Bytes, rs []int64) ([]int64, error) {
	return bytesToInt64(xs, rs)
}

func bytesToInt64Pure(xs *types.Bytes, rs []int64) ([]int64, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		rs[i] = val
	}
	return rs, nil
}

func Int64ToBytes(xs []int64, rs *types.Bytes) (*types.Bytes, error) {
	return int64ToBytes(xs, rs)
}

func int64ToBytesPure(xs []int64, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, x, 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToUint8(xs *types.Bytes, rs []uint8) ([]uint8, error) {
	return bytesToUint8(xs, rs)
}

func bytesToUint8Pure(xs *types.Bytes, rs []uint8) ([]uint8, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return nil, err
		}
		rs[i] = uint8(val)
	}
	return rs, nil
}

func Uint8ToBytes(xs []uint8, rs *types.Bytes) (*types.Bytes, error) {
	return uint8ToBytes(xs, rs)
}

func uint8ToBytesPure(xs []uint8, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToUint16(xs *types.Bytes, rs []uint16) ([]uint16, error) {
	return bytesToUint16(xs, rs)
}

func bytesToUint16Pure(xs *types.Bytes, rs []uint16) ([]uint16, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return nil, err
		}
		rs[i] = uint16(val)
	}
	return rs, nil
}

func Uint16ToBytes(xs []uint16, rs *types.Bytes) (*types.Bytes, error) {
	return uint16ToBytes(xs, rs)
}

func uint16ToBytesPure(xs []uint16, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToUint32(xs *types.Bytes, rs []uint32) ([]uint32, error) {
	return bytesToUint32(xs, rs)
}

func bytesToUint32Pure(xs *types.Bytes, rs []uint32) ([]uint32, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		rs[i] = uint32(val)
	}
	return rs, nil
}

func Uint32ToBytes(xs []uint32, rs *types.Bytes) (*types.Bytes, error) {
	return uint32ToBytes(xs, rs)
}

func uint32ToBytesPure(xs []uint32, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToUint64(xs *types.Bytes, rs []uint64) ([]uint64, error) {
	return bytesToUint64(xs, rs)
}

func bytesToUint64Pure(xs *types.Bytes, rs []uint64) ([]uint64, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		rs[i] = uint64(val)
	}
	return rs, nil
}

func Uint64ToBytes(xs []uint64, rs *types.Bytes) (*types.Bytes, error) {
	return uint64ToBytes(xs, rs)
}

func uint64ToBytesPure(xs []uint64, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendInt(rs.Data, int64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToFloat32(xs *types.Bytes, rs []float32) ([]float32, error) {
	return bytesToFloat32(xs, rs)
}

func bytesToFloat32Pure(xs *types.Bytes, rs []float32) ([]float32, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, err
		}
		rs[i] = float32(val)
	}
	return rs, nil
}

func Float32ToBytes(xs []float32, rs *types.Bytes) (*types.Bytes, error) {
	return float32ToBytes(xs, rs)
}

func float32ToBytesPure(xs []float32, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendFloat(rs.Data, float64(x), 'G', -1, 32)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func BytesToFloat64(xs *types.Bytes, rs []float64) ([]float64, error) {
	return bytesToFloat64(xs, rs)
}

func bytesToFloat64Pure(xs *types.Bytes, rs []float64) ([]float64, error) {
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		rs[i] = val
	}
	return rs, nil
}

func Float64ToBytes(xs []float64, rs *types.Bytes) (*types.Bytes, error) {
	return float64ToBytes(xs, rs)
}

func float64ToBytesPure(xs []float64, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendFloat(rs.Data, x, 'G', -1, 64)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}
