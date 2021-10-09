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
	"matrixone/pkg/container/types"
	"matrixone/pkg/vectorize"
	"strconv"
	"unsafe"

	"golang.org/x/sys/cpu"
)

var (
	Int16ToInt8   func([]int16, []int8) ([]int8, error)
	Int32ToInt8   func([]int32, []int8) ([]int8, error)
	Int64ToInt8   func([]int64, []int8) ([]int8, error)
	Uint8ToInt8   func([]uint8, []int8) ([]int8, error)
	Uint16ToInt8  func([]uint16, []int8) ([]int8, error)
	Uint32ToInt8  func([]uint32, []int8) ([]int8, error)
	Uint64ToInt8  func([]uint64, []int8) ([]int8, error)
	Float32ToInt8 func([]float32, []int8) ([]int8, error)
	Float64ToInt8 func([]float64, []int8) ([]int8, error)

	Int8ToInt16    func([]int8, []int16) ([]int16, error)
	Int32ToInt16   func([]int32, []int16) ([]int16, error)
	Int64ToInt16   func([]int64, []int16) ([]int16, error)
	Uint8ToInt16   func([]uint8, []int16) ([]int16, error)
	Uint16ToInt16  func([]uint16, []int16) ([]int16, error)
	Uint32ToInt16  func([]uint32, []int16) ([]int16, error)
	Uint64ToInt16  func([]uint64, []int16) ([]int16, error)
	Float32ToInt16 func([]float32, []int16) ([]int16, error)
	Float64ToInt16 func([]float64, []int16) ([]int16, error)

	Int8ToInt32    func([]int8, []int32) ([]int32, error)
	Int16ToInt32   func([]int16, []int32) ([]int32, error)
	Int64ToInt32   func([]int64, []int32) ([]int32, error)
	Uint8ToInt32   func([]uint8, []int32) ([]int32, error)
	Uint16ToInt32  func([]uint16, []int32) ([]int32, error)
	Uint32ToInt32  func([]uint32, []int32) ([]int32, error)
	Uint64ToInt32  func([]uint64, []int32) ([]int32, error)
	Float32ToInt32 func([]float32, []int32) ([]int32, error)
	Float64ToInt32 func([]float64, []int32) ([]int32, error)

	Int8ToInt64    func([]int8, []int64) ([]int64, error)
	Int16ToInt64   func([]int16, []int64) ([]int64, error)
	Int32ToInt64   func([]int32, []int64) ([]int64, error)
	Uint8ToInt64   func([]uint8, []int64) ([]int64, error)
	Uint16ToInt64  func([]uint16, []int64) ([]int64, error)
	Uint32ToInt64  func([]uint32, []int64) ([]int64, error)
	Uint64ToInt64  func([]uint64, []int64) ([]int64, error)
	Float32ToInt64 func([]float32, []int64) ([]int64, error)
	Float64ToInt64 func([]float64, []int64) ([]int64, error)

	Int8ToUint8    func([]int8, []uint8) ([]uint8, error)
	Int16ToUint8   func([]int16, []uint8) ([]uint8, error)
	Int32ToUint8   func([]int32, []uint8) ([]uint8, error)
	Int64ToUint8   func([]int64, []uint8) ([]uint8, error)
	Uint16ToUint8  func([]uint16, []uint8) ([]uint8, error)
	Uint32ToUint8  func([]uint32, []uint8) ([]uint8, error)
	Uint64ToUint8  func([]uint64, []uint8) ([]uint8, error)
	Float32ToUint8 func([]float32, []uint8) ([]uint8, error)
	Float64ToUint8 func([]float64, []uint8) ([]uint8, error)

	Int8ToUint16    func([]int8, []uint16) ([]uint16, error)
	Int16ToUint16   func([]int16, []uint16) ([]uint16, error)
	Int32ToUint16   func([]int32, []uint16) ([]uint16, error)
	Int64ToUint16   func([]int64, []uint16) ([]uint16, error)
	Uint8ToUint16   func([]uint8, []uint16) ([]uint16, error)
	Uint32ToUint16  func([]uint32, []uint16) ([]uint16, error)
	Uint64ToUint16  func([]uint64, []uint16) ([]uint16, error)
	Float32ToUint16 func([]float32, []uint16) ([]uint16, error)
	Float64ToUint16 func([]float64, []uint16) ([]uint16, error)

	Int8ToUint32    func([]int8, []uint32) ([]uint32, error)
	Int16ToUint32   func([]int16, []uint32) ([]uint32, error)
	Int32ToUint32   func([]int32, []uint32) ([]uint32, error)
	Int64ToUint32   func([]int64, []uint32) ([]uint32, error)
	Uint8ToUint32   func([]uint8, []uint32) ([]uint32, error)
	Uint16ToUint32  func([]uint16, []uint32) ([]uint32, error)
	Uint64ToUint32  func([]uint64, []uint32) ([]uint32, error)
	Float32ToUint32 func([]float32, []uint32) ([]uint32, error)
	Float64ToUint32 func([]float64, []uint32) ([]uint32, error)

	Int8ToUint64    func([]int8, []uint64) ([]uint64, error)
	Int16ToUint64   func([]int16, []uint64) ([]uint64, error)
	Int32ToUint64   func([]int32, []uint64) ([]uint64, error)
	Int64ToUint64   func([]int64, []uint64) ([]uint64, error)
	Uint8ToUint64   func([]uint8, []uint64) ([]uint64, error)
	Uint16ToUint64  func([]uint16, []uint64) ([]uint64, error)
	Uint32ToUint64  func([]uint32, []uint64) ([]uint64, error)
	Float32ToUint64 func([]float32, []uint64) ([]uint64, error)
	Float64ToUint64 func([]float64, []uint64) ([]uint64, error)

	Int8ToFloat32    func([]int8, []float32) ([]float32, error)
	Int16ToFloat32   func([]int16, []float32) ([]float32, error)
	Int32ToFloat32   func([]int32, []float32) ([]float32, error)
	Int64ToFloat32   func([]int64, []float32) ([]float32, error)
	Uint8ToFloat32   func([]uint8, []float32) ([]float32, error)
	Uint16ToFloat32  func([]uint16, []float32) ([]float32, error)
	Uint32ToFloat32  func([]uint32, []float32) ([]float32, error)
	Uint64ToFloat32  func([]uint64, []float32) ([]float32, error)
	Float64ToFloat32 func([]float64, []float32) ([]float32, error)

	Int8ToFloat64    func([]int8, []float64) ([]float64, error)
	Int16ToFloat64   func([]int16, []float64) ([]float64, error)
	Int32ToFloat64   func([]int32, []float64) ([]float64, error)
	Int64ToFloat64   func([]int64, []float64) ([]float64, error)
	Uint8ToFloat64   func([]uint8, []float64) ([]float64, error)
	Uint16ToFloat64  func([]uint16, []float64) ([]float64, error)
	Uint32ToFloat64  func([]uint32, []float64) ([]float64, error)
	Uint64ToFloat64  func([]uint64, []float64) ([]float64, error)
	Float32ToFloat64 func([]float32, []float64) ([]float64, error)

	BytesToInt8    func(*types.Bytes, []int8) ([]int8, error)
	Int8ToBytes    func([]int8, *types.Bytes) (*types.Bytes, error)
	BytesToInt16   func(*types.Bytes, []int16) ([]int16, error)
	Int16ToBytes   func([]int16, *types.Bytes) (*types.Bytes, error)
	BytesToInt32   func(*types.Bytes, []int32) ([]int32, error)
	Int32ToBytes   func([]int32, *types.Bytes) (*types.Bytes, error)
	BytesToInt64   func(*types.Bytes, []int64) ([]int64, error)
	Int64ToBytes   func([]int64, *types.Bytes) (*types.Bytes, error)
	BytesToUint8   func(*types.Bytes, []uint8) ([]uint8, error)
	Uint8ToBytes   func([]uint8, *types.Bytes) (*types.Bytes, error)
	BytesToUint16  func(*types.Bytes, []uint16) ([]uint16, error)
	Uint16ToBytes  func([]uint16, *types.Bytes) (*types.Bytes, error)
	BytesToUint32  func(*types.Bytes, []uint32) ([]uint32, error)
	Uint32ToBytes  func([]uint32, *types.Bytes) (*types.Bytes, error)
	BytesToUint64  func(*types.Bytes, []uint64) ([]uint64, error)
	Uint64ToBytes  func([]uint64, *types.Bytes) (*types.Bytes, error)
	BytesToFloat32 func(*types.Bytes, []float32) ([]float32, error)
	Float32ToBytes func([]float32, *types.Bytes) (*types.Bytes, error)
	BytesToFloat64 func(*types.Bytes, []float64) ([]float64, error)
	Float64ToBytes func([]float64, *types.Bytes) (*types.Bytes, error)
)

func init() {
	if cpu.X86.HasAVX512 {
		Int16ToInt8 = numericToNumeric[int16, int8]
		Int32ToInt8 = numericToNumeric[int32, int8]
		Int64ToInt8 = numericToNumeric[int64, int8]
		Uint8ToInt8 = numericToNumeric[uint8, int8]
		Uint16ToInt8 = numericToNumeric[uint16, int8]
		Uint32ToInt8 = numericToNumeric[uint32, int8]
		Uint64ToInt8 = numericToNumeric[uint64, int8]
		Float32ToInt8 = numericToNumeric[float32, int8]
		Float64ToInt8 = numericToNumeric[float64, int8]

		Int8ToInt16 = numericToNumeric[int8, int16]
		Int32ToInt16 = numericToNumeric[int32, int16]
		Int64ToInt16 = numericToNumeric[int64, int16]
		Uint8ToInt16 = numericToNumeric[uint8, int16]
		Uint16ToInt16 = numericToNumeric[uint16, int16]
		Uint32ToInt16 = numericToNumeric[uint32, int16]
		Uint64ToInt16 = numericToNumeric[uint64, int16]
		Float32ToInt16 = numericToNumeric[float32, int16]
		Float64ToInt16 = numericToNumeric[float64, int16]

		Int8ToInt32 = numericToNumeric[int8, int32]
		Int16ToInt32 = numericToNumeric[int16, int32]
		Int64ToInt32 = numericToNumeric[int64, int32]
		Uint8ToInt32 = numericToNumeric[uint8, int32]
		Uint16ToInt32 = numericToNumeric[uint16, int32]
		Uint32ToInt32 = numericToNumeric[uint32, int32]
		Uint64ToInt32 = numericToNumeric[uint64, int32]
		Float32ToInt32 = numericToNumeric[float32, int32]
		Float64ToInt32 = numericToNumeric[float64, int32]

		Int8ToInt64 = numericToNumeric[int8, int64]
		Int16ToInt64 = numericToNumeric[int16, int64]
		Int32ToInt64 = numericToNumeric[int32, int64]
		Uint8ToInt64 = numericToNumeric[uint8, int64]
		Uint16ToInt64 = numericToNumeric[uint16, int64]
		Uint32ToInt64 = numericToNumeric[uint32, int64]
		Uint64ToInt64 = numericToNumeric[uint64, int64]
		Float32ToInt64 = numericToNumeric[float32, int64]
		Float64ToInt64 = numericToNumeric[float64, int64]

		Int8ToUint8 = numericToNumeric[int8, uint8]
		Int16ToUint8 = numericToNumeric[int16, uint8]
		Int32ToUint8 = numericToNumeric[int32, uint8]
		Int64ToUint8 = numericToNumeric[int64, uint8]
		Uint16ToUint8 = numericToNumeric[uint16, uint8]
		Uint32ToUint8 = numericToNumeric[uint32, uint8]
		Uint64ToUint8 = numericToNumeric[uint64, uint8]
		Float32ToUint8 = numericToNumeric[float32, uint8]
		Float64ToUint8 = numericToNumeric[float64, uint8]

		Int8ToUint16 = numericToNumeric[int8, uint16]
		Int16ToUint16 = numericToNumeric[int16, uint16]
		Int32ToUint16 = numericToNumeric[int32, uint16]
		Int64ToUint16 = numericToNumeric[int64, uint16]
		Uint8ToUint16 = numericToNumeric[uint8, uint16]
		Uint32ToUint16 = numericToNumeric[uint32, uint16]
		Uint64ToUint16 = numericToNumeric[uint64, uint16]
		Float32ToUint16 = numericToNumeric[float32, uint16]
		Float64ToUint16 = numericToNumeric[float64, uint16]

		Int8ToUint32 = numericToNumeric[int8, uint32]
		Int16ToUint32 = numericToNumeric[int16, uint32]
		Int32ToUint32 = numericToNumeric[int32, uint32]
		Int64ToUint32 = numericToNumeric[int64, uint32]
		Uint8ToUint32 = numericToNumeric[uint8, uint32]
		Uint16ToUint32 = numericToNumeric[uint16, uint32]
		Uint64ToUint32 = numericToNumeric[uint64, uint32]
		Float32ToUint32 = numericToNumeric[float32, uint32]
		Float64ToUint32 = numericToNumeric[float64, uint32]

		Int8ToUint64 = numericToNumeric[int8, uint64]
		Int16ToUint64 = numericToNumeric[int16, uint64]
		Int32ToUint64 = numericToNumeric[int32, uint64]
		Int64ToUint64 = numericToNumeric[int64, uint64]
		Uint8ToUint64 = numericToNumeric[uint8, uint64]
		Uint16ToUint64 = numericToNumeric[uint16, uint64]
		Uint32ToUint64 = numericToNumeric[uint32, uint64]
		Float32ToUint64 = numericToNumeric[float32, uint64]
		Float64ToUint64 = numericToNumeric[float64, uint64]

		Int8ToFloat32 = numericToNumeric[int8, float32]
		Int16ToFloat32 = numericToNumeric[int16, float32]
		Int32ToFloat32 = numericToNumeric[int32, float32]
		Int64ToFloat32 = numericToNumeric[int64, float32]
		Uint8ToFloat32 = numericToNumeric[uint8, float32]
		Uint16ToFloat32 = numericToNumeric[uint16, float32]
		Uint32ToFloat32 = numericToNumeric[uint32, float32]
		Uint64ToFloat32 = numericToNumeric[uint64, float32]
		Float64ToFloat32 = numericToNumeric[float64, float32]

		Int8ToFloat64 = numericToNumeric[int8, float64]
		Int16ToFloat64 = numericToNumeric[int16, float64]
		Int32ToFloat64 = numericToNumeric[int32, float64]
		Int64ToFloat64 = numericToNumeric[int64, float64]
		Uint8ToFloat64 = numericToNumeric[uint8, float64]
		Uint16ToFloat64 = numericToNumeric[uint16, float64]
		Uint32ToFloat64 = numericToNumeric[uint32, float64]
		Uint64ToFloat64 = numericToNumeric[uint64, float64]
		Float32ToFloat64 = numericToNumeric[float32, float64]
	} else if cpu.X86.HasAVX2 {
		Int16ToInt8 = numericToNumeric[int16, int8]
		Int32ToInt8 = numericToNumeric[int32, int8]
		Int64ToInt8 = numericToNumeric[int64, int8]
		Uint8ToInt8 = numericToNumeric[uint8, int8]
		Uint16ToInt8 = numericToNumeric[uint16, int8]
		Uint32ToInt8 = numericToNumeric[uint32, int8]
		Uint64ToInt8 = numericToNumeric[uint64, int8]
		Float32ToInt8 = numericToNumeric[float32, int8]
		Float64ToInt8 = numericToNumeric[float64, int8]

		Int8ToInt16 = numericToNumeric[int8, int16]
		Int32ToInt16 = numericToNumeric[int32, int16]
		Int64ToInt16 = numericToNumeric[int64, int16]
		Uint8ToInt16 = numericToNumeric[uint8, int16]
		Uint16ToInt16 = numericToNumeric[uint16, int16]
		Uint32ToInt16 = numericToNumeric[uint32, int16]
		Uint64ToInt16 = numericToNumeric[uint64, int16]
		Float32ToInt16 = numericToNumeric[float32, int16]
		Float64ToInt16 = numericToNumeric[float64, int16]

		Int8ToInt32 = numericToNumeric[int8, int32]
		Int16ToInt32 = numericToNumeric[int16, int32]
		Int64ToInt32 = numericToNumeric[int64, int32]
		Uint8ToInt32 = numericToNumeric[uint8, int32]
		Uint16ToInt32 = numericToNumeric[uint16, int32]
		Uint32ToInt32 = numericToNumeric[uint32, int32]
		Uint64ToInt32 = numericToNumeric[uint64, int32]
		Float32ToInt32 = numericToNumeric[float32, int32]
		Float64ToInt32 = numericToNumeric[float64, int32]

		Int8ToInt64 = numericToNumeric[int8, int64]
		Int16ToInt64 = numericToNumeric[int16, int64]
		Int32ToInt64 = numericToNumeric[int32, int64]
		Uint8ToInt64 = numericToNumeric[uint8, int64]
		Uint16ToInt64 = numericToNumeric[uint16, int64]
		Uint32ToInt64 = numericToNumeric[uint32, int64]
		Uint64ToInt64 = numericToNumeric[uint64, int64]
		Float32ToInt64 = numericToNumeric[float32, int64]
		Float64ToInt64 = numericToNumeric[float64, int64]

		Int8ToUint8 = numericToNumeric[int8, uint8]
		Int16ToUint8 = numericToNumeric[int16, uint8]
		Int32ToUint8 = numericToNumeric[int32, uint8]
		Int64ToUint8 = numericToNumeric[int64, uint8]
		Uint16ToUint8 = numericToNumeric[uint16, uint8]
		Uint32ToUint8 = numericToNumeric[uint32, uint8]
		Uint64ToUint8 = numericToNumeric[uint64, uint8]
		Float32ToUint8 = numericToNumeric[float32, uint8]
		Float64ToUint8 = numericToNumeric[float64, uint8]

		Int8ToUint16 = numericToNumeric[int8, uint16]
		Int16ToUint16 = numericToNumeric[int16, uint16]
		Int32ToUint16 = numericToNumeric[int32, uint16]
		Int64ToUint16 = numericToNumeric[int64, uint16]
		Uint8ToUint16 = numericToNumeric[uint8, uint16]
		Uint32ToUint16 = numericToNumeric[uint32, uint16]
		Uint64ToUint16 = numericToNumeric[uint64, uint16]
		Float32ToUint16 = numericToNumeric[float32, uint16]
		Float64ToUint16 = numericToNumeric[float64, uint16]

		Int8ToUint32 = numericToNumeric[int8, uint32]
		Int16ToUint32 = numericToNumeric[int16, uint32]
		Int32ToUint32 = numericToNumeric[int32, uint32]
		Int64ToUint32 = numericToNumeric[int64, uint32]
		Uint8ToUint32 = numericToNumeric[uint8, uint32]
		Uint16ToUint32 = numericToNumeric[uint16, uint32]
		Uint64ToUint32 = numericToNumeric[uint64, uint32]
		Float32ToUint32 = numericToNumeric[float32, uint32]
		Float64ToUint32 = numericToNumeric[float64, uint32]

		Int8ToUint64 = numericToNumeric[int8, uint64]
		Int16ToUint64 = numericToNumeric[int16, uint64]
		Int32ToUint64 = numericToNumeric[int32, uint64]
		Int64ToUint64 = numericToNumeric[int64, uint64]
		Uint8ToUint64 = numericToNumeric[uint8, uint64]
		Uint16ToUint64 = numericToNumeric[uint16, uint64]
		Uint32ToUint64 = numericToNumeric[uint32, uint64]
		Float32ToUint64 = numericToNumeric[float32, uint64]
		Float64ToUint64 = numericToNumeric[float64, uint64]

		Int8ToFloat32 = numericToNumeric[int8, float32]
		Int16ToFloat32 = numericToNumeric[int16, float32]
		Int32ToFloat32 = numericToNumeric[int32, float32]
		Int64ToFloat32 = numericToNumeric[int64, float32]
		Uint8ToFloat32 = numericToNumeric[uint8, float32]
		Uint16ToFloat32 = numericToNumeric[uint16, float32]
		Uint32ToFloat32 = numericToNumeric[uint32, float32]
		Uint64ToFloat32 = numericToNumeric[uint64, float32]
		Float64ToFloat32 = numericToNumeric[float64, float32]

		Int8ToFloat64 = numericToNumeric[int8, float64]
		Int16ToFloat64 = numericToNumeric[int16, float64]
		Int32ToFloat64 = numericToNumeric[int32, float64]
		Int64ToFloat64 = numericToNumeric[int64, float64]
		Uint8ToFloat64 = numericToNumeric[uint8, float64]
		Uint16ToFloat64 = numericToNumeric[uint16, float64]
		Uint32ToFloat64 = numericToNumeric[uint32, float64]
		Uint64ToFloat64 = numericToNumeric[uint64, float64]
		Float32ToFloat64 = numericToNumeric[float32, float64]
	} else {
		Int16ToInt8 = numericToNumeric[int16, int8]
		Int32ToInt8 = numericToNumeric[int32, int8]
		Int64ToInt8 = numericToNumeric[int64, int8]
		Uint8ToInt8 = numericToNumeric[uint8, int8]
		Uint16ToInt8 = numericToNumeric[uint16, int8]
		Uint32ToInt8 = numericToNumeric[uint32, int8]
		Uint64ToInt8 = numericToNumeric[uint64, int8]
		Float32ToInt8 = numericToNumeric[float32, int8]
		Float64ToInt8 = numericToNumeric[float64, int8]

		Int8ToInt16 = numericToNumeric[int8, int16]
		Int32ToInt16 = numericToNumeric[int32, int16]
		Int64ToInt16 = numericToNumeric[int64, int16]
		Uint8ToInt16 = numericToNumeric[uint8, int16]
		Uint16ToInt16 = numericToNumeric[uint16, int16]
		Uint32ToInt16 = numericToNumeric[uint32, int16]
		Uint64ToInt16 = numericToNumeric[uint64, int16]
		Float32ToInt16 = numericToNumeric[float32, int16]
		Float64ToInt16 = numericToNumeric[float64, int16]

		Int8ToInt32 = numericToNumeric[int8, int32]
		Int16ToInt32 = numericToNumeric[int16, int32]
		Int64ToInt32 = numericToNumeric[int64, int32]
		Uint8ToInt32 = numericToNumeric[uint8, int32]
		Uint16ToInt32 = numericToNumeric[uint16, int32]
		Uint32ToInt32 = numericToNumeric[uint32, int32]
		Uint64ToInt32 = numericToNumeric[uint64, int32]
		Float32ToInt32 = numericToNumeric[float32, int32]
		Float64ToInt32 = numericToNumeric[float64, int32]

		Int8ToInt64 = numericToNumeric[int8, int64]
		Int16ToInt64 = numericToNumeric[int16, int64]
		Int32ToInt64 = numericToNumeric[int32, int64]
		Uint8ToInt64 = numericToNumeric[uint8, int64]
		Uint16ToInt64 = numericToNumeric[uint16, int64]
		Uint32ToInt64 = numericToNumeric[uint32, int64]
		Uint64ToInt64 = numericToNumeric[uint64, int64]
		Float32ToInt64 = numericToNumeric[float32, int64]
		Float64ToInt64 = numericToNumeric[float64, int64]

		Int8ToUint8 = numericToNumeric[int8, uint8]
		Int16ToUint8 = numericToNumeric[int16, uint8]
		Int32ToUint8 = numericToNumeric[int32, uint8]
		Int64ToUint8 = numericToNumeric[int64, uint8]
		Uint16ToUint8 = numericToNumeric[uint16, uint8]
		Uint32ToUint8 = numericToNumeric[uint32, uint8]
		Uint64ToUint8 = numericToNumeric[uint64, uint8]
		Float32ToUint8 = numericToNumeric[float32, uint8]
		Float64ToUint8 = numericToNumeric[float64, uint8]

		Int8ToUint16 = numericToNumeric[int8, uint16]
		Int16ToUint16 = numericToNumeric[int16, uint16]
		Int32ToUint16 = numericToNumeric[int32, uint16]
		Int64ToUint16 = numericToNumeric[int64, uint16]
		Uint8ToUint16 = numericToNumeric[uint8, uint16]
		Uint32ToUint16 = numericToNumeric[uint32, uint16]
		Uint64ToUint16 = numericToNumeric[uint64, uint16]
		Float32ToUint16 = numericToNumeric[float32, uint16]
		Float64ToUint16 = numericToNumeric[float64, uint16]

		Int8ToUint32 = numericToNumeric[int8, uint32]
		Int16ToUint32 = numericToNumeric[int16, uint32]
		Int32ToUint32 = numericToNumeric[int32, uint32]
		Int64ToUint32 = numericToNumeric[int64, uint32]
		Uint8ToUint32 = numericToNumeric[uint8, uint32]
		Uint16ToUint32 = numericToNumeric[uint16, uint32]
		Uint64ToUint32 = numericToNumeric[uint64, uint32]
		Float32ToUint32 = numericToNumeric[float32, uint32]
		Float64ToUint32 = numericToNumeric[float64, uint32]

		Int8ToUint64 = numericToNumeric[int8, uint64]
		Int16ToUint64 = numericToNumeric[int16, uint64]
		Int32ToUint64 = numericToNumeric[int32, uint64]
		Int64ToUint64 = numericToNumeric[int64, uint64]
		Uint8ToUint64 = numericToNumeric[uint8, uint64]
		Uint16ToUint64 = numericToNumeric[uint16, uint64]
		Uint32ToUint64 = numericToNumeric[uint32, uint64]
		Float32ToUint64 = numericToNumeric[float32, uint64]
		Float64ToUint64 = numericToNumeric[float64, uint64]

		Int8ToFloat32 = numericToNumeric[int8, float32]
		Int16ToFloat32 = numericToNumeric[int16, float32]
		Int32ToFloat32 = numericToNumeric[int32, float32]
		Int64ToFloat32 = numericToNumeric[int64, float32]
		Uint8ToFloat32 = numericToNumeric[uint8, float32]
		Uint16ToFloat32 = numericToNumeric[uint16, float32]
		Uint32ToFloat32 = numericToNumeric[uint32, float32]
		Uint64ToFloat32 = numericToNumeric[uint64, float32]
		Float64ToFloat32 = numericToNumeric[float64, float32]

		Int8ToFloat64 = numericToNumeric[int8, float64]
		Int16ToFloat64 = numericToNumeric[int16, float64]
		Int32ToFloat64 = numericToNumeric[int32, float64]
		Int64ToFloat64 = numericToNumeric[int64, float64]
		Uint8ToFloat64 = numericToNumeric[uint8, float64]
		Uint16ToFloat64 = numericToNumeric[uint16, float64]
		Uint32ToFloat64 = numericToNumeric[uint32, float64]
		Uint64ToFloat64 = numericToNumeric[uint64, float64]
		Float32ToFloat64 = numericToNumeric[float32, float64]

	}

	BytesToInt8 = bytesToInt[int8]
	Int8ToBytes = intToBytes[int8]
	BytesToInt16 = bytesToInt[int16]
	Int16ToBytes = intToBytes[int16]
	BytesToInt32 = bytesToInt[int32]
	Int32ToBytes = intToBytes[int32]
	BytesToInt64 = bytesToInt[int64]
	Int64ToBytes = intToBytes[int64]
	BytesToUint8 = bytesToUint[uint8]
	Uint8ToBytes = uintToBytes[uint8]
	BytesToUint16 = bytesToUint[uint16]
	Uint16ToBytes = uintToBytes[uint16]
	BytesToUint32 = bytesToUint[uint32]
	Uint32ToBytes = uintToBytes[uint32]
	BytesToUint64 = bytesToUint[uint64]
	Uint64ToBytes = uintToBytes[uint64]
	BytesToFloat32 = bytesToFloat[float32]
	Float32ToBytes = floatToBytes[float32]
	BytesToFloat64 = bytesToFloat[float64]
	Float64ToBytes = floatToBytes[float64]
}

func numericToNumeric[From, To vectorize.Numeric](xs []From, rs []To) ([]To, error) {
	for i, x := range xs {
		rs[i] = To(x)
	}
	return rs, nil
}

func intToBytes[T vectorize.SignedInt](xs []T, rs *types.Bytes) (*types.Bytes, error) {
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

func uintToBytes[T vectorize.UnsignedInt](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendUint(rs.Data, uint64(x), 10)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func floatToBytes[T vectorize.Float](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	oldLen := uint32(0)
	for _, x := range xs {
		rs.Data = strconv.AppendFloat(rs.Data, float64(x), 'G', -1, 64)
		newLen := uint32(len(rs.Data))
		rs.Offsets = append(rs.Offsets, oldLen)
		rs.Lengths = append(rs.Lengths, newLen-oldLen)
		oldLen = newLen
	}
	return rs, nil
}

func bytesToInt[T vectorize.SignedInt](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(rs[0]) * 8)
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseInt(s, 10, bitSize)
		if err != nil {
			return nil, err
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func bytesToUint[T vectorize.UnsignedInt](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(rs[0]) * 8)
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseUint(s, 10, bitSize)
		if err != nil {
			return nil, err
		}
		rs[i] = T(val)
	}
	return rs, nil
}

func bytesToFloat[T vectorize.Float](xs *types.Bytes, rs []T) ([]T, error) {
	var bitSize = int(unsafe.Sizeof(rs[0]) * 8)
	for i, o := range xs.Offsets {
		s := string(xs.Data[o : o+xs.Lengths[i]])
		val, err := strconv.ParseFloat(s, bitSize)
		if err != nil {
			return nil, err
		}
		rs[i] = T(val)
	}
	return rs, nil
}
