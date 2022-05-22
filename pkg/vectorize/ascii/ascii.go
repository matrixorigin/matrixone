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

package ascii

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	AsciiUint8   func([]uint8, []uint8) []uint8
	AsciiUint16  func([]uint16, []uint8) []uint8
	AsciiUint32  func([]uint32, []uint8) []uint8
	AsciiUint64  func([]uint64, []uint8) []uint8
	AsciiInt8    func([]int8, []uint8) []uint8
	AsciiInt16   func([]int16, []uint8) []uint8
	AsciiInt32   func([]int32, []uint8) []uint8
	AsciiInt64   func([]int64, []uint8) []uint8
	AsciiFloat32 func([]float32, []uint8) []uint8
	AsciiFloat64 func([]float64, []uint8) []uint8
	AsciiBytes   func(*types.Bytes, []uint8) AsciiResult
)

type AsciiResult struct {
	Result []uint8
	Nsp    *nulls.Nulls
}

func init() {
	AsciiUint8 = asciiNumeric[uint8]
	AsciiUint16 = asciiNumeric[uint16]
	AsciiUint32 = asciiNumeric[uint32]
	AsciiUint64 = asciiNumeric[uint64]
	AsciiInt8 = asciiNumeric[int8]
	AsciiInt16 = asciiNumeric[int16]
	AsciiInt32 = asciiNumeric[int32]
	AsciiInt64 = asciiNumeric[int64]
	AsciiFloat32 = asciiNumeric[float32]
	AsciiFloat64 = asciiNumeric[float64]
	AsciiBytes = asciiBytes
}

func asciiNumeric[T constraints.Integer | constraints.Float](xs []T, rs []uint8) []uint8 {
	for i := range xs {
		str := fmt.Sprintf("%v", xs[i])
		rs[i] = str[0]
	}
	return rs
}

func asciiBytes(xs *types.Bytes, rs []uint8) AsciiResult {
	result := AsciiResult{Result: rs, Nsp: new(nulls.Nulls)}
	for i, offset := range xs.Offsets {
		if xs.Lengths[i] == 0 {
			rs[i] = 0
		} else {
			rs[i] = xs.Data[offset]
		}

		if rs[i] > 127 {
			// not in ASCII range, return NULL
			nulls.Add(result.Nsp, uint64(i))
		}
	}
	return result
}
