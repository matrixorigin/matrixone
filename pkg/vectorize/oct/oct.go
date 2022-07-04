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

package oct

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

var (
	OctUint8  func([]uint8, *types.Bytes) *types.Bytes
	OctUint16 func([]uint16, *types.Bytes) *types.Bytes
	OctUint32 func([]uint32, *types.Bytes) *types.Bytes
	OctUint64 func([]uint64, *types.Bytes) *types.Bytes
	OctInt8   func([]int8, *types.Bytes) *types.Bytes
	OctInt16  func([]int16, *types.Bytes) *types.Bytes
	OctInt32  func([]int32, *types.Bytes) *types.Bytes
	OctInt64  func([]int64, *types.Bytes) *types.Bytes
)

func init() {
	OctUint8 = Oct[uint8]
	OctUint16 = Oct[uint16]
	OctUint32 = Oct[uint32]
	OctUint64 = Oct[uint64]
	OctInt8 = Oct[int8]
	OctInt16 = Oct[int16]
	OctInt32 = Oct[int32]
	OctInt64 = Oct[int64]
}

func Oct[T constraints.Unsigned | constraints.Signed](xs []T, rs *types.Bytes) *types.Bytes {
	var cursor uint32

	for idx := range xs {
		octbytes := uint64ToOctonary(uint64(xs[idx]))
		for i := range octbytes {
			rs.Data = append(rs.Data, octbytes[i])
		}
		rs.Offsets[idx] = cursor
		rs.Lengths[idx] = uint32(len(octbytes))
		cursor += uint32(len(octbytes))
	}

	return rs
}

func OctFloat[T constraints.Float](xs []T, rs *types.Bytes) (*types.Bytes, error) {
	var cursor uint32

	var octbytes []byte
	for idx := range xs {
		if xs[idx] < 0 {
			val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs[idx]), 10, 64)
			if err != nil {
				return nil, err
			}
			octbytes = uint64ToOctonary(uint64(val))
		} else {
			val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs[idx]), 10, 64)
			if err != nil {
				return nil, err
			}
			octbytes = uint64ToOctonary(val)
		}

		for i := range octbytes {
			rs.Data = append(rs.Data, octbytes[i])
		}
		rs.Offsets[idx] = cursor
		rs.Lengths[idx] = uint32(len(octbytes))
		cursor += uint32(len(octbytes))
	}
	return rs, nil
}

func uint64ToOctonary(x uint64) []byte {
	var a [21 + 1]byte // 64bit value in base 8 [64/3+(64%3!=0)]
	i := len(a)
	// Use shifts and masks instead of / and %.
	for x >= 8 {
		i--
		a[i] = byte(x&7 + '0')
		x >>= 3
	}
	// x < 8
	i--
	a[i] = byte(x + '0')
	return a[i:]
}
