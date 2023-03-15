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
	OctUint8  func([]uint8, []types.Decimal128) ([]types.Decimal128, error)
	OctUint16 func([]uint16, []types.Decimal128) ([]types.Decimal128, error)
	OctUint32 func([]uint32, []types.Decimal128) ([]types.Decimal128, error)
	OctUint64 func([]uint64, []types.Decimal128) ([]types.Decimal128, error)
	OctInt8   func([]int8, []types.Decimal128) ([]types.Decimal128, error)
	OctInt16  func([]int16, []types.Decimal128) ([]types.Decimal128, error)
	OctInt32  func([]int32, []types.Decimal128) ([]types.Decimal128, error)
	OctInt64  func([]int64, []types.Decimal128) ([]types.Decimal128, error)
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

func Oct[T constraints.Unsigned | constraints.Signed](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	for idx := range xs {
		res, err := oct(uint64(xs[idx]))
		if err != nil {
			return nil, err
		}
		rs[idx] = res
	}
	return rs, nil
}

func OctFloat[T constraints.Float](xs []T, rs []types.Decimal128) ([]types.Decimal128, error) {
	var res types.Decimal128
	for idx := range xs {
		if xs[idx] < 0 {
			val, err := strconv.ParseInt(fmt.Sprintf("%1.0f", xs[idx]), 10, 64)
			if err != nil {
				return nil, err
			}
			res, err = oct(uint64(val))
			if err != nil {
				return nil, err
			}
		} else {
			val, err := strconv.ParseUint(fmt.Sprintf("%1.0f", xs[idx]), 10, 64)
			if err != nil {
				return nil, err
			}
			res, err = oct(val)
			if err != nil {
				return nil, err
			}
		}
		rs[idx] = res
	}
	return rs, nil
}

func oct(val uint64) (types.Decimal128, error) {
	return types.ParseDecimal128(fmt.Sprintf("%o", val), 38, 0)
}
