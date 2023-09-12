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

package functionAgg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

type numeric interface {
	types.Ints | types.UInts | types.Floats
}

type compare interface {
	constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Timestamp
}

type maxScaleNumeric interface {
	int64 | uint64 | float64
}

type allTypes interface {
	types.OrderedT | types.Decimal | []byte | bool | types.Uuid
}

type numericSlice[T numeric] []T

func (s numericSlice[T]) Len() int {
	return len(s)
}
func (s numericSlice[T]) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s numericSlice[T]) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type decimal64Slice []types.Decimal64

func (s decimal64Slice) Len() int {
	return len(s)
}
func (s decimal64Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type decimal128Slice []types.Decimal128

func (s decimal128Slice) Len() int {
	return len(s)
}
func (s decimal128Slice) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}
func (s decimal128Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

var (
	// variance() supported input type and output type.
	AggVarianceSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggVarianceReturnType = func(typs []types.Type) types.Type {
		if typs[0].IsDecimal() {
			s := int32(12)
			if typs[0].Scale > s {
				s = typs[0].Scale
			}
			return types.New(types.T_decimal128, 38, s)
		}
		return types.New(types.T_float64, 0, 0)
	}

	// stddev_pop() supported input type and output type.
	AggStdDevSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggStdDevReturnType = AggVarianceReturnType

	// rank() supported input type and output type.
	WinRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// row_number() supported input type and output type.
	WinRowNumberReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// dense_rank() supported input type and output type.
	WinDenseRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)
