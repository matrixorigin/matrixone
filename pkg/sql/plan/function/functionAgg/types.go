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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

var (
	// count() supported input type and output type.
	AggCountReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// bit_and() supported input type and output type.
	AggBitAndSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	AggBitAndReturnType = func(typs []types.Type) types.Type {
		if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
			return typs[0]
		}
		return types.New(types.T_uint64, 0, 0)
	}

	// bit_or() supported input type and output type.
	AggBitOrSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	AggBitOrReturnType = AggBitAndReturnType

	// bit_xor() supported input type and output type.
	AggBitXorSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	AggBitXorReturnType = AggBitAndReturnType

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

	// any_value() supported input type and output type.
	AggAnyValueSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime,
		types.T_timestamp, types.T_time,
		types.T_decimal64, types.T_decimal128,
		types.T_bool,
		types.T_varchar, types.T_char, types.T_blob, types.T_text,
		types.T_uuid,
		types.T_binary, types.T_varbinary,
	}
	AggAnyValueReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}

	// median() supported input type and output type.
	AggMedianSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	AggMedianReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_decimal64:
			return types.New(types.T_decimal128, 38, typs[0].Scale+1)
		case types.T_decimal128:
			return types.New(types.T_decimal128, 38, typs[0].Scale+1)
		case types.T_float32, types.T_float64:
			return types.New(types.T_float64, 0, 0)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.New(types.T_float64, 0, 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.New(types.T_float64, 0, 0)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for median", typs[0]))
	}

	// group_concat() supported input type and output type.
	AggGroupConcatReturnType = func(typs []types.Type) types.Type {
		for _, p := range typs {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}

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
