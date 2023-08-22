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

package function

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

var (
	// max() supported input type and output type.
	aggMaxSupportedParameters = []types.T{
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
	aggMaxReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}

	// min() supported input type and output type.
	aggMinSupportedParameters = []types.T{
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
	aggMinxReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}

	// sum() supported input type and output type.
	aggSumSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	aggSumReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_float32, types.T_float64:
			return types.T_float64.ToType()
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.T_int64.ToType()
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.T_uint64.ToType()
		case types.T_decimal64:
			return types.New(types.T_decimal64, 18, typs[0].Scale)
		case types.T_decimal128:
			return types.New(types.T_decimal128, 38, typs[0].Scale)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for sum", typs[0]))
	}

	// avg() supported input type and output type.
	aggAvgSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	aggAvgReturnType = func(typs []types.Type) types.Type {
		switch typs[0].Oid {
		case types.T_decimal64:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_decimal128:
			s := int32(12)
			if s < typs[0].Scale {
				s = typs[0].Scale
			}
			if s > typs[0].Scale+6 {
				s = typs[0].Scale + 6
			}
			return types.New(types.T_decimal128, 18, s)
		case types.T_float32, types.T_float64:
			return types.New(types.T_float64, 0, 0)
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return types.New(types.T_float64, 0, 0)
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return types.New(types.T_float64, 0, 0)
		}
		panic(moerr.NewInternalErrorNoCtx("unsupported type '%v' for avg", typs[0]))
	}

	// count() supported input type and output type.
	aggCountReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// bit_and() supported input type and output type.
	aggBitAndSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	aggBitAndReturnType = func(typs []types.Type) types.Type {
		if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
			return typs[0]
		}
		return types.New(types.T_uint64, 0, 0)
	}

	// bit_or() supported input type and output type.
	aggBitOrSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	aggBitOrReturnType = aggBitAndReturnType

	// bit_xor() supported input type and output type.
	aggBitXorSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
		types.T_binary, types.T_varbinary,
	}
	aggBitXorReturnType = aggBitAndReturnType

	// variance() supported input type and output type.
	aggVarianceSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	aggVarianceReturnType = func(typs []types.Type) types.Type {
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
	aggStdDevSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	aggStdDevReturnType = aggVarianceReturnType

	// any_value() supported input type and output type.
	aggAnyValueSupportedParameters = []types.T{
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
	aggAnyValueReturnType = func(typs []types.Type) types.Type {
		return typs[0]
	}

	// median() supported input type and output type.
	aggMedianSupportedParameters = []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128,
	}
	aggMedianReturnType = func(typs []types.Type) types.Type {
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
	aggGroupConcatReturnType = func(typs []types.Type) types.Type {
		for _, p := range typs {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}
)
