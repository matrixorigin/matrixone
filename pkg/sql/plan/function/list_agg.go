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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/agg"
)

var supportedAggInNewFramework = []FuncNew{
	{
		functionId: COUNT,
		class:      plan.Function_AGG | plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "count",
					aggRegister: agg.RegisterCountColumn,
				},
			},
		},
	},

	{
		functionId: STARCOUNT,
		class:      plan.Function_AGG | plan.Function_PRODUCE_NO_NULL,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_int64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "count(*)",
					aggRegister: agg.RegisterCountStar,
				},
			},
		},
	},

	{
		functionId: MIN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, MinMaxSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    ReturnFirstArgType,
				aggFramework: aggregationLogicOfOverload{
					str:         "min",
					aggRegister: agg.RegisterMin,
				},
			},
		},
	},

	{
		functionId: MAX,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, MinMaxSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    ReturnFirstArgType,
				aggFramework: aggregationLogicOfOverload{
					str:         "max",
					aggRegister: agg.RegisterMax,
				},
			},
		},
	},

	{
		functionId: SUM,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.SumReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "sum",
					aggRegister: agg.RegisterSum,
				},
			},
		},
	},

	{
		functionId: AVG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg",
					aggRegister: agg.RegisterAvg,
				},
			},
		},
	},

	{
		functionId: AVG_TW_CACHE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgTwCacheSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgTwCacheReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg_tw_cache",
					aggRegister: agg.RegisterAvgTwCache,
				},
			},
		},
	},

	{
		functionId: AVG_TW_RESULT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, agg.AvgTwResultSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    agg.AvgTwResultReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "avg_tw_result",
					aggRegister: agg.RegisterAvgTwResult,
				},
			},
		},
	},

	{
		functionId: GROUP_CONCAT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) > 0 {
				kk := make([]types.Type, len(inputs))
				needCast := false
				for i, in := range inputs {
					if in.Oid == types.T_any {
						needCast = true
						kk[i] = types.T_text.ToType()
						continue
					}
					if !aggexec.IsGroupConcatSupported(in) {
						return newCheckResultWithFailure(failedAggParametersWrong)
					}

					kk[i] = in
				}
				if needCast {
					return newCheckResultWithCast(0, kk)
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.GroupConcatReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "group_concat",
					aggRegister: agg.RegisterGroupConcat,
				},
			},
		},
	},

	{
		functionId: JSON_ARRAYAGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(_ []overload, inputs []types.Type) checkResult {
			if len(inputs) != 1 {
				return newCheckResultWithFailure(failedAggParametersWrong)
			}
			if inputs[0].Oid == types.T_any {
				return newCheckResultWithCast(0, []types.Type{types.T_text.ToType()})
			}
			switch inputs[0].Oid {
			case types.T_binary, types.T_varbinary, types.T_blob:
				return newCheckResultWithFailure(failedAggParametersWrong)
			}
			return newCheckResultWithSuccess(0)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_json.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "json_arrayagg",
					aggRegister: agg.RegisterJsonArrayAgg,
				},
			},
		},
	},

	{
		functionId: JSON_OBJECTAGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(_ []overload, inputs []types.Type) checkResult {
			if len(inputs) != 2 {
				return newCheckResultWithFailure(failedAggParametersWrong)
			}
			key := inputs[0]
			val := inputs[1]
			if key.Oid == types.T_any {
				key = types.T_varchar.ToType()
			}
			if !key.Oid.IsMySQLString() {
				return newCheckResultWithFailure(failedAggParametersWrong)
			}
			switch val.Oid {
			case types.T_binary, types.T_varbinary, types.T_blob:
				return newCheckResultWithFailure(failedAggParametersWrong)
			}
			return newCheckResultWithCast(0, []types.Type{key, val})
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_json.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "json_objectagg",
					aggRegister: agg.RegisterJsonObjectAgg,
				},
			},
		},
	},

	{
		functionId: APPROX_COUNT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_uint64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "approx_count",
					aggRegister: agg.RegisterApproxCount,
				},
			},
		},
	},

	// todo: it's a better way to rewrite `approx_count_distinct` to `approx_count(distinct col)`?. not sure.
	{
		functionId: APPROX_COUNT_DISTINCT,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				if inputs[0].Oid == types.T_any {
					return newCheckResultWithCast(0, []types.Type{types.T_uint64.ToType()})
				}
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedAggParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				aggFramework: aggregationLogicOfOverload{
					str:         "approx_count_distinct",
					aggRegister: agg.RegisterApproxCount,
				},
			},
		},
	},

	{
		functionId: ANY_VALUE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, AnyValueSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    ReturnFirstArgType,
				aggFramework: aggregationLogicOfOverload{
					str:         "any_value",
					aggRegister: agg.RegisterAny,
				},
			},
		},
	},

	{
		functionId: BIT_AND,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, BitOpsSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    BitOpsReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_and",
					aggRegister: agg.RegisterBitAnd,
				},
			},
		},
	},

	{
		functionId: BIT_OR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, BitOpsSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    BitOpsReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_or",
					aggRegister: agg.RegisterBitOr,
				},
			},
		},
	},

	{
		functionId: BIT_XOR,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, BitOpsSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    BitOpsReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "bit_xor",
					aggRegister: agg.RegisterBitXor,
				},
			},
		},
	},

	{
		functionId: VAR_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "var_pop",
					aggRegister: agg.RegisterVarPop,
				},
			},
		},
	},

	{
		functionId: STDDEV_POP,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "stddev_pop",
					aggRegister: agg.RegisterStdDevPop,
				},
			},
		},
	},

	{
		functionId: VAR_SAMPLE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "var_sample",
					aggRegister: agg.RegisterVarSample,
				},
			},
		},
	},

	{
		functionId: STDDEV_SAMPLE,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, SumSupportedTypes)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.AvgReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "stddev_sample",
					aggRegister: agg.RegisterStdDevSample,
				},
			},
		},
	},

	{
		functionId: MEDIAN,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, aggexec.MedianSupportedType)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				isAgg:      true,
				retType:    aggexec.MedianReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "median",
					aggRegister: agg.RegisterMedian,
				},
			},
		},
	},

	// function `BITMAP_CONSTRUCT_AGG`
	{
		functionId: BITMAP_CONSTRUCT_AGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, []types.T{types.T_uint64})
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_construct_agg",
					aggRegister: agg.RegisterBitmapConstruct,
				},
			},
		},
	},

	// function `BITMAP_OR_AGG`
	{
		functionId: BITMAP_OR_AGG,
		class:      plan.Function_AGG,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			return fixedUnaryAggTypeCheck(inputs, []types.T{types.T_varbinary})
		},

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},

				isAgg: true,
				aggFramework: aggregationLogicOfOverload{
					str:         "bitmap_or_agg",
					aggRegister: agg.RegisterBitmapOr,
				},
			},
		},
	},
}

var SumSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
	types.T_bit, types.T_year,
}

var MinMaxSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time, types.T_year,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
}

var AnyValueSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bit, types.T_year,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink,
	types.T_uuid,
	types.T_binary, types.T_varbinary, types.T_json,
	types.T_Rowid,
}

func ReturnFirstArgType(typs []types.Type) types.Type {
	return typs[0]
}

var BitOpsSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_binary, types.T_varbinary,
	types.T_bit,
}

var BitOpsReturnType = func(typs []types.Type) types.Type {
	if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
		return typs[0]
	}
	return types.T_uint64.ToType()
}
