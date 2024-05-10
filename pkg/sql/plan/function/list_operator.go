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
)

var supportedOperators = []FuncNew{
	// operator `=`
	// return true if a = b, return false if a != b, return null if one of a and b is null
	{
		functionId: EQUAL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if equalAndNotEqualOperatorSupports(t1, t2) {
						if t1.Oid == t2.Oid && t1.Oid.IsDecimal() {
							if t1.Scale > t2.Scale {
								t2.Scale = t1.Scale
							} else {
								t1.Scale = t2.Scale
							}
						}
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if equalAndNotEqualOperatorSupports(inputs[0], inputs[1]) {
						if inputs[0].Oid.IsDecimal() && inputs[0].Scale != inputs[1].Scale {
							t1, t2 := inputs[0], inputs[1]
							if t1.Scale > t2.Scale {
								t2.Scale = t1.Scale
							} else {
								t1.Scale = t2.Scale
							}
							return newCheckResultWithCast(0, []types.Type{t1, t2})
						}
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return equalFn
				},
			},
		},
	},

	// operator `>`
	// return a > b, if any one of a and b is null, return null.
	{
		functionId: GREAT_THAN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if otherCompareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if otherCompareOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return greatThanFn
				},
			},
		},
	},

	// operator `>=`
	// return a >= b, if any one of a and b is null, return null.
	{
		functionId: GREAT_EQUAL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if otherCompareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if otherCompareOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return greatEqualFn
				},
			},
		},
	},

	// operator `<`
	// return a < b, if any one of a and b is null, return null.
	{
		functionId: LESS_THAN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if otherCompareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if otherCompareOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return lessThanFn
				},
			},
		},
	},

	// operator `<=`
	// return a <= b, if any one of a and b is null, return null.
	{
		functionId: LESS_EQUAL,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if otherCompareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if otherCompareOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return lessEqualFn
				},
			},
		},
	},

	// operator `between`
	{
		functionId: BETWEEN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BETWEEN_AND_EXPRESSION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) != 3 {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}

			has0, t01, t1 := fixedTypeCastRule1(inputs[0], inputs[1])
			has1, t02, t2 := fixedTypeCastRule1(inputs[0], inputs[2])
			if t01.Oid != t02.Oid {
				return newCheckResultWithFailure(failedFunctionParametersWrong)
			}

			if t01.Oid == types.T_decimal64 || t01.Oid == types.T_decimal128 || t01.Oid == types.T_decimal256 {
				if t01.Scale != t1.Scale || t02.Scale != t2.Scale {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
			}

			if has0 || has1 {
				if otherCompareOperatorSupports(t01, t1) && otherCompareOperatorSupports(t01, t2) {
					return newCheckResultWithCast(0, []types.Type{t01, t1, t2})
				}
			} else {
				if otherCompareOperatorSupports(t01, t1) && otherCompareOperatorSupports(t01, t2) {
					return newCheckResultWithSuccess(0)
				}
			}

			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return betweenImpl
				},
			},
		},
	},

	// operator `!=`
	// return a != b, if any one of a and b is null, return null.
	{
		functionId: NOT_EQUAL,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if equalAndNotEqualOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if equalAndNotEqualOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return notEqualFn
				},
			},
		},
	},

	// operator `not`
	{
		functionId: NOT,
		class:      plan.Function_STRICT,
		layout:     UNARY_LOGICAL_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return notFn
				},
			},
		},
	},

	// operator `and`
	{
		functionId: AND,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			cost := 0
			for _, source := range inputs {
				if source.Oid != types.T_bool {
					can, _ := fixedImplicitTypeCast(source, types.T_bool)
					if !can {
						return newCheckResultWithFailure(failedFunctionParametersWrong)
					}
					cost++
				}
			}

			if cost == 0 {
				return newCheckResultWithSuccess(0)
			} else {
				castTypes := make([]types.Type, len(inputs))
				boolType := types.T_bool.ToType()
				for i := range castTypes {
					castTypes[i] = boolType
				}
				return newCheckResultWithCast(0, castTypes)
			}
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return opMultiAnd
				},
			},
		},
	},

	// operator `or`
	{
		functionId: OR,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			cost := 0
			for _, source := range inputs {
				if source.Oid != types.T_bool {
					can, _ := fixedImplicitTypeCast(source, types.T_bool)
					if !can {
						return newCheckResultWithFailure(failedFunctionParametersWrong)
					}
					cost++
				}
			}

			if cost == 0 {
				return newCheckResultWithSuccess(0)
			} else {
				castTypes := make([]types.Type, len(inputs))
				boolType := types.T_bool.ToType()
				for i := range castTypes {
					castTypes[i] = boolType
				}
				return newCheckResultWithCast(0, castTypes)
			}
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return opMultiOr
				},
			},
		},
	},

	// operator `xor`
	{
		functionId: XOR,
		class:      plan.Function_STRICT,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool, types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return xorFn
				},
			},
		},
	},

	// operator `like`
	{
		functionId: LIKE,
		class:      plan.Function_STRICT,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args: []types.T{
					types.T_char,
					types.T_char,
				},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().likeFn
				},
			},
			{
				overloadId: 1,
				args: []types.T{
					types.T_varchar,
					types.T_varchar,
				},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().likeFn
				},
			},
			{
				overloadId: 2,
				args: []types.T{
					types.T_text,
					types.T_text,
				},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpBuiltInRegexp().likeFn
				},
			},
		},
	},

	// operator `in`
	{
		functionId: IN,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     IN_PREDICATE,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint8]().operatorIn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint16]().operatorIn
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint32]().operatorIn
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint64]().operatorIn
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int8]().operatorIn
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int16]().operatorIn
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int32]().operatorIn
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int64]().operatorIn
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[float32]().operatorIn
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[float64]().operatorIn
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Decimal64]().operatorIn
				},
			},
			{
				overloadId: 11,
				args:       []types.T{types.T_decimal128, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Decimal128]().operatorIn
				},
			},
			{
				overloadId: 12,
				args:       []types.T{types.T_varchar, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
			{
				overloadId: 13,
				args:       []types.T{types.T_char, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
			{
				overloadId: 14,
				args:       []types.T{types.T_date, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Date]().operatorIn
				},
			},
			{
				overloadId: 15,
				args:       []types.T{types.T_datetime, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Datetime]().operatorIn
				},
			},
			{
				overloadId: 16,
				args:       []types.T{types.T_bool, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[bool]().operatorIn
				},
			},
			{
				overloadId: 17,
				args:       []types.T{types.T_timestamp, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Timestamp]().operatorIn
				},
			},
			{
				overloadId: 18,
				args:       []types.T{types.T_blob, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
			{
				overloadId: 19,
				args:       []types.T{types.T_uuid, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Uuid]().operatorIn
				},
			},
			{
				overloadId: 20,
				args:       []types.T{types.T_text, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
			{
				overloadId: 21,
				args:       []types.T{types.T_time, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Time]().operatorIn
				},
			},
			{
				overloadId: 22,
				args:       []types.T{types.T_binary, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
			{
				overloadId: 23,
				args:       []types.T{types.T_varbinary, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorIn
				},
			},
		},
	},

	// operator `not in`
	{
		functionId: NOT_IN,
		class:      plan.Function_STRICT,
		layout:     IN_PREDICATE,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint8]().operatorNotIn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint16]().operatorNotIn
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint32]().operatorNotIn
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[uint64]().operatorNotIn
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int8]().operatorNotIn
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int16]().operatorNotIn
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int32]().operatorNotIn
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[int64]().operatorNotIn
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[float32]().operatorNotIn
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[float64]().operatorNotIn
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal64, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Decimal64]().operatorNotIn
				},
			},
			{
				overloadId: 11,
				args:       []types.T{types.T_decimal128, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Decimal128]().operatorNotIn
				},
			},
			{
				overloadId: 12,
				args:       []types.T{types.T_varchar, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
			{
				overloadId: 13,
				args:       []types.T{types.T_char, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
			{
				overloadId: 14,
				args:       []types.T{types.T_date, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Date]().operatorNotIn
				},
			},
			{
				overloadId: 15,
				args:       []types.T{types.T_datetime, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Datetime]().operatorNotIn
				},
			},
			{
				overloadId: 16,
				args:       []types.T{types.T_bool, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[bool]().operatorNotIn
				},
			},
			{
				overloadId: 17,
				args:       []types.T{types.T_timestamp, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Timestamp]().operatorNotIn
				},
			},
			{
				overloadId: 18,
				args:       []types.T{types.T_blob, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
			{
				overloadId: 19,
				args:       []types.T{types.T_uuid, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Uuid]().operatorNotIn
				},
			},
			{
				overloadId: 20,
				args:       []types.T{types.T_text, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
			{
				overloadId: 21,
				args:       []types.T{types.T_time, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorFixedIn[types.Time]().operatorNotIn
				},
			},
			{
				overloadId: 22,
				args:       []types.T{types.T_binary, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
			{
				overloadId: 23,
				args:       []types.T{types.T_varbinary, types.T_tuple},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return newOpOperatorStrIn().operatorNotIn
				},
			},
		},
	},

	// operator `+`
	// return a + b, return null if any of a and b is null.
	{
		functionId: PLUS,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if plusOperatorSupportsVectorScalar(t1, t2) {
						return newCheckResultWithCast(1, []types.Type{t1, t2})
					} else if plusOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if plusOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					if parameters[0].Oid == types.T_decimal64 {
						scale1 := parameters[0].Scale
						scale2 := parameters[1].Scale
						if scale1 < scale2 {
							scale1 = scale2
						}
						return types.New(types.T_decimal64, 18, scale1)
					}
					if parameters[0].Oid == types.T_decimal128 {
						scale1 := parameters[0].Scale
						scale2 := parameters[1].Scale
						if scale1 < scale2 {
							scale1 = scale2
						}
						return types.New(types.T_decimal128, 38, scale1)
					}
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return plusFn
				},
			},
			{
				overloadId: 1,
				retType: func(parameters []types.Type) types.Type {
					// (Vector + Scalar) or (Scalar + Vector) => Vector
					if parameters[0].Oid.IsArrayRelate() {
						return parameters[0]
					} else {
						return parameters[1]
					}
				},
				newOp: func() executeLogicOfOverload {
					return plusFnVectorScalar
				},
			},
		},
	},

	// operator `-`
	// return a - b, return null if any of a and b is null.
	{
		functionId: MINUS,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if minusOperatorSupportsVectorScalar(t1, t2) {
						return newCheckResultWithCast(1, []types.Type{t1, t2})
					} else if minusOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if minusOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					if parameters[0].Oid == types.T_decimal64 {
						scale1 := parameters[0].Scale
						scale2 := parameters[1].Scale
						if scale1 < scale2 {
							scale1 = scale2
						}
						return types.New(types.T_decimal64, 18, scale1)
					}
					if parameters[0].Oid == types.T_decimal128 {
						scale1 := parameters[0].Scale
						scale2 := parameters[1].Scale
						if scale1 < scale2 {
							scale1 = scale2
						}
						return types.New(types.T_decimal128, 38, scale1)
					}
					if parameters[0].Oid == types.T_date || parameters[0].Oid == types.T_datetime {
						return types.T_int64.ToType()
					}
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return minusFn
				},
			},
			{
				overloadId: 1,
				retType: func(parameters []types.Type) types.Type {
					// (Vector - Scalar) => Vector
					return parameters[0]

				},
				newOp: func() executeLogicOfOverload {
					return minusFnVectorScalar
				},
			},
		},
	},

	// operator `*`
	// return a * b. return null if any of parameters is null.
	{
		functionId: MULTI,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if multiOperatorSupportsVectorScalar(t1, t2) {
						return newCheckResultWithCast(1, []types.Type{t1, t2})
					} else if multiOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if multiOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					if parameters[0].Oid == types.T_decimal64 || parameters[0].Oid == types.T_decimal128 {
						scale := int32(12)
						scale1 := parameters[0].Scale
						scale2 := parameters[1].Scale
						if scale1 > scale {
							scale = scale1
						}
						if scale2 > scale {
							scale = scale2
						}
						if scale1+scale2 < scale {
							scale = scale1 + scale2
						}
						return types.New(types.T_decimal128, 38, scale)
					}
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return multiFn
				},
			},
			{
				overloadId: 1,
				retType: func(parameters []types.Type) types.Type {
					// (Vector * Scalar) or (Scalar * Vector) => Vector
					if parameters[0].Oid.IsArrayRelate() {
						return parameters[0]
					} else {
						return parameters[1]
					}
				},
				newOp: func() executeLogicOfOverload {
					return multiFnVectorScalar
				},
			},
		},
	},

	// operator `/`
	// return a / b if b was not zero. return null if any of a and b is null.
	{
		functionId: DIV,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule2(inputs[0], inputs[1])
				if has {
					if divOperatorSupportsVectorScalar(t1, t2) {
						return newCheckResultWithCast(1, []types.Type{t1, t2})
					} else if divOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if divOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					if parameters[0].Oid.IsDecimal() {
						scale := int32(12)
						scale1 := parameters[0].Scale
						if scale1 > scale {
							scale = scale1
						}
						if scale1+6 < scale {
							scale = scale1 + 6
						}
						return types.New(types.T_decimal128, 38, scale)
					}
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return divFn
				},
			},
			{
				overloadId: 1,
				retType: func(parameters []types.Type) types.Type {
					// Vector / Scalar => Vector
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return divFnVectorScalar

				},
			},
		},
	},

	// operator `div`
	// return integer part of a / b if b was not zero.
	// return null if any of parameters was null.
	{
		functionId: INTEGER_DIV,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule2(inputs[0], inputs[1])
				if has {
					if integerDivOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if integerDivOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return integerDivFn
				},
			},
		},
	},

	// operator `mod`
	{
		functionId: MOD,
		class:      plan.Function_STRICT,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if modOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if modOperatorSupports(inputs[0], inputs[1]) {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					typ := parameters[0]
					if typ.Oid.IsDecimal() {
						if typ.Scale < parameters[1].Scale {
							typ.Scale = parameters[1].Scale
						}
					}
					return typ
				},
				newOp: func() executeLogicOfOverload {
					return modFn
				},
			},
		},
	},

	// operator `unary_plus`
	// e.g : select +a;
	{
		functionId: UNARY_PLUS,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     UNARY_ARITHMETIC_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[uint8]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[uint16]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[uint32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[uint64]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[int8]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[int16]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[int32]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[int64]
				},
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[float32]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[float64]
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[types.Decimal64]
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryPlus[types.Decimal128]
				},
			},
		},
	},

	// operator `unary_minus`
	// e.g : select -a;
	{
		functionId: UNARY_MINUS,
		class:      plan.Function_STRICT | plan.Function_ZONEMAPPABLE,
		layout:     UNARY_ARITHMETIC_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[int8]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[int16]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[int32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[int64]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[float32]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinus[float64]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinusDecimal64
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryMinusDecimal128
				},
			},
		},
	},

	// operator `unary_tilde`
	{
		functionId: UNARY_TILDE,
		class:      plan.Function_STRICT,
		layout:     UNARY_ARITHMETIC_OPERATOR,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[int8]
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[int16]
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[int32]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[int64]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[uint8]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[uint16]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[uint32]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorUnaryTilde[uint64]
				},
			},
		},
	},

	// operator `case X then Y case X1 then Y1 ... (else Z)`
	{
		functionId: CASE,
		class:      plan.Function_NONE,
		layout:     CASE_WHEN_EXPRESSION,
		checkFn:    caseCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[1]
				},
				newOp: func() executeLogicOfOverload {
					return caseFn
				},
			},
		},
	},

	// operator `coalesce`
	{
		functionId: COALESCE,
		class:      plan.Function_STRICT,
		layout:     STANDARD_FUNCTION,
		checkFn:    coalesceCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_varchar},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varchar.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_char},
				retType: func(parameters []types.Type) types.Type {
					return types.T_char.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[int8]
				},
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int16.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[int16]
				},
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[int32]
				},
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[int64]
				},
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint8.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[uint8]
				},
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint16.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[uint16]
				},
			}, {
				overloadId: 8,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[uint32]
				},
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[uint64]
				},
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[float32]
				},
			},
			{
				overloadId: 11,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[float64]
				},
			},
			{
				overloadId: 12,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[bool]
				},
			},
			{
				overloadId: 13,
				args:       []types.T{types.T_datetime},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Datetime]
				},
			},
			{
				overloadId: 14,
				args:       []types.T{types.T_timestamp},
				retType: func(parameters []types.Type) types.Type {
					ret := types.T_timestamp.ToType()
					setMaxScaleFromSource(&ret, parameters)
					return ret
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Timestamp]
				},
			},
			{
				overloadId: 15,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Decimal64]
				},
			},
			{
				overloadId: 16,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Decimal128]
				},
			},
			{
				overloadId: 17,
				args:       []types.T{types.T_date},
				retType: func(parameters []types.Type) types.Type {
					return types.T_date.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Date]
				},
			},
			{
				overloadId: 18,
				args:       []types.T{types.T_uuid},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uuid.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Uuid]
				},
			},
			{
				overloadId: 19,
				args:       []types.T{types.T_time},
				retType: func(parameters []types.Type) types.Type {
					return types.T_time.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceGeneral[types.Time]
				},
			},
			{
				overloadId: 20,
				args:       []types.T{types.T_json},
				retType: func(parameters []types.Type) types.Type {
					return types.T_json.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 21,
				args:       []types.T{types.T_blob},
				retType: func(parameters []types.Type) types.Type {
					return types.T_blob.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 22,
				args:       []types.T{types.T_text},
				retType: func(parameters []types.Type) types.Type {
					return types.T_text.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 23,
				args:       []types.T{types.T_array_float32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float32.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
			{
				overloadId: 24,
				args:       []types.T{types.T_array_float64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_array_float64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return CoalesceStr
				},
			},
		},
	},

	// operator `cast`
	// TODO: we need to migrate the old code later on.
	{
		functionId: CAST,
		class:      plan.Function_STRICT,
		layout:     CAST_EXPRESSION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// cast-operator should check param types strictly
			if len(inputs) == 2 {
				if IfTypeCastSupported(inputs[0].Oid, inputs[1].Oid) {
					return newCheckResultWithSuccess(0)
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[1]
				},
				newOp: func() executeLogicOfOverload {
					return NewCast
				},
			},
		},
	},

	// operator `bit_cast`
	{
		functionId: BIT_CAST,
		class:      plan.Function_STRICT,
		layout:     CAST_EXPRESSION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				tid := inputs[0].Oid
				if tid == types.T_binary || tid == types.T_varbinary || tid == types.T_blob {
					if inputs[1].IsFixedLen() {
						return newCheckResultWithSuccess(0)
					}
				}
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(paramTypes []types.Type) types.Type {
					return paramTypes[1]
				},
				newOp: func() executeLogicOfOverload {
					return BitCast
				},
			},
		},
	},

	// operator `is`
	{
		functionId: IS,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool, types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpIs
				},
			},
		},
	},

	// operator `is not`
	{
		functionId: ISNOT,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool, types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpIsNot
				},
			},
		},
	},

	// operator `is null`
	{
		functionId: ISNULL,
		class:      plan.Function_PRODUCE_NO_NULL | plan.Function_ZONEMAPPABLE,
		layout:     IS_EXPRESSION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpIsNull
				},
			},
		},
	},

	// operator `is not null`
	{
		functionId: ISNOTNULL,
		class:      plan.Function_PRODUCE_NO_NULL | plan.Function_ZONEMAPPABLE,
		layout:     IS_NOT_EXPRESSION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpIsNotNull
				},
			},
		},
	},

	// operator `is true`
	{
		functionId: ISTRUE,
		class:      plan.Function_PRODUCE_NO_NULL,
		layout:     IS_EXPRESSION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorIsTrue
				},
			},
		},
	},

	// operator `is not true`
	{
		functionId: ISNOTTRUE,
		class:      plan.Function_PRODUCE_NO_NULL,
		layout:     IS_NOT_EXPRESSION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorIsNotTrue
				},
			},
		},
	},

	// operator `is false`
	{
		functionId: ISFALSE,
		class:      plan.Function_PRODUCE_NO_NULL,
		layout:     IS_EXPRESSION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorIsFalse
				},
			},
		},
	},

	// operator `is not false`
	{
		functionId: ISNOTFALSE,
		class:      plan.Function_PRODUCE_NO_NULL,
		layout:     IS_NOT_EXPRESSION,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_bool},
				retType: func(parameters []types.Type) types.Type {
					return types.T_bool.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorIsNotFalse
				},
			},
		},
	},

	// operator `&`
	{
		functionId: OP_BIT_AND,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitAndInt64Fn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitAndStrFn
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitAndStrFn
				},
			},
		},
	},

	// operator `|`
	{
		functionId: OP_BIT_OR,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitOrInt64Fn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitOrStrFn
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitOrStrFn
				},
			},
		},
	},

	// operator `^`
	{
		functionId: OP_BIT_XOR,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitXorInt64Fn
				},
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitXorStrFn
				},
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitXorStrFn
				},
			},
		},
	},

	// operator `<<`
	{
		functionId: OP_BIT_SHIFT_LEFT,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitShiftLeftInt64Fn
				},
			},
		},
	},

	// operator `>>`
	{
		functionId: OP_BIT_SHIFT_RIGHT,
		class:      plan.Function_STRICT,
		layout:     COMPARISON_OPERATOR,
		checkFn:    fixedTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int64, types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_int64.ToType()
				},
				newOp: func() executeLogicOfOverload {
					return operatorOpBitShiftRightInt64Fn
				},
			},
		},
	},
}
