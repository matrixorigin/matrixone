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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var supportedOperators = []FuncNew{
	// operator `=`
	// return true if a = b, return false if a != b, return null if one of a and b is null
	{
		functionId: EQUAL,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return equalFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `>`
	// return a > b, if any one of a and b is null, return null.
	{
		functionId: GREAT_THAN,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return greatThanFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `>=`
	// return a >= b, if any one of a and b is null, return null.
	{
		functionId: GREAT_EQUAL,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return greatEqualFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `!=`
	// return a != b, if any one of a and b is null, return null.
	{
		functionId: NOT_EQUAL,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return notEqualFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `<`
	// return a < b, if any one of a and b is null, return null.
	{
		functionId: LESS_THAN,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return lessThanFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `<=`
	// return a <= b, if any one of a and b is null, return null.
	{
		functionId: LESS_EQUAL,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     COMPARISON_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if compareOperatorSupports(t1, t2) {
						return newCheckResultWithCast(0, []types.Type{t1, t2})
					}
				} else {
					if compareOperatorSupports(inputs[0], inputs[1]) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return lessEqualFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `+`
	// return a + b, return null if any of a and b is null.
	{
		functionId: PLUS,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if plusOperatorSupports(t1, t2) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return plusFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `-`
	// return a - b, return null if any of a and b is null.
	{
		functionId: MINUS,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if minusOperatorSupports(t1, t2) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return minusFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `*`
	// return a * b. return null if any of parameters is null.
	{
		functionId: MULTI,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule1(inputs[0], inputs[1])
				if has {
					if multiOperatorSupports(t1, t2) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return multiFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `/`
	// return a / b if b was not zero. return null if any of a and b is null.
	{
		functionId: DIV,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule2(inputs[0], inputs[1])
				if has {
					if divOperatorSupports(t1, t2) {
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
					if parameters[0].Oid == types.T_decimal64 || parameters[0].Oid == types.T_decimal128 {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return divFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `div`
	// return integer part of a / b if b was not zero.
	// return null if any of parameters was null.
	{
		functionId: INTEGER_DIV,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return integerDivFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `mod`
	{
		functionId: MOD,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_ARITHMETIC_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				has, t1, t2 := fixedTypeCastRule2(inputs[0], inputs[1])
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
					return parameters[0]
				},
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return modFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `and`
	{
		functionId: AND,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 && inputs[0].Oid == types.T_bool && inputs[1].Oid == types.T_bool {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return andFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `or`
	{
		functionId: OR,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 && inputs[0].Oid == types.T_bool && inputs[1].Oid == types.T_bool {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return orFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `xor`
	{
		functionId: XOR,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 && inputs[0].Oid == types.T_bool && inputs[1].Oid == types.T_bool {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return xorFn(parameters, result, proc, length)
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
				if operator.IfTypeCastSupported(inputs[0].Oid, inputs[1].Oid) {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return operator.NewCast(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `like`
	{
		functionId: LIKE,
		class:      plan.Function_STRICT,
		layout:     BINARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 2 {
				if inputs[0].Oid.IsMySQLString() && inputs[1].Oid.IsMySQLString() {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return likeFn(parameters, result, proc, length)
				},
			},
		},
	},

	// operator `not`
	{
		functionId: NOT,
		class:      plan.Function_STRICT,
		layout:     UNARY_LOGICAL_OPERATOR,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 1 && inputs[0].Oid == types.T_bool {
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
				NewOp: func(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
					return notFn(parameters, result, proc, length)
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
				NewOp: caseFn,
			},
		},
	},

	// operator `If X THEN Y ELSE Z`, `Iff(x, y, z)`
	{
		functionId: IFF,
		class:      plan.Function_NONE,
		layout:     STANDARD_FUNCTION,
		checkFn:    iffCheck,

		Overloads: []overload{
			{
				overloadId: 0,
				retType: func(parameters []types.Type) types.Type {
					return parameters[1]
				},
				NewOp: iffFn,
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
				NewOp: operatorOpBitAndInt64Fn,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				NewOp: operatorOpBitAndStrFn,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				NewOp: operatorOpBitAndStrFn,
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
				NewOp: operatorOpBitXorInt64Fn,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				NewOp: operatorOpBitXorStrFn,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				NewOp: operatorOpBitXorStrFn,
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
				NewOp: operatorOpBitOrInt64Fn,
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_binary, types.T_binary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_binary.ToType()
				},
				NewOp: operatorOpBitOrStrFn,
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_varbinary, types.T_varbinary},
				retType: func(parameters []types.Type) types.Type {
					return types.T_varbinary.ToType()
				},
				NewOp: operatorOpBitOrStrFn,
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
				NewOp: operatorOpBitShiftLeftInt64Fn,
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
				NewOp: operatorOpBitShiftRightInt64Fn,
			},
		},
	},

	// operator `unary_plus`
	// e.g : select +a;
	{
		functionId: UNARY_PLUS,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     UNARY_ARITHMETIC_OPERATOR,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[uint8],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[uint16],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[uint32],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[uint64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[int8],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[int16],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[int32],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[int64],
			},
			{
				overloadId: 8,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[float32],
			},
			{
				overloadId: 9,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[float64],
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[types.Decimal64],
			},
			{
				overloadId: 10,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryPlus[types.Decimal128],
			},
		},
	},

	// operator `unary_minus`
	// e.g : select -a;
	{
		functionId: UNARY_MINUS,
		class:      plan.Function_STRICT | plan.Function_MONOTONIC,
		layout:     UNARY_ARITHMETIC_OPERATOR,
		checkFn:    fixedDirectlyTypeMatch,

		Overloads: []overload{
			{
				overloadId: 0,
				args:       []types.T{types.T_int8},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[int8],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[int16],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[int32],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[int64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_float32},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[float32],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_float64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinus[float64],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_decimal64},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinusDecimal64,
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_decimal128},
				retType: func(parameters []types.Type) types.Type {
					return parameters[0]
				},
				NewOp: operatorUnaryMinusDecimal128,
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
				NewOp: operatorUnaryTilde[int8],
			},
			{
				overloadId: 1,
				args:       []types.T{types.T_int16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[int16],
			},
			{
				overloadId: 2,
				args:       []types.T{types.T_int32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[int32],
			},
			{
				overloadId: 3,
				args:       []types.T{types.T_int64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[int64],
			},
			{
				overloadId: 4,
				args:       []types.T{types.T_uint8},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[uint8],
			},
			{
				overloadId: 5,
				args:       []types.T{types.T_uint16},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[uint16],
			},
			{
				overloadId: 6,
				args:       []types.T{types.T_uint32},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[uint32],
			},
			{
				overloadId: 7,
				args:       []types.T{types.T_uint64},
				retType: func(parameters []types.Type) types.Type {
					return types.T_uint64.ToType()
				},
				NewOp: operatorUnaryTilde[uint64],
			},
		},
	},
}
