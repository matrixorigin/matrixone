// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func divFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := divFnImpl(parameters, result, proc, length, selectList); err != nil {
		return err
	}
	appendDivisionByZeroWarnings(parameters, result.GetResultVector(), proc, length, selectList)
	return nil
}

func integerDivFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := integerDivFnImpl(parameters, result, proc, length, selectList); err != nil {
		return err
	}
	appendDivisionByZeroWarnings(parameters, result.GetResultVector(), proc, length, selectList)
	return nil
}

func modFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if err := modFnImpl(parameters, result, proc, length, selectList); err != nil {
		return err
	}
	appendDivisionByZeroWarnings(parameters, result.GetResultVector(), proc, length, selectList)
	return nil
}

// ScalarDivisionNeedsRowEvaluation is used only for scalar /, DIV and MOD.
// Their otherwise constant results must retain the actual evaluation row count
// when a successful call can produce SELECT diagnostics. Safe nonzero and NULL
// constants keep the runtime folding fast path. Resolve policy per execution,
// after prepared parameters have been materialized, rather than at construction.
func ScalarDivisionNeedsRowEvaluation(parameters []*vector.Vector, proc *process.Process) bool {
	if len(parameters) != 2 || !parameters[0].GetType().IsNumeric() || !parameters[1].GetType().IsNumeric() ||
		parameters[0].IsConstNull() || parameters[1].IsConstNull() || !proc.DivisionByZeroWarningsEnabled() {
		return false
	}
	right := parameters[1]
	if !right.IsConst() {
		// Literal vectors are also accepted by the runtime folder. They are not
		// a proven nonzero scalar, so preserve their diagnostic evaluation too.
		return true
	}
	switch right.GetType().Oid {
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](right, 0) == 0
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](right, 0) == 0
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](right, 0) == 0
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](right, 0) == 0
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](right, 0) == 0
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](right, 0) == 0
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](right, 0) == 0
	case types.T_uint64, types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](right, 0) == 0
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](right, 0) == 0
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](right, 0) == 0
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](right, 0) == 0
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](right, 0) == (types.Decimal128{})
	case types.T_decimal256:
		return vector.GetFixedAtNoTypeCheck[types.Decimal256](right, 0) == (types.Decimal256{})
	default:
		return false
	}
}

// The kernels own values and strict DML errors. Successful scalar arithmetic
// owns SELECT diagnostics here, including the heterogeneous integer DIV paths.
// Batches without NULL results do not resolve variables or scan input rows.
func appendDivisionByZeroWarnings(parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int, selectList *FunctionSelectList) {
	if length <= 0 || proc == nil || (!result.IsConstNull() && !result.GetNulls().Any()) {
		return
	}
	if selectList != nil && selectList.IgnoreAllRow() {
		return
	}
	if !proc.DivisionByZeroWarningsEnabled() {
		return
	}
	// Inspect the denominator's actual physical type; mixed DIV operands can
	// have different types. ARRAY/YEAR division retain their existing semantics.
	switch parameters[1].GetType().Oid {
	case types.T_int8:
		appendTypedDivisionByZeroWarnings[int8](parameters, result, proc, length, selectList)
	case types.T_int16:
		appendTypedDivisionByZeroWarnings[int16](parameters, result, proc, length, selectList)
	case types.T_int32:
		appendTypedDivisionByZeroWarnings[int32](parameters, result, proc, length, selectList)
	case types.T_int64:
		appendTypedDivisionByZeroWarnings[int64](parameters, result, proc, length, selectList)
	case types.T_uint8:
		appendTypedDivisionByZeroWarnings[uint8](parameters, result, proc, length, selectList)
	case types.T_uint16:
		appendTypedDivisionByZeroWarnings[uint16](parameters, result, proc, length, selectList)
	case types.T_uint32:
		appendTypedDivisionByZeroWarnings[uint32](parameters, result, proc, length, selectList)
	case types.T_uint64, types.T_bit:
		appendTypedDivisionByZeroWarnings[uint64](parameters, result, proc, length, selectList)
	case types.T_float32:
		appendTypedDivisionByZeroWarnings[float32](parameters, result, proc, length, selectList)
	case types.T_float64:
		appendTypedDivisionByZeroWarnings[float64](parameters, result, proc, length, selectList)
	case types.T_decimal64:
		appendTypedDivisionByZeroWarnings[types.Decimal64](parameters, result, proc, length, selectList)
	case types.T_decimal128:
		appendTypedDivisionByZeroWarnings[types.Decimal128](parameters, result, proc, length, selectList)
	case types.T_decimal256:
		appendTypedDivisionByZeroWarnings[types.Decimal256](parameters, result, proc, length, selectList)
	}
}

func appendTypedDivisionByZeroWarnings[T comparable](parameters []*vector.Vector, result *vector.Vector, proc *process.Process, length int, selectList *FunctionSelectList) {
	left, right := parameters[0], parameters[1]
	if left.IsConstNull() || right.IsConstNull() {
		return
	}
	divisors := vector.MustFixedColNoTypeCheck[T](right)
	var zero T
	var warnings process.WarningAccumulator
	defer warnings.Reset()
	masked := selectList != nil && !selectList.ShouldEvalAllRow()
	for i := 0; i < length; i++ {
		row := uint64(i)
		if (masked && selectList.Contains(row)) || !result.IsNull(row) || left.IsNull(row) || right.IsNull(row) {
			continue
		}
		index := i
		if right.IsConst() {
			index = 0
		}
		if divisors[index] != zero {
			continue
		}
		if warnings.Total == 0 {
			warnings.SetWarningRetentionForProcess(proc)
		}
		warnings.Add(moerr.ER_DIVISION_BY_ZERO, "Division by 0")
	}
	warnings.Flush(proc)
}
