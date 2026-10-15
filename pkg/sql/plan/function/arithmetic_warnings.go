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
