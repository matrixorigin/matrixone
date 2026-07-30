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

package colexec

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ExpressionExecutorRetainedBytes returns the mpool-backed vector capacity
// owned by an executor tree. Borrowed evaluation results are deliberately not
// counted: every owned vector is reached through exactly one executor field.
//
// Go-managed executor metadata is not included here. HashBuild's expression
// admission bound retains a separate per-vector allowance for that metadata.
func ExpressionExecutorRetainedBytes(executor ExpressionExecutor) (uint64, bool) {
	switch expr := executor.(type) {
	case nil:
		return 0, true
	case *ColumnExpressionExecutor:
		return expressionVectorRetainedBytes(expr.nullVecCache)
	case *FixedVectorExpressionExecutor:
		return fixedExpressionVectorRetainedBytes(expr.resultVector)
	case *ParamExpressionExecutor:
		return sumExpressionVectorRetainedBytes(expr.null, expr.maskedNull, expr.vec)
	case *VarExpressionExecutor:
		return sumExpressionVectorRetainedBytes(expr.null, expr.maskedNull, expr.vec)
	case *ListExpressionExecutor:
		total, ok := expressionVectorRetainedBytes(expr.resultVector)
		if !ok {
			return 0, false
		}
		return addExpressionExecutorRetainedBytes(total, expr.parameterExecutor)
	case *FunctionExpressionExecutor:
		var result *vector.Vector
		if expr.resultVector != nil {
			result = expr.resultVector.GetResultVector()
		}
		var selectedResult *vector.Vector
		if expr.selectedResult != nil {
			selectedResult = expr.selectedResult.GetResultVector()
		}
		total, ok := sumExpressionVectorRetainedBytes(
			result,
			selectedResult,
			expr.selectedNullResult,
			expr.iffNullResults[0],
			expr.iffNullResults[1],
		)
		if !ok {
			return 0, false
		}
		for _, parameter := range expr.selectedParameterVectors {
			bytes, valid := expressionVectorRetainedBytes(parameter)
			if !valid || total > math.MaxUint64-bytes {
				return 0, false
			}
			total += bytes
		}
		return addExpressionExecutorRetainedBytes(total, expr.parameterExecutor)
	default:
		// External test or extension executors do not expose an ownership graph.
		// Treat their retained capacity as unknown instead of silently claiming
		// that they own no memory.
		return 0, false
	}
}

func ExpressionExecutorsRetainedBytes(executors []ExpressionExecutor) (uint64, bool) {
	return addExpressionExecutorRetainedBytes(0, executors)
}

func addExpressionExecutorRetainedBytes(total uint64, executors []ExpressionExecutor) (uint64, bool) {
	for _, executor := range executors {
		bytes, ok := ExpressionExecutorRetainedBytes(executor)
		if !ok || total > math.MaxUint64-bytes {
			return 0, false
		}
		total += bytes
	}
	return total, true
}

func sumExpressionVectorRetainedBytes(vectors ...*vector.Vector) (uint64, bool) {
	var total uint64
	for _, vec := range vectors {
		bytes, ok := expressionVectorRetainedBytes(vec)
		if !ok || total > math.MaxUint64-bytes {
			return 0, false
		}
		total += bytes
	}
	return total, true
}

func expressionVectorRetainedBytes(vec *vector.Vector) (uint64, bool) {
	if vec == nil {
		return 0, true
	}
	if vec.NeedDup() {
		// Mmap/no-copy vectors point at bytes owned by the serialized plan or
		// another vector. They are stable fixed-expression inputs, not mpool
		// capacity owned by this executor.
		return 0, false
	}
	allocated := vec.Allocated()
	if allocated < 0 {
		return 0, false
	}
	return uint64(allocated), true
}

func fixedExpressionVectorRetainedBytes(vec *vector.Vector) (uint64, bool) {
	if vec == nil || vec.NeedDup() {
		return 0, true
	}
	return expressionVectorRetainedBytes(vec)
}
