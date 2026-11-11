// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// normalizeDictionaryParameters owns the compatibility storage for functions
// whose physical reads have not been converted. Never mutate borrowed arguments.
// The all-flat path returns its original slice and allocates no scratch.
func (expr *FunctionExpressionExecutor) normalizeDictionaryParameters(parameters []*vector.Vector, allowDirect bool) ([]*vector.Vector, error) {
	first := -1
	for i, p := range parameters {
		if p != nil && p.IsDictionary() {
			first = i
			break
		}
	}
	if first < 0 {
		return parameters, nil
	}
	direct := allowDirect && expr.dictionaryInput != nil && expr.dictionaryInput(parameters)
	if direct && len(expr.parameterTypes) == len(parameters) {
		for i, p := range parameters {
			if p != nil && p.IsDictionary() && types.IsUnicodeCollation(expr.parameterTypes[i].Charset) &&
				p.GetType().Oid == expr.parameterTypes[i].Oid && *p.GetType() != expr.parameterTypes[i] {
				direct = false
				break
			}
		}
	}
	if direct {
		return parameters, nil
	}

	if len(expr.dictionaryParameters) != len(parameters) {
		expr.freeDictionaryParameters()
		expr.dictionaryParameters = make([]*vector.Vector, len(parameters))
		expr.dictionaryScratch = make([]*vector.Vector, len(parameters))
	}
	copy(expr.dictionaryParameters, parameters)
	for i := first; i < len(parameters); i++ {
		source := parameters[i]
		if source == nil || !source.IsDictionary() {
			continue
		}
		if source.AllocationAccountSelection() != nil && expr.allocation == nil {
			return nil, mpool.ErrAllocationAccountInvalid
		}
		if flat, found, err := expr.dictionaryFrame.materialize(source, expr.allocation, expr.m); found {
			if err != nil {
				return nil, err
			}
			expr.dictionaryParameters[i] = flat
			continue
		}
		dst := expr.dictionaryScratch[i]
		if dst == nil {
			var err error
			dst, err = newExpressionOffHeapVector(*source.GetType(), expr.allocation)
			if err != nil {
				return nil, err
			}
			expr.dictionaryScratch[i] = dst
		} else {
			dst.Reset(*source.GetType())
		}
		if err := dst.UnionBatch(source, 0, source.Length(), nil, expr.m); err != nil {
			return nil, err
		}
		source.CopyExpressionMetadataTo(dst)
		expr.dictionaryParameters[i] = dst
	}
	return expr.dictionaryParameters, nil
}

func (expr *FunctionExpressionExecutor) freeDictionaryParameters() {
	for _, p := range expr.dictionaryScratch {
		if p != nil {
			p.Free(expr.m)
		}
	}
	expr.dictionaryScratch = nil
	expr.dictionaryParameters = nil
}

// booleanProducesNoNulls proves result validity for this evaluation, independently
// of whether the kernel can read dictionary payload. Eval uses this only for a
// full, unmasked, accounted Boolean result; other results retain normal admission.
func (expr *FunctionExpressionExecutor) booleanProducesNoNulls() bool {
	logical, comparison := false, false
	switch expr.fid {
	case function.NOT:
		if len(expr.parameterResults) != 1 || int32(expr.overloadID) != 0 {
			return false
		}
		logical = true
	case function.AND, function.OR, function.XOR:
		if len(expr.parameterResults) < 2 || int32(expr.overloadID) != 0 ||
			expr.fid == function.XOR && len(expr.parameterResults) != 2 {
			return false
		}
		logical = true
	case function.EQUAL, function.NOT_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL,
		function.LESS_THAN, function.LESS_EQUAL:
		if len(expr.parameterResults) != 2 || int32(expr.overloadID) < 0 || int32(expr.overloadID) > 1 {
			return false
		}
		comparison = true
	case function.BETWEEN:
	case function.IN, function.NOT_IN:
		// The kernel caches list membership, including its NULL bit. A
		// per-batch or unproven list cannot establish a non-NULL cache result.
		list := expr.fixedMembershipList()
		if list == nil || len(expr.parameterResults) != 2 || expr.parameterResults[1] != list {
			return false
		}
	default:
		return false
	}
	for _, argument := range expr.parameterResults {
		if argument == nil || argument.HasNull() || argument.HasGrouping() || argument.IsPreparedJSONComparisonParam() {
			return false
		}
		if logical && argument.GetType().Oid != types.T_bool {
			return false
		}
	}
	if logical {
		return true
	}
	if comparison && numericComparisonDomain(expr.parameterResults) {
		return true
	}
	// Strings, temporal comparisons, BETWEEN and fixed-list membership keep
	// their existing metadata/shape proof. A physical-only numeric kernel above
	// still receives ordinary normalization; it needs no result NULL bitmap.
	return expr.dictionaryInput != nil && expr.dictionaryInput(expr.parameterResults)
}

func numericComparisonDomain(parameters []*vector.Vector) bool {
	oid := parameters[0].GetType().Oid
	if oid != parameters[1].GetType().Oid {
		return false
	}
	// typeswitch:partial only native float32/64 comparison kernels are audited here.
	switch oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return true
	default:
		return false
	}
}

// decimal128ProducesNoNulls admits only the ordinary decimal multiply and
// lossless implicit D64->D128 cast paths. Their non-NULL branches write values
// or return the existing error; they do not synthesize NULLs. Physical input
// normalization and nullable/masked result admission remain separate contracts.
func (expr *FunctionExpressionExecutor) decimal128ProducesNoNulls() bool {
	if len(expr.parameterResults) != 2 || int32(expr.overloadID) != 0 {
		return false
	}
	switch expr.fid {
	case function.MULTI:
		left, right := expr.parameterResults[0], expr.parameterResults[1]
		if !decimalValueIsNonNull(left) || !decimalValueIsNonNull(right) {
			return false
		}
		oid := left.GetType().Oid
		return oid == right.GetType().Oid && (oid == types.T_decimal64 || oid == types.T_decimal128)
	case function.CAST:
		if expr.rowScopedDecimalCast || expr.rowScopedDiagnostics || !decimalValueIsNonNull(expr.parameterResults[0]) {
			return false
		}
		source := *expr.parameterResults[0].GetType()
		target := expr.resultType
		// The second operand is a typed NULL target placeholder, not a value
		// consumed by CAST. Still verify that its declared target agrees.
		placeholder := expr.parameterResults[1]
		if placeholder == nil || placeholder.GetType().Oid != target.Oid || placeholder.GetType().Width != target.Width || placeholder.GetType().Scale != target.Scale ||
			source.Oid != types.T_decimal64 || source.Width <= 0 || source.Width > 18 || source.Scale < 0 || source.Scale > source.Width ||
			target.Width <= 0 || target.Width > 38 || target.Scale < source.Scale || target.Scale > target.Width {
			return false
		}
		// Precision-18 D64 may carry the complete signed-int64 domain.
		// Reserve 19 coefficient digits before increasing the scale.
		width := source.Width
		if width == 18 {
			width = 19
		}
		return target.Width-target.Scale >= width-source.Scale
	default:
		return false
	}
}

func decimalValueIsNonNull(value *vector.Vector) bool {
	return value != nil && !value.HasNull() && !value.HasGrouping() && !value.IsPreparedJSONComparisonParam()
}

// fixedMembershipList identifies the execution-owned literal/runtime-filter
// list, whose row domain and contents stay independent of input batches. These
// executors do not resize the list in Eval. As for the existing membership
// cache, changing its contents requires ResetForNextQuery before evaluation.
// No new cache/generation state is introduced at the expression boundary.
func (expr *FunctionExpressionExecutor) fixedMembershipList() *vector.Vector {
	if len(expr.parameterExecutor) != 2 {
		return nil
	}
	list, ok := expr.parameterExecutor[1].(*FixedVectorExpressionExecutor)
	if !ok || !list.noNeedToSetLength || list.resultVector == nil {
		return nil
	}
	value := list.resultVector
	oid := value.GetType().Oid
	if !(oid.IsSignedInt() || oid.IsUnsignedInt() || oid.IsMySQLString()) ||
		value.IsDictionary() || value.HasGrouping() || value.IsPreparedJSONComparisonParam() {
		return nil
	}
	return value
}
