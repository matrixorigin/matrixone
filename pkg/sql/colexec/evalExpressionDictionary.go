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
