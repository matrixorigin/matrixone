// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// singleAggTypeInfo is the type info of single column agg.
type singleAggTypeInfo struct {
	argType types.Type
	retType types.Type
}

func (info singleAggTypeInfo) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{info.argType}, info.retType
}

type singleAggOptimizedInfo struct {
	receiveNull bool
}

type singleAggExecOptimized struct {
	partialGroup  int
	partialResult any
}

func (optimized *singleAggExecOptimized) SetPreparedResult(partialResult any, groupIndex int) {
	optimized.partialGroup = groupIndex
	optimized.partialResult = partialResult
}

// the executors of single column agg.
// singleAggFuncExec1 receives a fixed size type except string and returns a fixed size type except string.
// singleAggFuncExec2 receives a fixed size type except string and returns a byte type.
// singleAggFuncExec3 receives a byte type and returns a fixed size type except string.
// singleAggFuncExec4 receives a byte type and returns a byte type.
type singleAggFuncExec1[from, to types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    aggFuncArg[from]
	ret    aggFuncResult[to]
	groups []singleAggPrivateStructure1[from, to]
}
type singleAggFuncExec2[from types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    aggFuncArg[from]
	ret    aggFuncBytesResult
	groups []singleAggPrivateStructure2[from]
}
type singleAggFuncExec3[to types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    aggFuncBytesArg
	ret    aggFuncResult[to]
	groups []singleAggPrivateStructure3[to]
}
type singleAggFuncExec4 struct {
	singleAggTypeInfo
	singleAggOptimizedInfo
	singleAggExecOptimized

	arg    aggFuncBytesArg
	ret    aggFuncBytesResult
	groups []singleAggPrivateStructure4
}

func (exec *singleAggFuncExec1[from, to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		row = 0
	}

	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(row)) {
		if exec.receiveNull {
			exec.groups[groupIndex].fillNull()
		}
		return nil
	}

	exec.groups[groupIndex].fill(vector.MustFixedCol[from](vec)[row])
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	vec := vectors[0]
	length := vec.Length()
	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				exec.groups[groupIndex].fills(nil, true, length)
			}
		} else {
			exec.groups[groupIndex].fills(vector.MustFixedCol[from](vec)[0], false, length)
		}
		return nil
	}

	exec.arg.Prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if null {
					exec.groups[groupIndex].fillNull()
				} else {
					exec.groups[groupIndex].fill(v)
				}
			}
		} else {
			for i, j := uint64(0), uint64(length); i < j; i++ {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groupIndex].fill(v)
				}
			}
		}
		return nil
	}

	for i, j := uint64(0), uint64(length); i < j; i++ {
		v, _ := exec.arg.w.GetValue(i)
		exec.groups[groupIndex].fill(v)
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	vec := vectors[0]
	if vec.IsConst() {
		if vec.IsConstNull() {
			if exec.receiveNull {
				for i := 0; i < len(groups); i++ {
					if groups[i] != GroupNotMatched {
						exec.groups[groups[i]-1].fillNull()
					}
				}
			}
			return nil
		}

		value := vector.MustFixedCol[from](vec)[0]
		for i := 0; i < len(groups); i++ {
			if groups[i] != GroupNotMatched {
				exec.groups[groups[i]-1].fill(value)
			}
		}
		return nil
	}

	exec.arg.Prepare(vec)
	if exec.arg.w.WithAnyNullValue() {
		if exec.receiveNull {
			for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
				if groups[idx] != GroupNotMatched {
					v, null := exec.arg.w.GetValue(i)
					if null {
						exec.groups[groups[idx]-1].fillNull()
					} else {
						exec.groups[groups[idx]-1].fill(v)
					}
				}
				idx++
			}
			return nil
		}

		for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
			if groups[idx] != GroupNotMatched {
				v, null := exec.arg.w.GetValue(i)
				if !null {
					exec.groups[groups[idx]-1].fill(v)
				}
			}
			idx++
		}
		return nil
	}

	for i, j, idx := uint64(offset), uint64(offset+len(groups)), 0; i < j; i++ {
		if groups[idx] != GroupNotMatched {
			v, _ := exec.arg.w.GetValue(i)
			exec.groups[groups[idx]-1].fill(v)
		}
		idx++
	}
	return nil
}

func (exec *singleAggFuncExec1[from, to]) Flush() (*vector.Vector, error) {
	if exec.partialResult != nil {
		exec.groups[exec.partialGroup].fill(exec.partialResult.(from))
	}

	for i, group := range exec.groups {
		exec.ret.set(i, group.flush())
	}

	// if it was ordered window, should clean the nulls.

	// if it was count, should clean the nulls.
	// todo: this can be a method of the agg like 'nullsIfEmpty' or something.

	return exec.ret.flush(), nil
}

func (exec *singleAggFuncExec1[from, to]) Free() {
	exec.ret.free()
}
