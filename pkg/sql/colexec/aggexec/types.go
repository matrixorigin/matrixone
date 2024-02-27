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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

// AggFuncExec is an interface to do execution for aggregation.
// todo: use vector... to replace the []*vector.Vector may be better.
type AggFuncExec interface {
	AggID() int64
	IsDistinct() bool

	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	GroupGrow(more int) error

	// Fill BulkFill and BatchFill add the value to the aggregation.
	Fill(groupIndex int, row int, vectors []*vector.Vector) error
	BulkFill(groupIndex int, vectors []*vector.Vector) error
	BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error

	// Merge merges the aggregation result of two groups.
	// BatchMerge merges the aggregation result of multiple groups.
	Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error
	BatchMerge(next AggFuncExec, offset int, groups []uint64) error

	// SetPreparedResult add a partial result to speed up.
	// todo: the old implementation is not good, we should use the vector.Vector to replace the any.
	// but for first version, I will keep it.
	SetPreparedResult(partialResult any, groupIndex int)

	// Flush return the aggregation result.
	Flush() (*vector.Vector, error)

	// Free clean the resource and reuse the aggregation if possible.
	Free()
}

// indicate who implements the AggFuncExec interface.
var (
	_ AggFuncExec = (*singleAggFuncExec1[int8, int64])(nil)
	_ AggFuncExec = (*singleAggFuncExec2[int64])(nil)
	_ AggFuncExec = (*singleAggFuncExec3[int64])(nil)
	_ AggFuncExec = &singleAggFuncExec4{}
	_ AggFuncExec = (*multiAggFuncExec1[int8])(nil)
	_ AggFuncExec = (*multiAggFuncExec2)(nil)
	_ AggFuncExec = &groupConcatExec{}
)

// MarshalBinary and UnmarshalBinary are used to serialize and deserialize the aggregation function.
// todo: not implemented yet.
func MarshalBinary(agg AggFuncExec) ([]byte, error) {
	if agg.IsDistinct() {
		return nil, moerr.NewInternalErrorNoCtx("distinct agg should not be serialized")
	}
	return nil, nil
}

func UnmarshalBinary(data []byte) (AggFuncExec, error) {
	return nil, nil
}

// MakeAgg supports to create an aggregation function executor for single column.
// todo: if we support some methods to register the param, result type and the implementation,
//
//	we can only use the aggID to create the aggregation function executor.
func MakeAgg(
	proc *process.Process,
	aggID int64, isDistinct bool, emptyIsNull bool,
	param types.Type, result types.Type, implementationAllocator any) AggFuncExec {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   result,
		emptyNull: emptyIsNull,
	}
	opt := singleAggOptimizedInfo{
		receiveNull: true,
	}

	pIsVarLen, rIsVarLen := param.IsVarlen(), result.IsVarlen()
	if pIsVarLen && rIsVarLen {
		return newSingleAggFuncExec4(proc, info, opt, implementationAllocator)
	}

	if pIsVarLen && !rIsVarLen {
		return newSingleAggFuncExec2(proc, info, opt, implementationAllocator)
	}

	if !pIsVarLen && rIsVarLen {
		return newSingleAggFuncExec3(proc, info, opt, implementationAllocator)
	}
	return newSingleAggFuncExec1(proc, info, opt, implementationAllocator)
}

// MakeMultiAgg supports creating an aggregation function executor for multiple columns.
func MakeMultiAgg(
	proc *process.Process,
	aggID int64, isDistinct bool, emptyIsNull bool,
	param []types.Type, result types.Type, implementationAllocator any) AggFuncExec {
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   result,
		emptyNull: emptyIsNull,
	}
	return newMultiAggFuncExec(proc, info, implementationAllocator)
}

// MakeGroupConcat is one special case of MakeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func MakeGroupConcat(
	proc *process.Process,
	aggID int64, isDistinct bool,
	param []types.Type, result types.Type,
	separator string) AggFuncExec {
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   result,
		emptyNull: true,
	}
	return newGroupConcatExec(proc, info, separator)
}
