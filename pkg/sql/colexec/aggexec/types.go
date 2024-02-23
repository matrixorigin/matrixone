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

	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	GroupGrow(more int) error

	// Fill BulkFill and BatchFill add the value to the aggregation.
	Fill(groupIndex int, row int, vectors []*vector.Vector) error
	BulkFill(groupIndex int, vectors []*vector.Vector) error
	BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error

	// SetPreparedResult add a partial result to speed up.
	SetPreparedResult(partialResult any, groupIndex int)

	// Flush return the aggregation result.
	Flush() (*vector.Vector, error)

	// Copy returns a copy of the aggregation.
	// This copy will be allocated in the inputting memory pool.
	// todo: there is no place use this method now.
	//  please refer to the old codes' Dup(pool) method.
	//Copy(mp *mpool.MPool) AggFuncExec

	// Free free the aggregation.
	// This method will do the reset and reuse the aggregation if possible.
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
)

// MakeAgg supports to create an aggregation function executor for single column.
// todo: if we support some methods to register the param, result type and the implementation,
//
//	we can only use the aggID to create the aggregation function executor.
func MakeAgg(
	proc *process.Process,
	aggID int64, param types.Type, result types.Type, implementationAllocator any) AggFuncExec {
	info := singleAggInfo{
		aggID:   aggID,
		argType: param,
		retType: result,
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
	aggID int64, param []types.Type, result types.Type, implementationAllocator any) AggFuncExec {
	info := multiAggInfo{
		aggID:    aggID,
		argTypes: param,
		retType:  result,
	}
	return newMultiAggFuncExec(proc, info, implementationAllocator)
}

// MakeGroupConcat is one special case of MakeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func MakeGroupConcat(
	proc *process.Process,
	aggID int64, param []types.Type, result types.Type) AggFuncExec {
	info := multiAggInfo{
		aggID:    aggID,
		argTypes: param,
		retType:  result,
	}
	return newGroupConcatExec(proc, info)
}
