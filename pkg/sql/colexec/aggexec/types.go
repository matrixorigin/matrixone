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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

// AggFuncExec is an interface to do execution for aggregation.
// todo: use vector... to replace the []*vector.Vector may be better.
type AggFuncExec interface {
	marshal() ([]byte, error)
	unmarshal(result []byte, groups [][]byte) error

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

type AggMemoryManager interface {
	Mp() *mpool.MPool
	GetVector(typ types.Type) *vector.Vector
	PutVector(v *vector.Vector)
}

// MakeAgg supports to create an aggregation function executor for single column.
func MakeAgg(
	mg AggMemoryManager,
	aggID int64, isDistinct bool, emptyIsNull bool,
	param types.Type) AggFuncExec {
	implementationAllocator, result, err := getSingleAggImplByInfo(aggID, param)
	if err != nil {
		panic(err)
	}

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
		return newSingleAggFuncExec4(mg, info, opt, implementationAllocator)
	}

	if pIsVarLen && !rIsVarLen {
		return newSingleAggFuncExec2(mg, info, opt, implementationAllocator)
	}

	if !pIsVarLen && rIsVarLen {
		return newSingleAggFuncExec3(mg, info, opt, implementationAllocator)
	}
	return newSingleAggFuncExec1(mg, info, opt, implementationAllocator)
}

// MakeMultiAgg supports creating an aggregation function executor for multiple columns.
func MakeMultiAgg(
	mg AggMemoryManager,
	aggID int64, isDistinct bool, emptyIsNull bool,
	param []types.Type) AggFuncExec {
	implementationAllocator, result, err := getMultiArgAggImplByInfo(aggID, param)
	if err != nil {
		panic(err)
	}

	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   result,
		emptyNull: emptyIsNull,
	}
	return newMultiAggFuncExec(mg, info, implementationAllocator)
}

// MakeGroupConcat is one special case of MakeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func MakeGroupConcat(
	mg AggMemoryManager,
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
	return newGroupConcatExec(mg, info, separator)
}
