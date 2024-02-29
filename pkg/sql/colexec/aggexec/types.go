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
	"fmt"
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

var (
	fromAggInfoToSingleAggImpl func(aggID int64, args types.Type, ret types.Type) (any, error)
	fromAggInfoToMultiAggImpl  func(aggID int64, args []types.Type, ret types.Type) (any, error)
)

var (
	// todo: the following code is not good.
	// string is key like "aggID_argT_retT".
	singleAggImpls = make(map[string]any)
	genSingleKey   = func(id int64, arg, ret types.Type) string {
		return fmt.Sprintf("%d_%d_%d", id, arg.Oid, ret.Oid)
	}

	// string is key like "aggID_argTs_retT".
	multiAggImpls = make(map[string]any)
	genMultiKey   = func(id int64, args []types.Type, ret types.Type) string {
		key := fmt.Sprintf("%d_", id)
		for _, arg := range args {
			key += fmt.Sprintf("%d_", arg.Oid)
		}
		key += fmt.Sprintf("%d", ret.Oid)
		return key
	}
)

func RegisterSingleAggImpl(id int64, arg types.Type, ret types.Type, impl any) {
	// todo: should do legal check later. and remove the codes in function MakeAgg.
	singleAggImpls[genSingleKey(id, arg, ret)] = impl
}

func RegisterMultiAggImpl(id int64, args []types.Type, ret types.Type, impl any) {
	// todo: should do legal check later. and remove the codes in function MakeMultiAgg.
	multiAggImpls[genMultiKey(id, args, ret)] = impl
}

func init() {
	fromAggInfoToSingleAggImpl = func(aggID int64, args types.Type, ret types.Type) (any, error) {
		key := genSingleKey(aggID, args, ret)
		if impl, ok := singleAggImpls[key]; ok {
			return impl, nil
		}
		return nil, moerr.NewInternalErrorNoCtx("not found the implementation for the aggregation %d", aggID)
	}

	fromAggInfoToMultiAggImpl = func(aggID int64, args []types.Type, ret types.Type) (any, error) {
		key := genMultiKey(aggID, args, ret)
		if impl, ok := multiAggImpls[key]; ok {
			return impl, nil
		}
		return nil, moerr.NewInternalErrorNoCtx("not found the implementation for the aggregation %d", aggID)
	}
}

// MakeAgg supports to create an aggregation function executor for single column.
func MakeAgg(
	proc *process.Process,
	aggID int64, isDistinct bool, emptyIsNull bool,
	param types.Type, result types.Type) AggFuncExec {
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

	implementationAllocator, err := fromAggInfoToSingleAggImpl(aggID, param, result)
	if err != nil {
		panic(err)
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
	param []types.Type, result types.Type) AggFuncExec {
	info := multiAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argTypes:  param,
		retType:   result,
		emptyNull: emptyIsNull,
	}
	implementationAllocator, err := fromAggInfoToMultiAggImpl(aggID, param, result)
	if err != nil {
		panic(err)
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
