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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

// AggFuncExecExpression is the exporting structure for the aggregation information.
// it is used to indicate the information of the aggregation function for the operators like 'group' or 'merge group'.
type AggFuncExecExpression struct {
	aggID          int64
	isDistinct     bool
	argExpressions []*plan.Expr
	extraConfig    []byte
}

func MakeAggFunctionExpression(id int64, isDistinct bool, args []*plan.Expr, config []byte) AggFuncExecExpression {
	return AggFuncExecExpression{
		aggID:          id,
		isDistinct:     isDistinct,
		argExpressions: args,
		extraConfig:    config,
	}
}

func (expr AggFuncExecExpression) GetAggID() int64 {
	return expr.aggID
}

func (expr AggFuncExecExpression) IsDistinct() bool {
	return expr.isDistinct
}

func (expr AggFuncExecExpression) GetArgExpressions() []*plan.Expr {
	return expr.argExpressions
}

func (expr AggFuncExecExpression) GetExtraConfig() []byte {
	return expr.extraConfig
}

// AggFuncExec is an interface to do execution for aggregation.
type AggFuncExec interface {
	marshal() ([]byte, error)
	unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error
	GetOptResult() SplitResult

	AggID() int64
	IsDistinct() bool

	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	// GroupGrow increases the number of groups in the aggregation.
	GroupGrow(more int) error
	// PreAllocateGroups pre-allocates more additional groups to reduce garbage collection overhead.
	PreAllocateGroups(more int) error

	// Fill BulkFill and BatchFill add the value to the aggregation.
	// Fill : add one row to the aggregation for a specific group.
	// BulkFill : add values to the aggregation for a group in bulk.
	// BatchFill : add values to the aggregation for multiple groups at once.
	Fill(groupIndex int, row int, vectors []*vector.Vector) error
	BulkFill(groupIndex int, vectors []*vector.Vector) error
	BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error

	// Merge combines the result of a self group and a group from another aggregation.
	Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error
	// BatchMerge combines the aggregation result of multiple couples.
	// next: offset + i
	// self: groups[i] - 1
	BatchMerge(next AggFuncExec, offset int, groups []uint64) error

	// SetExtraInformation sets additional information for the aggregation executor,
	// such as partial results.
	//
	// but for the 'group_concat', it was a bad hack to use the method to set the separator.
	// and for the 'cluster_centers', it was used to set the fields of this agg.
	// todo: the old implementation is not good, we should use the vector.Vector to replace the any.
	//  and the hacks should be removed.
	//  but for first version, I will keep it.
	SetExtraInformation(partialResult any, groupIndex int) (err error)

	// Flush return the aggregation result.
	Flush() ([]*vector.Vector, error)

	// Free clean the resource and reuse the aggregation if possible.
	Free()
}

// indicate who implements the AggFuncExec interface.
var (
	_ AggFuncExec = (*aggregatorFromFixedToFixed[int32, int64])(nil)
	_ AggFuncExec = (*aggregatorFromFixedToBytes[int32])(nil)
	_ AggFuncExec = (*aggregatorFromBytesToFixed[int64])(nil)
	_ AggFuncExec = (*aggregatorFromBytesToBytes)(nil)
	_ AggFuncExec = &groupConcatExec{}
)

var (
	emptyExtraInfo = singleAggExecExtraInformation{
		partialGroup:  0,
		partialResult: nil,
	}
)

type AggMemoryManager interface {
	Mp() *mpool.MPool
}

type SimpleAggMemoryManager struct {
	mp *mpool.MPool
}

func NewSimpleAggMemoryManager(mp *mpool.MPool) AggMemoryManager {
	return SimpleAggMemoryManager{mp: mp}
}

func (m SimpleAggMemoryManager) Mp() *mpool.MPool {
	return m.mp
}

// MakeAgg is the only exporting method to create an aggregation function executor.
// all the aggID should be registered before calling this function.
func MakeAgg(
	mg AggMemoryManager,
	aggID int64, isDistinct bool,
	param ...types.Type) AggFuncExec {

	exec, ok, err := makeSpecialAggExec(mg, aggID, isDistinct, param...)
	if err != nil {
		panic(err)
	}
	if ok {
		return exec
	}
	if _, ok = singleAgg[aggID]; ok && len(param) == 1 {
		return makeSingleAgg(mg, aggID, isDistinct, param[0])
	}
	panic(fmt.Sprintf("unexpected aggID %d and param types %v.", aggID, param))
}

func MakeInitialAggListFromList(mg AggMemoryManager, list []AggFuncExec) []AggFuncExec {
	result := make([]AggFuncExec, 0, len(list))
	for _, v := range list {
		param, _ := v.TypesInfo()
		result = append(result, MakeAgg(mg, v.AggID(), v.IsDistinct(), param...))
	}
	return result
}

// makeSingleAgg supports to create an aggregation function executor for single column.
func makeSingleAgg(
	mg AggMemoryManager,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {
	agg, err := getSingleAggImplByInfo(aggID, param)
	if err != nil {
		panic(err)
	}

	result := agg.ret([]types.Type{param})
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   result,
		emptyNull: agg.setNullForEmptyGroup,
	}

	pIsVarLen, rIsVarLen := param.IsVarlen(), result.IsVarlen()
	if pIsVarLen && rIsVarLen {
		return newAggregatorFromBytesToBytes(mg, info, agg)
	}

	if !pIsVarLen && rIsVarLen {
		return newAggregatorFromFixedToBytes(mg, info, agg)
	}

	if pIsVarLen {
		return newAggregatorFromBytesToFixed(mg, info, agg)
	}
	return newSingleAggFuncExec1NewVersion(mg, info, agg)
}

func makeSpecialAggExec(
	mg AggMemoryManager,
	id int64, isDistinct bool, params ...types.Type) (AggFuncExec, bool, error) {
	if _, ok := specialAgg[id]; ok {
		switch id {
		case aggIdOfCountColumn:
			return makeCount(mg, false, id, isDistinct, params[0]), true, nil
		case aggIdOfCountStar:
			return makeCount(mg, true, id, isDistinct, params[0]), true, nil
		case aggIdOfMedian:
			exec, err := makeMedian(mg, id, isDistinct, params[0])
			return exec, true, err
		case aggIdOfGroupConcat:
			return makeGroupConcat(mg, id, isDistinct, params, getCroupConcatRet(params...), groupConcatSep), true, nil
		case aggIdOfApproxCount:
			return makeApproxCount(mg, id, params[0]), true, nil
		case aggIdOfClusterCenters:
			exec, err := makeClusterCenters(mg, id, isDistinct, params[0])
			return exec, true, err
		case winIdOfRowNumber, winIdOfRank, winIdOfDenseRank:
			exec, err := makeWindowExec(mg, id, isDistinct)
			return exec, true, err
		}
	}
	return nil, false, nil
}

// makeGroupConcat is one special case of makeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func makeGroupConcat(
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

func makeCount(
	mg AggMemoryManager, isStar bool,
	aggID int64, isDistinct bool,
	param types.Type) AggFuncExec {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}

	if isStar {
		return newCountStarExec(mg, info)
	}
	return newCountColumnExecExec(mg, info)
}

func makeMedian(
	mg AggMemoryManager, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   MedianReturnType([]types.Type{param}),
		emptyNull: true,
	}
	return newMedianExecutor(mg, info)
}

func makeClusterCenters(
	mg AggMemoryManager, aggID int64, isDistinct bool, param types.Type) (AggFuncExec, error) {
	info := singleAggInfo{
		aggID:     aggID,
		distinct:  isDistinct,
		argType:   param,
		retType:   ClusterCentersReturnType([]types.Type{param}),
		emptyNull: true,
	}
	return newClusterCentersExecutor(mg, info)
}

func makeWindowExec(
	mg AggMemoryManager, aggID int64, isDistinct bool) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewInternalErrorNoCtx("window function does not support `distinct`")
	}

	info := singleAggInfo{
		aggID:     aggID,
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	return makeRankDenseRankRowNumber(mg, info), nil
}
