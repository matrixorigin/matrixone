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
)

/*
	methods to register the aggregation function.
	after registered, the function `MakeAgg` can make the aggregation function executor.
*/

func RegisterCountColumnAgg(id int64) {
	specialAgg[id] = true
	aggIdOfCountColumn = id
}

func RegisterCountStarAgg(id int64) {
	specialAgg[id] = true
	aggIdOfCountStar = id
}

func RegisterGroupConcatAgg(id int64, sep string) {
	specialAgg[id] = true
	aggIdOfGroupConcat = id
	groupConcatSep = sep
}

func RegisterApproxCountAgg(id int64) {
	specialAgg[id] = true
	aggIdOfApproxCount = id
}

func RegisterMedian(id int64) {
	specialAgg[id] = true
	aggIdOfMedian = id
}

func RegisterClusterCenters(id int64) {
	specialAgg[id] = true
	aggIdOfClusterCenters = id
}

func RegisterRowNumberWin(id int64) {
	specialAgg[id] = true
	winIdOfRowNumber = id
}

func RegisterRankWin(id int64) {
	specialAgg[id] = true
	winIdOfRank = id
}

func RegisterDenseRankWin(id int64) {
	specialAgg[id] = true
	winIdOfDenseRank = id
}

type registeredAggInfo struct {
	isSingleAgg          bool
	acceptNull           bool
	setNullForEmptyGroup bool
}

type aggKey string

func generateKeyOfSingleColumnAgg(aggID int64, argType types.Type) aggKey {
	return aggKey(fmt.Sprintf("s_%d_%d", aggID, argType.Oid))
}

var (
	// agg type record map.
	singleAgg  = make(map[int64]bool)
	specialAgg = make(map[int64]bool)

	// agg implementation map.
	registeredAggFunctions = make(map[aggKey]aggImplementation)

	// list of special aggregation function IDs.
	aggIdOfCountColumn    = int64(-1)
	aggIdOfCountStar      = int64(-2)
	aggIdOfGroupConcat    = int64(-3)
	aggIdOfApproxCount    = int64(-4)
	aggIdOfMedian         = int64(-5)
	aggIdOfClusterCenters = int64(-6)
	winIdOfRowNumber      = int64(-7)
	winIdOfRank           = int64(-8)
	winIdOfDenseRank      = int64(-9)
	groupConcatSep        = ","
	getCroupConcatRet     = func(args ...types.Type) types.Type {
		for _, p := range args {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}
)

func getSingleAggImplByInfo(
	id int64, arg types.Type) (aggInfo aggImplementation, err error) {
	key := generateKeyOfSingleColumnAgg(id, arg)

	if impl, ok := registeredAggFunctions[key]; ok {
		return impl, nil
	}
	return aggImplementation{}, moerr.NewInternalErrorNoCtxf("no implementation for aggID %d with argType %s", id, arg)
}

type aggImplementation struct {
	registeredAggInfo
	ret func([]types.Type) types.Type

	ctx   aggContextImplementation
	logic aggLogicImplementation
}

type aggContextImplementation struct {
	hasCommonContext      bool
	generateCommonContext AggCommonContextInit

	hasGroupContext      bool
	generateGroupContext AggGroupContextInit
}

type aggLogicImplementation struct {
	init  any // func(result type, parameter types) result
	fill  any // func(commonContext, groupContext, value, getter, setter) error
	fills any // func(commonContext, groupContext, value, count, getter, setter) error
	merge any // func(commonContext, groupContext1, groupContext2, getter1, getter2, setter) error
	flush any // func(commonContext, groupContext, getter, setter) error
}

type SingleColumnAggInformation struct {
	id                   int64
	arg                  types.Type
	ret                  func(p []types.Type) types.Type
	setNullForEmptyGroup bool
}

func MakeSingleColumnAggInformation(
	id int64, paramType types.Type, getRetType func(p []types.Type) types.Type,
	setNullForEmptyGroup bool) SingleColumnAggInformation {
	return SingleColumnAggInformation{
		id:  id,
		arg: paramType,
		ret: getRetType,
		// do not support this optimization now.
		setNullForEmptyGroup: true,
	}
}
