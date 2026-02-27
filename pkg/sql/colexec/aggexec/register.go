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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
	methods to register the aggregation function.
	after registered, the function `MakeAgg` can make the aggregation function executor.
*/

func RegisterBitmapConstruct(id int64) {
	specialAgg[id] = true
	AggIdOfBitmapConstruct = id
}

func RegisterBitmapOr(id int64) {
	specialAgg[id] = true
	AggIdOfBitmapOr = id
}

func RegisterBitXorAgg(id int64) {
	specialAgg[id] = true
	AggIdOfBitXor = id
}

func RegisterBitAndAgg(id int64) {
	specialAgg[id] = true
	AggIdOfBitAnd = id
}

func RegisterBitOrAgg(id int64) {
	specialAgg[id] = true
	AggIdOfBitOr = id
}

func RegisterVarPop(id int64) {
	specialAgg[id] = true
	AggIdOfVarPop = id
}

func RegisterStdDevPop(id int64) {
	specialAgg[id] = true
	AggIdOfStdDevPop = id
}

func RegisterVarSample(id int64) {
	specialAgg[id] = true
	AggIdOfVarSample = id
}

func RegisterStdDevSample(id int64) {
	specialAgg[id] = true
	AggIdOfStdDevSample = id
}

func RegisterAny(id int64) {
	specialAgg[id] = true
	AggIdOfAny = id
}

func RegisterMin(id int64) {
	specialAgg[id] = true
	AggIdOfMin = id
}

func RegisterMax(id int64) {
	specialAgg[id] = true
	AggIdOfMax = id
}

func RegisterSum(id int64) {
	specialAgg[id] = true
	AggIdOfSum = id
}

func RegisterAvg(id int64) {
	specialAgg[id] = true
	AggIdOfAvg = id
}

func RegisterCountColumnAgg(id int64) {
	specialAgg[id] = true
	AggIdOfCountColumn = id
}

func RegisterCountStarAgg(id int64) {
	specialAgg[id] = true
	AggIdOfCountStar = id
}

func RegisterGroupConcatAgg(id int64, sep string) {
	specialAgg[id] = true
	AggIdOfGroupConcat = id
	groupConcatSep = sep
}

func RegisterApproxCountAgg(id int64) {
	specialAgg[id] = true
	AggIdOfApproxCount = id
}

func RegisterMedian(id int64) {
	specialAgg[id] = true
	AggIdOfMedian = id
}

func RegisterRowNumberWin(id int64) {
	specialAgg[id] = true
	WinIdOfRowNumber = id
}

func RegisterRankWin(id int64) {
	specialAgg[id] = true
	WinIdOfRank = id
}

func RegisterDenseRankWin(id int64) {
	specialAgg[id] = true
	WinIdOfDenseRank = id
}

func RegisterNtileWin(id int64) {
	specialAgg[id] = true
	WinIdOfNtile = id
}

func RegisterLagWin(id int64) {
	specialAgg[id] = true
	WinIdOfLag = id
}

func RegisterLeadWin(id int64) {
	specialAgg[id] = true
	WinIdOfLead = id
}

func RegisterFirstValueWin(id int64) {
	specialAgg[id] = true
	WinIdOfFirstValue = id
}

func RegisterLastValueWin(id int64) {
	specialAgg[id] = true
	WinIdOfLastValue = id
}

func RegisterNthValueWin(id int64) {
	specialAgg[id] = true
	WinIdOfNthValue = id
}

func RegisterCumeDistWin(id int64) {
	specialAgg[id] = true
	WinIdOfCumeDist = id
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
	AggIdOfCountColumn     = int64(-1)
	AggIdOfCountStar       = int64(-2)
	AggIdOfGroupConcat     = int64(-3)
	AggIdOfApproxCount     = int64(-4)
	AggIdOfMedian          = int64(-5)
	AggIdOfJsonArrayAgg    = int64(-6)
	AggIdOfJsonObjectAgg   = int64(-7)
	WinIdOfRowNumber       = int64(-8)
	WinIdOfRank            = int64(-9)
	WinIdOfDenseRank       = int64(-10)
	WinIdOfLag             = int64(-11)
	WinIdOfLead            = int64(-12)
	WinIdOfFirstValue      = int64(-13)
	WinIdOfLastValue       = int64(-14)
	WinIdOfNthValue        = int64(-15)
	AggIdOfSum             = int64(-16)
	AggIdOfAvg             = int64(-17)
	AggIdOfMin             = int64(-18)
	AggIdOfMax             = int64(-19)
	AggIdOfAny             = int64(-20)
	AggIdOfVarPop          = int64(-21)
	AggIdOfStdDevPop       = int64(-22)
	AggIdOfVarSample       = int64(-23)
	AggIdOfStdDevSample    = int64(-24)
	AggIdOfBitXor          = int64(-25)
	AggIdOfBitAnd          = int64(-26)
	AggIdOfBitOr           = int64(-27)
	AggIdOfBitmapConstruct = int64(-28)
	AggIdOfBitmapOr        = int64(-29)
	WinIdOfCumeDist        = int64(-30)
	WinIdOfNtile           = int64(-31)
	groupConcatSep         = ","
	getCroupConcatRet      = func(args ...types.Type) types.Type {
		for _, p := range args {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}
)

func SingleAggValuesString() string {
	pairs := make([]string, 0, len(singleAgg))
	for key, value := range singleAgg {
		pairs = append(pairs, fmt.Sprintf("%d:%v", key, value))
	}
	return strings.Join(pairs, "; ")
}

func SpecialAggValuesString() string {
	pairs := make([]string, 0, len(specialAgg))
	for key, value := range specialAgg {
		pairs = append(pairs, fmt.Sprintf("%d:%v", key, value))
	}
	return strings.Join(pairs, "; ")
}

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
