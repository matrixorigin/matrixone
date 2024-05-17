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

func generateKeyOfMultiColumnsAgg(overloadID int64, argTypes []types.Type) aggKey {
	key := fmt.Sprintf("m_%d", overloadID)
	for _, argType := range argTypes {
		key += fmt.Sprintf("_%d", argType.Oid)
	}
	return aggKey(key)
}

var (
	// agg type record map.
	singleAgg  = make(map[int64]bool)
	multiAgg   = make(map[int64]bool)
	specialAgg = make(map[int64]bool)

	// agg implementation map.
	registeredAggFunctions            = make(map[aggKey]aggImplementation)
	registeredMultiColumnAggFunctions = make(map[aggKey]multiColumnAggImplementation)

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
	return aggImplementation{}, moerr.NewInternalErrorNoCtx("no implementation for aggID %d with argType %s", id, arg)
}

func getMultiArgAggImplByInfo(
	id int64, args []types.Type) (aggInfo multiColumnAggImplementation, err error) {
	key := generateKeyOfMultiColumnsAgg(id, args)

	if impl, ok := registeredMultiColumnAggFunctions[key]; ok {
		return impl, nil
	}
	return multiColumnAggImplementation{}, moerr.NewInternalErrorNoCtx("no implementation for aggID %d with argTypes %v", id, args)
}

type aggImplementation struct {
	registeredAggInfo

	generator any
	ret       func([]types.Type) types.Type

	init     any
	fill     any
	fillNull any
	fills    any
	merge    any
	flush    any
}

type multiColumnAggImplementation struct {
	setNullForEmptyGroup bool

	generator any
	ret       func([]types.Type) types.Type

	init          any
	fillWhich     []any
	fillNullWhich any
	rowValid      any
	merge         any
	eval          any
	flush         any
}

type SingleColumnAggInformation struct {
	id                   int64
	arg                  types.Type
	ret                  func(p []types.Type) types.Type
	acceptNull           bool
	setNullForEmptyGroup bool
}

type MultiColumnAggInformation struct {
	id                   int64
	arg                  []types.Type
	ret                  func(p []types.Type) types.Type
	setNullForEmptyGroup bool
}

func MakeSingleColumnAggInformation(
	id int64, paramType types.Type, getRetType func(p []types.Type) types.Type,
	acceptNull bool, setNullForEmptyGroup bool) SingleColumnAggInformation {
	return SingleColumnAggInformation{
		id:                   id,
		arg:                  paramType,
		ret:                  getRetType,
		acceptNull:           acceptNull,
		setNullForEmptyGroup: setNullForEmptyGroup,
	}
}

func MakeMultiColumnAggInformation(
	id int64, params []types.Type, getRetType func(p []types.Type) types.Type,
	setNullForEmptyGroup bool) MultiColumnAggInformation {
	return MultiColumnAggInformation{
		id:                   id,
		arg:                  params,
		ret:                  getRetType,
		setNullForEmptyGroup: setNullForEmptyGroup,
	}
}

type SingleAggImplementationFixedFixed[from, to types.FixedSizeTExceptStrType] struct {
	SingleColumnAggInformation
	generator func() SingleAggFromFixedRetFixed[from, to]

	init     SingleAggInit1[from, to]
	fill     SingleAggFill1[from, to]
	fillNull SingleAggFillNull1[from, to]
	fills    SingleAggFills1[from, to]
	merge    SingleAggMerge1[from, to]
	flush    SingleAggFlush1[from, to]
}

func MakeSingleAgg1RegisteredInfo[from, to types.FixedSizeTExceptStrType](
	info SingleColumnAggInformation,
	impl func() SingleAggFromFixedRetFixed[from, to],
	init SingleAggInit1[from, to],
	fill SingleAggFill1[from, to],
	fillNull SingleAggFillNull1[from, to],
	fills SingleAggFills1[from, to],
	merge SingleAggMerge1[from, to],
	flush SingleAggFlush1[from, to],
) SingleAggImplementationFixedFixed[from, to] {

	registeredInfo1 := SingleAggImplementationFixedFixed[from, to]{
		SingleColumnAggInformation: info,
		generator:                  impl,
		init:                       init,
		fill:                       fill,
		fillNull:                   fillNull,
		fills:                      fills,
		merge:                      merge,
		flush:                      flush,
	}
	return registeredInfo1
}

func RegisterSingleAggFromFixedToFixed[from, to types.FixedSizeTExceptStrType](
	info SingleAggImplementationFixedFixed[from, to]) {

	key := generateKeyOfSingleColumnAgg(info.id, info.arg)
	if _, ok := registeredAggFunctions[key]; ok {
		panic(fmt.Sprintf("aggID %d with argType %s has been registered", info.id, info.arg))
	}

	registeredAggFunctions[key] = aggImplementation{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		generator: info.generator,
		ret:       info.ret,
		init:      info.init,
		fill:      info.fill,
		fillNull:  info.fillNull,
		fills:     info.fills,
		merge:     info.merge,
		flush:     info.flush,
	}
	singleAgg[info.id] = true
}

type SingleAggImplementationFixedVar[from types.FixedSizeTExceptStrType] struct {
	SingleColumnAggInformation
	generator func() SingleAggFromFixedRetVar[from]

	init     SingleAggInit2[from]
	fill     SingleAggFill2[from]
	fillNull SingleAggFillNull2[from]
	fills    SingleAggFills2[from]
	merge    SingleAggMerge2[from]
	flush    SingleAggFlush2[from]
}

func MakeSingleAgg2RegisteredInfo[from types.FixedSizeTExceptStrType](
	info SingleColumnAggInformation,
	impl func() SingleAggFromFixedRetVar[from],
	init SingleAggInit2[from],
	fill SingleAggFill2[from],
	fillNull SingleAggFillNull2[from],
	fills SingleAggFills2[from],
	merge SingleAggMerge2[from],
	flush SingleAggFlush2[from],
) SingleAggImplementationFixedVar[from] {

	registeredInfo2 := SingleAggImplementationFixedVar[from]{
		SingleColumnAggInformation: info,
		generator:                  impl,
		init:                       init,
		fill:                       fill,
		fillNull:                   fillNull,
		fills:                      fills,
		merge:                      merge,
		flush:                      flush,
	}
	return registeredInfo2
}

func RegisterSingleAggFromFixedToVar[from types.FixedSizeTExceptStrType](
	info SingleAggImplementationFixedVar[from]) {

	key := generateKeyOfSingleColumnAgg(info.id, info.arg)
	if _, ok := registeredAggFunctions[key]; ok {
		panic(fmt.Sprintf("aggID %d with argType %s has been registered", info.id, info.arg))
	}

	registeredAggFunctions[key] = aggImplementation{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		generator: info.generator,
		ret:       info.ret,
		init:      info.init,
		fill:      info.fill,
		fillNull:  info.fillNull,
		fills:     info.fills,
		merge:     info.merge,
		flush:     info.flush,
	}
	singleAgg[info.id] = true
}

type SingleAggImplementationVarVar struct {
	SingleColumnAggInformation
	generator func() SingleAggFromVarRetVar
	init      SingleAggInit4
	fill      SingleAggFill4
	fillNull  SingleAggFillNull4
	fills     SingleAggFills4
	merge     SingleAggMerge4
	flush     SingleAggFlush4
}

func MakeSingleAgg4RegisteredInfo(
	info SingleColumnAggInformation,
	impl func() SingleAggFromVarRetVar,
	init SingleAggInit4,
	fill SingleAggFill4,
	fillNull SingleAggFillNull4,
	fills SingleAggFills4,
	merge SingleAggMerge4,
	flush SingleAggFlush4,
) SingleAggImplementationVarVar {

	registeredInfo4 := SingleAggImplementationVarVar{
		SingleColumnAggInformation: info,
		generator:                  impl,
		init:                       init,
		fill:                       fill,
		fillNull:                   fillNull,
		fills:                      fills,
		merge:                      merge,
		flush:                      flush,
	}
	return registeredInfo4
}

func RegisterSingleAggFromVarToVar(
	info SingleAggImplementationVarVar) {

	key := generateKeyOfSingleColumnAgg(info.id, info.arg)
	if _, ok := registeredAggFunctions[key]; ok {
		panic(fmt.Sprintf("aggID %d with argType %s has been registered", info.id, info.arg))
	}

	registeredAggFunctions[key] = aggImplementation{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		generator: info.generator,
		ret:       info.ret,
		init:      info.init,
		fill:      info.fill,
		fillNull:  info.fillNull,
		fills:     info.fills,
		merge:     info.merge,
		flush:     info.flush,
	}
	singleAgg[info.id] = true
}

type MultiColumnAggRetFixedRegisteredInfo[to types.FixedSizeTExceptStrType] struct {
	MultiColumnAggInformation
	generator     func() MultiAggRetFixed[to]
	init          MultiAggInit1[to]
	fillWhich     []any
	fillNullWhich []MultiAggFillNull1[to]
	rowValid      rowValidForMultiAgg1[to]
	merge         MultiAggMerge1[to]
	evaluateRow   MultiAggEval1[to]
	flush         MultiAggFlush1[to]
}

func MakeMultiAggRetFixedRegisteredInfo[to types.FixedSizeTExceptStrType](
	info MultiColumnAggInformation,
	impl func() MultiAggRetFixed[to],
	init MultiAggInit1[to],
	fillWhich []any,
	fillNullWhich []MultiAggFillNull1[to],
	rowValid rowValidForMultiAgg1[to],
	eval MultiAggEval1[to],
	merge MultiAggMerge1[to],
	flush MultiAggFlush1[to],
) MultiColumnAggRetFixedRegisteredInfo[to] {
	// legal check.
	if len(fillWhich) != len(info.arg) || len(fillNullWhich) != len(fillWhich) || len(info.arg) < 2 {
		panic("illegal info.arg or fillWhich or fillNullWhich")
	}
	// todo: need more check here. fillWhich[i] should be the same type as info.arg[i].

	registeredInfo := MultiColumnAggRetFixedRegisteredInfo[to]{
		MultiColumnAggInformation: info,
		generator:                 impl,
		init:                      init,
		fillWhich:                 fillWhich,
		fillNullWhich:             fillNullWhich,
		rowValid:                  rowValid,
		merge:                     merge,
		evaluateRow:               eval,
		flush:                     flush,
	}
	return registeredInfo
}

func RegisterMultiAggRetFixed[to types.FixedSizeTExceptStrType](
	info MultiColumnAggRetFixedRegisteredInfo[to]) {

	key := generateKeyOfMultiColumnsAgg(info.id, info.arg)
	if _, ok := registeredMultiColumnAggFunctions[key]; ok {
		panic(fmt.Sprintf("aggID %d with argTypes %v has been registered", info.id, info.arg))
	}

	registeredMultiColumnAggFunctions[key] = multiColumnAggImplementation{
		setNullForEmptyGroup: info.setNullForEmptyGroup,
		generator:            info.generator,
		ret:                  info.ret,
		init:                 info.init,
		fillWhich:            info.fillWhich,
		fillNullWhich:        any(info.fillNullWhich),
		rowValid:             info.rowValid,
		merge:                info.merge,
		eval:                 info.evaluateRow,
		flush:                info.flush,
	}
	multiAgg[info.id] = true
}
