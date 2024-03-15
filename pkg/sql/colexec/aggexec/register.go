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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
	methods to register the aggregation function.
	after registered, the function `makeSingleAgg` can make the aggregation function executor.
*/

// todo: the implementation argument should deliver the returnType maybe ?
//  or the Init() method need to do this ?

func RegisterDeterminedSingleAgg(info DeterminedSingleAggInfo, impl any) {
	// impl legal check.

	key := generateKeyOfDeterminedSingleAgg(info.id, info.arg)
	registeredDeterminedAggFunctions[key] = determinedAggInfo{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		retType:        info.ret,
		implementation: impl,
	}

	singleAgg[info.id] = true
}

func RegisterDeterminedMultiAgg(info DeterminedMultiAggInfo, impl any) {
	// impl legal check.

	key := generateKeyOfDeterminedArgAgg(info.id, info.args)
	registeredDeterminedAggFunctions[key] = determinedAggInfo{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          false,
			acceptNull:           true,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		retType:        info.ret,
		implementation: impl,
	}

	multiAgg[info.id] = true
}

func RegisterFlexibleSingleAgg(info FlexibleAggInfo, getReturnType func([]types.Type) types.Type, getImplementation func(args []types.Type, ret types.Type) any) {
	// impl legal check.

	key := generateKeyOfFlexibleSingleAgg(info.id)
	registeredFlexibleAggFunctions[key] = flexibleAggInfo{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          true,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		getReturnType:     getReturnType,
		getImplementation: getImplementation,
	}

	singleAgg[info.id] = true
}

func RegisterFlexibleMultiAgg(info FlexibleAggInfo, getReturnType func([]types.Type) types.Type, getImplementation func(args []types.Type, ret types.Type) any) {
	// impl legal check.

	key := generateKeyOfFlexibleMultiAgg(info.id)
	registeredFlexibleAggFunctions[key] = flexibleAggInfo{
		registeredAggInfo: registeredAggInfo{
			isSingleAgg:          false,
			acceptNull:           info.acceptNull,
			setNullForEmptyGroup: info.setNullForEmptyGroup,
		},
		getReturnType:     getReturnType,
		getImplementation: getImplementation,
	}

	multiAgg[info.id] = true
}

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

type registeredAggInfo struct {
	isSingleAgg          bool
	acceptNull           bool
	setNullForEmptyGroup bool
}

type determinedAggInfo struct {
	registeredAggInfo
	retType types.Type

	// implementation is a function about how to make a structure to do the aggregation.
	implementation any
}

type flexibleAggInfo struct {
	registeredAggInfo
	getReturnType func([]types.Type) types.Type

	// getImplementation will return a function to make a structure to do the aggregation.
	getImplementation func(args []types.Type, ret types.Type) any
}

type aggKey string

func generateKeyOfDeterminedSingleAgg(aggID int64, argType types.Type) aggKey {
	return aggKey(fmt.Sprintf("s_d_%d_%d", aggID, argType.Oid))
}

func generateKeyOfDeterminedArgAgg(overloadID int64, argTypes []types.Type) aggKey {
	key := fmt.Sprintf("m_d_%d", overloadID)
	for _, argType := range argTypes {
		key += fmt.Sprintf("_%d", argType.Oid)
	}
	return aggKey(key)
}

func generateKeyOfFlexibleSingleAgg(aggID int64) aggKey {
	return aggKey(fmt.Sprintf("s_f_%d", aggID))
}

func generateKeyOfFlexibleMultiAgg(aggID int64) aggKey {
	return aggKey(fmt.Sprintf("m_f_%d", aggID))
}

var (
	registeredDeterminedAggFunctions = make(map[aggKey]determinedAggInfo)
	registeredFlexibleAggFunctions   = make(map[aggKey]flexibleAggInfo)
)

var (
	singleAgg  = make(map[int64]bool)
	multiAgg   = make(map[int64]bool)
	specialAgg = make(map[int64]bool)

	aggIdOfCountColumn = int64(-1)
	aggIdOfCountStar   = int64(-2)
	aggIdOfGroupConcat = int64(-3)
	aggIdOfApproxCount = int64(-4)
	aggIdOfMedian      = int64(-5)
	groupConcatSep     = ","
	getCroupConcatRet  = func(args ...types.Type) types.Type {
		for _, p := range args {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}
)

func getSingleAggImplByInfo(
	id int64, arg types.Type) (implementationAllocator any, ret types.Type, raInfo registeredAggInfo, err error) {
	if info, ok := registeredDeterminedAggFunctions[generateKeyOfDeterminedSingleAgg(id, arg)]; ok {
		return info.implementation, info.retType, info.registeredAggInfo, nil
	}
	if info, ok := registeredFlexibleAggFunctions[generateKeyOfFlexibleSingleAgg(id)]; ok {
		ret = info.getReturnType([]types.Type{arg})
		return info.getImplementation([]types.Type{arg}, ret), ret, info.registeredAggInfo, nil
	}
	return nil, ret, raInfo, fmt.Errorf("no implementation for aggID %d with argType %s", id, arg)
}

func getMultiArgAggImplByInfo(
	id int64, args []types.Type) (implementationAllocator any, ret types.Type, raInfo registeredAggInfo, err error) {
	if info, ok := registeredDeterminedAggFunctions[generateKeyOfDeterminedArgAgg(id, args)]; ok {
		return info.implementation, info.retType, info.registeredAggInfo, nil
	}
	if info, ok := registeredFlexibleAggFunctions[generateKeyOfFlexibleMultiAgg(id)]; ok {
		ret = info.getReturnType(args)
		return info.getImplementation(args, ret), ret, info.registeredAggInfo, nil
	}
	return nil, ret, raInfo, fmt.Errorf("no implementation for aggID %d with argTypes %v", id, args)
}

var (
	_ = RegisterDeterminedSingleAgg
	_ = RegisterDeterminedMultiAgg
	_ = RegisterFlexibleSingleAgg
	_ = RegisterFlexibleMultiAgg
	_ = MakeFlexibleAggInfo
)

type DeterminedSingleAggInfo struct {
	id                   int64
	arg                  types.Type
	ret                  types.Type
	acceptNull           bool
	setNullForEmptyGroup bool
}

func MakeDeterminedSingleAggInfo(id int64, arg types.Type, ret types.Type, acceptNull bool, setNullForEmptyGroup bool) DeterminedSingleAggInfo {
	return DeterminedSingleAggInfo{
		id:                   id,
		arg:                  arg,
		ret:                  ret,
		acceptNull:           acceptNull,
		setNullForEmptyGroup: setNullForEmptyGroup,
	}
}

type DeterminedMultiAggInfo struct {
	id                   int64
	args                 []types.Type
	ret                  types.Type
	setNullForEmptyGroup bool
}

func MakeDeterminedMultiAggInfo(id int64, args []types.Type, ret types.Type, setNullForEmptyGroup bool) DeterminedMultiAggInfo {
	return DeterminedMultiAggInfo{
		id:                   id,
		args:                 args,
		ret:                  ret,
		setNullForEmptyGroup: setNullForEmptyGroup,
	}
}

type FlexibleAggInfo struct {
	id                   int64
	acceptNull           bool
	setNullForEmptyGroup bool
}

func MakeFlexibleAggInfo(id int64, acceptNull bool, setNullForEmptyGroup bool) FlexibleAggInfo {
	return FlexibleAggInfo{
		id:                   id,
		acceptNull:           acceptNull,
		setNullForEmptyGroup: setNullForEmptyGroup,
	}
}
