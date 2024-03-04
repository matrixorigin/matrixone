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

type determinedAggInfo struct {
	retType types.Type

	// implementation is a function about how to make a structure to do the aggregation.
	implementation any
}

type flexibleAggInfo struct {
	getReturnType func([]types.Type) types.Type

	// getImplementation will return a function to make a structure to do the aggregation.
	getImplementation func(args []types.Type, ret types.Type) any
}

type aggKey string

func generateKeyOfSingleAgg(aggID int64, argType types.Type) aggKey {
	return aggKey(fmt.Sprintf("s%d_%d", aggID, argType.Oid))
}

func generateKeyOfMultiArgAgg(overloadID int64, argTypes []types.Type) aggKey {
	key := fmt.Sprintf("m%d", overloadID)
	for _, argType := range argTypes {
		key += fmt.Sprintf("_%d", argType.Oid)
	}
	return aggKey(key)
}

var (
	registeredDeterminedAggFunctions = make(map[aggKey]determinedAggInfo)
	registeredFlexibleAggFunctions   = make(map[aggKey]flexibleAggInfo)
)

func getSingleAggImplByInfo(
	id int64, arg types.Type) (implementationAllocator any, ret types.Type, err error) {
	key := generateKeyOfSingleAgg(id, arg)
	if info, ok := registeredDeterminedAggFunctions[key]; ok {
		return info.implementation, info.retType, nil
	}
	if info, ok := registeredFlexibleAggFunctions[key]; ok {
		ret = info.getReturnType([]types.Type{arg})
		return info.getImplementation([]types.Type{arg}, ret), ret, nil
	}
	return nil, ret, fmt.Errorf("no implementation for aggID %d with argType %s", id, arg)
}

func getMultiArgAggImplByInfo(
	id int64, args []types.Type) (implementationAllocator any, ret types.Type, err error) {
	key := generateKeyOfMultiArgAgg(id, args)
	if info, ok := registeredDeterminedAggFunctions[key]; ok {
		return info.implementation, info.retType, nil
	}
	if info, ok := registeredFlexibleAggFunctions[key]; ok {
		ret = info.getReturnType(args)
		return info.getImplementation(args, ret), ret, nil
	}
	return nil, ret, fmt.Errorf("no implementation for aggID %d with argTypes %v", id, args)
}

var (
	_ = RegisterDeterminedSingleAgg
	_ = RegisterDeterminedMultiAgg
	_ = RegisterFlexibleSingleAgg
	_ = RegisterFlexibleMultiAgg
)

func RegisterDeterminedSingleAgg(id int64, arg types.Type, ret types.Type, impl any) {
	// impl legal check.

	key := generateKeyOfSingleAgg(id, arg)
	registeredDeterminedAggFunctions[key] = determinedAggInfo{
		retType:        ret,
		implementation: impl,
	}
}

func RegisterDeterminedMultiAgg(id int64, args []types.Type, ret types.Type, impl any) {
	// impl legal check.

	key := generateKeyOfMultiArgAgg(id, args)
	registeredDeterminedAggFunctions[key] = determinedAggInfo{
		retType:        ret,
		implementation: impl,
	}
}

func RegisterFlexibleSingleAgg(id int64, getReturnType func([]types.Type) types.Type, getImplementation func(args []types.Type, ret types.Type) any) {
	// impl legal check.

	key := generateKeyOfSingleAgg(id, types.Type{})
	registeredFlexibleAggFunctions[key] = flexibleAggInfo{
		getReturnType:     getReturnType,
		getImplementation: getImplementation,
	}
}

func RegisterFlexibleMultiAgg(id int64, getReturnType func([]types.Type) types.Type, getImplementation func(args []types.Type, ret types.Type) any) {
	// impl legal check.

	key := generateKeyOfMultiArgAgg(id, []types.Type{})
	registeredFlexibleAggFunctions[key] = flexibleAggInfo{
		getReturnType:     getReturnType,
		getImplementation: getImplementation,
	}
}
