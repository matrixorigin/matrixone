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

// singleAggInfo is the basic information of single column agg.
type singleAggInfo struct {
	aggID    int64
	distinct bool
	argType  types.Type
	retType  types.Type

	// emptyNull indicates that whether we should return null for a group without any input value.
	emptyNull bool
}

func (info singleAggInfo) eq(other singleAggInfo) bool {
	return info.aggID == other.aggID &&
		info.distinct == other.distinct &&
		info.argType.Eq(other.argType) &&
		info.retType.Eq(other.retType) &&
		info.emptyNull == other.emptyNull
}

func (info singleAggInfo) String() string {
	return fmt.Sprintf("{aggID: %d, argType: %s, retType: %s}", info.aggID, info.argType.String(), info.retType.String())
}

func (info singleAggInfo) AggID() int64 {
	return info.aggID
}

func (info singleAggInfo) IsDistinct() bool {
	return info.distinct
}

func (info singleAggInfo) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{info.argType}, info.retType
}

func (info singleAggInfo) getEncoded() *EncodedBasicInfo {
	return &EncodedBasicInfo{
		Id:         info.aggID,
		IsDistinct: info.distinct,
		Args:       []types.Type{info.argType},
		Ret:        info.retType,
	}
}

// singleAggExecExtraInformation is the extra information of single column agg to optimize the execution.
type singleAggExecExtraInformation struct {
	partialGroup  int
	partialResult any
}

func (optimized *singleAggExecExtraInformation) SetExtraInformation(partialResult any, groupIndex int) error {
	optimized.partialGroup = groupIndex
	optimized.partialResult = partialResult
	return nil
}

// multiAggInfo is the basic information of multi column agg.
type multiAggInfo struct {
	aggID    int64
	distinct bool
	argTypes []types.Type
	retType  types.Type

	// emptyNull indicates that whether we should return null for a group without any input value.
	emptyNull bool
}

func (info multiAggInfo) String() string {
	args := "[" + info.argTypes[0].String()
	for i := 1; i < len(info.argTypes); i++ {
		args += ", " + info.argTypes[i].String()
	}
	args += "]"
	return fmt.Sprintf("{aggID: %d, argTypes: %s, retType: %s}", info.aggID, args, info.retType.String())
}

func (info multiAggInfo) AggID() int64 {
	return info.aggID
}

func (info multiAggInfo) IsDistinct() bool {
	return info.distinct
}

func (info multiAggInfo) TypesInfo() ([]types.Type, types.Type) {
	return info.argTypes, info.retType
}

func (info multiAggInfo) getEncoded() *EncodedBasicInfo {
	return &EncodedBasicInfo{
		Id:         info.aggID,
		IsDistinct: info.distinct,
		Args:       info.argTypes,
		Ret:        info.retType,
	}
}
