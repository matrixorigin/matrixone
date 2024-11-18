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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// the definition of functions to get and set the aggregation result.
type (
	AggSetter[T types.FixedSizeTExceptStrType] func(value T)
	AggBytesSetter                             func(value []byte) error

	AggGetter[T types.FixedSizeTExceptStrType] func() T
	AggBytesGetter                             func() []byte
)

// the function definitions of single-column aggregation.
// singleAgg1 for the fixed-length input type and fixed-length result type.
// singleAgg2 for the fixed-length input type and variable-length result type.
// singleAgg3 for the variable-length input type and fixed-length result type.
// singleAgg4 for the variable-length input type and variable-length result type.
type (
	// SingleAggInitResultFixed and SingleAggInitResultVar
	// return the original result of a new aggregation group.
	SingleAggInitResultFixed[to types.FixedSizeTExceptStrType] func(
		resultType types.Type, parameters ...types.Type) to
	SingleAggInitResultVar func(
		resultType types.Type, parameters ...types.Type) []byte

	// SingleAggFill1NewVersion ...  SingleAggFlush1NewVersion : singleAgg1's function definitions.
	SingleAggFill1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	SingleAggFills1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, count int, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	SingleAggMerge1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggGetter[to],
		resultSetter AggSetter[to]) error
	SingleAggFlush1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error

	// SingleAggFill2NewVersion ... SingleAggFlush2NewVersion : singleAgg2's function definitions.
	SingleAggFill2NewVersion[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	SingleAggFills2NewVersion[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, count int, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	SingleAggMerge2NewVersion[from types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggBytesGetter,
		resultSetter AggBytesSetter) error
	SingleAggFlush2NewVersion[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error

	// SingleAggFill3NewVersion ... SingleAggFlush3NewVersion : singleAgg3's function definitions.
	SingleAggFill3NewVersion[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	SingleAggFills3NewVersion[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, count int, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	SingleAggMerge3NewVersion[to types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggGetter[to],
		resultSetter AggSetter[to]) error
	SingleAggFlush3NewVersion[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error

	// SingleAggFill4NewVersion ... SingleAggFlush4NewVersion : singleAgg4's function definitions.
	SingleAggFill4NewVersion func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	SingleAggFills4NewVersion func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, count int, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	SingleAggMerge4NewVersion func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggBytesGetter,
		resultSetter AggBytesSetter) error
	SingleAggFlush4NewVersion func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
)

// AggCanMarshal interface is used for agg structures' multi-node communication.
// each context of aggregation should implement the AggCanMarshal interface.
//
// todo: consider that change them to deliver []byte directly, and agg developer choose how to use the []byte.
//
//	because the []byte can be set to one byte vector, and we can use the `mpool` to manage the memory easily.
type AggCanMarshal interface {
	Marshal() []byte
	Unmarshal([]byte)
}
