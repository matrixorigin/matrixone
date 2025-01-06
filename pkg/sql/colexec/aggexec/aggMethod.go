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

type (
	// InitFixedResultOfAgg and InitBytesResultOfAgg were methods to
	// return the initial value for the aggregator before any action.
	InitFixedResultOfAgg[to types.FixedSizeTExceptStrType] func(
		resultType types.Type, parameters ...types.Type) to
	InitBytesResultOfAgg func(
		resultType types.Type, parameters ...types.Type) []byte
)

type (
	// fixedFixedFill ...  fixedFixedFlush : aggregatorFromFixedToFixed required function definitions.
	fixedFixedFill[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	fixedFixedFills[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, count int, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	fixedFixedMerge[from, to types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggGetter[to],
		resultSetter AggSetter[to]) error
	fixedFixedFlush[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
)

type (
	// fixedBytesFill ... fixedBytesFlush : aggregatorFromFixedToBytes required function definitions.
	fixedBytesFill[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	fixedBytesFills[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from, count int, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	fixedBytesMerge[from types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggBytesGetter,
		resultSetter AggBytesSetter) error
	fixedBytesFlush[from types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
)

type (
	// bytesFixedFill ... bytesFixedFlush : aggregatorFromBytesToFixed required function definitions.
	bytesFixedFill[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	bytesFixedFills[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, count int, aggIsEmpty bool,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
	bytesFixedMerge[to types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggGetter[to],
		resultSetter AggSetter[to]) error
	bytesFixedFlush[to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
)

type (
	// bytesBytesFill ... bytesBytesFlush : aggregatorFromBytesToBytes required function definitions.
	bytesBytesFill func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	bytesBytesFills func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value []byte, count int, aggIsEmpty bool,
		resultGetter AggBytesGetter, resultSetter AggBytesSetter) error
	bytesBytesMerge func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		aggIsEmpty1, aggIsEmpty2 bool,
		resultGetter1, resultGetter2 AggBytesGetter,
		resultSetter AggBytesSetter) error
	bytesBytesFlush func(
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
