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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// set origin result to a new group.
type (
	SingleAggInitResultFixed[to types.FixedSizeTExceptStrType] func(
		resultType types.Type, parameters ...types.Type) to
	SingleAggInitResultVar func(
		resultType types.Type, parameters ...types.Type) []byte
)

// single aggregation function, input is fixed size, output is fixed size.
type (
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
)

// single aggregation function, input is fixed size, output is variable size.
type (
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
)

// single aggregation function, input is variable size, output is fixed size.
type (
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
)

// single aggregation function, input and output are both variable size.
type (
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
