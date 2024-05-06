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

type (
	SingleAggInitCommonContext[from, to types.FixedSizeTExceptStrType] func(
		resultType types.Type, parameters ...types.Type) AggCommonExecContext

	SingleAggFill1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error

	SingleAggFillNull1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		value from,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error

	SingleAggFills1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		values from, isNull bool, count int,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error

	SingleAggMerge1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		ctx1, ctx2 AggGroupExecContext,
		commonContext AggCommonExecContext,
		resultGetter1, resultGetter2 AggGetter[to],
		resultSetter AggSetter[to]) error

	SingleAggFlush1NewVersion[from, to types.FixedSizeTExceptStrType] func(
		execContext AggGroupExecContext, commonContext AggCommonExecContext,
		resultGetter AggGetter[to], resultSetter AggSetter[to]) error
)
