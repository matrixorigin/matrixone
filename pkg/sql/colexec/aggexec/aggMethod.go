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

// the definition of functions to initialize the aggregation.
type (
	SingleAggInit1[from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetFixed[from, to], setter AggSetter[to], arg, ret types.Type) error

	SingleAggInit2[from types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetVar[from], setter AggBytesSetter, arg, ret types.Type) error

	SingleAggInit3[to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromVarRetFixed[to], setter AggSetter[to], arg, ret types.Type) error

	SingleAggInit4 func(
		exec SingleAggFromVarRetVar, setter AggBytesSetter, arg, ret types.Type) error

	MultiAggInit1[to types.FixedSizeTExceptStrType] func(
		exec MultiAggRetFixed[to], setter AggSetter[to], args []types.Type, ret types.Type)

	MultiAggInit2 func(
		exec MultiAggRetVar, setter AggBytesSetter, args []types.Type, ret types.Type)
)

// the definition of functions to fill the aggregation with one value.
type (
	SingleAggFill1[from, to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetFixed[from, to], value from, getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFill2[from types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetVar[from], value from, getter AggBytesGetter, setter AggBytesSetter) error

	SingleAggFill3[to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromVarRetFixed[to], value []byte, getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFill4 func(
		exec SingleAggFromVarRetVar, value []byte, getter AggBytesGetter, setter AggBytesSetter) error
)

// the definition of functions to fill the aggregation with one null value.
type (
	SingleAggFillNull1[from, to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFillNull2[from types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error

	SingleAggFillNull3[to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFillNull4 func(
		exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error
)

// the definition of functions to fill the aggregation with multiple values.
type (
	SingleAggFills1[from, to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetFixed[from, to], value from, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFills2[from types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetVar[from], value from, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error

	SingleAggFills3[to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromVarRetFixed[to], value []byte, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFills4 func(
		exec SingleAggFromVarRetVar, value []byte, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error
)

// the definition of functions to merge two aggregations.
type (
	SingleAggMerge1[from, to types.FixedSizeTExceptStrType] func(
		exec1, exec2 SingleAggFromFixedRetFixed[from, to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error

	SingleAggMerge2[from types.FixedSizeTExceptStrType] func(
		exec1, exec2 SingleAggFromFixedRetVar[from], getter1, getter2 AggBytesGetter, setter AggBytesSetter) error

	SingleAggMerge3[to types.FixedSizeTExceptStrType] func(
		exec1, exec2 SingleAggFromVarRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error

	SingleAggMerge4 func(
		exec1, exec2 SingleAggFromVarRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
)

// the definition of functions to return the final result of the aggregation.
type (
	SingleAggFlush1[from, to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFlush2[from types.FixedSizeTExceptStrType] func(
		exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error

	SingleAggFlush3[to types.FixedSizeTExceptStrType] func(
		exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error

	SingleAggFlush4 func(
		exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error
)

// the definition of functions used for multi-column agg whose result type is a fixed-length type.
type (
	MultiAggFillNull1[to types.FixedSizeTExceptStrType] func(
		exec MultiAggRetFixed[to]) error
	rowValidForMultiAgg1[to types.FixedSizeTExceptStrType] func(
		exec MultiAggRetFixed[to]) bool
	MultiAggEval1[to types.FixedSizeTExceptStrType] func(
		exec MultiAggRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
	MultiAggMerge1[to types.FixedSizeTExceptStrType] func(
		exec1, exec2 MultiAggRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
	MultiAggFlush1[to types.FixedSizeTExceptStrType] func(
		exec MultiAggRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
)

// the definition of functions used for multi-column agg whose result type is a var-len type.
type (
	MultiAggFillNull2 func(
		exec MultiAggRetVar) error
	rowValidForMultiAgg2 func(
		exec MultiAggRetVar) bool
	MultiAggEval2 func(
		exec MultiAggRetVar, getter AggBytesGetter, setter AggBytesSetter) error
	MultiAggMerge2 func(
		exec1, exec2 MultiAggRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
	MultiAggFlush2 func(
		exec MultiAggRetVar, getter AggBytesGetter, setter AggBytesSetter) error
)

// AggCanMarshal interface is used for agg structures' multi-node communication.
// each private structure of aggregation should implement the AggCanMarshal interface.
// todo: change to deliver []byte directly, and agg developer choose how to use the []byte.
type AggCanMarshal interface {
	Marshal() []byte
	Unmarshal([]byte)
}

/*
	All the codes bellow were the interface of aggregations' execute context.
	Each aggregation should implement one of the interfaces.

	1. SingleAggFromFixedRetFixed: aggregation receives a fixed length type and returns a fixed length type.
	2. SingleAggFromFixedRetVar: aggregation receives a fixed length type and returns a variable length type.
	3. SingleAggFromVarRetFixed: aggregation receives a variable length type and returns a fixed length type.
	4. SingleAggFromVarRetVar: aggregation receives a variable length type and returns a variable length type.
	5. MultiAggRetFixed: aggregation receives multi columns and returns a fixed length type.
	6. MultiAggRetVar: aggregation receives multi columns and returns a variable length type.

	If the aggregation needn't store any context,
	you can use the EmptyContextOfSingleAggRetFixed or EmptyContextOfSingleAggRetBytes.
	If the aggregation only needs to store a flag to indicate whether it is empty,
	you can use the ContextWithEmptyFlagOfSingleAggRetFixed or ContextWithEmptyFlagOfSingleAggRetBytes.
*/

type SingleAggFromFixedRetFixed[from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface{ AggCanMarshal }
type SingleAggFromFixedRetVar[from types.FixedSizeTExceptStrType] interface{ AggCanMarshal }
type SingleAggFromVarRetFixed[to types.FixedSizeTExceptStrType] interface{ AggCanMarshal }
type SingleAggFromVarRetVar interface{ AggCanMarshal }
type MultiAggRetFixed[to types.FixedSizeTExceptStrType] interface{ AggCanMarshal }
type MultiAggRetVar interface{ AggCanMarshal }
