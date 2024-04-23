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

/*
	result get method and set method for aggregation.
*/

type AggSetter[T types.FixedSizeTExceptStrType] func(value T)
type AggBytesSetter func(value []byte) error
type AggGetter[T types.FixedSizeTExceptStrType] func() T
type AggBytesGetter func() []byte

/*
	the definition of aggregation's basic method.
*/

// SingleAggInit1 ... MultiAggInit2
// is the method how does an agg initialize for each group.
type SingleAggInit1[from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], setter AggSetter[to], arg, ret types.Type) error
type SingleAggInit2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], setter AggBytesSetter, arg, ret types.Type) error
type SingleAggInit3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], setter AggSetter[to], arg, ret types.Type) error
type SingleAggInit4 func(
	exec SingleAggFromVarRetVar, setter AggBytesSetter, arg, ret types.Type) error
type MultiAggInit1[to types.FixedSizeTExceptStrType] func(
	exec MultiAggRetFixed[to], setter AggSetter[to], args []types.Type, ret types.Type)
type MultiAggInit2 func(
	exec MultiAggRetVar, setter AggBytesSetter, args []types.Type, ret types.Type)

// SingleAggFill1 ... SingleAggFill4
// is the method how does single-column agg fill one value.
type SingleAggFill1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], value from, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFill2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], value from, getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFill3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], value []byte, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFill4 func(
	exec SingleAggFromVarRetVar, value []byte, getter AggBytesGetter, setter AggBytesSetter) error

// SingleAggFillNull1 ... SingleAggFillNull4
// is the method how does single-column agg fill one NULL value.
type SingleAggFillNull1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFillNull2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFillNull3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFillNull4 func(
	exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error

// SingleAggFills1 ... SingleAggFills4
// is the method how does single-column agg fill multiple rows with the same value.
// count is the number of rows, and isNull is the flag of NULL.
type SingleAggFills1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], value from, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFills2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], value from, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFills3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], value []byte, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFills4 func(
	exec SingleAggFromVarRetVar, value []byte, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error

// SingleAggMerge1 ... SingleAggMerge4
// is the method how does single-column agg merge two aggregation results.
type SingleAggMerge1[from, to types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromFixedRetFixed[from, to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
type SingleAggMerge2[from types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromFixedRetVar[from], getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
type SingleAggMerge3[to types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromVarRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
type SingleAggMerge4 func(
	exec1, exec2 SingleAggFromVarRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error

// SingleAggFlush1 ... SingleAggFlush4
// is the method how does single-column agg return the final result after all fill and merge operations.
type SingleAggFlush1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFlush2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFlush3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFlush4 func(
	exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error

// MultiAggFillNull1 ... MultiAggFlush1
// is the method how does multi-column agg (which return type is not a variable-length type, like int64) fill null value, merge, and return the final result.
type MultiAggFillNull1[to types.FixedSizeTExceptStrType] func(
	exec MultiAggRetFixed[to]) error
type rowValidForMultiAgg1[to types.FixedSizeTExceptStrType] func(
	exec MultiAggRetFixed[to]) bool
type MultiAggEval1[to types.FixedSizeTExceptStrType] func(
	exec MultiAggRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
type MultiAggMerge1[to types.FixedSizeTExceptStrType] func(
	exec1, exec2 MultiAggRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
type MultiAggFlush1[to types.FixedSizeTExceptStrType] func(
	exec MultiAggRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error

// MultiAggFillNull2 ... MultiAggFlush2
// is the method how does multi-column agg (which return type is a variable-length type, like varchar) fill null value, merge, and return the final result.
type MultiAggFillNull2 func(
	exec MultiAggRetVar) error
type rowValidForMultiAgg2 func(
	exec MultiAggRetVar) bool
type MultiAggEval2 func(
	exec MultiAggRetVar, getter AggBytesGetter, setter AggBytesSetter) error
type MultiAggMerge2 func(
	exec1, exec2 MultiAggRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
type MultiAggFlush2 func(
	exec MultiAggRetVar, getter AggBytesGetter, setter AggBytesSetter) error

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

type SingleAggFromFixedRetFixed[
	from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
}

type SingleAggFromFixedRetVar[
	from types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggBytesSetter, arg types.Type, ret types.Type) error
}

type SingleAggFromVarRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggSetter[to], arg types.Type, ret types.Type) error
}

type SingleAggFromVarRetVar interface {
	AggCanMarshal
	Init(setter AggBytesSetter, arg types.Type, ret types.Type) error
}

type MultiAggRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggSetter[to], args []types.Type, ret types.Type)
}

type MultiAggRetVar interface {
	AggCanMarshal
	Init(setter AggBytesSetter, args []types.Type, ret types.Type)
}

/*
	basic structures for agg.
*/

type EmptyContextOfSingleAggRetFixed[T types.FixedSizeTExceptStrType] struct{}

func (a EmptyContextOfSingleAggRetFixed[T]) Marshal() []byte                            { return nil }
func (a EmptyContextOfSingleAggRetFixed[T]) Unmarshal([]byte)                           {}
func (a EmptyContextOfSingleAggRetFixed[T]) Init(_ AggSetter[T], _, _ types.Type) error { return nil }
func GenerateEmptyContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType]() SingleAggFromFixedRetFixed[from, to] {
	return EmptyContextOfSingleAggRetFixed[to]{}
}
func GenerateEmptyContextFromVarToFixed[to types.FixedSizeTExceptStrType]() SingleAggFromVarRetFixed[to] {
	return EmptyContextOfSingleAggRetFixed[to]{}
}

type EmptyContextOfSingleAggRetBytes struct{}

func (a EmptyContextOfSingleAggRetBytes) Marshal() []byte                              { return nil }
func (a EmptyContextOfSingleAggRetBytes) Unmarshal([]byte)                             {}
func (a EmptyContextOfSingleAggRetBytes) Init(_ AggBytesSetter, _, _ types.Type) error { return nil }
func GenerateEmptyContextFromFixedToVar[from types.FixedSizeTExceptStrType]() SingleAggFromFixedRetVar[from] {
	return EmptyContextOfSingleAggRetBytes{}
}
func GenerateEmptyContextFromVarToVar() SingleAggFromVarRetVar {
	return EmptyContextOfSingleAggRetBytes{}
}

type ContextWithEmptyFlagOfSingleAggRetFixed[T types.FixedSizeTExceptStrType] struct {
	IsEmpty bool
}

func (a *ContextWithEmptyFlagOfSingleAggRetFixed[T]) Marshal() []byte {
	return types.EncodeBool(&a.IsEmpty)
}
func (a *ContextWithEmptyFlagOfSingleAggRetFixed[T]) Unmarshal(data []byte) {
	a.IsEmpty = types.DecodeBool(data)
}
func (a *ContextWithEmptyFlagOfSingleAggRetFixed[T]) Init(_ AggSetter[T], _, _ types.Type) error {
	a.IsEmpty = true
	return nil
}
func GenerateFlagContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType]() SingleAggFromFixedRetFixed[from, to] {
	return &ContextWithEmptyFlagOfSingleAggRetFixed[to]{}
}
func InitFlagContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType](exec SingleAggFromFixedRetFixed[from, to], setter AggSetter[to], arg, ret types.Type) error {
	a := exec.(*ContextWithEmptyFlagOfSingleAggRetFixed[to])
	a.IsEmpty = true
	return nil
}

//func GenerateFlagContextFromVarToFixed[to types.FixedSizeTExceptStrType]() SingleAggFromVarRetFixed[to] {
//	return &ContextWithEmptyFlagOfSingleAggRetFixed[to]{}
//}

type ContextWithEmptyFlagOfSingleAggRetBytes struct {
	IsEmpty bool
}

func (a *ContextWithEmptyFlagOfSingleAggRetBytes) Marshal() []byte {
	return types.EncodeBool(&a.IsEmpty)
}
func (a *ContextWithEmptyFlagOfSingleAggRetBytes) Unmarshal(data []byte) {
	a.IsEmpty = types.DecodeBool(data)
}
func (a *ContextWithEmptyFlagOfSingleAggRetBytes) Init(_ AggBytesSetter, _, _ types.Type) error {
	a.IsEmpty = true
	return nil
}

//	func GenerateFlagContextFromFixedToVar[from types.FixedSizeTExceptStrType]() SingleAggFromFixedRetVar[from] {
//		return &ContextWithEmptyFlagOfSingleAggRetBytes{}
//	}
func GenerateFlagContextFromVarToVar() SingleAggFromVarRetVar {
	return &ContextWithEmptyFlagOfSingleAggRetBytes{}
}
