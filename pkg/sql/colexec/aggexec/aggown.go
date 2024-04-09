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

type SingleAggFill1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], value from, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFill2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], value from, getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFill3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], value []byte, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFill4 func(
	exec SingleAggFromVarRetVar, value []byte, getter AggBytesGetter, setter AggBytesSetter) error

type SingleAggFillNull1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFillNull2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFillNull3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFillNull4 func(
	exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error

type SingleAggFills1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], value from, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFills2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], value from, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFills3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], value []byte, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFills4 func(
	exec SingleAggFromVarRetVar, value []byte, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error

type SingleAggMerge1[from, to types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromFixedRetFixed[from, to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
type SingleAggMerge2[from types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromFixedRetVar[from], getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
type SingleAggMerge3[to types.FixedSizeTExceptStrType] func(
	exec1, exec2 SingleAggFromVarRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
type SingleAggMerge4 func(
	exec1, exec2 SingleAggFromVarRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error

type SingleAggFlush1[from, to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetFixed[from, to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFlush2[from types.FixedSizeTExceptStrType] func(
	exec SingleAggFromFixedRetVar[from], getter AggBytesGetter, setter AggBytesSetter) error
type SingleAggFlush3[to types.FixedSizeTExceptStrType] func(
	exec SingleAggFromVarRetFixed[to], getter AggGetter[to], setter AggSetter[to]) error
type SingleAggFlush4 func(
	exec SingleAggFromVarRetVar, getter AggBytesGetter, setter AggBytesSetter) error

/*
	Functions for aggregation which do nothing while filling a null value.
*/

func SingleAggDoNothingFill1[from, to types.FixedSizeTExceptStrType](
	_ SingleAggFromFixedRetFixed[from, to], _ AggGetter[to], _ AggSetter[to]) error {
	return nil
}
func SingleAggDoNothingFill2[from types.FixedSizeTExceptStrType](
	_ SingleAggFromFixedRetVar[from], _ AggBytesGetter, _ AggBytesSetter) error {
	return nil
}
func SingleAggDoNothingFill3[to types.FixedSizeTExceptStrType](
	_ SingleAggFromVarRetFixed[to], _ AggGetter[to], _ AggSetter[to]) error {
	return nil
}
func SingleAggDoNothingFill4(
	_ SingleAggFromVarRetVar, _ AggBytesGetter, _ AggBytesSetter) error {
	return nil
}

/*
	Functions for aggregation which do nothing while flushing the result.
*/

func SingleAggDoNothingFlush1[from, to types.FixedSizeTExceptStrType](
	_ SingleAggFromFixedRetFixed[from, to], _ AggGetter[to], _ AggSetter[to]) error {
	return nil
}
func SingleAggDoNothingFlush2[from types.FixedSizeTExceptStrType](
	_ SingleAggFromFixedRetVar[from], _ AggBytesGetter, _ AggBytesSetter) error {
	return nil
}
func SingleAggDoNothingFlush3[to types.FixedSizeTExceptStrType](
	_ SingleAggFromVarRetFixed[to], _ AggGetter[to], _ AggSetter[to]) error {
	return nil
}
func SingleAggDoNothingFlush4(
	_ SingleAggFromVarRetVar, _ AggBytesGetter, _ AggBytesSetter) error {
	return nil
}

// AggCanMarshal interface is used for multi-node communication.
// each private structure of aggregation should implement the AggCanMarshal interface.
// todo: change to deliver []byte directly, and agg developer choose how to use the []byte.
type AggCanMarshal interface {
	Marshal() []byte
	Unmarshal([]byte)
}

/*
	all codes below are the interface of aggregation's private structure.
	each aggregation has its own private structure to do the aggregation.
	we use the interface to hide the private structure's detail.

    we have 4 kinds of aggregation for single-column aggregation and 2 kinds of aggregation for multi-column aggregation:
	1. SingleAggFromFixedRetFixed: aggregation receives a fixed length type and returns a fixed length type.
	2. SingleAggFromFixedRetVar: aggregation receives a fixed length type and returns a variable length type.
	3. SingleAggFromVarRetFixed: aggregation receives a variable length type and returns a fixed length type.
	4. SingleAggFromVarRetVar: aggregation receives a variable length type and returns a variable length type.
	5. MultiAggRetFixed: aggregation receives multi columns and returns a fixed length type.
	6. MultiAggRetVar: aggregation receives multi columns and returns a variable length type.
*/

type SingleAggFromFixedRetFixed[
	from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggSetter[to], arg, ret types.Type) error
}

type SingleAggFromFixedRetVar[
	from types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggBytesSetter, arg types.Type, ret types.Type) error
	Fill(from, AggBytesGetter, AggBytesSetter) error
	FillNull(AggBytesGetter, AggBytesSetter) error
	Fills(value from, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error
	Merge(other SingleAggFromFixedRetVar[from], getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
	Flush(AggBytesGetter, AggBytesSetter) error
}

type SingleAggFromVarRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggSetter[to], arg types.Type, ret types.Type) error
	FillBytes([]byte, AggGetter[to], AggSetter[to]) error
	FillNull(AggGetter[to], AggSetter[to]) error
	Fills(value []byte, isNull bool, count int, getter AggGetter[to], setter AggSetter[to]) error
	Merge(other SingleAggFromVarRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to]) error
	Flush(getter AggGetter[to], setter AggSetter[to]) error
}

type SingleAggFromVarRetVar interface {
	AggCanMarshal
	Init(setter AggBytesSetter, arg types.Type, ret types.Type) error
	FillBytes([]byte, AggBytesGetter, AggBytesSetter) error
	FillNull(AggBytesGetter, AggBytesSetter) error
	Fills(value []byte, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter) error
	Merge(other SingleAggFromVarRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter) error
	Flush(AggBytesGetter, AggBytesSetter) error
}

type MultiAggRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init(setter AggSetter[to], args []types.Type, ret types.Type)
	GetWhichFill(idx int) any                        // return func Fill(MultiAggRetFixed[to], value)
	GetWhichFillNull(idx int) any                    // return func FillNull(MultiAggRetFixed[to])
	Valid() bool                                     // return true if the row is valid.
	Eval(getter AggGetter[to], setter AggSetter[to]) // after Fill one row, do eval.
	Merge(other MultiAggRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to])
	Flush(getter AggGetter[to], setter AggSetter[to]) // return the result.
}

type MultiAggRetVar interface {
	AggCanMarshal
	Init(setter AggBytesSetter, args []types.Type, ret types.Type)
	GetWhichFill(idx int) any                          // return func Fill(MultiAggRetVar, value)
	GetWhichFillNull(idx int) any                      // return func FillNull(MultiAggRetVar)
	Valid() bool                                       // return true if the row is valid.
	Eval(getter AggBytesGetter, setter AggBytesSetter) // after Fill one row, do eval.
	Merge(other MultiAggRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter)
	Flush(getter AggBytesGetter, setter AggBytesSetter) error // return the result.
}
