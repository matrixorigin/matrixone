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

// AggCanMarshal interface is used for multi-node communication.
// each private structure of aggregation should implement the AggCanMarshal interface.
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
	Init()
	Fill(from, AggGetter[to], AggSetter[to])
	FillNull(AggGetter[to], AggSetter[to])
	Fills(value from, isNull bool, count int, getter AggGetter[to], setter AggSetter[to])
	Merge(other SingleAggFromFixedRetFixed[from, to], getter1, getter2 AggGetter[to], setter AggSetter[to])
	Flush(getter AggGetter[to], setter AggSetter[to])
}

type SingleAggFromFixedRetVar[
	from types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init()
	Fill(from, AggBytesGetter, AggBytesSetter)
	FillNull(AggBytesGetter, AggBytesSetter)
	Fills(value from, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter)
	Merge(other SingleAggFromFixedRetVar[from], getter1, getter2 AggBytesGetter, setter AggBytesSetter)
	Flush(AggBytesGetter, AggBytesSetter)
}

type SingleAggFromVarRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init()
	FillBytes([]byte, AggGetter[to], AggSetter[to])
	FillNull(AggGetter[to], AggSetter[to])
	Fills(value []byte, isNull bool, count int, getter AggGetter[to], setter AggSetter[to])
	Merge(other SingleAggFromVarRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to])
	Flush(getter AggGetter[to], setter AggSetter[to])
}

type SingleAggFromVarRetVar interface {
	AggCanMarshal
	Init()
	FillBytes([]byte, AggBytesGetter, AggBytesSetter)
	FillNull(AggBytesGetter, AggBytesSetter)
	Fills(value []byte, isNull bool, count int, getter AggBytesGetter, setter AggBytesSetter)
	Merge(other SingleAggFromVarRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter)
	Flush(AggBytesGetter, AggBytesSetter)
}

type MultiAggRetFixed[
	to types.FixedSizeTExceptStrType] interface {
	AggCanMarshal
	Init()
	GetWhichFill(idx int) any                        // return func Fill(MultiAggRetFixed[to], value)
	GetWhichFillNull(idx int) any                    // return func FillNull(MultiAggRetFixed[to])
	Eval(getter AggGetter[to], setter AggSetter[to]) // after Fill one row, do eval.
	Merge(other MultiAggRetFixed[to], getter1, getter2 AggGetter[to], setter AggSetter[to])
	Flush(getter AggGetter[to], setter AggSetter[to]) // return the result.
}

type MultiAggRetVar interface {
	AggCanMarshal
	Init()
	GetWhichFill(idx int) any                          // return func Fill(MultiAggRetVar, value)
	GetWhichFillNull(idx int) any                      // return func FillNull(MultiAggRetVar)
	Eval(getter AggBytesGetter, setter AggBytesSetter) // after Fill one row, do eval.
	Merge(other MultiAggRetVar, getter1, getter2 AggBytesGetter, setter AggBytesSetter)
	Flush(getter AggBytesGetter, setter AggBytesSetter) error // return the result.
}
