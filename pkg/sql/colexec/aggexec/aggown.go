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

type aggSetter[T types.FixedSizeTExceptStrType] func(value T)
type aggBytesSetter func(value []byte) error
type aggGetter[T types.FixedSizeTExceptStrType] func() T
type aggBytesGetter func() []byte

type singleAggPrivateStructure1[
	from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface {
	init()
	fill(from, aggGetter[to], aggSetter[to])
	fillNull(aggGetter[to], aggSetter[to])
	fills(value from, isNull bool, count int, getter aggGetter[to], setter aggSetter[to])
	flush(getter aggGetter[to], setter aggSetter[to])
}

type singleAggPrivateStructure2[
	from types.FixedSizeTExceptStrType] interface {
	init()
	fill(from, aggBytesGetter, aggBytesSetter)
	fillNull(aggBytesGetter, aggBytesSetter)
	fills(value from, isNull bool, count int, getter aggBytesGetter, setter aggBytesSetter)
	flush(aggBytesGetter, aggBytesSetter)
}

type singleAggPrivateStructure3[
	to types.FixedSizeTExceptStrType] interface {
	init()
	fillBytes([]byte, aggGetter[to], aggSetter[to])
	fillNull(aggGetter[to], aggSetter[to])
	fills(value []byte, isNull bool, count int, getter aggGetter[to], setter aggSetter[to])
	flush(getter aggGetter[to], setter aggSetter[to])
}

type singleAggPrivateStructure4 interface {
	init()
	fillBytes([]byte, aggBytesGetter, aggBytesSetter)
	fillNull(aggBytesGetter, aggBytesSetter)
	fills(value []byte, isNull bool, count int, getter aggBytesGetter, setter aggBytesSetter)
	flush(aggBytesGetter, aggBytesSetter)
}

type multiAggPrivateStructure1[
	to types.FixedSizeTExceptStrType] interface {
	init()
	getFillWhich(idx int) any                        // return func fill(multiAggPrivateStructure1[to], value)
	getFillNullWhich(idx int) any                    // return func fillNull(multiAggPrivateStructure1[to])
	eval(getter aggGetter[to], setter aggSetter[to]) // after fill one row, do eval.

	flush(getter aggGetter[to], setter aggSetter[to]) // return the result.
}

type multiAggPrivateStructure2 interface {
	init()
	getFillWhich(idx int) any                          // return func fill(multiAggPrivateStructure2, value)
	getFillNullWhich(idx int) any                      // return func fillNull(multiAggPrivateStructure2)
	eval(getter aggBytesGetter, setter aggBytesSetter) // after fill one row, do eval.

	flush(getter aggBytesGetter, setter aggBytesSetter) error // return the result.
}
