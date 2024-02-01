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

type singleAggPrivateStructure1[
	from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface {
	init()
	fill(from, aggSetter[to])
	fillNull(aggSetter[to])
	fills(value from, isNull bool, count int, setter aggSetter[to])
	// todo: change `flush() to` to be `flush(setter aggSetter[to])`
	flush() to
}

type singleAggPrivateStructure2[
	from types.FixedSizeTExceptStrType] interface {
	init()
	fill(from, aggBytesSetter)
	fillNull(aggBytesSetter)
	fills(value from, isNull bool, count int, setter aggBytesSetter)
	flush() []byte
}

type singleAggPrivateStructure3[
	to types.FixedSizeTExceptStrType] interface {
	init()
	fillBytes([]byte, aggSetter[to])
	fillNull(aggSetter[to])
	fills(value []byte, isNull bool, count int, setter aggSetter[to])
	flush() to
}

type singleAggPrivateStructure4 interface {
	init()
	fillBytes([]byte, aggBytesSetter)
	fillNull(aggBytesSetter)
	fills(value []byte, isNull bool, count int, setter aggBytesSetter)
	flush() []byte
}

type multiAggPrivateStructure1[
	to types.FixedSizeTExceptStrType] interface {
	init()
	getFillWhich(idx int) any     // return func fill(value)
	getFillNullWhich(idx int) any // return func fillNull()
	flush(setter aggSetter[to])
}

type multiAggPrivateStructure2 interface {
	init()
	getFillWhich(idx int) any     // return func fill(value)
	getFillNullWhich(idx int) any // return func fillNull()
	flush(setter aggBytesSetter) error
}
