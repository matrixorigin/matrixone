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

type singleAggPrivateStructure1[
	from types.FixedSizeTExceptStrType, to types.FixedSizeTExceptStrType] interface {
	fill(from)
	fillNull()
	fills(value from, isNull bool, count int)
	flush() to
}

type singleAggPrivateStructure2[
	from types.FixedSizeTExceptStrType] interface {
	fill(from)
	fillNull()
	fills(value from, isNull bool, count int)
	flush() []byte
}

type singleAggPrivateStructure3[
	to types.FixedSizeTExceptStrType] interface {
	fillBytes([]byte)
	fillNull()
	fills(value []byte, isNull bool, count int)
	flush() to
}

type singleAggPrivateStructure4 interface {
	fillBytes([]byte)
	fillNull()
	fills(value []byte, isNull bool, count int)
	flush() []byte
}
