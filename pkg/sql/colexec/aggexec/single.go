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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// singleAggTypeInfo is the type info of single column agg.
type singleAggTypeInfo struct {
	argType types.Type
	retType types.Type
}

func (exec singleAggTypeInfo) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{exec.argType}, exec.retType
}

// the executors of single column agg.
// singleAggFuncExec1 receives a fixed size type except string and returns a fixed size type except string.
// singleAggFuncExec2 receives a fixed size type except string and returns a byte type.
// singleAggFuncExec3 receives a byte type and returns a fixed size type except string.
// singleAggFuncExec4 receives a byte type and returns a byte type.
type singleAggFuncExec1[from, to types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo

	arg aggFuncArg[from]
	ret aggFuncResult[to]
}
type singleAggFuncExec2[from types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo

	arg aggFuncArg[from]
	ret aggFuncBytesResult
}
type singleAggFuncExec3[to types.FixedSizeTExceptStrType] struct {
	singleAggTypeInfo

	arg aggFuncBytesArg
	ret aggFuncResult[to]
}
type singleAggFuncExec4 struct {
	singleAggTypeInfo

	arg aggFuncBytesArg
	ret aggFuncBytesResult
}

func (exec *singleAggFuncExec1[from, to]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return nil
}
