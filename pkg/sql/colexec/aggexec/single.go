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

// singleAggFuncExec1 and singleAggFuncExec2 are the executors of single column agg.
// 1's return type is a fixed length type.
// 2's return type is bytes.
type singleAggFuncExec1[T types.FixedSizeTExceptStrType] struct {
	args types.Type
	ret  aggFuncResult[T]

	// the method to add values to the agg.
	add any
}
type singleAggFuncExec2 struct {
	args types.Type
	ret  aggFuncBytesResult

	// the method to add values to the agg.
	add any
}
