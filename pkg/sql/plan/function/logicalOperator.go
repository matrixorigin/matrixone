// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var andFn = generalFunctionTemplateFactor[bool, bool](
	func(v1 bool) (bool, bool) { return false, v1 }, false,
	func(v2 bool) (bool, bool) { return false, v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 && v2, false }, true,
	nil, true, true,
)

var orFn = generalFunctionTemplateFactor[bool, bool](
	func(v1 bool) (bool, bool) { return v1, !v1 }, false,
	func(v2 bool) (bool, bool) { return v2, !v2 }, false,
	func(v1, v2 bool) (bool, bool) { return v1 || v2, false }, true,
	nil, true, true,
)

var xorFn = generalFunctionTemplateFactor[bool, bool](
	nil, true,
	nil, true,
	func(v1, v2 bool) (bool, bool) { return v1 != v2, false }, true,
	nil, true, true,
)

func notFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return opUnaryFixedToFixed[bool, bool](parameters, result, proc, length, func(v bool) bool {
		return !v
	})
}
