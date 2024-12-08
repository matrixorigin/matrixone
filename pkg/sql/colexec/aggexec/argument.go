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

//nolint:all
package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// sFixedArg and sBytesArg were used to get value from input vector.
type sFixedArg[T types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[T]
}
type sBytesArg struct {
	w vector.FunctionParameterWrapper[types.Varlena]
}

func (arg *sFixedArg[T]) prepare(v *vector.Vector) {
	if arg.w == nil {
		arg.w = vector.GenerateFunctionFixedTypeParameter[T](v)
	} else {
		ok := vector.ReuseFunctionFixedTypeParameter(v, arg.w)
		if !ok {
			arg.w = vector.GenerateFunctionFixedTypeParameter[T](v)
		}
	}
}

func (arg *sBytesArg) prepare(v *vector.Vector) {
	if arg.w == nil {
		arg.w = vector.GenerateFunctionStrParameter(v)
	} else {
		ok := vector.ReuseFunctionStrParameter(v, arg.w)
		if !ok {
			arg.w = vector.GenerateFunctionStrParameter(v)
		}
	}
}
