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

type aggArg interface {
	Prepare(*vector.Vector)
	Reset()
}

// aggFuncArg and aggFuncBytesArg were used to get value from input vector.
type aggFuncArg[T types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[T]
}
type aggFuncBytesArg struct {
	w vector.FunctionParameterWrapper[types.Varlena]
}

func (arg *aggFuncArg[T]) Prepare(v *vector.Vector) {
	arg.w = vector.GenerateFunctionFixedTypeParameter[T](v)
}
func (arg *aggFuncArg[T]) Reset() {}

func (arg *aggFuncBytesArg) Prepare(v *vector.Vector) {
	arg.w = vector.GenerateFunctionStrParameter(v)
}
func (arg *aggFuncBytesArg) Reset() {}
