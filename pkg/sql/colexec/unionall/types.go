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

package unionall

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(UnionAll)

type UnionAll struct {
	vm.OperatorBase
}

func (unionall *UnionAll) Free(proc *process.Process, pipelineFailed bool, err error) {
}

func (unionall *UnionAll) Reset(proc *process.Process, pipelineFailed bool, err error) {
}

func (unionall *UnionAll) Release() {
	if unionall != nil {
		reuse.Free[UnionAll](unionall, nil)
	}
}

func (unionall *UnionAll) GetOperatorBase() *vm.OperatorBase {
	return &unionall.OperatorBase
}

func NewArgument() *UnionAll {
	return reuse.Alloc[UnionAll](nil)
}

func init() {
	reuse.CreatePool[UnionAll](
		func() *UnionAll {
			return &UnionAll{}
		},
		func(a *UnionAll) {
			*a = UnionAll{}
		},
		reuse.DefaultOptions[UnionAll]().
			WithEnableChecker(),
	)
}

func (unionall UnionAll) TypeName() string {
	return opName
}
