// Copyright 2021 Matrix Origin
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

package extend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (_ *FuncExtend) IsLogical() bool {
	return false
}

func (_ *FuncExtend) IsConstant() bool {
	return false
}

func (a *FuncExtend) Attributes() []string {
	return []string{a.Name}
}

func (a *FuncExtend) ReturnType() types.T {
	return 0
}

func (a *FuncExtend) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return nil, 0, nil
}

func (a *FuncExtend) Eq(e Extend) bool {
	if b, ok := e.(*FuncExtend); ok {
		if a.Name != b.Name {
			return false
		}
		for i, arg := range a.Args {
			if !arg.Eq(b.Args[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func (a *FuncExtend) String() string {
	r := fmt.Sprintf("%s(", a.Name)
	for i, arg := range a.Args {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", arg)
		default:
			r += fmt.Sprintf(", %s", arg)
		}
	}
	r += fmt.Sprintf(")")
	return r
}
