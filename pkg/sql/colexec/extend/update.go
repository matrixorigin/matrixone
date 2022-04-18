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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (p *UpdateExtend) IsLogical() bool {
	return false
}

func (_ *UpdateExtend) IsConstant() bool {
	return false
}

func (p *UpdateExtend) ReturnType() types.T {
	return types.T_any
}

func (_ *UpdateExtend) Attributes() []string {
	return nil
}

func (p *UpdateExtend) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	vs, typ, err := p.UpdateExtend.Eval(bat, proc)
	if err != nil {
		return nil, 0, err
	}
	vec, err := overload.UpdateEval(typ, p.Attr.Type, p.UpdateExtend.IsConstant(), vs, proc)
	if err != nil {
		return nil, 0, err
	}
	return vec, p.Attr.Type, nil
}

func (p *UpdateExtend) Eq(e Extend) bool {
	return false
}

func (p *UpdateExtend) String() string {
	return "update extend"
}