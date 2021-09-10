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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

func (a *Attribute) IsLogical() bool {
	return false
}

func (_ *Attribute) IsConstant() bool {
	return false
}

func (a *Attribute) Attributes() []string {
	return []string{a.Name}
}

func (a *Attribute) ReturnType() types.T {
	return a.Type
}

func (a *Attribute) Eval(bat *batch.Batch, proc *process.Process) (*vector.Vector, types.T, error) {
	vec := bat.GetVector(a.Name)
	if len(bat.Sels) > 0 {
		vec, err := vec.Shuffle(bat.Sels, proc)
		return vec, a.Type, err
	}
	return vec, a.Type, nil
}

func (a *Attribute) Eq(e Extend) bool {
	if b, ok := e.(*Attribute); ok {
		return a.Name == b.Name && a.Type == b.Type
	}
	return false
}

func (a *Attribute) String() string {
	return a.Name
}
