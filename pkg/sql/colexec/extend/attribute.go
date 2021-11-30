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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func (a *Attribute) Eval(bat *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return batch.GetVector(bat, a.Name), a.Type, nil
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
