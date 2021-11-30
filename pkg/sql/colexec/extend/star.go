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

func (_ *StarExtend) IsLogical() bool {
	return false
}

func (_ *StarExtend) IsConstant() bool {
	return false
}

func (a *StarExtend) Attributes() []string {
	return []string{}
}

func (a *StarExtend) ReturnType() types.T {
	return 0
}

func (a *StarExtend) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return nil, 0, nil
}

func (a *StarExtend) Eq(e Extend) bool {
	if _, ok := e.(*StarExtend); ok {
		return true
	}
	return false
}

func (a *StarExtend) String() string {
	return "*"
}
