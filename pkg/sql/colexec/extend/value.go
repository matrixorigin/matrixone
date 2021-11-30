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

func (a *ValueExtend) IsLogical() bool {
	return false
}

func (_ *ValueExtend) IsConstant() bool {
	return true
}

func (a *ValueExtend) ReturnType() types.T {
	return a.V.Typ.Oid
}

func (_ *ValueExtend) Attributes() []string {
	return nil
}

func (a *ValueExtend) Eval(_ *batch.Batch, _ *process.Process) (*vector.Vector, types.T, error) {
	return a.V, a.V.Typ.Oid, nil
}

func (a *ValueExtend) Eq(e Extend) bool {
	if b, ok := e.(*ValueExtend); ok {
		return a.V.String() == b.V.String()
	}
	return false
}

func (a *ValueExtend) String() string {
	return a.V.String()
}
