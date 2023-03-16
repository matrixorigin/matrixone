// Copyright 2023 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func constructLockWithInsert(
	n *plan.Node,
	eg engine.Engine,
	proc *process.Process) (*lockop.Argument, error) {
	ctx := n.InsertCtx
	tableDef := ctx.TableDef
	if tableDef == nil {
		panic("missing table def")
	}
	// no primary key, no lock needed
	if tableDef.Pkey == nil {
		return nil, nil
	}

	pkIdx := -1
	var pkType types.Type
	name := tableDef.Pkey.PkeyColName
	for idx, c := range tableDef.Cols {
		if c.Name == name {
			pkIdx = idx
			pkType = types.Type{
				Oid:   types.T(c.Typ.Id),
				Width: c.Typ.Width,
				Size:  c.Typ.Size,
				Scale: c.Typ.Scale,
			}
			break
		}
	}
	if pkIdx == -1 {
		panic("pk column not found")
	}

	return lockop.NewArgument(
		tableDef.TblId,
		tableDef.Name,
		int32(pkIdx),
		pkType,
		lock.LockMode_Exclusive,
		proc.LockService,
	), nil
}
