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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func constructLock(
	n *plan.Node,
	eg engine.Engine,
	proc *process.Process) (*lockop.Argument, error) {
	n.TableDef.
		insertCtx := n.InsertCtx
	schemaName := insertCtx.Ref.SchemaName
	insertCtx.TableDef.TblId = uint64(insertCtx.Ref.Obj)

	if insertCtx.Ref.SchemaName != "" {
		dbSource, err := eg.Database(proc.Ctx, insertCtx.Ref.SchemaName, proc.TxnOperator)
		if err != nil {
			return nil, err
		}
		if _, err = dbSource.Relation(proc.Ctx, insertCtx.Ref.ObjName); err != nil {
			schemaName = defines.TEMPORARY_DBNAME
		}
	}

	return &preinsert.Argument{
		Ctx:        proc.Ctx,
		Eg:         eg,
		SchemaName: schemaName,
		TableDef:   insertCtx.TableDef,
		ParentIdx:  insertCtx.ParentIdx,
	}, nil
}
