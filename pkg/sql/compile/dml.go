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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
)

func (s *Scope) Delete(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deletion.Argument)

	if arg.DeleteCtx.CanTruncate {
		var err error
		var affectRows int64
		delCtx := arg.DeleteCtx

		err = delCtx.Source.UpdateBlockInfos(c.ctx)
		if err != nil {
			return 0, err
		}
		affectRow, err := delCtx.Source.Rows(s.Proc.Ctx)
		if err != nil {
			return 0, err
		}
		affectRows = affectRows + affectRow

		dbName := delCtx.Ref.SchemaName
		tblName := delCtx.Ref.ObjName
		oldId := uint64(delCtx.Ref.Obj)
		dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
		if err != nil {
			return 0, err
		}

		// truncate origin table
		newId, err := dbSource.Truncate(c.ctx, tblName)
		if err != nil {
			return 0, err
		}

		// keep old offset.
		err = incrservice.GetAutoIncrementService(c.ctx).Reset(
			c.ctx,
			oldId,
			newId,
			true,
			c.proc.TxnOperator)
		if err != nil {
			return 0, err
		}

		return uint64(affectRows), nil
	}

	if err := s.MergeRun(c); err != nil {
		return 0, err
	}
	return arg.AffectedRows(), nil
}

func (s *Scope) Insert(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*insert.Argument)
	if err := s.MergeRun(c); err != nil {
		return 0, err
	}
	return arg.AffectedRows(), nil
}
