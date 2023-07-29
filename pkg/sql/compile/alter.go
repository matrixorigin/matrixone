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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) AlterTableCopy(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	dbName := c.db
	tblName := qry.GetTableDef().GetName()

	dbSource, err := c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
	if err != nil {
		return err
	}

	originRel, err := dbSource.Relation(c.ctx, tblName, nil)
	if err != nil {
		return err
	}

	if c.proc.TxnOperator.Txn().IsPessimistic() {
		var retryErr error
		// 1. lock origin table metadata in catalog
		if err = lockMoTable(c, dbName, tblName); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) {
				return err
			}
			retryErr = err
		}

		// 2. lock origin table
		if err = lockTable(c.e, c.proc, originRel); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) {
				return err
			}
			retryErr = err
		}
		if retryErr != nil {
			return retryErr
		}
	}

	// 3. create a replica table of the original table
	err = c.runSql(qry.CreateTableSql)
	if err != nil {
		return err
	}

	// 4. copy the original table data to the replica table
	err = c.runSql(qry.InsertDataSql)
	if err != nil {
		return err
	}

	// TODO 5. Migrate Foreign Key Dependencies
	// 6. drop original table
	if err = dbSource.Delete(c.ctx, tblName); err != nil {
		return err
	}

	// 7. rename copy table name to original table name
	copyRel, err := dbSource.Relation(c.ctx, qry.CopyTableDef.Name, nil)
	if err != nil {
		return err
	}

	var alterKind []api.AlterKind
	alterKind = addAlterKind(alterKind, api.AlterKind_RenameTable)
	oldName := copyRel.GetTableName()
	newName := originRel.GetTableName()

	// reset origin table's constraint
	newCt := &engine.ConstraintDef{
		Cts: []engine.Constraint{},
	}

	constraint := make([][]byte, 0)
	for _, kind := range alterKind {
		var req *api.AlterTableReq
		switch kind {
		case api.AlterKind_RenameTable:
			req = api.NewRenameTableReq(copyRel.GetDBID(c.ctx), copyRel.GetTableID(c.ctx), oldName, newName)
		default:
		}
		tmp, err := req.Marshal()
		if err != nil {
			return err
		}
		constraint = append(constraint, tmp)
	}

	if err = copyRel.AlterTable(c.ctx, newCt, constraint); err != nil {
		return err
	}
	return nil
}

func (s *Scope) AlterTable(c *Compile) error {
	qry := s.Plan.GetDdl().GetAlterTable()
	if qry.AlgorithmType == plan.AlterTable_COPY {
		return s.AlterTableCopy(c)
	} else {
		return s.AlterTableInplace(c)
	}
}
