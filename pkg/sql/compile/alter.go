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
	dbName := qry.Database
	if dbName == "" {
		dbName = c.db
	}
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
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return err
			}
			retryErr = err
		}

		// 2. lock origin table
		if err = lockTable(c.e, c.proc, originRel, true); err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
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

	// get and update the change mapping information of table colIds
	if err = updateNewTableColId(c, copyRel, qry.ChangeTblColIdMap); err != nil {
		return err
	}

	if len(qry.CopyTableDef.RefChildTbls) > 0 {
		// Restore the original table's foreign key child table ids to the copy table definition
		if err = restoreNewTableRefChildTbls(c, copyRel, qry.CopyTableDef.RefChildTbls); err != nil {
			return err
		}

		// update foreign key child table references
		for _, tblId := range qry.CopyTableDef.RefChildTbls {
			if err = updateTableForeignKeyColId(c, qry.ChangeTblColIdMap, tblId, originRel.GetTableID(c.ctx), copyRel.GetTableID(c.ctx)); err != nil {
				return err
			}
		}
	}

	if len(qry.TableDef.Fkeys) > 0 {
		for _, fkey := range qry.CopyTableDef.Fkeys {
			if err = notifyParentTableFkTableIdChange(c, fkey, originRel.GetTableID(c.ctx), copyRel.GetTableID(c.ctx)); err != nil {
				return err
			}
		}
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

// updateTableForeignKeyColId update foreign key colid of child table references
func updateTableForeignKeyColId(c *Compile, changColDefMap map[uint64]*plan.ColDef, childTblId uint64, oldParentTblId uint64, newParentTblId uint64) error {
	_, _, childRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, childTblId)
	if err != nil {
		return err
	}
	childTableDef, err := childRelation.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	for _, def := range childTableDef {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.ForeignKeyDef); ok1 {
			for i := 0; i < len(def.Fkeys); i++ {
				fkey := def.Fkeys[i]
				if fkey.ForeignTbl == oldParentTblId {
					for j := 0; j < len(fkey.ForeignCols); j++ {
						if newColDef, ok2 := changColDefMap[fkey.ForeignCols[j]]; ok2 {
							fkey.ForeignCols[j] = newColDef.ColId
						}
					}
					fkey.ForeignTbl = newParentTblId
				}
			}
		}
	}
	return childRelation.UpdateConstraint(c.ctx, oldCt)
}

func updateNewTableColId(c *Compile, copyRel engine.Relation, changColDefMap map[uint64]*plan.ColDef) error {
	engineDefs, err := copyRel.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	for _, def := range engineDefs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			for _, vColDef := range changColDefMap {
				if vColDef.Name == attr.Attr.Name {
					vColDef.ColId = attr.Attr.ID
					break
				}
			}
		}
	}
	return nil
}

// restoreNewTableRefChildTbls Restore the original table's foreign key child table ids to the copy table definition
func restoreNewTableRefChildTbls(c *Compile, copyRel engine.Relation, refChildTbls []uint64) error {
	copyTableDef, err := copyRel.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	for _, def := range copyTableDef {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}

	if oldCt == nil {
		oldCt = &engine.ConstraintDef{
			Cts: []engine.Constraint{},
		}
	}
	oldCt.Cts = append(oldCt.Cts, &engine.RefChildTableDef{
		Tables: refChildTbls,
	})
	return copyRel.UpdateConstraint(c.ctx, oldCt)
}

// notifyParentTableFkTableIdChange Notify the parent table of changes in the tableid of the foreign key table
func notifyParentTableFkTableIdChange(c *Compile, fkey *plan.ForeignKeyDef, oldTableId uint64, newTableId uint64) error {
	foreignTblId := fkey.ForeignTbl
	_, _, fatherRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, foreignTblId)
	if err != nil {
		return err
	}
	fatherTableDef, err := fatherRelation.TableDefs(c.ctx)
	if err != nil {
		return err
	}
	var oldCt *engine.ConstraintDef
	for _, def := range fatherTableDef {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			oldCt = ct
			break
		}
	}
	for _, ct := range oldCt.Cts {
		if def, ok1 := ct.(*engine.RefChildTableDef); ok1 {
			for i := 0; i < len(def.Tables); i++ {
				if def.Tables[i] == oldTableId {
					// delete target element
					def.Tables = append(def.Tables[:i], def.Tables[i+1:]...)
					// Because the length of the slice has become shorter, it is necessary to move i forward
					i--
				}
			}
		}
	}
	return fatherRelation.UpdateConstraint(c.ctx, oldCt)
}
