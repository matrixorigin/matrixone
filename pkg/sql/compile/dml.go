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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) Delete(c *Compile) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deletion.Argument)

	if arg.DeleteCtx.CanTruncate {
		var dbSource engine.Database
		var rel engine.Relation
		var err error
		var isTemp bool
		var newId uint64

		delCtx := arg.DeleteCtx

		dbName := delCtx.Ref.SchemaName
		tblName := delCtx.Ref.ObjName
		oldId := uint64(delCtx.Ref.Obj)
		affectRows, err := delCtx.Source.Rows(c.ctx)

		if err != nil {
			return 0, err
		}

		dbSource, err = c.e.Database(c.ctx, dbName, c.proc.TxnOperator)
		if err != nil {
			return 0, err
		}

		if rel, err = dbSource.Relation(c.ctx, tblName, nil); err != nil {
			var e error // avoid contamination of error messages
			dbSource, e = c.e.Database(c.ctx, defines.TEMPORARY_DBNAME, c.proc.TxnOperator)
			if e != nil {
				return 0, err
			}
			rel, e = dbSource.Relation(c.ctx, engine.GetTempTableName(dbName, tblName), nil)
			if e != nil {
				return 0, err
			}
			isTemp = true
		}

		if !isTemp && c.proc.TxnOperator.Txn().IsPessimistic() {
			var err error
			if e := lockMoTable(c, dbName, tblName, lock.LockMode_Shared); e != nil {
				if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
					!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
					return 0, e
				}
				err = e
			}
			// before dropping table, lock it.
			if e := lockTable(c.ctx, c.e, c.proc, rel, dbName, delCtx.PartitionTableNames, false); e != nil {
				if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
					!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
					return 0, e
				}
				err = e
			}
			if err != nil {
				return 0, err
			}
		}

		if isTemp {
			// memoryengine truncate always return 0, so for temporary table, just use origin tableId as newId
			_, err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, tblName))
			newId = rel.GetTableID(c.ctx)
		} else {
			newId, err = dbSource.Truncate(c.ctx, tblName)
		}

		if err != nil {
			return 0, err
		}

		// Truncate Index Tables if needed
		for _, name := range delCtx.IndexTableNames {
			var err error
			if isTemp {
				_, err = dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, name))
			} else {
				_, err = dbSource.Truncate(c.ctx, name)
			}
			if err != nil {
				return 0, err
			}
		}

		//Truncate Partition subtable if needed
		for _, name := range delCtx.PartitionTableNames {
			var err error
			if isTemp {
				dbSource.Truncate(c.ctx, engine.GetTempTableName(dbName, name))
			} else {
				_, err = dbSource.Truncate(c.ctx, name)
			}
			if err != nil {
				return 0, err
			}
		}

		// update tableDef of foreign key's table with new table id
		for _, fTblId := range delCtx.ForeignTbl {
			_, _, fkRelation, err := c.e.GetRelationById(c.ctx, c.proc.TxnOperator, fTblId)
			if err != nil {
				return 0, err
			}
			fkTableDef, err := fkRelation.TableDefs(c.ctx)
			if err != nil {
				return 0, err
			}
			var oldCt *engine.ConstraintDef
			for _, def := range fkTableDef {
				if ct, ok := def.(*engine.ConstraintDef); ok {
					oldCt = ct
					break
				}
			}
			for _, ct := range oldCt.Cts {
				if def, ok := ct.(*engine.RefChildTableDef); ok {
					for idx, refTable := range def.Tables {
						if refTable == oldId {
							def.Tables[idx] = newId
							break
						}
					}
					break
				}
			}
			if err != nil {
				return 0, err
			}
			err = fkRelation.UpdateConstraint(c.ctx, oldCt)
			if err != nil {
				return 0, err
			}

		}

		if isTemp {
			oldId = rel.GetTableID(c.ctx)
		}
		err = incrservice.GetAutoIncrementService(c.ctx).Reset(
			c.ctx,
			oldId,
			newId,
			true,
			c.proc.TxnOperator)
		if err != nil {
			return 0, err
		}

		// update index information in mo_catalog.mo_indexes
		updateSql := fmt.Sprintf(updateMoIndexesTruncateTableFormat, newId, oldId)
		err = c.runSql(updateSql)
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
