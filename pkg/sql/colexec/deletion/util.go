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

package deletion

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func runSql(proc *process.Process, dbName string, sql string) error {
	if sql == "" {
		return nil
	}
	v, ok := moruntime.ProcessLevelRuntime().GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(proc.TxnOperator).
		WithDatabase(dbName).
		WithTimeZone(proc.SessionInfo.TimeZone)
	res, err := exec.Exec(proc.Ctx, sql, opts)
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func TruncateTable(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	dbName string,
	tableName string,
	tableId uint64,
	partitionTableNames []string,
	indexTableNames []string,
	foreignTbl []uint64,
	keepAutoIncrement bool,
) error {
	var dbSource engine.Database
	var rel engine.Relation
	var err error
	var isTemp bool
	var newTblId uint64
	dbSource, err = eng.Database(ctx, dbName, proc.TxnOperator)
	if err != nil {
		return err
	}

	if rel, err = dbSource.Relation(ctx, tableName, nil); err != nil {
		var e error // avoid contamination of error messages
		dbSource, e = eng.Database(ctx, defines.TEMPORARY_DBNAME, proc.TxnOperator)
		if e != nil {
			return err
		}
		rel, e = dbSource.Relation(ctx, engine.GetTempTableName(dbName, tableName), nil)
		if e != nil {
			return err
		}
		isTemp = true
	}

	if !isTemp && proc.TxnOperator.Txn().IsPessimistic() {
		var err error
		if e := lockop.LockMoTable(ctx, eng, proc, dbName, tableName, lock.LockMode_Shared); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		// before dropping table, lock it.
		if e := lockop.LockTable(ctx, eng, proc, rel, dbName, partitionTableNames, false); e != nil {
			if !moerr.IsMoErrCode(e, moerr.ErrTxnNeedRetry) &&
				!moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
				return e
			}
			err = e
		}
		if err != nil {
			return err
		}
	}

	if isTemp {
		// memory engine truncate always return 0, so for temporary table, just use origin tableId as newTblId
		_, err = dbSource.Truncate(ctx, engine.GetTempTableName(dbName, tableName))
		newTblId = rel.GetTableID(ctx)
	} else {
		newTblId, err = dbSource.Truncate(ctx, tableName)
	}

	if err != nil {
		return err
	}

	// Truncate Index Tables if needed
	for _, name := range indexTableNames {
		var err error
		if isTemp {
			_, err = dbSource.Truncate(ctx, engine.GetTempTableName(dbName, name))
		} else {
			_, err = dbSource.Truncate(ctx, name)
		}
		if err != nil {
			return err
		}
	}

	//Truncate Partition subTable if needed
	for _, name := range partitionTableNames {
		var err error
		if isTemp {
			_, err = dbSource.Truncate(ctx, engine.GetTempTableName(dbName, name))
		} else {
			_, err = dbSource.Truncate(ctx, name)
		}
		if err != nil {
			return err
		}
	}

	// update tableDef of foreign key's table with new table id
	for _, fTblId := range foreignTbl {
		_, _, fkRelation, err := eng.GetRelationById(ctx, proc.TxnOperator, fTblId)
		if err != nil {
			return err
		}
		fkTableDef, err := fkRelation.TableDefs(ctx)
		if err != nil {
			return err
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
					if refTable == tableId {
						def.Tables[idx] = newTblId
						break
					}
				}
				break
			}
		}
		if err != nil {
			return err
		}
		err = fkRelation.UpdateConstraint(ctx, oldCt)
		if err != nil {
			return err
		}

	}

	if isTemp {
		tableId = rel.GetTableID(ctx)
	}
	err = incrservice.GetAutoIncrementService(ctx).Reset(
		ctx,
		tableId,
		newTblId,
		keepAutoIncrement,
		proc.TxnOperator)
	if err != nil {
		return err
	}

	// update index information in mo_catalog.mo_indexes
	updateSql := fmt.Sprintf(`update mo_catalog.mo_indexes set table_id = %v where table_id = %v`, newTblId, tableId)
	err = runSql(proc, dbName, updateSql)
	if err != nil {
		return err
	}
	return nil
}
