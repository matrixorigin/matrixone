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
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type LockMeta struct {
	database_table_id uint64 //table id of mo_database
	table_table_id    uint64 //table id of mo_tables

	metaTables   map[string]struct{}        //key: (db_name table_name)
	lockDbExe    colexec.ExpressionExecutor //executor to serial function to lock mo_database
	lockTableExe colexec.ExpressionExecutor //executor to serial function to lock mo_tables
	lockMetaVecs []*vector.Vector           //paramters for serial function
}

func NewLockMeta() *LockMeta {
	return &LockMeta{}
}

func (l *LockMeta) reset(_ *process.Process) {
	if l.lockDbExe != nil {
		l.lockDbExe.ResetForNextQuery()
	}
	if l.lockTableExe != nil {
		l.lockTableExe.ResetForNextQuery()
	}
}

func (l *LockMeta) clear(proc *process.Process) {
	for k := range l.metaTables {
		delete(l.metaTables, k)
	}
	if l.lockDbExe != nil {
		l.lockDbExe.Free()
		l.lockDbExe = nil
	}
	if l.lockTableExe != nil {
		l.lockTableExe.Free()
		l.lockTableExe = nil
	}
	for _, vec := range l.lockMetaVecs {
		vec.Free(proc.Mp())
	}
	l.lockMetaVecs = nil
}

func (l *LockMeta) appendMetaTables(objRes *plan.ObjectRef) {
	if l.metaTables == nil {
		l.metaTables = make(map[string]struct{})
	}

	if objRes.SchemaName == catalog.MO_CATALOG && (objRes.ObjName == catalog.MO_DATABASE || objRes.ObjName == catalog.MO_TABLES || objRes.ObjName == catalog.MO_COLUMNS) {
		// do not lock meta table for meta table
	} else {
		key := fmt.Sprintf("%s %s", objRes.SchemaName, objRes.ObjName)
		l.metaTables[key] = struct{}{}
	}
}

func (l *LockMeta) doLock(e engine.Engine, proc *process.Process) error {
	lockLen := len(l.metaTables)
	if lockLen == 0 {
		return nil
	}

	tables := make([]string, 0, lockLen)
	for table := range l.metaTables {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	if err := l.initLockExe(e, proc); err != nil {
		return err
	}

	if len(l.lockMetaVecs) == 0 {
		accountIdVec := vector.NewVec(types.T_uint32.ToType())
		dbName := vector.NewVec(types.T_varchar.ToType())
		tableName := vector.NewVec(types.T_varchar.ToType())
		l.lockMetaVecs = []*vector.Vector{
			accountIdVec,
			dbName,
			tableName,
		}
	} else {
		for _, vec := range l.lockMetaVecs {
			vec.CleanOnlyData()
		}
	}

	accountId := proc.GetSessionInfo().AccountId
	lockDbs := make(map[string]struct{})
	bat := batch.NewWithSize(3)
	for _, table := range tables {
		names := strings.SplitN(table, " ", 2)
		err := vector.AppendFixed(l.lockMetaVecs[0], accountId, false, proc.GetMPool()) //account_id
		if err != nil {
			return err
		}
		err = vector.AppendBytes(l.lockMetaVecs[1], []byte(names[0]), false, proc.GetMPool()) //db_name
		if err != nil {
			return err
		}
		err = vector.AppendBytes(l.lockMetaVecs[2], []byte(names[1]), false, proc.GetMPool()) //table_name
		if err != nil {
			return err
		}
		lockDbs[names[0]] = struct{}{}
	}

	// call serial function to lock mo_tables
	bat.Vecs = l.lockMetaVecs
	err := l.lockMetaRows(e, proc, l.lockTableExe, bat, accountId, l.table_table_id)
	if err != nil {
		return err
	}

	// recall serial function to lock mo_databases
	l.lockMetaVecs[0].CleanOnlyData()
	l.lockMetaVecs[1].CleanOnlyData()
	if len(lockDbs) > 1 {
		for dbName := range lockDbs {
			err := vector.AppendFixed(l.lockMetaVecs[0], accountId, false, proc.GetMPool()) //account_id
			if err != nil {
				return err
			}
			err = vector.AppendBytes(l.lockMetaVecs[1], []byte(dbName), false, proc.GetMPool()) //db_name
			if err != nil {
				return err
			}
		}
	}
	bat.Vecs = l.lockMetaVecs[:2]
	return l.lockMetaRows(e, proc, l.lockDbExe, bat, accountId, l.database_table_id)
}

func (l *LockMeta) lockMetaRows(e engine.Engine, proc *process.Process, executor colexec.ExpressionExecutor, bat *batch.Batch, accountId uint32, tableId uint64) error {
	executor.ResetForNextQuery()
	bat.SetRowCount(l.lockMetaVecs[0].Length())
	lockVec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return err
	}
	if err := lockop.LockRows(e, proc, nil, tableId, lockVec, *lockVec.GetType(), lock.LockMode_Shared, lock.Sharding_None, accountId); err != nil {
		// if get error in locking mocatalog.mo_tables by it's dbName & tblName
		// that means the origin table's schema was changed. then return NeedRetryWithDefChanged err
		if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
			moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		}

		// other errors, just throw  out
		return err
	}
	return nil
}

func (l *LockMeta) initLockExe(e engine.Engine, proc *process.Process) error {
	if l.lockTableExe != nil {
		return nil
	}

	accountTyp := types.T_uint32.ToType()
	accountIdExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&accountTyp),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 0,
			},
		},
	}

	dbNameTyp := types.T_varchar.ToType()
	dbNameExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&dbNameTyp),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 1,
			},
		},
	}

	tblNameTyp := types.T_varchar.ToType()
	tblNameExpr := &plan.Expr{
		Typ: plan2.MakePlan2Type(&tblNameTyp),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 2,
			},
		},
	}

	lockDbExpr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, function.SerialFunctionName, []*plan.Expr{accountIdExpr, dbNameExpr})
	if err != nil {
		return err
	}
	exec, err := colexec.NewExpressionExecutor(proc, lockDbExpr)
	if err != nil {
		return err
	}
	l.lockDbExe = exec

	lockTblxpr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, function.SerialFunctionName, []*plan.Expr{accountIdExpr, dbNameExpr, tblNameExpr})
	if err != nil {
		return err
	}
	exec, err = colexec.NewExpressionExecutor(proc, lockTblxpr)
	if err != nil {
		return err
	}
	l.lockTableExe = exec

	dbSource, err := e.Database(proc.Ctx, catalog.MO_CATALOG, proc.GetTxnOperator())
	if err != nil {
		return err
	}
	rel, err := dbSource.Relation(proc.Ctx, catalog.MO_DATABASE, nil)
	if err != nil {
		return err
	}
	l.database_table_id = rel.GetTableID(proc.Ctx)

	rel, err = dbSource.Relation(proc.Ctx, catalog.MO_TABLES, nil)
	if err != nil {
		return err
	}
	l.table_table_id = rel.GetTableID(proc.Ctx)

	return nil
}
