// Copyright 2025 Matrix Origin
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

package frontend

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	dataBranchLevel_Table    = "table"
	dataBranchLevel_Database = "database"
	dataBranchLevel_Account  = "account"
)

const (
	insertIntoBranchMetadataSql = `insert into %s.%s values(%d, %d, %d, %d, '%s', false)`
)

type cloneReceipt struct {
	dstDb  string
	dstTbl string
	srcDb  string
	srcTbl string
	// this valid only when the snapshot is nil
	snapshotTS int64
	snapshot   *plan.Snapshot

	opAccount  uint32
	toAccount  uint32
	srcAccount uint32
}

func handleDataBranch(
	execCtx *ExecCtx,
	ses *Session,
	stmt tree.Statement,
) error {

	switch st := stmt.(type) {
	case *tree.DataBranchCreateTable:
		return dataBranchCreateTable(execCtx, ses, st)
	case *tree.DataBranchCreateDatabase:
	case *tree.DataBranchDeleteTable:
	case *tree.DataBranchDeleteDatabase:
	default:
		return moerr.NewNotSupportedNoCtxf("data branch not supported: %v", st)
	}

	return nil
}

func dataBranchCreateTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchCreateTable,
) (err error) {

	var (
		reqCtx = execCtx.reqCtx
		bh     = ses.GetBackgroundExec(reqCtx)

		deferred    func(error) error
		newSql      string
		tempExecCtx *ExecCtx
		cloneTable  *tree.CloneTable
	)

	// do not open another transaction,
	// if the clone already executed within a transaction.
	if bh, deferred, err = getBackExecutor(reqCtx, ses); err != nil {
		return err
	}

	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	newSql = rewriteBranchNewTableToClone(execCtx.input.sql, stmt)

	tempExecCtx = &ExecCtx{
		reqCtx: execCtx.reqCtx,
		input: &UserInput{
			sql: newSql,
		},
	}

	cloneTable = &tree.CloneTable{
		CreateTable:  stmt.CreateTable,
		SrcTable:     stmt.SrcTable,
		ToAccountOpt: stmt.ToAccountOpt,
	}

	var (
		srcTblDef *plan.TableDef
		dstTblDef *plan.TableDef

		dstDB      engine.Database
		dstRel     engine.Relation
		cloneTxnOp TxnOperator
		receipt    cloneReceipt
	)

	// 1. clone table
	if err = handleCloneTable(
		tempExecCtx, ses, cloneTable, &receipt, bh,
	); err != nil {
		return err
	}

	// 2.1 get src table id
	if _, srcTblDef, err = ses.GetTxnCompileCtx().Resolve(
		receipt.srcDb, receipt.srcTbl, receipt.snapshot,
	); err != nil {
		return err
	}

	// 2.2 get dst table id
	// the back session did the clone operation,
	// we need it's txnOp to read the uncommit table info.
	cloneTxnOp = bh.(*backExec).backSes.GetTxnHandler().GetTxn()
	if dstDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
		reqCtx, receipt.dstDb, cloneTxnOp,
	); err != nil {
		return err
	}

	if dstRel, err = dstDB.Relation(reqCtx, receipt.dstTbl, nil); err != nil {
		return err
	}

	dstTblDef = dstRel.GetTableDef(reqCtx)

	if receipt.snapshot != nil {
		receipt.snapshotTS = receipt.snapshot.TS.PhysicalTime
	}

	// 2.3 write branch info into branch_metadata table
	updateMetadataSql := fmt.Sprintf(
		insertIntoBranchMetadataSql,
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		dstTblDef.TblId,
		receipt.snapshotTS,
		srcTblDef.TblId,
		receipt.opAccount,
		dataBranchLevel_Table,
	)

	tempCtx := reqCtx
	if receipt.opAccount != sysAccountID {
		tempCtx = defines.AttachAccountId(tempCtx, sysAccountID)
	}

	if err = bh.Exec(tempCtx, updateMetadataSql); err != nil {
		return err
	}

	return nil
}

func rewriteBranchNewTableToClone(
	oldSql string,
	stmt *tree.DataBranchCreateTable,
) (newSql string) {
	// create table d2.t2 clone d1.t1 {snapshot} to x

	var (
		dstTable    string
		srcTable    string
		dstDatabase string
		srcDatabase string

		sp, src, dst string
	)

	dstTable = stmt.CreateTable.Table.ObjectName.String()
	dstDatabase = stmt.CreateTable.Table.SchemaName.String()

	srcTable = stmt.SrcTable.ObjectName.String()
	srcDatabase = stmt.SrcTable.SchemaName.String()

	src = srcTable
	if srcDatabase != "" {
		src = fmt.Sprintf("`%s`.`%s`", srcDatabase, srcTable)
	}

	dst = dstTable
	if dstDatabase != "" {
		dst = fmt.Sprintf("`%s`.`%s`", dstDatabase, dstTable)
	}

	sp = snapConditionRegex.FindString(oldSql)
	newSql = fmt.Sprintf("create table %s clone %s %s", dst, src, sp)

	if stmt.ToAccountOpt != nil {
		newSql += fmt.Sprintf(" to account %s", stmt.ToAccountOpt.AccountName.String())
	}

	return newSql
}
