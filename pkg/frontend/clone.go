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
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	injectedError = "injected table clone error"
)

const (
	dataBranchLevel_Table    = "table"
	dataBranchLevel_Database = "database"
	dataBranchLevel_Account  = "account"
)

const (
	insertIntoBranchMetadataSql = `insert into %s.%s values(%d, %d, %d, %d, '%s', false)`
	scanBranchMetadataSql       = `select * from %s.%s`
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

func getBackExecutor(
	ctx context.Context,
	ses *Session,
) (BackgroundExec, func(error) error, error) {

	var (
		err      error
		bh       BackgroundExec
		deferred func(error) error
	)

	if ses.proc.GetTxnOperator().TxnOptions().ByBegin {
		bh = ses.GetShareTxnBackgroundExec(ctx, false)
		bh.ClearExecResultSet()
		return bh, func(err error) error {
			bh.Close()
			return nil
		}, nil
	}

	bh = ses.GetBackgroundExec(ctx)
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, "begin"); err != nil {
		return nil, func(err error) error {
			bh.Close()
			return nil
		}, err
	}

	deferred = func(err2 error) error {
		err2 = finishTxn(ctx, bh, err2)
		bh.Close()
		return err2
	}

	return bh, deferred, nil
}

func resolveSnapshot(
	ses *Session, atTsExpr *tree.AtTimeStamp,
) (*plan.Snapshot, error) {

	var (
		err      error
		snapshot *plan.Snapshot
	)

	if atTsExpr != nil {
		builder := plan.NewQueryBuilder(plan2.Query_INSERT, ses.txnCompileCtx, false, true)
		if snapshot, err = builder.ResolveTsHint(atTsExpr); err != nil {
			return nil, err
		}
	}

	return snapshot, nil
}

func getOpAndToAccountId(
	reqCtx context.Context,
	ses *Session,
	bh BackgroundExec,
	toAccountOpt *tree.ToAccountOpt,
	atTsExpr *tree.AtTimeStamp,
) (opAccountId, toAccountId uint32, snapshot *plan2.Snapshot, err error) {

	if snapshot, err = resolveSnapshot(ses, atTsExpr); err != nil {
		return 0, 0, nil, err
	}

	if opAccountId, err = defines.GetAccountId(reqCtx); err != nil {
		return 0, 0, nil, err
	}

	if toAccountOpt == nil {
		return opAccountId, opAccountId, snapshot, nil
	}

	if toAccountId, err = getAccountId(reqCtx, bh, toAccountOpt.AccountName.String()); err != nil {
		return 0, 0, nil, err
	}

	return opAccountId, toAccountId, snapshot, nil
}

// create table x.y clone r.s {MO_TS, SNAPSHOT}
// create table x.y clone r.s {MO_TS, SNAPSHOT} to account t
func handleCloneTable(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.CloneTable,
	bh BackgroundExec,
) (err error) {

	var (
		ctx    context.Context
		reqCtx = execCtx.reqCtx

		deferred      func(error) error
		faultInjected bool

		snapshot   *plan2.Snapshot
		snapshotTS int64

		toAccountId   uint32
		opAccountId   uint32
		fromAccountId uint32

		receipt cloneReceipt
	)

	if reqCtx.Value(tree.CloneLevelCtxKey{}) == nil {
		reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelTable)
	}

	if bh == nil {
		// do not open another transaction,
		// if the clone already executed within a transaction.
		if bh, deferred, err = getBackExecutor(reqCtx, ses); err != nil {
			return
		}

		defer func() {
			if deferred != nil {
				//if r := recover(); r != nil {
				//	err = moerr.ConvertPanicError(reqCtx, r)
				//}
				err = deferred(err)
			}
		}()
	}

	if opAccountId, toAccountId, snapshot, err = getOpAndToAccountId(
		reqCtx, ses, bh, stmt.ToAccountOpt, stmt.SrcTable.AtTsExpr,
	); err != nil {
		return
	}

	if snapshot == nil && opAccountId != toAccountId {
		err = moerr.NewInternalErrorNoCtxf("clone table between different accounts need a snapshot")
		return
	}

	fromAccountId = opAccountId
	if snapshot != nil && snapshot.Tenant != nil {
		fromAccountId = snapshot.Tenant.TenantID
	}

	if stmt.SrcTable.SchemaName == "" {
		// src acc = op acc
		// src acc = to acc
		// src != op acc and src != to acc
		if fromAccountId == opAccountId {
			stmt.SrcTable.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.SrcTable.SchemaName == "" {
		err = moerr.NewInternalErrorNoCtxf(
			"no db selected for the src table %s", stmt.SrcTable.ObjectName)
		return
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		if toAccountId == opAccountId {
			stmt.CreateTable.Table.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		err = moerr.NewInternalErrorNoCtxf(
			"no db selected for the dst table %s", stmt.CreateTable.Table.ObjectName)
		return
	}

	oldDefault := bh.(*backExec).backSes.GetDatabaseName()
	bh.(*backExec).backSes.SetDatabaseName(ses.GetTxnCompileCtx().DefaultDatabase())
	defer func() {
		bh.(*backExec).backSes.SetDatabaseName(oldDefault)
	}()

	if stmt.CreateTable.Table.SchemaName == moCatalog {
		err = moerr.NewInternalErrorNoCtxf("cannot clone data into system database")
		return
	}

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		err = moerr.NewInternalErrorNoCtxf("only sys can clone table to another account")
		return
	}

	ctx = defines.AttachAccountId(reqCtx, toAccountId)

	sql := execCtx.input.sql
	if stmt.ToAccountOpt != nil {
		// create table to account x
		sql, _, _ = strings.Cut(strings.ToLower(sql), " to account ")
	}

	if snapshot == nil {
		if snapshotTS, err = tryToIncreaseTxnPhysicalTS(
			reqCtx, ses.proc.GetTxnOperator(),
		); err != nil {
			return
		}

		sql, _ = strings.CutSuffix(sql, ";")
		sql = sql + fmt.Sprintf(" {MO_TS = %d}", snapshotTS)
	}

	if err = bh.ExecRestore(ctx, sql, opAccountId, toAccountId); err != nil {
		return
	}

	receipt.srcDb = stmt.SrcTable.SchemaName.String()
	receipt.srcTbl = stmt.SrcTable.ObjectName.String()
	receipt.dstDb = stmt.CreateTable.Table.SchemaName.String()
	receipt.dstTbl = stmt.CreateTable.Table.ObjectName.String()
	receipt.snapshot = snapshot
	receipt.snapshotTS = snapshotTS
	receipt.toAccount = toAccountId
	receipt.opAccount = opAccountId
	receipt.srcAccount = fromAccountId

	if err = updateBranchMetaTable(reqCtx, ses, bh, receipt); err != nil {
		return
	}

	if faultInjected, _ = objectio.LogCNCloneFailedInjected(
		stmt.CreateTable.Table.SchemaName.String(), stmt.CreateTable.Table.ObjectName.String(),
	); faultInjected {
		err = moerr.NewInternalErrorNoCtx(injectedError)
	}

	return
}

var snapConditionRegex = regexp.MustCompile(`\{[^}]+}`)

// create database x clone y {MO_TS, SNAPSHOT}
// create database x clone y {MO_TS, SNAPSHOT} to account t
func handleCloneDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.CloneDatabase,
) (err error) {

	var (
		reqCtx = execCtx.reqCtx

		bh       BackgroundExec
		deferred func(error) error

		toAccountId uint32
		opAccountId uint32

		ctx1 context.Context

		srcTblInfos []*tableInfo
		snapshot    *plan2.Snapshot

		viewMap = make(map[string]*tableInfo)

		sortedViews   []string
		sortedFkTbls  []string
		fkTableMap    map[string]*tableInfo
		snapCondition string

		snapshotTS int64
	)

	oldDefault := ses.GetTxnCompileCtx().DefaultDatabase()
	defer func() {
		ses.GetTxnCompileCtx().SetDatabase(oldDefault)
	}()

	if reqCtx.Value(tree.CloneLevelCtxKey{}) == nil {
		reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.NormalCloneLevelDatabase)
	}

	// do not open another transaction,
	// if the clone already executed within a transaction.
	if bh, deferred, err = getBackExecutor(reqCtx, ses); err != nil {
		return err
	}

	defer func() {
		if deferred != nil {
			//if r := recover(); r != nil {
			//	err = moerr.ConvertPanicError(reqCtx, r)
			//}
			err = deferred(err)
		}
	}()

	if opAccountId, toAccountId, snapshot, err = getOpAndToAccountId(
		reqCtx, ses, bh, stmt.ToAccountOpt, stmt.AtTsExpr,
	); err != nil {
		return err
	}

	if snapshot == nil && opAccountId != toAccountId {
		return moerr.NewInternalErrorNoCtxf("clone database between different accounts need a snapshot")
	}

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return moerr.NewInternalError(reqCtx, "only sys can clone table to another account")
	}

	ctx1 = defines.AttachAccountId(reqCtx, toAccountId)
	if err = bh.Exec(ctx1,
		fmt.Sprintf("create database `%s`", stmt.DstDatabase),
	); err != nil {
		return err
	}

	if srcTblInfos, err = getTableInfos(
		reqCtx, ses.GetService(), bh, snapshot,
		stmt.SrcDatabase.String(), "",
	); err != nil {
		return err
	}

	snapCondition = snapConditionRegex.FindString(execCtx.input.sql)

	if sortedFkTbls, err = fkTablesTopoSort(
		reqCtx, bh, snapshot, stmt.SrcDatabase.String(), "",
	); err != nil {
		return err
	}

	if fkTableMap, err = getTableInfoMap(
		reqCtx, ses.GetService(), bh, snapshot, stmt.SrcDatabase.String(), "", sortedFkTbls,
	); err != nil {
		return err
	}

	if len(snapCondition) == 0 {
		// consider the following example:
		// (within a session)
		//   ...
		// insert into t1 values (1) ---> commit ts (P2-L3)
		// insert into t1 values (2) ---> commit ts (P2-L3)
		// create table t2 clone t1 ---> the read snapshot ts is P2.
		//
		// limited by the format for the snapshot read TS, the logic TS is truncated,
		// so in this example, the clone cannot read the newly inserted data.
		//
		// so we try to increase the txn physical ts here to make sure the snapshot TS
		// the clone will get is greater than P2.
		if snapshotTS, err = tryToIncreaseTxnPhysicalTS(
			reqCtx, ses.proc.GetTxnOperator(),
		); err != nil {
			return err
		}
	}

	cloneTable := func(dstDb, dstTbl, srcDb, srcTbl string) error {
		sql := fmt.Sprintf(
			"create table `%s`.`%s` clone `%s`.`%s`",
			dstDb, dstTbl, srcDb, srcTbl,
		)

		if len(snapCondition) != 0 {
			sql = sql + " " + snapCondition
		} else {
			sql = sql + fmt.Sprintf(" {MO_TS = %d}", snapshotTS)
		}

		if stmt.ToAccountOpt != nil {
			sql = sql + fmt.Sprintf(" to account %s", stmt.ToAccountOpt.AccountName)
		}

		var (
			cloneStmts  []tree.Statement
			tempExecCtx = &ExecCtx{
				reqCtx: reqCtx,
				input:  &UserInput{sql: sql},
			}
		)

		if cloneStmts, err = parsers.Parse(reqCtx, dialect.MYSQL, sql, 0); err != nil {
			return err
		}

		defer func() {
			cloneStmts[0].Free()
		}()

		if err = handleCloneTable(
			tempExecCtx, ses, cloneStmts[0].(*tree.CloneTable), bh,
		); err != nil {
			return err
		}

		return nil
	}

	for _, srcTbl := range srcTblInfos {

		key := genKey(srcTbl.dbName, srcTbl.tblName)
		if _, ok := fkTableMap[key]; ok {
			continue
		}

		if srcTbl.typ == view {
			viewMap[key] = srcTbl
			continue
		}

		if err = cloneTable(
			stmt.DstDatabase.String(), srcTbl.tblName,
			stmt.SrcDatabase.String(), srcTbl.tblName,
		); err != nil {
			return err
		}
	}

	// clone foreign key related table
	for _, key := range sortedFkTbls {
		if tblInfo := fkTableMap[key]; tblInfo != nil {
			if err = cloneTable(
				stmt.DstDatabase.String(), tblInfo.tblName,
				stmt.SrcDatabase.String(), tblInfo.tblName,
			); err != nil {
				return err
			}
		}
	}

	// clone view table
	if len(viewMap) != 0 {
		fromAccount := opAccountId
		if snapshot != nil && snapshot.Tenant != nil {
			fromAccount = snapshot.Tenant.TenantID
		}

		if sortedViews, err = sortedViewInfos(
			reqCtx, ses, bh, "", snapshot, viewMap, fromAccount, toAccountId,
		); err != nil {
			return err
		}

		for i := range sortedViews {
			sortedViews[i] = strings.ReplaceAll(
				sortedViews[i], stmt.SrcDatabase.String(), stmt.DstDatabase.String())
		}

		newViewMap := make(map[string]*tableInfo)
		for key, info := range viewMap {
			key = strings.ReplaceAll(key, stmt.SrcDatabase.String(), stmt.DstDatabase.String())
			info.createSql = strings.ReplaceAll(info.createSql, stmt.SrcDatabase.String(), stmt.DstDatabase.String())
			info.dbName = stmt.DstDatabase.String()

			newViewMap[key] = info
		}

		if err = restoreViews(reqCtx, ses, bh, "", newViewMap, toAccountId, sortedViews); err != nil {
			return err
		}
	}

	return nil
}

func tryToIncreaseTxnPhysicalTS(
	ctx context.Context, txnOp client.TxnOperator,
) (updatedPhysical int64, err error) {

	curTxnPhysicalTS := txnOp.SnapshotTS().PhysicalTime

	if ctx.Value(defines.TenantIDKey{}) == nil {
		return curTxnPhysicalTS, nil
	}

	// a slight increase added to the physical to make sure
	// the updated ts is greater than the old txn timestamp (physical + logic)
	curTxnPhysicalTS += int64(time.Microsecond)
	if err = txnOp.UpdateSnapshot(ctx, timestamp.Timestamp{
		PhysicalTime: curTxnPhysicalTS,
	}); err != nil {
		return
	}

	updatedPhysical = txnOp.SnapshotTS().PhysicalTime
	if updatedPhysical <= curTxnPhysicalTS {
		return 0, moerr.NewInternalErrorNoCtxf("try to update the snapshot ts failed in clone database")
	}

	// return a nanosecond precision
	updatedPhysical -= int64(time.Nanosecond)

	return updatedPhysical, nil
}

func updateBranchMetaTable(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	receipt cloneReceipt,
) (err error) {

	var (
		srcTblDef  *plan.TableDef
		dstTblDef  *plan.TableDef
		dstDB      engine.Database
		dstRel     engine.Relation
		cloneTxnOp TxnOperator
		level      string
	)

	switch ctx.Value(tree.CloneLevelCtxKey{}).(tree.CloneLevelType) {
	case tree.NormalCloneLevelTable:
		level = dataBranchLevel_Table
	case tree.NormalCloneLevelDatabase:
		level = dataBranchLevel_Database
	case tree.NormalCloneLevelAccount:
		level = dataBranchLevel_Account
	default:
		// we do not record the branch metadata for restore
		return nil
	}

	if _, srcTblDef, err = ses.GetTxnCompileCtx().Resolve(
		receipt.srcDb, receipt.srcTbl, receipt.snapshot,
	); err != nil {
		return err
	}

	dstCtx := defines.AttachAccountId(ctx, receipt.toAccount)

	// the back session did the clone operation,
	// we need it's txnOp to read the uncommit table info.
	cloneTxnOp = bh.(*backExec).backSes.GetTxnHandler().GetTxn()
	if dstDB, err = ses.proc.GetSessionInfo().StorageEngine.Database(
		dstCtx, receipt.dstDb, cloneTxnOp,
	); err != nil {
		return err
	}

	if dstRel, err = dstDB.Relation(dstCtx, receipt.dstTbl, nil); err != nil {
		return err
	}
	dstTblDef = dstRel.GetTableDef(dstCtx)

	if receipt.snapshot != nil {
		receipt.snapshotTS = receipt.snapshot.TS.PhysicalTime
	}

	// write branch info into branch_metadata table
	updateMetadataSql := fmt.Sprintf(
		insertIntoBranchMetadataSql,
		catalog.MO_CATALOG,
		catalog.MO_BRANCH_METADATA,
		dstTblDef.TblId,
		receipt.snapshotTS,
		srcTblDef.TblId,
		receipt.opAccount,
		level,
	)

	tempCtx := ctx
	if receipt.opAccount != sysAccountID {
		tempCtx = defines.AttachAccountId(tempCtx, sysAccountID)
	}
	if err = bh.Exec(tempCtx, updateMetadataSql); err != nil {
		return err
	}

	return nil
}
