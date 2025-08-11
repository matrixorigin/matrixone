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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func getOpAndToAccountId(
	reqCtx context.Context,
	ses *Session,
	bh BackgroundExec,
	toAccountName string,
	atTsExpr *tree.AtTimeStamp,
) (opAccountId, toAccountId uint32, snapshot *plan2.Snapshot, err error) {

	if atTsExpr != nil {
		builder := plan.NewQueryBuilder(plan2.Query_INSERT, ses.txnCompileCtx, false, true)
		if snapshot, err = builder.ResolveTsHint(atTsExpr); err != nil {
			return 0, 0, nil, err
		}
	}

	if opAccountId, err = defines.GetAccountId(reqCtx); err != nil {
		return 0, 0, nil, err
	}

	if len(toAccountName) == 0 {
		return opAccountId, opAccountId, snapshot, nil
	}

	if toAccountId, err = getAccountId(reqCtx, bh, toAccountName); err != nil {
		return 0, 0, nil, err
	}

	return opAccountId, toAccountId, snapshot, nil
}

// create table x.y clone r.s {MO_TS, SNAPSHOT} to account t
func handleCloneTableAcrossAccounts(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.CloneTable,
) error {

	if len(stmt.ToAccountName.String()) == 0 {
		panic("expected a non-empty to_account_name")
	}

	var (
		err    error
		ctx    context.Context
		reqCtx = execCtx.reqCtx

		bh BackgroundExec

		snapshot *plan2.Snapshot

		toAccountId   uint32
		opAccountId   uint32
		fromAccountId uint32
	)

	reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.CloneLevelTable)

	bh = ses.GetBackgroundExec(reqCtx)
	if err = bh.Exec(reqCtx, "begin"); err != nil {
		return err
	}

	defer func() {
		err = finishTxn(reqCtx, bh, err)
	}()

	if opAccountId, toAccountId, snapshot, err = getOpAndToAccountId(
		reqCtx, ses, bh, stmt.ToAccountName.String(), stmt.SrcTable.AtTsExpr,
	); err != nil {
		return err
	}

	if snapshot == nil && opAccountId != toAccountId {
		return moerr.NewInternalErrorNoCtxf("clone table between different accounts need a snapshot")
	}

	if stmt.SrcTable.SchemaName == "" {
		fromAccountId = opAccountId
		if snapshot != nil && snapshot.Tenant != nil {
			fromAccountId = snapshot.Tenant.TenantID
		}

		// src acc = op acc
		// src acc = to acc
		// src != op acc and src != to acc
		if fromAccountId == opAccountId {
			stmt.SrcTable.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.SrcTable.SchemaName == "" {
		return moerr.NewInternalErrorNoCtxf(
			"no db selected for the src table %s", stmt.SrcTable.ObjectName)
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		if toAccountId == opAccountId {
			stmt.CreateTable.Table.SchemaName = tree.Identifier(ses.GetTxnCompileCtx().DefaultDatabase())
		}
	}

	if stmt.CreateTable.Table.SchemaName == "" {
		return moerr.NewInternalErrorNoCtxf(
			"no db selected for the dst table %s", stmt.CreateTable.Table.ObjectName)
	}

	bh.(*backExec).backSes.SetDatabaseName(ses.GetTxnCompileCtx().DefaultDatabase())

	if stmt.CreateTable.Table.SchemaName == moCatalog {
		return moerr.NewInternalErrorNoCtxf("cannot create table under the mo_catalog")
	}

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return moerr.NewInternalErrorNoCtxf("only sys can clone table to another account")
	}

	ctx = defines.AttachAccountId(reqCtx, toAccountId)

	sql := strings.Split(strings.ToLower(execCtx.input.sql), " to ")[0]

	if err = bh.ExecRestore(ctx, sql, opAccountId, toAccountId); err != nil {
		return err
	}

	return nil
}

var snapConditionRegex = regexp.MustCompile(`\{[^}]+}`)

// create database x clone y {MO_TS, SNAPSHOT}
// create database x clone y {MO_TS, SNAPSHOT} to account t
func handleCloneDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.CloneDatabase,
) error {

	var (
		err    error
		reqCtx = execCtx.reqCtx

		bh BackgroundExec

		toAccountId uint32
		opAccountId uint32

		ctx1, ctx2 context.Context

		srcTblInfos []*tableInfo
		snapshot    *plan2.Snapshot

		viewMap = make(map[string]*tableInfo)

		sortedViews   []string
		sortedFkTbls  []string
		fkTableMap    map[string]*tableInfo
		snapCondition string

		snapshotTS int64
	)

	reqCtx = context.WithValue(reqCtx, tree.CloneLevelCtxKey{}, tree.CloneLevelDatabase)

	bh = ses.GetBackgroundExec(reqCtx)
	if err = bh.Exec(reqCtx, "begin"); err != nil {
		return err
	}

	defer func() {
		err = finishTxn(reqCtx, bh, err)
	}()

	if opAccountId, toAccountId, snapshot, err = getOpAndToAccountId(
		reqCtx, ses, bh, stmt.ToAccountName.String(), stmt.AtTsExpr,
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

	ctx2 = defines.AttachAccountId(reqCtx, toAccountId)

	cloneTable := func(dstDb, dstTbl, srcDb, srcTbl string) error {
		sql := fmt.Sprintf(
			"create table `%s`.`%s` clone `%s`.`%s`",
			dstDb, dstTbl, srcDb, srcTbl,
		)

		if snapCondition != "" {
			sql = sql + " " + snapCondition
		} else {
			sql = sql + fmt.Sprintf(" {MO_TS = %d}", snapshotTS)
		}

		if err = bh.ExecRestore(ctx2, sql, opAccountId, toAccountId); err != nil {
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
