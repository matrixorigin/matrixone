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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"regexp"
	"strings"
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

	sql := strings.Split(strings.ToLower(execCtx.input.sql), "to")[0]

	if snapshot == nil {
		suffix := fmt.Sprintf(" {MO_TS = %d}", ses.proc.GetTxnOperator().SnapshotTS().PhysicalTime)
		sql += suffix
	}

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

		ctx1, ctx2, ctx3 context.Context

		srcTblInfos []*tableInfo
		snapshot    *plan2.Snapshot

		snapCondition string
	)

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

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return moerr.NewInternalError(reqCtx, "only sys can clone table to another account")
	}

	ctx1 = defines.AttachAccountId(reqCtx, toAccountId)
	if err = bh.Exec(ctx1,
		fmt.Sprintf("create database `%s`", stmt.DstDatabase),
	); err != nil {
		return err
	}

	if snapshot == nil {
		if srcTblInfos, err = showFullTables(
			reqCtx, ses.GetService(), bh, "", stmt.SrcDatabase.String(), "",
		); err != nil {
			return err
		}
	} else {
		snapCondition = snapConditionRegex.FindString(execCtx.input.sql)

		ctx2 = reqCtx
		if snapshot.Tenant != nil {
			ctx2 = defines.AttachAccountId(reqCtx, snapshot.Tenant.TenantID)
		}

		if srcTblInfos, err = showFullTablesWitsTs(
			ctx2, ses.GetService(), bh, "", snapshot.TS.PhysicalTime, stmt.SrcDatabase.String(), "",
		); err != nil {
			return err
		}
	}

	now := ses.proc.GetTxnOperator().SnapshotTS().PhysicalTime
	ctx3 = defines.AttachAccountId(reqCtx, toAccountId)
	for _, srcTbl := range srcTblInfos {
		sql := fmt.Sprintf(
			"create table `%s`.`%s` clone `%s`.`%s`",
			stmt.DstDatabase, srcTbl.tblName, srcTbl.dbName, srcTbl.tblName)

		if snapCondition != "" {
			sql = sql + " " + snapCondition
		} else {
			suffix := fmt.Sprintf(" {MO_TS = %d}", now)
			sql += suffix
		}

		if err = bh.ExecRestore(ctx3, sql, opAccountId, toAccountId); err != nil {
			return err
		}
	}

	return nil
}
