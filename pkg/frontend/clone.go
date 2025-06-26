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
	bh BackgroundExec,
	toAccountName string,
) (opAccountId, toAccountId uint32, err error) {

	if opAccountId, err = defines.GetAccountId(reqCtx); err != nil {
		return
	}

	if len(toAccountName) == 0 {
		return opAccountId, opAccountId, nil
	}

	if toAccountId, err = getAccountId(reqCtx, bh, toAccountName); err != nil {
		return
	}

	return opAccountId, toAccountId, nil
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

		newDbName string

		toAccountId uint32
		opAccountId uint32
	)

	bh = ses.GetBackgroundExec(reqCtx)
	if err = bh.Exec(reqCtx, "begin"); err != nil {
		return err
	}

	defer func() {
		err = finishTxn(reqCtx, bh, err)
	}()

	if opAccountId, toAccountId, err = getOpAndToAccountId(
		reqCtx, bh, stmt.ToAccountName.String(),
	); err != nil {
		return err
	}

	newDbName = stmt.CreateTable.Table.SchemaName.String()
	if newDbName == "" {
		newDbName = ses.GetTxnCompileCtx().DefaultDatabase()
		bh.(*backExec).backSes.SetDatabaseName(newDbName)
	}

	// the the opAccount is not sys, opAccount must equals to the toAccount
	if newDbName == moCatalog ||
		(opAccountId != sysAccountID && opAccountId != toAccountId) {
		return moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
	}

	ctx = defines.AttachAccountId(reqCtx, toAccountId)

	sql := strings.Split(strings.ToLower(execCtx.input.sql), "to")[0]

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

	if opAccountId, toAccountId, err = getOpAndToAccountId(
		reqCtx, bh, stmt.ToAccountName.String(),
	); err != nil {
		return err
	}

	if opAccountId != sysAccountID && opAccountId != toAccountId {
		return moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
	}

	ctx1 = defines.AttachAccountId(reqCtx, toAccountId)
	if err = bh.Exec(ctx1,
		fmt.Sprintf("create database `%s`", stmt.DstDatabase),
	); err != nil {
		return err
	}

	if stmt.AtTsExpr == nil {
		if srcTblInfos, err = showFullTables(
			reqCtx, ses.GetService(), bh, "", stmt.SrcDatabase.String(), "",
		); err != nil {
			return err
		}
	} else {
		snapCondition = snapConditionRegex.FindString(execCtx.input.sql)

		builder := plan.NewQueryBuilder(plan2.Query_INSERT, ses.txnCompileCtx, false, true)
		if snapshot, err = builder.ResolveTsHint(stmt.AtTsExpr); err != nil {
			return err
		}

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

	ctx3 = defines.AttachAccountId(reqCtx, toAccountId)
	for _, srcTbl := range srcTblInfos {
		sql := fmt.Sprintf(
			"create table `%s`.`%s` clone `%s`.`%s`",
			stmt.DstDatabase, srcTbl.tblName, srcTbl.dbName, srcTbl.tblName)

		if snapCondition != "" {
			sql = sql + " " + snapCondition
		}

		if err = bh.ExecRestore(ctx3, sql, opAccountId, toAccountId); err != nil {
			return err
		}
	}

	return nil
}
