// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildCloneTable(
	stmt *tree.CloneTable,
	ctx CompilerContext,
) (*Plan, error) {

	var (
		err       error
		srcTblDef *TableDef
		srcObj    *ObjectRef

		createTablePlan *Plan

		builder *QueryBuilder
		bindCtx *BindContext
	)

	builder = NewQueryBuilder(plan.Query_INSERT, ctx, false, true)
	bindCtx = NewBindContext(builder, nil)

	old := builder.compCtx.GetSnapshot()
	defer func() {
		builder.compCtx.SetSnapshot(old)
	}()

	if stmt.IsRestore {
		snapshot := &Snapshot{
			Tenant: &SnapshotTenant{
				TenantID: stmt.FromAccount,
			},
		}

		builder.compCtx.SetSnapshot(snapshot)
		builder.isRestore = stmt.IsRestore
		builder.isRestoreByTs = stmt.IsRestoreByTS
	}

	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	if stmt.SrcTable.AtTsExpr != nil {
		bindCtx.snapshot, err = builder.ResolveTsHint(stmt.SrcTable.AtTsExpr)
		if err != nil {
			return nil, err
		}

		// the TS and the account id of the data source
		builder.compCtx.SetSnapshot(bindCtx.snapshot)
	}

	srcTableName := stmt.SrcTable.Name()
	srcDatabaseName := stmt.SrcTable.Schema()
	if srcObj, srcTblDef, err = builder.compCtx.Resolve(
		srcDatabaseName.String(), srcTableName.String(), bindCtx.snapshot,
	); err != nil {
		return nil, err
	} else if srcTblDef == nil {
		return nil, moerr.NewParseErrorf(builder.GetContext(),
			"table %v-%v does not exist", srcDatabaseName.String(), srcTableName.String())
	}

	var (
		dstTableName    string
		dstDatabaseName string
	)

	dstTableName = stmt.CreateTable.Table.ObjectName.String()
	dstDatabaseName = stmt.CreateTable.Table.SchemaName.String()

	if dstDatabaseName == "" {
		dstDatabaseName = ctx.DefaultDatabase()
	}

	var (
		opAccount  uint32
		dstAccount uint32
		srcAccount uint32
	)

	if opAccount, err = ctx.GetAccountId(); err != nil {
		return nil, err
	}

	dstAccount = opAccount
	srcAccount = opAccount

	if stmt.IsRestoreByTS {
		dstAccount = stmt.ToAccountId
		srcAccount = stmt.FromAccount
	}

	if bindCtx.snapshot != nil && bindCtx.snapshot.Tenant != nil {
		srcAccount = bindCtx.snapshot.Tenant.TenantID
	}

	stmt.StmtType = tree.DecideCloneStmtType(
		ctx.GetContext(), stmt,
		srcTblDef.DbName, dstDatabaseName,
		dstAccount, srcAccount,
	)

	if err = checkPrivilege(
		ctx.GetContext(), opAccount, srcAccount, srcTblDef, dstDatabaseName, bindCtx.snapshot, stmt.StmtType,
	); err != nil {
		return nil, err
	}

	if createTablePlan, err = buildCreateTable(ctx, &stmt.CreateTable, stmt); err != nil {
		return nil, err
	}

	ctx.GetProcess().GetTxnOperator().GetWorkspace().SetCloneTxn(bindCtx.snapshot.TS.PhysicalTime)

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE_WITH_CLONE,
				Definition: &plan.DataDefinition_CloneTable{
					CloneTable: &plan.CloneTable{
						SrcTableDef:     srcTblDef,
						SrcObjDef:       srcObj,
						DstDatabaseName: dstDatabaseName,
						DstTableName:    dstTableName,
						CreateTable:     createTablePlan,
						ScanSnapshot:    bindCtx.snapshot,
					},
				},
			},
		},
	}, nil
}

func checkPrivilege(
	ctx context.Context,
	opAccount uint32,
	srcAccount uint32,
	srcTblDef *TableDef,
	dstDatabaseName string,
	scanSnapshot *Snapshot,
	cloneType tree.CloneStmtType,
) (err error) {

	var (
		misMsg string

		snapshotMisMatch = false
	)

	if scanSnapshot != nil && scanSnapshot.ExtraInfo != nil {
		switch scanSnapshot.ExtraInfo.Level {
		case tree.SNAPSHOTLEVELCLUSTER.String():
		case tree.SNAPSHOTLEVELACCOUNT.String():
			if scanSnapshot.ExtraInfo.ObjId != uint64(srcAccount) {
				misMsg = fmt.Sprintf(
					"account-level snapshot(%s) does not belong to the account(%d)",
					scanSnapshot.ExtraInfo.Name, srcAccount,
				)
				snapshotMisMatch = true
			}
		case tree.SNAPSHOTLEVELDATABASE.String():
			if cloneType == tree.CloneCluster || cloneType == tree.CloneAccount {
				snapshotMisMatch = true
				misMsg = "cannot use a database-level snapshot to clone cluster/account"
			} else if scanSnapshot.ExtraInfo.ObjId != uint64(srcTblDef.DbId) {
				snapshotMisMatch = true
				misMsg = fmt.Sprintf(
					"database-level snapshot(%s) does not belong to the database(%s)",
					scanSnapshot.ExtraInfo.Name, srcTblDef.DbName,
				)
			}
		case tree.SNAPSHOTLEVELTABLE.String():
			if cloneType == tree.CloneCluster || cloneType == tree.CloneAccount ||
				cloneType == tree.WithinAccCloneDB || cloneType == tree.BetweenAccCloneDB {
				snapshotMisMatch = true
				misMsg = "cannot use a table-level snapshot to clone cluster/account/database"
			} else if scanSnapshot.ExtraInfo.ObjId != uint64(srcTblDef.TblId) {
				misMsg = fmt.Sprintf(
					"table-level snapshot(%s) does not belong to the table(%s-%s)",
					scanSnapshot.ExtraInfo.Name, srcTblDef.DbName, srcTblDef.Name,
				)
				snapshotMisMatch = true
			}
		}
	}

	if snapshotMisMatch {
		logutil.Error(
			"SNAPSHOT-MISMATCH",
			zap.String("snapshot",
				fmt.Sprintf("%s-%s-%d",
					scanSnapshot.ExtraInfo.Name,
					scanSnapshot.ExtraInfo.Level,
					scanSnapshot.ExtraInfo.ObjId)),
			zap.String("table",
				fmt.Sprintf("%s(%d)-%s(%d)",
					srcTblDef.DbName,
					srcTblDef.DbId,
					srcTblDef.Name,
					srcTblDef.TblId)),
		)

		return moerr.NewInternalErrorNoCtx(misMsg)
	}

	// 1. only sys can clone from system databases
	// 2. sys and non-sys both cannot clone to system database
	// 3. if this is a restore clone stmt, skip this check

	if val := ctx.Value(tree.CloneLevelCtxKey{}); val != nil {
		switch val.(tree.CloneLevelType) {
		case tree.RestoreCloneLevelAccount,
			tree.RestoreCloneLevelCluster,
			tree.RestoreCloneLevelDatabase,
			tree.RestoreCloneLevelTable:
			// skip this check
			return nil
		default:
		}
	}

	var (
		typ int
	)

	if slices.Index(
		catalog.SystemDatabases, strings.ToLower(srcTblDef.DbName),
	) != -1 {
		// clone from system databases
		typ = 1
	} else if slices.Index(
		catalog.SystemDatabases, strings.ToLower(dstDatabaseName),
	) != -1 {
		// clone to a system database
		typ = 2
	}

	if typ == 2 {
		return moerr.NewInternalErrorNoCtx("cannot clone data into system database")
	} else if typ == 1 {
		if opAccount != catalog.System_Account {
			return moerr.NewInternalErrorNoCtx("non-sys account cannot clone data from system database")
		}
	}

	return nil
}
