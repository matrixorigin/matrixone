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
	"go.uber.org/zap"
	"slices"
	"strings"

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
		err error
		id  int32

		srcTblDef *TableDef
		srcObj    *ObjectRef
		query     *Query

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

	dstTblDef := DeepCopyTableDef(srcTblDef, true)
	dstTblDef.Name = stmt.CreateTable.Table.ObjectName.String()
	dstTblDef.DbName = stmt.CreateTable.Table.SchemaName.String()

	if dstTblDef.DbName == "" {
		dstTblDef.DbName = ctx.DefaultDatabase()
	}

	id = builder.appendNode(&plan.Node{
		ObjRef:       srcObj,
		NodeType:     plan.Node_TABLE_CLONE,
		TableDef:     srcTblDef,
		ScanSnapshot: bindCtx.snapshot,
		InsertCtx: &plan.InsertCtx{
			TableDef: dstTblDef,
		},
		BindingTags: []int32{builder.genNewTag()},
	}, bindCtx)

	builder.qry.Steps = append(builder.qry.Steps, id)
	builder.qry.Nodes[0].Stats.ForceOneCN = true
	builder.skipStats = true

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
		srcTblDef.DbName, dstTblDef.DbName,
		dstAccount, srcAccount,
	)

	if err = checkPrivilege(
		ctx.GetContext(), stmt, opAccount, srcAccount, dstAccount, srcTblDef, dstTblDef, bindCtx.snapshot,
	); err != nil {
		return nil, err
	}

	if createTablePlan, err = buildCreateTable(ctx, &stmt.CreateTable, stmt); err != nil {
		return nil, err
	}

	if query, err = builder.createQuery(); err != nil {
		return nil, err
	}

	createTablePlan.Plan.(*plan.Plan_Ddl).Ddl.Query = query
	createTablePlan.Plan.(*plan.Plan_Ddl).Ddl.DdlType = plan.DataDefinition_CREATE_TABLE_WITH_CLONE

	return createTablePlan, nil
}

func checkPrivilege(
	ctx context.Context,
	stmt *tree.CloneTable,
	opAccount uint32,
	srcAccount uint32,
	dstAccount uint32,
	srcTblDef *TableDef,
	dstTblDef *TableDef,
	scanSnapshot *Snapshot,
) (err error) {

	var (
		snapshotMisMatch = false
	)

	if scanSnapshot != nil && scanSnapshot.ExtraInfo != nil {
		switch scanSnapshot.ExtraInfo.Level {
		case tree.SNAPSHOTLEVELCLUSTER.String():
		case tree.SNAPSHOTLEVELACCOUNT.String():
			if scanSnapshot.ExtraInfo.ObjId != uint64(srcAccount) {
				snapshotMisMatch = true
			}
		case tree.SNAPSHOTLEVELDATABASE.String():
			if scanSnapshot.ExtraInfo.ObjId != uint64(srcTblDef.DbId) {
				snapshotMisMatch = true
			}
		case tree.SNAPSHOTLEVELTABLE.String():
			if scanSnapshot.ExtraInfo.ObjId != uint64(srcTblDef.TblId) {
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

		return moerr.NewInternalErrorNoCtxf(
			"the snapshot %s doesnot contain the table %s", scanSnapshot.ExtraInfo.Name, srcTblDef.Name,
		)
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
		catalog.SystemDatabases, strings.ToLower(dstTblDef.DbName),
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
