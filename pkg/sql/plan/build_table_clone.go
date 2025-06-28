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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildCloneTable(
	stmt *tree.CloneTable,
	ctx CompilerContext,
) (*Plan, error) {

	var (
		err error

		id int32

		srcTblDef *TableDef

		srcObj *ObjectRef

		query *Query

		createTablePlan *Plan

		builder *QueryBuilder
		bindCtx *BindContext
	)

	builder = NewQueryBuilder(plan.Query_INSERT, ctx, false, true)
	bindCtx = NewBindContext(builder, nil)

	if stmt.IsRestore {
		snapshot := &Snapshot{
			Tenant: &SnapshotTenant{
				TenantID: stmt.FromAccount,
			},
		}

		old := builder.compCtx.GetSnapshot()

		builder.compCtx.SetSnapshot(snapshot)
		builder.isRestore = stmt.IsRestore
		builder.isRestoreByTs = stmt.IsRestoreByTS

		defer func() {
			builder.compCtx.SetSnapshot(old)
		}()
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

	if createTablePlan, err = buildCreateTable(&stmt.CreateTable, ctx); err != nil {
		return nil, err
	}

	if query, err = builder.createQuery(); err != nil {
		return nil, err
	}

	createTablePlan.Plan.(*plan.Plan_Ddl).Ddl.Query = query
	createTablePlan.Plan.(*plan.Plan_Ddl).Ddl.DdlType = plan.DataDefinition_CREATE_TABLE_WITH_CLONE

	return createTablePlan, nil
}
