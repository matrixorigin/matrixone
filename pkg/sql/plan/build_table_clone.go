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

	if IsSnapshotValid(ctx.GetSnapshot()) {
		bindCtx.snapshot = ctx.GetSnapshot()
	}

	if stmt.SrcTable.AtTsExpr != nil {
		bindCtx.snapshot, err = builder.resolveTsHint(stmt.SrcTable.AtTsExpr)
		if err != nil {
			return nil, err
		}
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

	// check schema
	//ignoreCol := func(def *ColDef) bool {
	//	return def.Name == catalog.Row_ID || def.ClusterBy || catalog.IsFakePkName(def.Name)
	//}
	//
	//i, j := 0, 0
	//for i < len(srcTblDef.Cols) && j < len(dstTblDef.Cols) {
	//	if ignoreCol(srcTblDef.Cols[i]) {
	//		i++
	//		continue
	//	}
	//
	//	if ignoreCol(dstTblDef.Cols[j]) {
	//		j++
	//		continue
	//	}
	//
	//	t1 := ExprType2Type(&srcTblDef.Cols[i].Typ)
	//	t2 := ExprType2Type(&dstTblDef.Cols[j].Typ)
	//
	//	if !t1.Eq(t2) {
	//		return nil, moerr.NewInternalErrorNoCtxf(
	//			"%v.%v has a different column type than %v.%v at idx %v. expect %v, got %v",
	//			srcTblDef.DbName, srcTblDef.Name,
	//			dstTblDef.DbName, dstTblDef.Name, i, t1.String(), t2.String())
	//	}
	//
	//	i++
	//	j++
	//}
	//
	//for i < len(srcTblDef.Cols) {
	//	if ignoreCol(srcTblDef.Cols[i]) {
	//		i++
	//	}
	//}
	//
	//for j < len(dstTblDef.Cols) {
	//	if ignoreCol(dstTblDef.Cols[j]) {
	//		j++
	//	}
	//}
	//
	//if !(i >= len(srcTblDef.Cols) && (j >= len(dstTblDef.Cols))) {
	//	return nil, moerr.NewInternalErrorNoCtxf(
	//		"%v.%v has a different number of columns than %v.%v.",
	//		srcTblDef.DbName, srcTblDef.Name, dstTblDef.DbName, dstTblDef.Name)
	//}

	dstTblDef := DeepCopyTableDef(srcTblDef, true)
	dstTblDef.Name = stmt.CreateTable.Table.ObjectName.String()
	dstTblDef.DbName = stmt.CreateTable.Table.SchemaName.String()

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
