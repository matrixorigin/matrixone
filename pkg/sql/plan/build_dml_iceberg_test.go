// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIcebergDeleteBuildsDMLWriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "delete from gold_orders where id = 1", 1)
	if err != nil {
		t.Fatalf("parse delete: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg delete plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_DELETE {
		t.Fatalf("expected DELETE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	var scan *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLDeletePlanExtraOptions {
			dmlSink = node
		}
		if node.GetExternScan() != nil && node.GetExternScan().GetType() == int32(planpb.ExternType_ICEBERG_TB) {
			scan = node
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg DELETE DML sink node")
	}
	if dmlSink.NodeType != planpb.Node_INSERT || dmlSink.GetInsertCtx() == nil {
		t.Fatalf("expected INSERT-shaped Iceberg DML sink, got %+v", dmlSink)
	}
	if !tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) {
		t.Fatalf("DML sink table def is missing metadata columns: %+v", dmlSink.GetTableDef())
	}
	if scan == nil {
		t.Fatalf("expected Iceberg external scan")
	}
	if !tableDefHasCol(scan.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(scan.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) {
		t.Fatalf("scan table def is missing DML metadata columns: %+v", scan.GetTableDef())
	}
	if _, ok := scan.GetExternScan().GetTbColToDataCol()[icebergapi.DMLDataFilePathColumnName]; !ok {
		t.Fatalf("scan TbColToDataCol missing data-file metadata column: %+v", scan.GetExternScan().GetTbColToDataCol())
	}
	if _, ok := scan.GetExternScan().GetTbColToDataCol()[icebergapi.DMLRowOrdinalColumnName]; !ok {
		t.Fatalf("scan TbColToDataCol missing row-ordinal metadata column: %+v", scan.GetExternScan().GetTbColToDataCol())
	}
}

func TestIcebergUpdateBuildsDMLWriteIntent(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, nil)
	ctx.tables["gold_orders"].Cols = append(ctx.tables["gold_orders"].Cols,
		&planpb.ColDef{
			Name:    "balance",
			Typ:     planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"},
			Default: &planpb.Default{NullAbility: true},
		},
		&planpb.ColDef{
			Name:    "region",
			Typ:     planpb.Type{Id: int32(types.T_varchar), Width: 32, Table: "gold_orders"},
			Default: &planpb.Default{NullAbility: true},
		},
	)

	stmt, err := mysql.ParseOne(context.Background(), "update gold_orders set balance = balance + 1 where id = 1", 1)
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}
	p, err := BuildPlan(ctx, stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg update plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_UPDATE {
		t.Fatalf("expected UPDATE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	var scan *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLUpdatePlanExtraOptions {
			dmlSink = node
		}
		if node.GetExternScan() != nil && node.GetExternScan().GetType() == int32(planpb.ExternType_ICEBERG_TB) {
			scan = node
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg UPDATE DML sink node")
	}
	if dmlSink.NodeType != planpb.Node_INSERT || dmlSink.GetInsertCtx() == nil {
		t.Fatalf("expected INSERT-shaped Iceberg UPDATE DML sink, got %+v", dmlSink)
	}
	if !tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) {
		t.Fatalf("UPDATE DML sink table def is missing metadata columns: %+v", dmlSink.GetTableDef())
	}
	updateProject := icebergDMLSinkChildProject(t, query, dmlSink)
	if len(updateProject.GetProjectList()) != len(dmlSink.GetTableDef().GetCols()) {
		t.Fatalf("UPDATE child project/table shape mismatch: projects=%d cols=%d", len(updateProject.GetProjectList()), len(dmlSink.GetTableDef().GetCols()))
	}
	if scan == nil {
		t.Fatalf("expected Iceberg external scan")
	}
	if !tableDefHasCol(scan.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(scan.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) {
		t.Fatalf("scan table def is missing DML metadata columns: %+v", scan.GetTableDef())
	}
	for _, name := range []string{"id", "balance", "region"} {
		if !tableDefHasCol(scan.GetTableDef(), name) {
			t.Fatalf("UPDATE scan table def must retain target column %s: %+v", name, scan.GetTableDef())
		}
		if _, ok := scan.GetExternScan().GetTbColToDataCol()[name]; !ok {
			t.Fatalf("UPDATE scan TbColToDataCol must retain target column %s: %+v", name, scan.GetExternScan().GetTbColToDataCol())
		}
	}
	if len(scan.GetProjectList()) < 5 {
		t.Fatalf("UPDATE scan project list must include target columns and metadata, got %d projects", len(scan.GetProjectList()))
	}
}

func TestIcebergMergeMatchedUpdateBuildsDMLWriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched then update set id = s.id", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg merge plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	var scan *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
		}
		if node.GetExternScan() != nil && node.GetExternScan().GetType() == int32(planpb.ExternType_ICEBERG_TB) {
			scan = node
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink node")
	}
	if dmlSink.NodeType != planpb.Node_INSERT || dmlSink.GetInsertCtx() == nil {
		t.Fatalf("expected INSERT-shaped Iceberg MERGE sink, got %+v", dmlSink)
	}
	if !tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLMergeActionColumnName) {
		t.Fatalf("MERGE DML sink table def is missing metadata/action columns: %+v", dmlSink.GetTableDef())
	}
	if scan == nil {
		t.Fatalf("expected Iceberg external scan")
	}
	if !tableDefHasCol(scan.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(scan.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) {
		t.Fatalf("scan table def is missing DML metadata columns: %+v", scan.GetTableDef())
	}
	if tableDefHasCol(scan.GetTableDef(), icebergapi.DMLMergeActionColumnName) {
		t.Fatalf("merge action column should be sink-local, scan table def: %+v", scan.GetTableDef())
	}
}

func TestIcebergMergeMatchedDeleteBuildsDMLWriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched then delete", 1)
	if err != nil {
		t.Fatalf("parse merge delete: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg merge delete plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	found := false
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			found = true
			if !tableDefHasCol(node.GetTableDef(), icebergapi.DMLMergeActionColumnName) {
				t.Fatalf("MERGE DELETE sink table def is missing action column: %+v", node.GetTableDef())
			}
		}
	}
	if !found {
		t.Fatalf("expected Iceberg MERGE DELETE DML sink")
	}
}

func TestIcebergMergeMatchedUpdateAndNotMatchedInsertBuildsDMLWriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched then update set id = s.id when not matched then insert (id) values (s.id)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg merge with insert plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	var sourceDrivenJoin *planpb.Node
	joinSummary := ""
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
		}
		if node.GetNodeType() == planpb.Node_JOIN {
			joinSummary += node.GetJoinType().String() + " "
		}
		if node.GetNodeType() == planpb.Node_JOIN && (node.GetJoinType() == planpb.Node_LEFT || node.GetJoinType() == planpb.Node_RIGHT) {
			sourceDrivenJoin = node
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink node")
	}
	if sourceDrivenJoin == nil {
		t.Fatalf("expected source-driven outer join for matched/not-matched MERGE, joins=%s", joinSummary)
	}
	if !tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLRowOrdinalColumnName) ||
		!tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLMergeActionColumnName) {
		t.Fatalf("MERGE DML sink table def is missing metadata/action columns: %+v", dmlSink.GetTableDef())
	}
	if len(dmlSink.GetProjectList()) != len(dmlSink.GetTableDef().GetCols()) {
		t.Fatalf("MERGE sink project/table shape mismatch: projects=%d cols=%d", len(dmlSink.GetProjectList()), len(dmlSink.GetTableDef().GetCols()))
	}
	project := icebergDMLSinkChildProject(t, query, dmlSink)
	if len(project.GetProjectList()) != len(dmlSink.GetProjectList()) {
		t.Fatalf("MERGE sink child project shape mismatch: child projects=%d sink projects=%d", len(project.GetProjectList()), len(dmlSink.GetProjectList()))
	}
}

func TestIcebergMergeEmbeddedFourColumnProjectionShape(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, nil)
	ctx.tables["gold_orders"].Cols = []*planpb.ColDef{
		{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "hidden_key", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "bucket", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "amount", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"}, Default: &planpb.Default{NullAbility: true}},
	}
	ctx.tables["dim_orders"].Cols = []*planpb.ColDef{
		{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "dim_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "hidden_key", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "dim_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "bucket", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "dim_orders"}, Default: &planpb.Default{NullAbility: true}},
		{Name: "amount", Typ: planpb.Type{Id: int32(types.T_int64), Width: 64, Table: "dim_orders"}, Default: &planpb.Default{NullAbility: true}},
	}
	ctx.tables["gold_orders"].Createsql = sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{
		Namespace:  "sales",
		TableName:  "orders",
		DefaultRef: model.DefaultRefMain,
		ReadMode:   model.ReadModeMergeOnRead,
		WriteMode:  model.WriteModeMergeOnRead,
	}, "ksa_gold")

	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched then update set hidden_key = s.hidden_key, bucket = s.bucket, amount = s.amount when not matched then insert (id, hidden_key, bucket, amount) values (s.id, s.hidden_key, s.bucket, s.amount)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(ctx, stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg merge plan: %v", err)
	}
	query := p.GetQuery()
	var dmlSink *planpb.Node
	for _, node := range query.GetNodes() {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
			break
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink")
	}
	assertLocalProjectRefsWithinChildShape(t, query, dmlSink.GetChildren()[0])
}

func TestIcebergMergeWithIcebergSourceAnnotatesOnlyTargetScan(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, nil)
	ctx.tables["dim_orders"].TableType = catalog.SystemExternalRel
	ctx.tables["dim_orders"].Createsql = sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{
		Namespace:  "sales",
		TableName:  "dim_orders",
		DefaultRef: model.DefaultRefMain,
		ReadMode:   model.ReadModeAppendOnly,
		WriteMode:  model.WriteModeReadOnly,
	}, "ksa_gold")

	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched then update set id = s.id when not matched then insert (id) values (s.id)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(ctx, stmt, false)
	if err != nil {
		t.Fatalf("build Iceberg merge with Iceberg source plan: %v", err)
	}
	query := p.GetQuery()
	var targetAnnotated, sourceAnnotated bool
	for _, node := range query.GetNodes() {
		if node.GetExternScan() == nil || node.GetExternScan().GetType() != int32(planpb.ExternType_ICEBERG_TB) {
			continue
		}
		if node.GetObjRef().GetObjName() == "gold_orders" {
			targetAnnotated = tableDefHasCol(node.GetTableDef(), icebergapi.DMLDataFilePathColumnName) &&
				tableDefHasCol(node.GetTableDef(), icebergapi.DMLRowOrdinalColumnName)
		}
		if node.GetObjRef().GetObjName() == "dim_orders" {
			sourceAnnotated = tableDefHasCol(node.GetTableDef(), icebergapi.DMLDataFilePathColumnName) ||
				tableDefHasCol(node.GetTableDef(), icebergapi.DMLRowOrdinalColumnName)
		}
	}
	if !targetAnnotated {
		t.Fatalf("expected MERGE target Iceberg scan to carry DML metadata columns")
	}
	if sourceAnnotated {
		t.Fatalf("source Iceberg scan must not carry target DML metadata columns")
	}
}

func TestIcebergMergeInsertColumnListFillsMissingNullableColumns(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, nil)
	target := icebergDeleteTarget{
		tableName: "accounts",
		tableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{Name: "account_id", Typ: planpb.Type{NotNullable: true}},
				{Name: "balance", Typ: planpb.Type{NotNullable: true}},
				{Name: "region"},
			},
		},
	}
	values, err := icebergMergeInsertExprMap(ctx, target, target.tableDef.Cols, &tree.MergeClause{
		Action:        tree.MergeActionInsert,
		InsertColumns: tree.IdentifierList{"account_id", "balance"},
		InsertValues: tree.Exprs{
			tree.NewNumVal("1", "1", false, tree.P_int64),
			tree.NewNumVal("220", "220", false, tree.P_int64),
		},
	})
	if err != nil {
		t.Fatalf("merge insert map: %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("expected all target columns after nullable fill, got %+v", values)
	}
	if got := tree.String(values["region"], dialect.MYSQL); got != "null" {
		t.Fatalf("expected missing nullable region to be NULL, got %s", got)
	}
}

func TestIcebergMergeInsertColumnListRejectsMissingRequiredColumns(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, nil)
	target := icebergDeleteTarget{
		tableName: "accounts",
		tableDef: &planpb.TableDef{
			Cols: []*planpb.ColDef{
				{Name: "account_id", Typ: planpb.Type{NotNullable: true}},
				{Name: "balance", Typ: planpb.Type{NotNullable: true}},
			},
		},
	}
	_, err := icebergMergeInsertExprMap(ctx, target, target.tableDef.Cols, &tree.MergeClause{
		Action:        tree.MergeActionInsert,
		InsertColumns: tree.IdentifierList{"account_id"},
		InsertValues: tree.Exprs{
			tree.NewNumVal("1", "1", false, tree.P_int64),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "required target column balance") {
		t.Fatalf("expected missing required column error, got %v", err)
	}
}

func TestIcebergMergeMatchedAndNotMatchedConditionsBuildNoopGuard(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched and s.id > 10 then update set id = s.id when not matched and s.id > 20 then insert (id) values (s.id)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build conditional Iceberg merge plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
			break
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink node")
	}
	if !tableDefHasCol(dmlSink.GetTableDef(), icebergapi.DMLMergeActionColumnName) {
		t.Fatalf("MERGE DML sink table def is missing action column: %+v", dmlSink.GetTableDef())
	}
	if len(dmlSink.GetProjectList()) != len(dmlSink.GetTableDef().GetCols()) {
		t.Fatalf("MERGE sink project/table shape mismatch: projects=%d cols=%d", len(dmlSink.GetProjectList()), len(dmlSink.GetTableDef().GetCols()))
	}
	project := icebergDMLSinkChildProject(t, query, dmlSink)
	if len(project.GetProjectList()) != len(dmlSink.GetProjectList()) {
		t.Fatalf("MERGE sink child project shape mismatch: child projects=%d sink projects=%d", len(project.GetProjectList()), len(dmlSink.GetProjectList()))
	}
	actionExpr := dmlSink.GetProjectList()[len(dmlSink.GetProjectList())-1]
	if !icebergDMLExprContainsStringLiteral(actionExpr, icebergapi.DMLMergeActionNoop) {
		t.Fatalf("conditional MERGE action expression should include noop guard: %+v", actionExpr)
	}
}

func TestIcebergMergeNotMatchedOnlyBuildsInsertWithNoopGuard(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when not matched then insert (id) values (s.id)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build not-matched Iceberg merge plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
			break
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink node")
	}
	actionExpr := dmlSink.GetProjectList()[len(dmlSink.GetProjectList())-1]
	if !icebergDMLExprContainsStringLiteral(actionExpr, icebergapi.DMLMergeActionNoop) {
		t.Fatalf("not-matched MERGE action expression should include noop guard: %+v", actionExpr)
	}
}

func TestIcebergMergeMultipleActionClausesBuildFirstMatchChain(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "merge into gold_orders as t using dim_orders as s on t.id = s.id when matched and s.id < 0 then delete when matched and s.id > 10 then update set id = s.id when not matched and s.id > 20 then insert (id) values (s.id) when not matched and s.id < -10 then insert (id) values (s.id)", 1)
	if err != nil {
		t.Fatalf("parse merge: %v", err)
	}
	p, err := BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err != nil {
		t.Fatalf("build multi-clause Iceberg merge plan: %v", err)
	}
	query := p.GetQuery()
	if query == nil || query.StmtType != planpb.Query_MERGE {
		t.Fatalf("expected MERGE query, got %+v", query)
	}
	var dmlSink *planpb.Node
	for _, node := range query.Nodes {
		if node.GetExtraOptions() == icebergapi.DMLMergePlanExtraOptions {
			dmlSink = node
			break
		}
	}
	if dmlSink == nil {
		t.Fatalf("expected Iceberg MERGE DML sink node")
	}
	if len(dmlSink.GetProjectList()) != len(dmlSink.GetTableDef().GetCols()) {
		t.Fatalf("MERGE sink project/table shape mismatch: projects=%d cols=%d", len(dmlSink.GetProjectList()), len(dmlSink.GetTableDef().GetCols()))
	}
	actionExpr := dmlSink.GetProjectList()[len(dmlSink.GetProjectList())-1]
	for _, want := range []string{
		icebergapi.DMLMergeActionDelete,
		icebergapi.DMLMergeActionUpdate,
		icebergapi.DMLMergeActionInsert,
		icebergapi.DMLMergeActionNoop,
	} {
		if !icebergDMLExprContainsStringLiteral(actionExpr, want) {
			t.Fatalf("multi-clause MERGE action expression should include %q: %+v", want, actionExpr)
		}
	}
}

func TestIcebergReplaceStillFailsBeforeNativeFallback(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "replace into gold_orders values (1)", 1)
	if err != nil {
		t.Fatalf("parse replace: %v", err)
	}
	_, err = BuildPlan(newIcebergTestCompilerContext(t, nil), stmt, false)
	if err == nil || !strings.Contains(err.Error(), "Iceberg row-level DML is not implemented") {
		t.Fatalf("expected Iceberg row-level DML error, got %v", err)
	}
	if strings.Contains(err.Error(), "cannot insert/update/delete from external table") {
		t.Fatalf("Iceberg DML should not fall back to generic external table error: %v", err)
	}
}

func tableDefHasCol(tableDef *planpb.TableDef, name string) bool {
	if tableDef == nil {
		return false
	}
	for _, col := range tableDef.Cols {
		if col != nil && strings.EqualFold(col.Name, name) {
			return true
		}
	}
	return false
}

func icebergDMLSinkChildProject(t *testing.T, query *planpb.Query, sink *planpb.Node) *planpb.Node {
	t.Helper()
	if query == nil || sink == nil || len(sink.GetChildren()) != 1 {
		t.Fatalf("expected MERGE DML sink to have one child, sink=%+v", sink)
	}
	childID := sink.GetChildren()[0]
	if childID < 0 || int(childID) >= len(query.GetNodes()) {
		t.Fatalf("MERGE DML sink child id is invalid: %d", childID)
	}
	project := query.GetNodes()[childID]
	if project.GetNodeType() != planpb.Node_PROJECT {
		t.Fatalf("MERGE DML sink child must materialize rewritten projection, got %s", project.GetNodeType())
	}
	return project
}

func assertLocalProjectRefsWithinChildShape(t *testing.T, query *planpb.Query, nodeID int32) {
	t.Helper()
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.GetNodes()) {
		t.Fatalf("invalid node id %d", nodeID)
	}
	node := query.GetNodes()[nodeID]
	for _, childID := range node.GetChildren() {
		assertLocalProjectRefsWithinChildShape(t, query, childID)
	}
	if node.GetNodeType() != planpb.Node_PROJECT || len(node.GetChildren()) != 1 {
		return
	}
	childCols := planNodeOutputColumnCount(query, node.GetChildren()[0])
	for idx, expr := range node.GetProjectList() {
		for _, pos := range colRefPositions(expr) {
			if int(pos) >= childCols {
				t.Fatalf("project node %d expr %d references child column %d, but child %d outputs %d columns: expr=%+v",
					nodeID, idx, pos, node.GetChildren()[0], childCols, expr)
			}
		}
	}
}

func planNodeOutputColumnCount(query *planpb.Query, nodeID int32) int {
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.GetNodes()) {
		return 0
	}
	node := query.GetNodes()[nodeID]
	if len(node.GetProjectList()) > 0 {
		return len(node.GetProjectList())
	}
	if node.GetTableDef() != nil {
		return len(node.GetTableDef().GetCols())
	}
	return 0
}

func colRefPositions(expr *planpb.Expr) []int32 {
	if expr == nil {
		return nil
	}
	if col := expr.GetCol(); col != nil {
		return []int32{col.GetColPos()}
	}
	var out []int32
	if f := expr.GetF(); f != nil {
		for _, arg := range f.GetArgs() {
			out = append(out, colRefPositions(arg)...)
		}
	}
	return out
}

func icebergDMLExprContainsStringLiteral(expr *planpb.Expr, value string) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil && lit.GetSval() == value {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if icebergDMLExprContainsStringLiteral(arg, value) {
				return true
			}
		}
	}
	return false
}
