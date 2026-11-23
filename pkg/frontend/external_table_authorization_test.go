// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeExternalTableSelectPrivilegePlan(dbName, tableName string, externType plan.ExternType) *plan2.Plan {
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: plan.Query_SELECT,
		Nodes: []*plan2.Node{{
			NodeType:   plan.Node_EXTERNAL_SCAN,
			ObjRef:     &plan2.ObjectRef{SchemaName: dbName, ObjName: tableName},
			TableDef:   &plan2.TableDef{TableType: catalog.SystemExternalRel},
			ExternScan: &plan.ExternScan{Type: int32(externType)},
		}},
	}}}
}

func makeCSVExternalTablePrivilegePlan(dbName, tableName string) *plan2.Plan {
	return makeExternalTableSelectPrivilegePlan(dbName, tableName, plan.ExternType_EXTERNAL_TB)
}

func TestExtractPrivilegeTipsFromPlanCSVExternalTable(t *testing.T) {
	for _, tableName := range []string{"events", "__mo_index_events"} {
		t.Run(tableName, func(t *testing.T) {
			arr, err := extractPrivilegeTipsFromPlan(makeCSVExternalTablePrivilegePlan("csv_db", tableName))

			require.NoError(t, err)
			require.Len(t, arr, 1)
			assert.Equal(t, PrivilegeTypeSelect, arr[0].typ)
			assert.Equal(t, objectTypeTable, arr[0].objType)
			assert.Equal(t, "csv_db", arr[0].databaseName)
			assert.Equal(t, tableName, arr[0].tableName)
		})
	}
}

func TestExtractPrivilegeTipsFromPlanAdditionalExternalTableSelects(t *testing.T) {
	for _, externType := range []plan.ExternType{
		plan.ExternType_ICEBERG_TB,
		plan.ExternType_DATASTREAM_TB,
		plan.ExternType_FOREIGN_TB,
		plan.ExternType_KAFKA_TB,
	} {
		for _, tableName := range []string{"events", "__mo_index_events"} {
			t.Run(externType.String()+"/"+tableName, func(t *testing.T) {
				arr, err := extractPrivilegeTipsFromPlan(makeExternalTableSelectPrivilegePlan("external_db", tableName, externType))
				require.NoError(t, err)
				require.Len(t, arr, 1)
				assert.Equal(t, PrivilegeTypeSelect, arr[0].typ)
				assert.Equal(t, "external_db", arr[0].databaseName)
				assert.Equal(t, tableName, arr[0].tableName)
			})
		}
	}
}

func TestAuthenticateAdditionalExternalTableSelectPrivilege(t *testing.T) {
	grants := []struct {
		name    string
		allowed map[PrivilegeType]bool
		want    bool
	}{
		{name: "revoked"},
		{name: "granted", allowed: map[PrivilegeType]bool{PrivilegeTypeSelect: true}, want: true},
		{name: "insert only", allowed: map[PrivilegeType]bool{PrivilegeTypeInsert: true}},
	}
	for _, externType := range []plan.ExternType{
		plan.ExternType_DATASTREAM_TB,
		plan.ExternType_FOREIGN_TB,
		plan.ExternType_KAFKA_TB,
	} {
		for _, tableName := range []string{"events", "__mo_index_events"} {
			for _, grant := range grants {
				t.Run(externType.String()+"/"+tableName+"/"+grant.name, func(t *testing.T) {
					ok := authenticatePlanWithTablePrivileges(
						t, &tree.Select{}, makeExternalTableSelectPrivilegePlan("external_db", tableName, externType),
						tablePrivilegeFixture{dbName: "external_db", tableName: tableName, allowed: grant.allowed})
					assert.Equal(t, grant.want, ok)
				})
			}
		}
	}
}

func TestExtractPrivilegeTipsFromPlanSkipsNonSelectExternalScans(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stmtType   plan.Query_StatementType
		externType plan.ExternType
		tableType  string
	}{
		{name: "load", stmtType: plan.Query_INSERT, externType: plan.ExternType_LOAD, tableType: catalog.SystemExternalRel},
		{name: "result scan", stmtType: plan.Query_SELECT, externType: plan.ExternType_RESULT_SCAN, tableType: "query_result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
				StmtType: tc.stmtType,
				Nodes: []*plan2.Node{{
					NodeType:   plan.Node_EXTERNAL_SCAN,
					ObjRef:     &plan2.ObjectRef{SchemaName: "csv_db", ObjName: "events"},
					TableDef:   &plan2.TableDef{TableType: tc.tableType},
					ExternScan: &plan.ExternScan{Type: int32(tc.externType)},
				}},
			}}}
			arr, err := extractPrivilegeTipsFromPlan(p)
			require.NoError(t, err)
			assert.Empty(t, arr)
		})
	}
}

type tablePrivilegeFixture struct {
	dbName    string
	tableName string
	allowed   map[PrivilegeType]bool
}

func addExactTablePrivilegeResultsForRole(
	t *testing.T,
	sql2result map[string]ExecResult,
	roleID int64,
	fixture tablePrivilegeFixture,
) {
	t.Helper()
	for _, privType := range []PrivilegeType{
		PrivilegeTypeSelect,
		PrivilegeTypeInsert,
		PrivilegeTypeUpdate,
		PrivilegeTypeDelete,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
	} {
		entry := privilegeEntriesMap[privType]
		entry.objType = objectTypeTable
		entry.databaseName = fixture.dbName
		entry.tableName = fixture.tableName
		levels, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
		require.NoError(t, err)
		for _, level := range levels {
			sql, err := getSqlForPrivilege(context.TODO(), roleID, entry, level)
			require.NoError(t, err)
			rows := [][]interface{}{}
			exactRelation := level == privilegeLevelDatabaseTable || level == privilegeLevelTable
			if exactRelation && fixture.allowed[privType] {
				rows = [][]interface{}{{entry.privilegeId, true}}
			}
			sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
		}
	}
}

func authenticatePlanWithTablePrivileges(
	t *testing.T,
	stmt tree.Statement,
	p *plan2.Plan,
	tables ...tablePrivilegeFixture,
) bool {
	t.Helper()
	return authenticatePlanWithTablePrivilegesInDatabase(t, "", stmt, p, tables...)
}

func authenticatePlanWithTablePrivilegesInDatabase(
	t *testing.T,
	databaseName string,
	stmt tree.Statement,
	p *plan2.Plan,
	tables ...tablePrivilegeFixture,
) bool {
	t.Helper()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newSes(determinePrivilegeSetOfStatement(stmt), ctrl)
	t.Cleanup(ses.Close)
	ses.SetDatabaseName(databaseName)
	sql2result := makeSql2ExecResult2(0, [][]interface{}{{0, false}}, nil, nil, nil, nil, nil, nil, nil)
	for _, table := range tables {
		addExactTablePrivilegeResultsForRole(t, sql2result, 0, table)
	}
	sql2result[getSqlForInheritedRoleIdOfRoleId(0)] = newMrsForInheritedRoleIdOfRoleId(nil)

	bh := newBh(ctrl, sql2result)
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(
		ses.GetTxnHandler().GetTxnCtx(), ses, stmt, p)
	require.NoError(t, err)
	return ok
}

func TestAuthenticateCSVExternalTableSelectPrivilege(t *testing.T) {
	for _, tc := range []struct {
		name    string
		granted bool
	}{
		{name: "revoked"},
		{name: "granted", granted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok := authenticatePlanWithTablePrivileges(
				t, &tree.Select{}, makeCSVExternalTablePrivilegePlan("csv_db", "events"),
				tablePrivilegeFixture{dbName: "csv_db", tableName: "events", allowed: map[PrivilegeType]bool{
					PrivilegeTypeSelect: tc.granted,
				}})
			assert.Equal(t, tc.granted, ok)
		})
	}
}

func makeExternalLoadPrivilegePlan(dbName, tableName string) *plan2.Plan {
	objRef := &plan.ObjectRef{SchemaName: dbName, ObjName: tableName}
	tableDef := &plan.TableDef{DbName: dbName, Name: tableName, TableType: catalog.SystemExternalRel}
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: plan.Query_INSERT,
		Nodes: []*plan.Node{
			{
				NodeType:   plan.Node_EXTERNAL_SCAN,
				ObjRef:     objRef,
				TableDef:   tableDef,
				ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_LOAD)},
			},
			{
				NodeType:  plan.Node_INSERT,
				ObjRef:    objRef,
				TableDef:  tableDef,
				InsertCtx: &plan.InsertCtx{Ref: objRef, TableDef: tableDef, AddAffectedRows: true},
			},
		},
	}}}
}

func TestAuthenticateExternalLoadRequiresOnlyInsert(t *testing.T) {
	for _, tc := range []struct {
		name          string
		tableName     string
		selectGranted bool
		insertGranted bool
		want          bool
	}{
		{name: "insert only", tableName: "events", insertGranted: true, want: true},
		{name: "select only", tableName: "events", selectGranted: true},
		{name: "insert only with index prefixed target", tableName: "__mo_index_events", insertGranted: true, want: true},
		{name: "no privileges with index prefixed target", tableName: "__mo_index_events"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok := authenticatePlanWithTablePrivileges(
				t, &tree.Load{}, makeExternalLoadPrivilegePlan("csv_db", tc.tableName),
				tablePrivilegeFixture{dbName: "csv_db", tableName: tc.tableName, allowed: map[PrivilegeType]bool{
					PrivilegeTypeSelect: tc.selectGranted,
					PrivilegeTypeInsert: tc.insertGranted,
				}})
			assert.Equal(t, tc.want, ok)
		})
	}
}

func makeCSVInsertSelectPrivilegePlan(sourceTable, targetTable, targetType string) *plan2.Plan {
	sourceRef := &plan.ObjectRef{SchemaName: "csv_db", ObjName: sourceTable}
	targetRef := &plan.ObjectRef{SchemaName: "csv_db", ObjName: targetTable}
	sourceDef := &plan.TableDef{DbName: "csv_db", Name: sourceTable, TableType: catalog.SystemExternalRel}
	targetDef := &plan.TableDef{DbName: "csv_db", Name: targetTable, TableType: targetType}
	nodes := []*plan.Node{{
		NodeType:   plan.Node_EXTERNAL_SCAN,
		ObjRef:     sourceRef,
		TableDef:   sourceDef,
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_EXTERNAL_TB)},
	}}
	if targetType != catalog.SystemExternalRel {
		nodes = append(nodes, &plan.Node{
			NodeType:     plan.Node_PRE_INSERT,
			PreInsertCtx: &plan.PreInsertCtx{Ref: targetRef, TableDef: targetDef},
		})
	}
	nodes = append(nodes, &plan.Node{
		NodeType:  plan.Node_INSERT,
		ObjRef:    targetRef,
		TableDef:  targetDef,
		InsertCtx: &plan.InsertCtx{Ref: targetRef, TableDef: targetDef, AddAffectedRows: true},
	})
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: plan.Query_INSERT,
		Nodes:    nodes,
	}}}
}

func insertInto(dbName, tableName string) *tree.Insert {
	return tree.NewInsert(tree.NewTableName(
		tree.Identifier(tableName),
		tree.ObjectNamePrefix{SchemaName: tree.Identifier(dbName), ExplicitSchema: true},
		nil,
	), nil, nil, nil)
}

func TestAuthenticateCSVInsertSelectRequiresSourceSelectAndTargetInsert(t *testing.T) {
	const targetTable = "__mo_index_target"
	for _, tc := range []struct {
		name         string
		sourceSelect bool
		targetInsert bool
		want         bool
	}{
		{name: "source select only", sourceSelect: true},
		{name: "target insert only", targetInsert: true},
		{name: "source select and target insert", sourceSelect: true, targetInsert: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok := authenticatePlanWithTablePrivileges(
				t, insertInto("csv_db", targetTable), makeCSVInsertSelectPrivilegePlan("events", targetTable, catalog.SystemExternalRel),
				tablePrivilegeFixture{dbName: "csv_db", tableName: "events", allowed: map[PrivilegeType]bool{
					PrivilegeTypeSelect: tc.sourceSelect,
				}},
				tablePrivilegeFixture{dbName: "csv_db", tableName: targetTable, allowed: map[PrivilegeType]bool{
					PrivilegeTypeInsert: tc.targetInsert,
				}})
			assert.Equal(t, tc.want, ok)
		})
	}
}

func TestAuthenticateCSVInsertSelectRejectsOtherTableInsert(t *testing.T) {
	const targetTable = "__mo_index_target"
	ok := authenticatePlanWithTablePrivileges(
		t, insertInto("csv_db", targetTable), makeCSVInsertSelectPrivilegePlan("events", targetTable, catalog.SystemExternalRel),
		tablePrivilegeFixture{dbName: "csv_db", tableName: "events", allowed: map[PrivilegeType]bool{
			PrivilegeTypeSelect: true,
		}},
		tablePrivilegeFixture{dbName: "csv_db", tableName: targetTable, allowed: map[PrivilegeType]bool{}},
		tablePrivilegeFixture{dbName: "csv_db", tableName: "other_table", allowed: map[PrivilegeType]bool{
			PrivilegeTypeInsert: true,
		}})
	assert.False(t, ok)
}

func TestAuthenticateCSVInsertSelectRequiresInsertOnOrdinaryPrefixNamedTarget(t *testing.T) {
	const targetTable = "__mo_index_target"
	for _, tc := range []struct {
		name         string
		sourceSelect bool
		targetInsert bool
		want         bool
	}{
		{name: "source select only", sourceSelect: true},
		{name: "target insert only", targetInsert: true},
		{name: "source select and target insert", sourceSelect: true, targetInsert: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok := authenticatePlanWithTablePrivileges(
				t, insertInto("csv_db", targetTable), makeCSVInsertSelectPrivilegePlan("events", targetTable, ""),
				tablePrivilegeFixture{dbName: "csv_db", tableName: "events", allowed: map[PrivilegeType]bool{
					PrivilegeTypeSelect: tc.sourceSelect,
				}},
				tablePrivilegeFixture{dbName: "csv_db", tableName: targetTable, allowed: map[PrivilegeType]bool{
					PrivilegeTypeInsert: tc.targetInsert,
				}})
			assert.Equal(t, tc.want, ok)
		})
	}
}

func makeInsertTargetPrivilegePlan(dbName, tableName string, modern bool) *plan2.Plan {
	objRef := &plan.ObjectRef{SchemaName: dbName, ObjName: tableName}
	tableDef := &plan.TableDef{DbName: dbName, Name: tableName}
	node := &plan.Node{
		NodeType:  plan.Node_INSERT,
		ObjRef:    objRef,
		TableDef:  tableDef,
		InsertCtx: &plan.InsertCtx{Ref: objRef, TableDef: tableDef, AddAffectedRows: true},
	}
	if modern {
		node = &plan.Node{
			NodeType:      plan.Node_MULTI_UPDATE,
			UpdateCtxList: []*plan.UpdateCtx{{ObjRef: objRef, TableDef: tableDef}},
		}
	}
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: plan.Query_INSERT,
		Nodes:    []*plan.Node{node},
	}}}
}

func TestAuthenticateInsertUsesPlanBoundTarget(t *testing.T) {
	for _, tc := range []struct {
		name               string
		planDB, sessionDB  string
		modern             bool
		grantPlanTarget    bool
		grantSessionTarget bool
		want               bool
	}{
		{name: "prepared statement after database switch", planDB: "db_a", sessionDB: "db_b", grantPlanTarget: true, want: true},
		{name: "remapped default database", planDB: "physical", sessionDB: "logical", grantPlanTarget: true, want: true},
		{name: "session database grant does not cover plan target", planDB: "db_a", sessionDB: "db_b", grantSessionTarget: true},
		{name: "modern prepared statement after database switch", planDB: "db_a", sessionDB: "db_b", modern: true, grantPlanTarget: true, want: true},
		{name: "modern remapped default database", planDB: "physical", sessionDB: "logical", modern: true, grantPlanTarget: true, want: true},
		{name: "modern session database grant does not cover plan target", planDB: "db_a", sessionDB: "db_b", modern: true, grantSessionTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok := authenticatePlanWithTablePrivilegesInDatabase(
				t, tc.sessionDB, insertInto("", "target"), makeInsertTargetPrivilegePlan(tc.planDB, "target", tc.modern),
				tablePrivilegeFixture{dbName: tc.planDB, tableName: "target", allowed: map[PrivilegeType]bool{
					PrivilegeTypeInsert: tc.grantPlanTarget,
				}},
				tablePrivilegeFixture{dbName: tc.sessionDB, tableName: "target", allowed: map[PrivilegeType]bool{
					PrivilegeTypeInsert: tc.grantSessionTarget,
				}})
			assert.Equal(t, tc.want, ok)
		})
	}
}

func makeExternalDMLTargetPrivilegePlan(stmtType plan.Query_StatementType, tableName, tableType, extraOptions string) *plan2.Plan {
	objRef := &plan.ObjectRef{SchemaName: "external_db", ObjName: tableName}
	tableDef := &plan.TableDef{DbName: "external_db", Name: tableName, TableType: tableType}
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: stmtType,
		Nodes: []*plan.Node{{
			NodeType:     plan.Node_INSERT,
			ObjRef:       objRef,
			TableDef:     tableDef,
			ExtraOptions: extraOptions,
			InsertCtx:    &plan.InsertCtx{Ref: objRef, TableDef: tableDef},
		}},
	}}}
}

func TestExtractPrivilegeTipsFromPlanExternalInsertTarget(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stmtType     plan.Query_StatementType
		tableType    string
		extraOptions string
		want         bool
		priv         PrivilegeType
	}{
		{name: "external insert", stmtType: plan.Query_INSERT, tableType: catalog.SystemExternalRel, want: true, priv: PrivilegeTypeInsert},
		{name: "iceberg update", stmtType: plan.Query_UPDATE, tableType: catalog.SystemExternalRel, extraOptions: icebergapi.DMLUpdatePlanExtraOptions, want: true, priv: PrivilegeTypeUpdate},
		{name: "iceberg update missing marker", stmtType: plan.Query_UPDATE, tableType: catalog.SystemExternalRel, want: true, priv: PrivilegeTypeUpdate},
		{name: "iceberg update wrong delete marker", stmtType: plan.Query_UPDATE, tableType: catalog.SystemExternalRel, extraOptions: icebergapi.DMLDeletePlanExtraOptions, want: true, priv: PrivilegeTypeUpdate},
		{name: "iceberg delete", stmtType: plan.Query_DELETE, tableType: catalog.SystemExternalRel, extraOptions: icebergapi.DMLDeletePlanExtraOptions, want: true, priv: PrivilegeTypeDelete},
		{name: "iceberg delete missing marker", stmtType: plan.Query_DELETE, tableType: catalog.SystemExternalRel, want: true, priv: PrivilegeTypeDelete},
		{name: "iceberg delete wrong update marker", stmtType: plan.Query_DELETE, tableType: catalog.SystemExternalRel, extraOptions: icebergapi.DMLUpdatePlanExtraOptions, want: true, priv: PrivilegeTypeDelete},
		{name: "ordinary prefix named insert target", stmtType: plan.Query_INSERT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := makeExternalDMLTargetPrivilegePlan(tc.stmtType, "__mo_index_target", tc.tableType, tc.extraOptions)
			arr, err := extractPrivilegeTipsFromPlan(p)
			require.NoError(t, err)
			if !tc.want {
				assert.Empty(t, arr)
				return
			}
			require.Len(t, arr, 1)
			assert.Equal(t, tc.priv, arr[0].typ)
			assert.Equal(t, "__mo_index_target", arr[0].tableName)
		})
	}
}

func TestExtractPrivilegeTipsFromPlanIcebergDMLTargetScan(t *testing.T) {
	for _, tc := range []struct {
		name            string
		stmtType        plan.Query_StatementType
		sinkOption      string
		privilege       PrivilegeType
		wantNoPrivilege bool
	}{
		{name: "update", stmtType: plan.Query_UPDATE, sinkOption: icebergapi.DMLUpdatePlanExtraOptions, privilege: PrivilegeTypeUpdate},
		{name: "delete", stmtType: plan.Query_DELETE, sinkOption: icebergapi.DMLDeletePlanExtraOptions, privilege: PrivilegeTypeDelete},
		{name: "merge", stmtType: plan.Query_MERGE, sinkOption: icebergapi.DMLMergePlanExtraOptions, wantNoPrivilege: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := makeExternalDMLTargetPrivilegePlan(tc.stmtType, "orders", catalog.SystemExternalRel, tc.sinkOption)
			q := p.GetQuery()
			q.Nodes = append([]*plan.Node{{
				NodeType:     plan.Node_EXTERNAL_SCAN,
				ObjRef:       &plan.ObjectRef{SchemaName: "external_db", ObjName: "orders"},
				TableDef:     &plan.TableDef{DbName: "external_db", Name: "orders", TableType: catalog.SystemExternalRel},
				ExternScan:   &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB)},
				ExtraOptions: icebergapi.DMLTargetScanPlanExtraOptions,
			}}, q.Nodes...)

			arr, err := extractPrivilegeTipsFromPlan(p)
			require.NoError(t, err)
			if tc.wantNoPrivilege {
				require.Empty(t, arr)
				return
			}
			require.Len(t, arr, 1)
			assert.Equal(t, tc.privilege, arr[0].typ)
			assert.Equal(t, "orders", arr[0].tableName)
		})
	}
}

func TestExtractPrivilegeTipsFromPlanIcebergDMLSourceAndTarget(t *testing.T) {
	p := makeExternalDMLTargetPrivilegePlan(plan.Query_UPDATE, "orders", catalog.SystemExternalRel, icebergapi.DMLUpdatePlanExtraOptions)
	q := p.GetQuery()
	q.Nodes = append([]*plan.Node{{
		NodeType:     plan.Node_EXTERNAL_SCAN,
		ObjRef:       &plan.ObjectRef{SchemaName: "external_db", ObjName: "orders"},
		TableDef:     &plan.TableDef{DbName: "external_db", Name: "orders", TableType: catalog.SystemExternalRel},
		ExternScan:   &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB)},
		ExtraOptions: icebergapi.DMLTargetScanPlanExtraOptions,
	}, {
		NodeType:   plan.Node_EXTERNAL_SCAN,
		ObjRef:     &plan.ObjectRef{SchemaName: "external_db", ObjName: "source_orders"},
		TableDef:   &plan.TableDef{DbName: "external_db", Name: "source_orders", TableType: catalog.SystemExternalRel},
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB)},
	}}, q.Nodes...)

	arr, err := extractPrivilegeTipsFromPlan(p)
	require.NoError(t, err)
	require.Len(t, arr, 2)
	privileges := make(map[string]PrivilegeType, len(arr))
	for _, tip := range arr {
		privileges[tip.tableName] = tip.typ
	}
	assert.Equal(t, PrivilegeTypeUpdate, privileges["orders"])
	assert.Equal(t, PrivilegeTypeSelect, privileges["source_orders"])
}

func TestExtractPrivilegeTipsFromPlanIcebergDMLTargetMarkerMustMatchSink(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stmtType   plan.Query_StatementType
		withSink   bool
		sinkTarget string
	}{
		{name: "read statement", stmtType: plan.Query_SELECT},
		{name: "missing sink", stmtType: plan.Query_UPDATE},
		{name: "different sink target", stmtType: plan.Query_UPDATE, withSink: true, sinkTarget: "other_orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := &plan2.Query{
				StmtType: tc.stmtType,
				Nodes: []*plan.Node{{
					NodeType:     plan.Node_EXTERNAL_SCAN,
					ObjRef:       &plan.ObjectRef{SchemaName: "external_db", ObjName: "orders"},
					TableDef:     &plan.TableDef{DbName: "external_db", Name: "orders", TableType: catalog.SystemExternalRel},
					ExternScan:   &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB)},
					ExtraOptions: icebergapi.DMLTargetScanPlanExtraOptions,
				}},
			}
			if tc.withSink {
				query.Nodes = append(query.Nodes, &plan.Node{
					NodeType:     plan.Node_INSERT,
					ObjRef:       &plan.ObjectRef{SchemaName: "external_db", ObjName: tc.sinkTarget},
					TableDef:     &plan.TableDef{DbName: "external_db", Name: tc.sinkTarget, TableType: catalog.SystemExternalRel},
					ExtraOptions: icebergapi.DMLUpdatePlanExtraOptions,
					InsertCtx: &plan.InsertCtx{
						Ref: &plan.ObjectRef{SchemaName: "external_db", ObjName: tc.sinkTarget},
					},
				})
			}
			p := &plan2.Plan{Plan: &plan2.Plan_Query{Query: query}}

			tips, err := extractPrivilegeTipsFromPlan(p)
			require.NoError(t, err)
			foundSelect := false
			for _, tip := range tips {
				if tip.tableName == "orders" && tip.typ == PrivilegeTypeSelect {
					foundSelect = true
				}
			}
			assert.True(t, foundSelect, "unmatched scan marker must retain source SELECT")
		})
	}
}

func TestAuthenticateExternalUpdateDeleteTargetPrivileges(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stmtType     plan.Query_StatementType
		extraOptions string
		required     PrivilegeType
		stmt         tree.Statement
	}{
		{name: "update", stmtType: plan.Query_UPDATE, extraOptions: icebergapi.DMLUpdatePlanExtraOptions, required: PrivilegeTypeUpdate, stmt: &tree.Update{}},
		{name: "update missing marker", stmtType: plan.Query_UPDATE, required: PrivilegeTypeUpdate, stmt: &tree.Update{}},
		{name: "update wrong delete marker", stmtType: plan.Query_UPDATE, extraOptions: icebergapi.DMLDeletePlanExtraOptions, required: PrivilegeTypeUpdate, stmt: &tree.Update{}},
		{name: "delete", stmtType: plan.Query_DELETE, extraOptions: icebergapi.DMLDeletePlanExtraOptions, required: PrivilegeTypeDelete, stmt: &tree.Delete{}},
		{name: "delete missing marker", stmtType: plan.Query_DELETE, required: PrivilegeTypeDelete, stmt: &tree.Delete{}},
		{name: "delete wrong update marker", stmtType: plan.Query_DELETE, extraOptions: icebergapi.DMLUpdatePlanExtraOptions, required: PrivilegeTypeDelete, stmt: &tree.Delete{}},
	} {
		for _, grant := range []string{"none", "exact", "insert only", "select only", "wrong table"} {
			t.Run(tc.name+"/"+grant, func(t *testing.T) {
				target := tablePrivilegeFixture{dbName: "external_db", tableName: "__mo_index_target"}
				fixtures := []tablePrivilegeFixture{target}
				want := false
				switch grant {
				case "exact":
					target.allowed = map[PrivilegeType]bool{tc.required: true}
					fixtures[0] = target
					want = true
				case "insert only":
					target.allowed = map[PrivilegeType]bool{PrivilegeTypeInsert: true}
					fixtures[0] = target
				case "select only":
					target.allowed = map[PrivilegeType]bool{PrivilegeTypeSelect: true}
					fixtures[0] = target
				case "wrong table":
					fixtures = append(fixtures, tablePrivilegeFixture{
						dbName: "external_db", tableName: "other_table",
						allowed: map[PrivilegeType]bool{tc.required: true},
					})
				}
				ok := authenticatePlanWithTablePrivileges(
					t, tc.stmt, makeExternalDMLTargetPrivilegePlan(tc.stmtType, "__mo_index_target", catalog.SystemExternalRel, tc.extraOptions), fixtures...)
				assert.Equal(t, want, ok)
			})
		}
	}
}

func makeExternalReadAndWriteTargetPrivilegePlan(stmtType plan.Query_StatementType, subscriptionName string) (*plan2.Plan, *plan.ObjectRef) {
	scanRef := &plan.ObjectRef{SchemaName: "publisher_db", ObjName: "events", SubscriptionName: subscriptionName}
	targetRef := &plan.ObjectRef{SchemaName: "publisher_db", ObjName: "events"}
	scanNode := &plan.Node{
		NodeType:   plan.Node_EXTERNAL_SCAN,
		ObjRef:     scanRef,
		TableDef:   &plan.TableDef{TableType: catalog.SystemExternalRel},
		ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_EXTERNAL_TB)},
	}
	targetDef := &plan.TableDef{DbName: "publisher_db", Name: "events"}
	var targetNode *plan.Node
	switch stmtType {
	case plan.Query_UPDATE:
		targetNode = &plan.Node{
			NodeType:      plan.Node_MULTI_UPDATE,
			UpdateCtxList: []*plan.UpdateCtx{{ObjRef: targetRef, TableDef: targetDef}},
		}
	case plan.Query_DELETE:
		targetNode = &plan.Node{
			NodeType:  plan.Node_DELETE,
			ObjRef:    targetRef,
			TableDef:  targetDef,
			DeleteCtx: &plan.DeleteCtx{Ref: targetRef},
		}
	default:
		panic("unexpected statement type")
	}
	return &plan2.Plan{Plan: &plan2.Plan_Query{Query: &plan2.Query{
		StmtType: stmtType,
		Nodes:    []*plan.Node{scanNode, targetNode},
	}}}, scanRef
}

func TestExternalScanRemainsSelectWhenNameMatchesDMLTarget(t *testing.T) {
	for _, stmtType := range []plan.Query_StatementType{plan.Query_UPDATE, plan.Query_DELETE} {
		for _, subscriptionName := range []string{"", "subscriber_db"} {
			t.Run(stmtType.String()+"/"+subscriptionName, func(t *testing.T) {
				p, scanRef := makeExternalReadAndWriteTargetPrivilegePlan(stmtType, subscriptionName)
				arr, err := extractPrivilegeTipsFromPlan(p)
				require.NoError(t, err)
				require.Len(t, arr, 2)
				var scanTip *privilegeTips
				for i := range arr {
					if arr[i].objectRef == scanRef {
						scanTip = &arr[i]
					}
				}
				require.NotNil(t, scanTip)
				assert.Equal(t, PrivilegeTypeSelect, scanTip.typ)
				expectedDB := subscriptionName
				if expectedDB == "" {
					expectedDB = "publisher_db"
				}
				assert.Equal(t, expectedDB, scanTip.databaseName)
			})
		}
	}
}

func TestAuthenticateExternalReadRequiresSelectForSubscriptionAlias(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sourceSelect bool
		sourceUpdate bool
		targetUpdate bool
		want         bool
	}{
		{name: "source select and target update", sourceSelect: true, targetUpdate: true, want: true},
		{name: "source update cannot replace select", sourceUpdate: true, targetUpdate: true},
		{name: "source select cannot replace target update", sourceSelect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := makeExternalReadAndWriteTargetPrivilegePlan(plan.Query_UPDATE, "subscriber_db")
			ok := authenticatePlanWithTablePrivileges(
				t, &tree.Update{}, p,
				tablePrivilegeFixture{dbName: "subscriber_db", tableName: "events", allowed: map[PrivilegeType]bool{
					PrivilegeTypeSelect: tc.sourceSelect,
					PrivilegeTypeUpdate: tc.sourceUpdate,
				}},
				tablePrivilegeFixture{dbName: "publisher_db", tableName: "events", allowed: map[PrivilegeType]bool{
					PrivilegeTypeUpdate: tc.targetUpdate,
				}})
			assert.Equal(t, tc.want, ok)
		})
	}
}
