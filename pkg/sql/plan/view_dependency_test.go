// Copyright 2021 Matrix Origin
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
	"encoding/json"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

type namedSnapshotViewContext struct {
	*rootSQLCompilerContext
	snapshot *Snapshot
}

type physicalOwnerViewContext struct {
	*rootSQLCompilerContext
	accountID uint32
}

func (c *physicalOwnerViewContext) ResolveViewDependencyAccount(
	*ObjectRef, *TableDef, *Snapshot,
) (uint32, error) {
	return c.accountID, nil
}

func (c *namedSnapshotViewContext) ResolveSnapshotWithSnapshotName(
	_ string,
) (*Snapshot, error) {
	return DeepCopySnapshot(c.snapshot), nil
}

func TestAuthoritativeViewGenerationCapturesDirectDependency(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	ctx.tables["nation"].DbId = 7
	ctx.tables["nation"].TblId = 11
	ctx.tables["nation"].LogicalId = 13
	ctx.tables["nation"].Version = 17

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Equal(t, []ViewDependency{{
		AccountID:           42,
		DatabaseID:          7,
		RelationID:          11,
		LogicalID:           13,
		DatabaseName:        "tpch",
		RelationName:        "nation",
		BindingDatabaseName: "tpch",
		BindingRelationName: "nation",
		RelationKind:        catalog.SystemOrdinaryRel,
		Version:             17,
		LowerCaseTableNames: 1,
	}}, data.Dependencies)
}

func TestViewDependencyPhysicalOwnerSurvivesRegeneration(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation"
	ctx := &physicalOwnerViewContext{
		rootSQLCompilerContext: &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
			rootSQL:             rootSQL,
		},
		accountID: 0,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	persisted := p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()

	var created ViewData
	require.NoError(t, json.Unmarshal([]byte(persisted), &created))
	require.Len(t, created.Dependencies, 1)
	require.Equal(t, uint32(0), created.Dependencies[0].AccountID)

	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	require.Len(t, regenerated.Dependencies, 1)
	require.Equal(t, uint32(0), regenerated.Dependencies[0].AccountID)
}

func TestAuthoritativeViewGenerationCapturesViewNotItsSources(t *testing.T) {
	const rootSQL = "create view v2 as select n_nationkey from v1"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	viewJSON, err := json.Marshal(ViewData{
		Stmt:            "create view v1 as select n_nationkey from nation",
		DefaultDatabase: "tpch",
	})
	require.NoError(t, err)
	ctx.objects["v1"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "v1", Obj: 101}
	ctx.tables["v1"] = &planpb.TableDef{
		DbId:      7,
		TblId:     101,
		LogicalId: 103,
		Version:   5,
		Name:      "v1",
		TableType: catalog.SystemViewRel,
		Cols:      DeepCopyColDefList(ctx.tables["nation"].Cols),
		ViewSql:   &planpb.ViewDef{View: string(viewJSON)},
	}

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "v1", data.Dependencies[0].RelationName)
	require.Equal(t, uint64(101), data.Dependencies[0].RelationID)
	require.Equal(t, uint64(103), data.Dependencies[0].LogicalID)
}

func TestAuthoritativeViewGenerationCapturesSnapshotBinding(t *testing.T) {
	const rootSQL = "create view v as select n_nationkey from nation {snapshot = 'daily'}"
	snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	ctx := &namedSnapshotViewContext{
		rootSQLCompilerContext: &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false), rootSQL: rootSQL,
		},
		snapshot: snapshot,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "daily", data.Dependencies[0].SnapshotName)
	require.Equal(t, snapshot, data.Dependencies[0].Snapshot)
}

func TestRegenerateViewDefinitionUsesAuthoritativeGeneratorAndPreservesJSON(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	ctx.tables["nation"].DbId = 7
	ctx.tables["nation"].TblId = 11
	ctx.tables["nation"].LogicalId = 13
	ctx.tables["nation"].Cols[1].Typ.Width = 60
	persisted := `{"Stmt":"create view v as select n_name from nation",` +
		`"DefaultDatabase":"tpch","security_type":"DEFINER",` +
		`"future_field":{"keep":true}}`

	regenerated, err := RegenerateViewDefinition(ctx, persisted)
	require.NoError(t, err)
	require.Len(t, regenerated.TableDef.Cols, 1)
	require.Equal(t, int32(60), regenerated.TableDef.Cols[0].Typ.Width)
	require.Len(t, regenerated.Dependencies, 1)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &fields))
	require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
	require.JSONEq(t, `"create view v as select n_name from nation"`, string(fields["Stmt"]))
	require.Contains(t, fields, "dependencies")
	require.Contains(t, fields, "lower_case_table_names")
}

func TestRegenerateViewDefinitionPersistsExpandedStar(t *testing.T) {
	for _, rootSQL := range []string{
		"create view v as select * from nation",
		"create view v (k, name, rkey, comment) as select * from nation",
	} {
		t.Run(rootSQL, func(t *testing.T) {
			ctx := NewMockCompilerContext(false)
			ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
			persisted, err := json.Marshal(map[string]any{
				"Stmt": rootSQL, "DefaultDatabase": "tpch", "future_field": map[string]bool{"keep": true},
			})
			require.NoError(t, err)

			first, err := RegenerateViewDefinition(ctx, string(persisted))
			require.NoError(t, err)
			require.Len(t, first.TableDef.Cols, 4)
			var firstData ViewData
			require.NoError(t, json.Unmarshal([]byte(first.TableDef.ViewSql.View), &firstData))
			require.NotContains(t, firstData.Stmt, "*")

			ctx.tables["nation"].Cols = append(ctx.tables["nation"].Cols, &planpb.ColDef{
				Name:       "n_extra",
				OriginName: "n_extra",
				Typ:        planpb.Type{Id: int32(types.T_int32)},
				Default:    &planpb.Default{NullAbility: true},
			})
			second, err := RegenerateViewDefinition(ctx, first.TableDef.ViewSql.View)
			require.NoError(t, err)
			require.Len(t, second.TableDef.Cols, 4)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(second.TableDef.ViewSql.View), &fields))
			require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
		})
	}
}

func TestAuthoritativeViewGenerationCapturesLimitZeroStarDependency(t *testing.T) {
	const rootSQL = "create view v as select * from nation limit 0"
	ctx := &rootSQLCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		rootSQL:             rootSQL,
	}
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(p.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.Len(t, data.Dependencies, 1)
	require.Equal(t, "nation", data.Dependencies[0].RelationName)
}

func TestReplaceRegeneratedViewDependenciesPreservesViewData(t *testing.T) {
	regenerated := &RegeneratedViewDefinition{
		TableDef: &planpb.TableDef{ViewSql: &planpb.ViewDef{View: `{
			"stmt":"create view v as select a from t",
			"default_database":"db",
			"lower_case_table_names":1,
			"future_field":{"keep":true},
			"dependencies":[]}`}},
	}
	dependencies := []ViewDependency{{
		AccountID: 1, DatabaseID: 2, RelationID: 4, LogicalID: 3,
		DatabaseName: "db", RelationName: "t",
	}}
	require.NoError(t, ReplaceRegeneratedViewDependencies(regenerated, dependencies))
	require.Equal(t, dependencies, regenerated.Dependencies)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(regenerated.TableDef.ViewSql.View), &fields))
	require.JSONEq(t, `{"keep":true}`, string(fields["future_field"]))
	require.JSONEq(t, `1`, string(fields["lower_case_table_names"]))
	require.JSONEq(t, `[{"account_id":1,"database_id":2,"relation_id":4,"logical_id":3,"database_name":"db","relation_name":"t","relation_kind":"","version":0}]`, string(fields["dependencies"]))
}

func TestViewDependencyIdentityKeepsDistinctBindingEnvironments(t *testing.T) {
	base := ViewDependency{
		AccountID: 1, DatabaseID: 2, RelationID: 3,
		DatabaseName: "PhysicalDB", RelationName: "PhysicalTable",
		BindingDatabaseName: "sub_one", BindingRelationName: "PhysicalTable",
		SubscriptionName: "sub_one", LowerCaseTableNames: 1,
	}
	secondSubscription := base
	secondSubscription.BindingDatabaseName = "sub_two"
	secondSubscription.SubscriptionName = "sub_two"
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(secondSubscription))

	snapshot := base
	snapshot.SubscriptionName = ""
	snapshot.SnapshotName = "daily"
	snapshot.Snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(snapshot))

	caseVariant := base
	caseVariant.BindingDatabaseName = "SUB_ONE"
	require.Equal(t, viewDependencyKey(base), viewDependencyKey(caseVariant))
	caseVariant.LowerCaseTableNames = 0
	require.NotEqual(t, viewDependencyKey(base), viewDependencyKey(caseVariant))
}

func TestViewDependencyCaptureScopeAndIdentityFallbacks(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }
	capture := newViewDependencyCaptureContext(ctx)

	capture.enterNestedView()
	_, _, err := capture.Resolve("tpch", "nation", nil)
	require.NoError(t, err)
	require.Empty(t, capture.dependencies())
	capture.leaveNestedView()
	require.Panics(t, capture.leaveNestedView)

	obj := &planpb.ObjectRef{PubInfo: &planpb.PubInfo{TenantId: 11}}
	tableDef := &planpb.TableDef{DbId: 2, TblId: 3, LogicalId: 4, DbName: "physical_db", Name: "physical_t"}
	require.NoError(t, capture.record(obj, tableDef, nil, "", ""))
	dependencies := capture.dependencies()
	require.Len(t, dependencies, 1)
	require.Equal(t, uint32(11), dependencies[0].AccountID)
	require.Equal(t, "physical_db", dependencies[0].DatabaseName)
	require.Equal(t, "physical_t", dependencies[0].RelationName)
	require.Equal(t, uint32(11), dependencies[0].PublisherAccount)

	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 456},
		Tenant: &planpb.SnapshotTenant{TenantID: 13},
	}
	capture.snapshotNames[snapshot.String()] = "daily"
	require.NoError(t, capture.record(
		&planpb.ObjectRef{SchemaName: "snapshot_db", ObjName: "snapshot_t"},
		tableDef, snapshot, "snapshot_db", "snapshot_t"))
	dependencies = capture.dependencies()
	require.Len(t, dependencies, 2)
	require.Equal(t, uint32(13), dependencies[1].AccountID)
	require.Equal(t, "daily", dependencies[1].SnapshotName)
	require.NotSame(t, snapshot, dependencies[1].Snapshot)

	expected := errors.New("account unavailable")
	ctx.GetAccountIdFunc = func() (uint32, error) { return 0, expected }
	require.ErrorIs(t, capture.record(&planpb.ObjectRef{}, tableDef, nil, "", ""), expected)
}

func TestViewDependencyCaptureResolveByID(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 7, nil }
	capture := newViewDependencyCaptureContext(ctx)
	tableID := ctx.tables["nation"].TblId
	obj, tableDef, err := capture.ResolveById(tableID, nil)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.NotNil(t, tableDef)
	require.Len(t, capture.dependencies(), 1)
}

func TestRegenerateViewDefinitionRejectsInvalidPersistedDefinitions(t *testing.T) {
	ctx := NewMockCompilerContext(false)

	_, err := RegenerateViewDefinition(ctx, `{`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select ("}`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select 1; select 2"}`)
	require.Error(t, err)
	_, err = RegenerateViewDefinition(ctx, `{"Stmt":"select 1"}`)
	require.Error(t, err)

	for _, regenerated := range []*RegeneratedViewDefinition{
		nil,
		{},
		{TableDef: &planpb.TableDef{}},
	} {
		require.Error(t, ReplaceRegeneratedViewDependencies(regenerated, nil))
	}
	require.Error(t, ReplaceRegeneratedViewDependencies(&RegeneratedViewDefinition{
		TableDef: &planpb.TableDef{ViewSql: &planpb.ViewDef{View: `{`}},
	}, nil))
}

func TestRegenerateAlterViewUsesPersistedParserEnvironment(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	ctx.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	mode := ""
	lowerCaseTableNames := int64(0)
	persisted, err := json.Marshal(ViewData{
		Stmt:                "alter view v (renamed) as select n_name from nation",
		DefaultDatabase:     "tpch",
		SQLMode:             &mode,
		LowerCaseTableNames: &lowerCaseTableNames,
	})
	require.NoError(t, err)
	regenerated, err := RegenerateViewDefinition(ctx, string(persisted))
	require.NoError(t, err)
	require.Equal(t, "renamed", regenerated.TableDef.Cols[0].Name)
}
