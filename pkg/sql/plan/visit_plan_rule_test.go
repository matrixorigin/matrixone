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
	"encoding/json"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type resolveErrorCompilerContext struct {
	*MockCompilerContext
	err error
}

func (c *resolveErrorCompilerContext) Resolve(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	return nil, nil, c.err
}

type viewDependencyCompilerContext struct {
	*MockCompilerContext
	views    []string
	snapshot *Snapshot
	resolve  func(string, string, *Snapshot) (*ObjectRef, *TableDef, error)
}

func (c *viewDependencyCompilerContext) GetViews() []string {
	return c.views
}

func (c *viewDependencyCompilerContext) SetViews(views []string) {
	c.views = append([]string(nil), views...)
}

func (c *viewDependencyCompilerContext) GetSnapshot() *Snapshot {
	return c.snapshot
}

func (c *viewDependencyCompilerContext) Resolve(
	databaseName string,
	tableName string,
	snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	return c.resolve(databaseName, tableName, snapshot)
}

func TestCollectPrepareDdlSchemas(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		expected []string
	}{
		{name: "alter table", sql: "alter table t1 add column c int", expected: []string{"t1"}},
		{name: "create index", sql: "create index idx on t1(c)", expected: []string{"t1"}},
		{name: "drop index", sql: "drop index idx on t1", expected: []string{"t1"}},
		{name: "truncate table", sql: "truncate table t1", expected: []string{"t1"}},
		{name: "drop tables", sql: "drop table t1, t2", expected: []string{"t1", "t2"}},
		{name: "rename tables", sql: "rename table t1 to n1, t2 to n2", expected: []string{"t1", "t2"}},
		{name: "create table like", sql: "create table n1 like t1", expected: []string{"n1", "t1"}},
		{
			name:     "alter table foreign key",
			sql:      "alter table t1 add foreign key (c) references t2(c)",
			expected: []string{"t1", "t2"},
		},
		{name: "drop view", sql: "drop view t1", expected: []string{"t1"}},
		{name: "drop sequence", sql: "drop sequence t1, t2", expected: []string{"t1", "t2"}},
		{name: "alter sequence", sql: "alter sequence t1 increment by 2", expected: []string{"t1"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			for i, name := range []string{"t1", "t2"} {
				mock.objects[name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: name}
				mock.tables[name] = &planpb.TableDef{Name: name, DbId: 10, TblId: uint64(20 + i), Version: 30}
			}
			statements, err := mysql.Parse(context.Background(), testCase.sql, 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()

			schemas, err := collectPrepareDdlSchemas(mock, statements[0], &planpb.Plan{
				Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
			})
			require.NoError(t, err)
			require.Len(t, schemas, len(testCase.expected))
			for i, expected := range testCase.expected {
				require.Equal(t, expected, schemas[i].ObjName)
				if schemas[i].Obj != 0 {
					require.Equal(t, int64(30), schemas[i].Server)
				}
			}
		})
	}
}

func TestCollectPrepareDdlSchemasRecordsMissingTable(t *testing.T) {
	mock := NewMockCompilerContext(false)
	statements, err := mysql.Parse(context.Background(), "drop table if exists missing", 1)
	require.NoError(t, err)
	defer statements[0].Free()

	schemas, err := collectPrepareDdlSchemas(mock, statements[0], &planpb.Plan{
		Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
	})
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{{SchemaName: "tpch", ObjName: "missing"}}, schemas)
}

func TestCollectPrepareDdlSchemasRecordsMissingDatabase(t *testing.T) {
	mock := NewMockCompilerContext(false)
	mock.DatabaseExistsFunc = func(string, *Snapshot) bool { return false }
	statements, err := mysql.Parse(context.Background(), "drop table if exists future_db.missing", 1)
	require.NoError(t, err)
	defer statements[0].Free()

	schemas, err := collectPrepareDdlSchemas(mock, statements[0], &planpb.Plan{
		Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
	})
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{{SchemaName: "future_db", ObjName: "missing"}}, schemas)
}

func TestCollectPrepareDdlSchemasPropagatesResolveError(t *testing.T) {
	expected := errors.New("resolve failed")
	ctx := &resolveErrorCompilerContext{MockCompilerContext: NewMockCompilerContext(false), err: expected}
	statements, err := mysql.Parse(context.Background(), "truncate table t1", 1)
	require.NoError(t, err)
	defer statements[0].Free()

	_, err = collectPrepareDdlSchemas(ctx, statements[0], &planpb.Plan{
		Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
	})
	require.ErrorIs(t, err, expected)
}

func TestCollectPrepareDdlSchemasUsesCloneSourceMetadata(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), "create table dst clone src", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	clonePlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CloneTable{CloneTable: &planpb.CloneTable{
			SrcObjDef: &planpb.ObjectRef{
				SchemaName:       "tpch",
				ObjName:          "src",
				SubscriptionName: "sub",
				PubInfo:          &planpb.PubInfo{TenantId: 12},
			},
			SrcTableDef:  &planpb.TableDef{Name: "src", DbName: "tpch", DbId: 10, TblId: 20, Version: 30},
			ScanSnapshot: &planpb.Snapshot{},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(NewMockCompilerContext(false), statements[0], clonePlan)
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{
		{
			Server: 30, Db: 10, Schema: 10, Obj: 20, SchemaName: "tpch", ObjName: "src",
			SubscriptionName: "sub", PubInfo: &planpb.PubInfo{TenantId: 12},
		},
		{SchemaName: "tpch"},
	}, schemas)
}

func TestCollectPrepareDdlSchemasTracksCreateTargetDatabase(t *testing.T) {
	testCases := []string{
		"create sequence db1.seq as bigint",
		"create source db1.src (i int) with (type='kafka')",
	}
	for _, sql := range testCases {
		t.Run(sql, func(t *testing.T) {
			statements, err := mysql.Parse(context.Background(), sql, 1)
			require.NoError(t, err)
			defer statements[0].Free()

			mock := NewMockCompilerContext(false)
			mock.DatabaseExistsFunc = func(name string, _ *Snapshot) bool { return name == "db1" }
			schemas, err := collectPrepareDdlSchemas(mock, statements[0], &planpb.Plan{
				Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
			})
			require.NoError(t, err)
			require.Len(t, schemas, 1)
			require.Equal(t, "db1", schemas[0].SchemaName)
			require.Empty(t, schemas[0].ObjName)
		})
	}
}

func TestCollectPrepareDdlSchemasCollectsForeignKeyParents(t *testing.T) {
	statements, err := mysql.Parse(
		context.Background(),
		"create table child (id int, foreign key (id) references parent(id))",
		1,
	)
	require.NoError(t, err)
	defer statements[0].Free()
	mock := NewMockCompilerContext(false)
	mock.objects["parent"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "parent"}
	mock.tables["parent"] = &planpb.TableDef{Name: "parent", DbId: 10, TblId: 20, Version: 30}
	createPlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{
			FkDbs: []string{"tpch"}, FkTables: []string{"parent"},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(mock, statements[0], createPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 3)
	require.Equal(t, "child", schemas[0].ObjName)
	require.Equal(t, "parent", schemas[1].ObjName)
	require.Empty(t, schemas[2].ObjName)
}

func TestCollectPrepareDdlSchemasCollectsExpandedForeignKeyParents(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), "create table child like src", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	mock := NewMockCompilerContext(false)
	for i, name := range []string{"src", "parent"} {
		mock.objects[name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: name}
		mock.tables[name] = &planpb.TableDef{Name: name, DbId: 10, TblId: uint64(20 + i), Version: 30}
	}
	createPlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{
			FkDbs: []string{"tpch"}, FkTables: []string{"parent"},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(mock, statements[0], createPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 4)
	require.Equal(t, "child", schemas[0].ObjName)
	require.Equal(t, "src", schemas[1].ObjName)
	require.Empty(t, schemas[2].ObjName)
	require.Equal(t, "parent", schemas[3].ObjName)
}

func TestCollectPrepareDdlSchemasCollectsForwardReferenceChildren(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), "create table parent (id int primary key)", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	mock := NewMockCompilerContext(false)
	mock.objects["child"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "child"}
	mock.tables["child"] = &planpb.TableDef{Name: "child", DbId: 10, TblId: 20, Version: 30}
	createPlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{
			FksReferToMe: []*planpb.ForeignKeyInfo{{Db: "tpch", Table: "child"}},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(mock, statements[0], createPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 3)
	require.Equal(t, "parent", schemas[0].ObjName)
	require.Empty(t, schemas[1].ObjName)
	require.Equal(t, "child", schemas[2].ObjName)
}

func TestCollectPrepareDdlSchemasCollectsViewQuery(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), "create view v as select n_name from nation", 1)
	require.NoError(t, err)
	defer statements[0].Free()

	schemas, err := collectPrepareDdlSchemas(NewMockCompilerContext(false), statements[0], &planpb.Plan{
		Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
	})
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "nation", schemas[0].ObjName)
	require.Equal(t, "v", schemas[1].ObjName)
}

func TestCollectPrepareDdlSchemasCollectsDynamicTableQuery(t *testing.T) {
	statements, err := mysql.Parse(
		context.Background(),
		"create dynamic table dt as select n_name from nation with (\"type\"='kafka')",
		1,
	)
	require.NoError(t, err)

	schemas, err := collectPrepareDdlSchemas(NewMockCompilerContext(false), statements[0], &planpb.Plan{
		Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{}},
	})
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "nation", schemas[0].ObjName)
	require.Equal(t, "dt", schemas[1].ObjName)
}

func TestAppendPrepareSchemasDeduplicatesByNameWithoutObjectID(t *testing.T) {
	schemas := appendPrepareSchemas(nil,
		&planpb.ObjectRef{SchemaName: "db", ObjName: "tbl"},
		&planpb.ObjectRef{SchemaName: "db", ObjName: "tbl"},
	)
	require.Len(t, schemas, 1)
}

func TestAppendPrepareSchemasKeepsSameNameFromDifferentPublishers(t *testing.T) {
	schemas := appendPrepareSchemas(nil,
		&planpb.ObjectRef{
			SchemaName: "db", ObjName: "tbl", PubInfo: &planpb.PubInfo{TenantId: 1},
		},
		&planpb.ObjectRef{
			SchemaName: "db", ObjName: "tbl", PubInfo: &planpb.PubInfo{TenantId: 2},
		},
	)
	require.Len(t, schemas, 2)
}

func TestResetPreparePlanCollectsDdlQuerySchemas(t *testing.T) {
	ddlPlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Query: &planpb.Query{
			Steps: []int32{0},
			Nodes: []*planpb.Node{{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef: &planpb.ObjectRef{
					SchemaName: "db", ObjName: "src", Obj: 20,
				},
				TableDef: &planpb.TableDef{Name: "src", DbId: 10, TblId: 20, Version: 30},
			}},
		},
	}}}

	schemas, _, err := ResetPreparePlan(NewMockCompilerContext(false), ddlPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, "src", schemas[0].ObjName)
	require.Equal(t, int64(30), schemas[0].Server)
}

func TestResetPreparePlanCollectsExternalAndSourceScans(t *testing.T) {
	for _, nodeType := range []planpb.Node_NodeType{
		planpb.Node_EXTERNAL_SCAN,
		planpb.Node_SOURCE_SCAN,
	} {
		t.Run(nodeType.String(), func(t *testing.T) {
			queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
				Steps: []int32{0},
				Nodes: []*planpb.Node{{
					NodeType: nodeType,
					ObjRef: &planpb.ObjectRef{
						SchemaName: "db", ObjName: "src", Obj: 20,
					},
					TableDef: &planpb.TableDef{Name: "src", DbId: 10, TblId: 20, Version: 30},
				}},
			}}}

			schemas, _, err := ResetPreparePlan(NewMockCompilerContext(false), queryPlan)
			require.NoError(t, err)
			require.Len(t, schemas, 1)
			require.Equal(t, "src", schemas[0].ObjName)
			require.Equal(t, int64(30), schemas[0].Server)
		})
	}
}

func TestCollectPrepareViewSchemasPreservesIdentity(t *testing.T) {
	mock := NewMockCompilerContext(false)
	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
		Tenant: &SnapshotTenant{TenantID: 11, TenantName: "publisher"},
	}
	viewKey, err := FormatViewDependencyKeyWithSnapshot("sub#src_v", snapshot)
	require.NoError(t, err)
	ctx := &viewDependencyCompilerContext{
		MockCompilerContext: mock,
		views: []string{
			viewKey,
		},
	}
	ctx.resolve = func(databaseName, tableName string, gotSnapshot *Snapshot) (*ObjectRef, *TableDef, error) {
		require.Equal(t, "sub", databaseName)
		require.Equal(t, "src_v", tableName)
		require.NotSame(t, snapshot, gotSnapshot)
		require.Equal(t, snapshot.TS, gotSnapshot.TS)
		require.Equal(t, snapshot.Tenant, gotSnapshot.Tenant)
		return &ObjectRef{
				SchemaName: "publisher_db", ObjName: tableName, Obj: 20,
				SubscriptionName: databaseName, PubInfo: &planpb.PubInfo{TenantId: 11},
			},
			&TableDef{Name: tableName, DbId: 10, TblId: 20, Version: 30},
			nil
	}

	schemas, err := collectPrepareViewSchemas(ctx)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, "sub", schemas[0].SubscriptionName)
	require.Equal(t, int32(11), schemas[0].GetPubInfo().GetTenantId())
	require.Equal(t, int64(30), schemas[0].Server)
}

func TestViewDependencySnapshotKeyValidation(t *testing.T) {
	key, err := FormatViewDependencyKeyWithSnapshot("db#v", nil)
	require.NoError(t, err)
	require.Equal(t, "db#v", key)

	base, snapshot, err := parseViewDependencyKeySnapshot(key)
	require.NoError(t, err)
	require.Equal(t, "db#v", base)
	require.Nil(t, snapshot)

	_, _, err = parseViewDependencyKeySnapshot("db#v" + viewDependencySnapshotKeySuffix + "!")
	require.Error(t, err)
	_, _, err = parseViewDependencyKeySnapshot("db#v" + viewDependencySnapshotKeySuffix + "eA")
	require.Error(t, err)
}

func TestBindViewRecordsCompleteTableSnapshot(t *testing.T) {
	viewJSON, err := json.Marshal(ViewData{
		Stmt:            "create view v as select 1",
		DefaultDatabase: "db",
	})
	require.NoError(t, err)
	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
		Tenant: &SnapshotTenant{TenantID: 11, TenantName: "publisher"},
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), true, false)
	bindCtx := NewBindContext(builder, nil)

	_, err = builder.bindView(
		bindCtx,
		&TableDef{ViewSql: &planpb.ViewDef{View: string(viewJSON)}},
		snapshot,
		&ObjectRef{},
		"db",
		"v",
	)
	require.NoError(t, err)
	require.Len(t, bindCtx.views, 1)

	base, recorded, err := parseViewDependencyKeySnapshot(bindCtx.views[0])
	require.NoError(t, err)
	require.Equal(t, "db#v", base)
	require.Equal(t, snapshot.TS, recorded.TS)
	require.Equal(t, snapshot.Tenant, recorded.Tenant)

	nodeID, err := builder.bindView(
		NewBindContext(builder, nil),
		&TableDef{ViewSql: &planpb.ViewDef{}},
		nil,
		&ObjectRef{},
		"db",
		"empty",
	)
	require.NoError(t, err)
	require.Zero(t, nodeID)
}

func TestCollectPrepareViewSchemasRejectsInvalidDependencies(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		view  string
		match string
	}{
		{
			name:  "encoded snapshot",
			view:  "db#v" + viewDependencySnapshotKeySuffix + "!",
			match: "invalid view dependency snapshot",
		},
		{
			name:  "legacy snapshot",
			view:  "db#v" + ViewSnapshotKeySuffix + "bad",
			match: "invalid view dependency snapshot",
		},
		{
			name:  "view key",
			view:  "invalid",
			match: "invalid view dependency",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := &viewDependencyCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				views:               []string{testCase.view},
				resolve: func(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
					t.Fatal("invalid dependency must fail before resolution")
					return nil, nil, nil
				},
			}
			_, err := collectPrepareViewSchemas(ctx)
			require.ErrorContains(t, err, testCase.match)
		})
	}
}

func TestCollectPrepareViewSchemasPropagatesResolutionFailures(t *testing.T) {
	resolveErr := errors.New("resolve failed")
	ctx := &viewDependencyCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		views:               []string{"db#v"},
		resolve: func(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
			return nil, nil, resolveErr
		},
	}
	_, err := collectPrepareViewSchemas(ctx)
	require.ErrorIs(t, err, resolveErr)

	ctx.resolve = func(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
		return nil, nil, nil
	}
	_, err = collectPrepareViewSchemas(ctx)
	require.ErrorContains(t, err, "no such table db.v")
}

func TestBuildPrepareClearsViewsFromPreviousStatement(t *testing.T) {
	ctx := &viewDependencyCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		views:               []string{"dropped_db#dropped_view"},
		resolve: func(databaseName, tableName string, _ *Snapshot) (*ObjectRef, *TableDef, error) {
			require.NotEqual(t, "dropped_db", databaseName)
			require.NotEqual(t, "dropped_view", tableName)
			return nil, nil, nil
		},
	}

	prepared, err := buildPrepare(
		tree.NewPrepareString("s", "drop table if exists unrelated"),
		ctx,
	)
	require.NoError(t, err)
	require.Empty(t, ctx.GetViews())
	require.Len(t, prepared.GetDcl().GetPrepare().GetSchemas(), 1)
	require.Equal(t, "unrelated", prepared.GetDcl().GetPrepare().GetSchemas()[0].ObjName)
}

func TestResetPreparePlanPreservesSubscriptionIdentity(t *testing.T) {
	ddlPlan := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Query: &planpb.Query{
			Steps: []int32{0},
			Nodes: []*planpb.Node{{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef: &planpb.ObjectRef{
					SchemaName: "publisher_db", ObjName: "src", Obj: 20,
					SubscriptionName: "subscriber_db", PubInfo: &planpb.PubInfo{TenantId: 11},
				},
				TableDef: &planpb.TableDef{Name: "src", DbId: 10, TblId: 20, Version: 30},
			}},
		},
	}}}

	schemas, _, err := ResetPreparePlan(NewMockCompilerContext(false), ddlPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, "subscriber_db", schemas[0].SubscriptionName)
	require.Equal(t, int32(11), schemas[0].GetPubInfo().GetTenantId())
}

func TestResetPreparePlanCollectsHiddenIndexSchemas(t *testing.T) {
	const hiddenTable = "__mo_index_hidden"
	mock := NewMockCompilerContext(false)
	mock.objects[hiddenTable] = &planpb.ObjectRef{
		Db:         10,
		Obj:        20,
		SchemaName: "db",
		ObjName:    hiddenTable,
	}
	mock.tables[hiddenTable] = &planpb.TableDef{Name: hiddenTable, DbId: 10, TblId: 20, Version: 30}

	queryPlan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef: &planpb.ObjectRef{
					Db:         1,
					Obj:        2,
					SchemaName: "db",
					ObjName:    "src",
				},
				TableDef: &planpb.TableDef{
					Name:    "src",
					DbId:    1,
					TblId:   2,
					Version: 3,
					Indexes: []*planpb.IndexDef{{
						IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
						IndexTableName: hiddenTable,
					}},
				},
			}},
		}},
	}

	schemas, _, err := ResetPreparePlan(mock, queryPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "src", schemas[0].ObjName)
	require.Equal(t, hiddenTable, schemas[1].ObjName)
	require.Equal(t, int64(30), schemas[1].Server)
	require.Equal(t, int64(10), schemas[1].Db)
	require.Equal(t, int64(20), schemas[1].Obj)
}
