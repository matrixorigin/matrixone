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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

type resolveErrorCompilerContext struct {
	*MockCompilerContext
	err error
}

func (c *resolveErrorCompilerContext) Resolve(string, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	return nil, nil, c.err
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
			SrcObjDef:   &planpb.ObjectRef{SchemaName: "tpch", ObjName: "src"},
			SrcTableDef: &planpb.TableDef{Name: "src", DbName: "tpch", DbId: 10, TblId: 20, Version: 30},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(NewMockCompilerContext(false), statements[0], clonePlan)
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{{
		Server: 30, Db: 10, Schema: 10, Obj: 20, SchemaName: "tpch", ObjName: "src",
	}}, schemas)
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
