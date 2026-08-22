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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

var errWindowParameterVisit = errors.New("window parameter visit failed")

type failWindowParameterVisitRule struct {
	failOn int
	calls  int
}

func (*failWindowParameterVisitRule) MatchNode(*planpb.Node) bool  { return false }
func (*failWindowParameterVisitRule) IsApplyExpr() bool            { return true }
func (*failWindowParameterVisitRule) ApplyNode(*planpb.Node) error { return nil }
func (r *failWindowParameterVisitRule) ApplyExpr(expr *planpb.Expr) (*planpb.Expr, error) {
	r.calls++
	if r.calls == r.failOn {
		return nil, errWindowParameterVisit
	}
	return expr, nil
}

func TestPrepareRulesTraverseEveryWindowSpecParameter(t *testing.T) {
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	window := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc:  param(5),
		PartitionBy: []*planpb.Expr{param(3), nil},
		OrderBy:     []*planpb.OrderBySpec{{Expr: param(4)}, nil},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: param(2)},
			End:   &planpb.FrameBound{Val: param(1)},
		},
	}}}

	get := NewGetParamRule()
	require.NoError(t, applyRuleToWindowSpec(get, nil))
	_, err := get.ApplyExpr(window)
	require.NoError(t, err)
	require.Equal(t, map[int]int{1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, get.params)
	get.SetParamOrder()

	_, err = NewResetParamOrderRule(get.params).ApplyExpr(window)
	require.NoError(t, err)
	require.Equal(t, []int32{4, 2, 3, 1, 0}, []int32{
		window.GetW().WindowFunc.GetP().Pos,
		window.GetW().PartitionBy[0].GetP().Pos,
		window.GetW().OrderBy[0].Expr.GetP().Pos,
		window.GetW().Frame.Start.Val.GetP().Pos,
		window.GetW().Frame.End.Val.GetP().Pos,
	})
}

func TestPrepareRulesTraverseVectorIndexScanExpressions(t *testing.T) {
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeId:   0,
			NodeType: planpb.Node_VECTOR_INDEX_SCAN,
			VectorIndexScan: &planpb.VectorIndexScan{
				QueryVector:     param(4),
				CandidateLimit:  param(3),
				FirstRoundLimit: param(0),
				PreFilters:      []*planpb.Expr{param(2)},
				DistanceRange:   &planpb.DistRange{LowerBound: param(1)},
			},
		}},
	}}}
	rule := NewGetParamRule()
	require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
	require.Equal(t, map[int]int{0: 0, 1: 0, 2: 0, 3: 0, 4: 0}, rule.params)
}

func TestApplyRuleToWindowSpecPropagatesFieldErrors(t *testing.T) {
	newWindow := func() *planpb.WindowSpec {
		param := func() *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{}}}
		}
		return &planpb.WindowSpec{
			WindowFunc:  param(),
			PartitionBy: []*planpb.Expr{param()},
			OrderBy:     []*planpb.OrderBySpec{{Expr: param()}},
			Frame: &planpb.FrameClause{
				Start: &planpb.FrameBound{Val: param()},
				End:   &planpb.FrameBound{Val: param()},
			},
		}
	}

	for failOn := 1; failOn <= 5; failOn++ {
		t.Run("field", func(t *testing.T) {
			rule := &failWindowParameterVisitRule{failOn: failOn}
			require.ErrorIs(t, applyRuleToWindowSpec(rule, newWindow()), errWindowParameterVisit)
		})
	}
}

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
		// targetSchema, when set, is the expected schema of the last dependency.
		targetSchema string
	}{
		{name: "alter table", sql: "alter table t1 add column c int", expected: []string{"t1"}},
		{name: "create index", sql: "create index idx on t1(c)", expected: []string{"t1"}},
		{name: "drop index", sql: "drop index idx on t1", expected: []string{"t1"}},
		{name: "truncate table", sql: "truncate table t1", expected: []string{"t1"}},
		{name: "drop tables", sql: "drop table t1, t2", expected: []string{"t1", "t2"}},
		{
			name:     "rename tables",
			sql:      "rename table t1 to n1, t2 to n2",
			expected: []string{"t1", "n1", "t2", "n2"},
		},
		{
			name:     "alter table rename",
			sql:      "alter table t1 rename to n1",
			expected: []string{"t1", "n1"},
		},
		{
			name:         "cross database rename table uses source database",
			sql:          "rename table db1.t1 to db2.n1",
			expected:     []string{"t1", "n1"},
			targetSchema: "db1",
		},
		{
			name:         "cross database alter rename uses source database",
			sql:          "alter table db1.t1 rename to db2.n1",
			expected:     []string{"t1", "n1"},
			targetSchema: "db1",
		},
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
			if testCase.targetSchema != "" {
				require.Equal(t, testCase.targetSchema, schemas[len(schemas)-1].SchemaName)
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
			SrcTableDef: &planpb.TableDef{Name: "src", DbName: "tpch", DbId: 10, TblId: 20, Version: 30},
			ScanSnapshot: &planpb.Snapshot{
				TS: &timestamp.Timestamp{PhysicalTime: 42},
			},
		}},
	}}}

	schemas, err := collectPrepareDdlSchemas(NewMockCompilerContext(false), statements[0], clonePlan)
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{
		{
			Server: 30, Db: 10, Schema: 10, Obj: 20, SchemaName: "tpch", ObjName: "src",
			SubscriptionName: "sub", PubInfo: &planpb.PubInfo{TenantId: 12},
			Snapshot: &planpb.Snapshot{
				TS: &timestamp.Timestamp{PhysicalTime: 42},
			},
		},
		{SchemaName: "tpch"},
	}, schemas)
}

func TestCollectPrepareDdlSchemasTracksCreateTargetDatabase(t *testing.T) {
	testCases := []string{
		"create sequence db1.seq as bigint",
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

func TestAppendPrepareSchemasKeepsSamePublisherTableFromDifferentSubscriptions(t *testing.T) {
	schemas := appendPrepareSchemas(nil,
		&planpb.ObjectRef{
			SchemaName: "publisher_db", ObjName: "tbl", Db: 10, Obj: 20,
			SubscriptionName: "subscription_one",
			PubInfo:          &planpb.PubInfo{TenantId: 1},
		},
		&planpb.ObjectRef{
			SchemaName: "publisher_db", ObjName: "tbl", Db: 10, Obj: 20,
			SubscriptionName: "subscription_two",
			PubInfo:          &planpb.PubInfo{TenantId: 1},
		},
		&planpb.ObjectRef{
			SchemaName: "publisher_db", ObjName: "tbl", Db: 10, Obj: 20,
			SubscriptionName: "subscription_one",
			PubInfo:          &planpb.PubInfo{TenantId: 1},
		},
	)
	require.Len(t, schemas, 2)
	require.Equal(t, "subscription_one", schemas[0].SubscriptionName)
	require.Equal(t, "subscription_two", schemas[1].SubscriptionName)
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

func TestResetPreparePlanCollectsExternalScans(t *testing.T) {
	for _, nodeType := range []planpb.Node_NodeType{
		planpb.Node_EXTERNAL_SCAN,
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

func TestResetPreparePlanSkipsScanWithoutCatalogIdentity(t *testing.T) {
	queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeType: planpb.Node_EXTERNAL_SCAN,
			TableDef: &planpb.TableDef{
				Name:      "result_scan",
				TableType: "query_result",
			},
		}},
	}}}

	schemas, _, err := ResetPreparePlan(NewMockCompilerContext(false), queryPlan)
	require.NoError(t, err)
	require.Empty(t, schemas)
}

func TestResetPreparePlanPreservesScanSnapshot(t *testing.T) {
	snapshot := &planpb.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
	}
	queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:     planpb.Node_TABLE_SCAN,
			ObjRef:       &planpb.ObjectRef{SchemaName: "db", ObjName: "t"},
			TableDef:     &planpb.TableDef{Name: "t", DbId: 10, TblId: 20, Version: 30},
			ScanSnapshot: snapshot,
		}},
	}}}

	schemas, _, err := ResetPreparePlan(NewMockCompilerContext(false), queryPlan)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, snapshot, schemas[0].GetSnapshot())
	require.NotSame(t, snapshot, schemas[0].GetSnapshot())
}

func TestAppendPrepareSchemasKeepsDistinctSnapshots(t *testing.T) {
	base := &planpb.ObjectRef{SchemaName: "db", ObjName: "t", Db: 10, Obj: 20}
	first := DeepCopyObjectRef(base)
	first.Snapshot = &planpb.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
	}
	second := DeepCopyObjectRef(base)
	second.Snapshot = &planpb.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 43},
	}

	schemas := appendPrepareSchemas(nil, first, second, DeepCopyObjectRef(first))
	require.Len(t, schemas, 2)
}

func TestCollectPrepareViewSchemasPreservesIdentity(t *testing.T) {
	mock := NewMockCompilerContext(false)
	snapshot := &Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
		Tenant: &SnapshotTenant{TenantID: 11, TenantName: "publisher"},
	}
	viewKey, err := FormatViewDependencyKey("sub", "src_v", snapshot)
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
	require.Equal(t, snapshot, schemas[0].GetSnapshot())
}

func TestCollectPrepareViewSchemasKeepsLogicalSubscriptions(t *testing.T) {
	ctx := &viewDependencyCompilerContext{
		MockCompilerContext: NewMockCompilerContext(false),
		views:               []string{"subscription_one#v", "subscription_two#v"},
	}
	ctx.resolve = func(databaseName, tableName string, _ *Snapshot) (*ObjectRef, *TableDef, error) {
		return &ObjectRef{
				SchemaName:       "publisher_db",
				ObjName:          tableName,
				Obj:              20,
				SubscriptionName: databaseName,
				PubInfo:          &planpb.PubInfo{TenantId: 11},
			}, &TableDef{
				DbName: "publisher_db", Name: tableName,
				DbId: 10, TblId: 20, Version: 30,
			}, nil
	}

	schemas, err := collectPrepareViewSchemas(ctx)
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "subscription_one", schemas[0].SubscriptionName)
	require.Equal(t, "subscription_two", schemas[1].SubscriptionName)
}

func TestViewDependencySnapshotKeyValidation(t *testing.T) {
	key, err := FormatViewDependencyKey("db#part", "v#part", nil)
	require.NoError(t, err)

	databaseName, viewName, snapshot, err := ParseViewDependencyKey(key)
	require.NoError(t, err)
	require.Equal(t, "db#part", databaseName)
	require.Equal(t, "v#part", viewName)
	require.Nil(t, snapshot)

	for _, testCase := range []struct {
		key      string
		database string
		view     string
	}{
		{key: "db#v@snapshot=x", database: "db", view: "v@snapshot=x"},
		{key: "db@snapshot=x#v", database: "db@snapshot=x", view: "v"},
		{key: "db#v@ts=not-a-timestamp", database: "db", view: "v@ts=not-a-timestamp"},
	} {
		databaseName, viewName, snapshot, err = ParseViewDependencyKey(testCase.key)
		require.NoError(t, err)
		require.Equal(t, testCase.database, databaseName)
		require.Equal(t, testCase.view, viewName)
		require.Nil(t, snapshot)
	}

	_, _, _, err = ParseViewDependencyKey(viewDependencyKeyPrefix + "!")
	require.Error(t, err)
	_, _, _, err = ParseViewDependencyKey(viewDependencyKeyPrefix + "eA.!.")
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
	viewRef := &ObjectRef{
		SchemaName: "db",
		ObjName:    "v",
		Obj:        20,
	}
	viewDef := &TableDef{
		DbName:  "db",
		Name:    "v",
		DbId:    10,
		TblId:   20,
		Version: 30,
		ViewSql: &planpb.ViewDef{View: string(viewJSON)},
	}

	_, err = builder.bindView(
		bindCtx,
		viewDef,
		snapshot,
		viewRef,
		"db",
		"v",
	)
	require.NoError(t, err)
	require.Len(t, bindCtx.views, 1)

	databaseName, viewName, recorded, err := ParseViewDependencyKey(bindCtx.views[0])
	require.NoError(t, err)
	require.Equal(t, "db", databaseName)
	require.Equal(t, "v", viewName)
	require.Equal(t, snapshot.TS, recorded.TS)
	require.Equal(t, snapshot.Tenant, recorded.Tenant)
	require.Len(t, builder.qry.GetCatalogDependencies(), 1)
	dependency := builder.qry.GetCatalogDependencies()[0]
	require.Equal(t, "db", dependency.GetSchemaName())
	require.Equal(t, "v", dependency.GetObjName())
	require.Equal(t, int64(10), dependency.GetDb())
	require.Equal(t, int64(20), dependency.GetObj())
	require.Equal(t, int64(30), dependency.GetServer())
	require.Equal(t, snapshot, dependency.GetSnapshot())
	require.NotSame(t, snapshot, dependency.GetSnapshot())

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
			view:  viewDependencyKeyPrefix + "!",
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

func TestDecrementParamOrdinalRuleTraversesFunctionsAndLists(t *testing.T) {
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	first := param(1)
	second := param(3)
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Args: []*planpb.Expr{{
			Expr: &planpb.Expr_List{List: &planpb.ExprList{
				List: []*planpb.Expr{first, second},
			}},
		}},
	}}}
	rule := &decrementParamOrdinalRule{seen: make(map[*planpb.ParamRef]struct{})}

	_, err := rule.ApplyExpr(expr)
	require.NoError(t, err)
	require.Equal(t, int32(0), first.GetP().Pos)
	require.Equal(t, int32(2), second.GetP().Pos)

	_, err = rule.ApplyExpr(first)
	require.NoError(t, err)
	require.Equal(t, int32(0), first.GetP().Pos)

	expr.GetF().Args[0].GetList().List = append(
		expr.GetF().Args[0].GetList().List,
		param(0),
	)
	_, err = rule.ApplyExpr(expr)
	require.ErrorContains(t, err, "prepared parameter ordinal is not one-based")
}

func TestResetPreparePlanCollectsHiddenIndexSchemas(t *testing.T) {
	const hiddenTable = "__mo_index_hidden"
	mock := NewMockCompilerContext(false)
	mock.objects[hiddenTable] = &planpb.ObjectRef{
		Db:               10,
		Obj:              20,
		SchemaName:       "publisher_db",
		ObjName:          hiddenTable,
		SubscriptionName: "subscriber_alias",
		PubInfo:          &planpb.PubInfo{TenantId: 42},
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
					Indexes: []*planpb.IndexDef{nil, {
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

func TestRecordPreparedPluginDependenciesSurvivesScanRemoval(t *testing.T) {
	const hiddenTable = "__mo_index_hidden"
	snapshot := &planpb.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 7},
	}
	mock := NewMockCompilerContext(false)
	mock.objects[hiddenTable] = &planpb.ObjectRef{
		Db:         10,
		Obj:        20,
		SchemaName: "db",
		ObjName:    hiddenTable,
	}
	mock.tables[hiddenTable] = &planpb.TableDef{
		Name: hiddenTable, DbId: 10, TblId: 20, Version: 30,
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, mock, true, true)
	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		ObjRef: &planpb.ObjectRef{
			Db: 1, Obj: 2, SchemaName: "publisher_db", ObjName: "src",
			SubscriptionName: "subscriber_alias", PubInfo: &planpb.PubInfo{TenantId: 42},
		},
		TableDef: &planpb.TableDef{
			Name: "src", DbId: 1, TblId: 2, Version: 3,
			Indexes: []*planpb.IndexDef{nil, {
				IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
				IndexTableName: hiddenTable,
			}},
		},
		ScanSnapshot: snapshot,
	}

	require.NoError(t, builder.recordPreparedPluginDependencies(scanNode))
	require.NoError(t, builder.recordPreparedPluginDependencies(scanNode))
	require.Len(t, builder.qry.GetCatalogDependencies(), 2)
	require.Equal(t, "src", builder.qry.CatalogDependencies[0].ObjName)
	require.Equal(t, "subscriber_alias", builder.qry.CatalogDependencies[0].SubscriptionName)
	require.Equal(t, int32(42), builder.qry.CatalogDependencies[0].GetPubInfo().GetTenantId())
	require.Equal(t, int64(3), builder.qry.CatalogDependencies[0].Server)
	require.Equal(t, hiddenTable, builder.qry.CatalogDependencies[1].ObjName)
	require.Equal(t, "subscriber_alias", builder.qry.CatalogDependencies[1].SubscriptionName)
	require.Equal(t, int32(42), builder.qry.CatalogDependencies[1].GetPubInfo().GetTenantId())
	require.Equal(t, int64(30), builder.qry.CatalogDependencies[1].Server)

	encoded, err := builder.qry.Marshal()
	require.NoError(t, err)
	var decoded planpb.Query
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, builder.qry.CatalogDependencies, decoded.CatalogDependencies)

	builder.qry.Steps = []int32{0}
	builder.qry.Nodes = []*planpb.Node{scanNode}
	schemas, _, err := ResetPreparePlan(mock, &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: builder.qry},
	})
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, snapshot, schemas[0].GetSnapshot())
	require.Equal(t, snapshot, schemas[1].GetSnapshot())

	builder.qry.Nodes = []*planpb.Node{{
		NodeType: planpb.Node_FUNCTION_SCAN,
		TableDef: &planpb.TableDef{
			TblFunc: &planpb.TableFunction{Name: "plugin_search"},
		},
	}}
	schemas, _, err = ResetPreparePlan(mock, &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: builder.qry},
	})
	require.NoError(t, err)
	require.Len(t, schemas, 2)
	require.Equal(t, "src", schemas[0].ObjName)
	require.Equal(t, hiddenTable, schemas[1].ObjName)

	cloned := DeepCopyQuery(builder.qry)
	require.Equal(t, builder.qry.CatalogDependencies, cloned.CatalogDependencies)
	require.NotSame(t, builder.qry.CatalogDependencies[0], cloned.CatalogDependencies[0])
}

func TestPrepareSkipsNilIndexMetadata(t *testing.T) {
	mock := NewMockOptimizer(true)
	tableDef := mock.ctxt.tables["single_idx_t"]
	require.NotNil(t, tableDef)
	require.NotEmpty(t, tableDef.Indexes)
	tableDef.Indexes = append([]*planpb.IndexDef{nil}, tableDef.Indexes...)

	logicPlan, err := runOneStmt(mock, t,
		"prepare sparse_index_stmt from 'select val from single_idx_t where val = ?'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Len(t, prepare.ParamTypes, 1)
	require.NotEmpty(t, prepare.Schemas)
	foundBaseSchema := false
	for _, schema := range prepare.Schemas {
		if schema.GetObjName() == "single_idx_t" {
			foundBaseSchema = true
			break
		}
	}
	require.True(t, foundBaseSchema)
}

func TestResetPreparedSetMergesTransientCatalogDependencies(t *testing.T) {
	dependency := &planpb.ObjectRef{
		Db: 10, Obj: 20, SchemaName: "db", ObjName: "__mo_index_hidden", Server: 30,
	}
	preparePlan := &planpb.Plan{
		Plan: &planpb.Plan_Dcl{Dcl: &planpb.DataControl{
			DclType: planpb.DataControl_SET_VARIABLES,
			Control: &planpb.DataControl_SetVariables{
				SetVariables: &planpb.SetVariables{},
			},
		}},
	}
	transientQuery := &planpb.Query{
		CatalogDependencies: []*planpb.ObjectRef{dependency},
	}

	schemas, _, err := resetPreparePlan(
		NewMockCompilerContext(false), preparePlan, transientQuery)
	require.NoError(t, err)
	require.Equal(t, []*planpb.ObjectRef{dependency}, schemas)
}

func TestRecordPreparedPluginDependenciesIsAtomicOnResolutionFailure(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), true, true)
	scanNode := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		ObjRef: &planpb.ObjectRef{
			Db: 1, Obj: 2, SchemaName: "db", ObjName: "src",
		},
		TableDef: &planpb.TableDef{
			Name: "src", DbId: 1, TblId: 2, Version: 3,
			Indexes: []*planpb.IndexDef{{
				IndexAlgo:      catalog.MOIndexFullTextAlgo.ToString(),
				IndexTableName: "__missing_hidden",
			}},
		},
	}

	require.Error(t, builder.recordPreparedPluginDependencies(scanNode))
	require.Empty(t, builder.qry.GetCatalogDependencies())
}

func TestResetPreparePlanResetsWindowParameterOrder(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	window := &planpb.WindowSpec{
		WindowFunc: &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{paramExpr(9)}}}},
		PartitionBy: []*planpb.Expr{
			paramExpr(3),
		},
		OrderBy: []*planpb.OrderBySpec{{Expr: paramExpr(7)}},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: paramExpr(1)},
			End:   &planpb.FrameBound{Val: paramExpr(5)},
		},
	}
	queryPlan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeId:      0,
				NodeType:    planpb.Node_WINDOW,
				WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}},
			}},
		}},
	}

	_, paramTypes, err := ResetPreparePlan(NewMockCompilerContext(false), queryPlan)
	require.NoError(t, err)
	require.Len(t, paramTypes, 5)
	require.Equal(t, int32(4), window.WindowFunc.GetF().Args[0].GetP().Pos)
	require.Equal(t, int32(1), window.PartitionBy[0].GetP().Pos)
	require.Equal(t, int32(3), window.OrderBy[0].Expr.GetP().Pos)
	require.Equal(t, int32(0), window.Frame.Start.Val.GetP().Pos)
	require.Equal(t, int32(2), window.Frame.End.Val.GetP().Pos)
}

func TestResetParamRefRuleReplacesWindowParameters(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	windowFunc, err := BindFuncExprImplByPlanExpr(context.Background(), "abs", []*planpb.Expr{paramExpr(0)})
	require.NoError(t, err)
	window := &planpb.WindowSpec{
		WindowFunc:  windowFunc,
		PartitionBy: []*planpb.Expr{paramExpr(1)},
		OrderBy:     []*planpb.OrderBySpec{{Expr: paramExpr(2)}},
		Frame: &planpb.FrameClause{
			Start: &planpb.FrameBound{Val: paramExpr(3)},
			End:   &planpb.FrameBound{Val: paramExpr(4)},
		},
	}
	node := &planpb.Node{
		NodeId:      0,
		NodeType:    planpb.Node_WINDOW,
		WinSpecList: []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}},
	}
	query := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	params := []*planpb.Expr{
		makePlan2Int64ConstExprWithType(10),
		makePlan2Int64ConstExprWithType(11),
		makePlan2Int64ConstExprWithType(12),
		makePlan2Int64ConstExprWithType(13),
		makePlan2Int64ConstExprWithType(14),
	}
	visitor := NewVisitPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{NewResetParamRefRule(context.Background(), params)})

	require.NoError(t, visitor.Visit(context.Background()))
	require.Equal(t, int64(10), window.WindowFunc.GetF().Args[0].GetLit().GetI64Val())
	require.Equal(t, int64(11), window.PartitionBy[0].GetLit().GetI64Val())
	require.Equal(t, int64(12), window.OrderBy[0].Expr.GetLit().GetI64Val())
	require.Equal(t, int64(13), window.Frame.Start.Val.GetLit().GetI64Val())
	require.Equal(t, int64(14), window.Frame.End.Val.GetLit().GetI64Val())
}

func TestResetParamRefRulePreservesAggregateConfig(t *testing.T) {
	param := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "abs", []*planpb.Expr{param})
	require.NoError(t, err)
	expr.GetF().AggConfig = []byte{1, 2, 3}
	expr.GetF().AggConfigType = planpb.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER

	rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		makePlan2Int64ConstExprWithType(7),
	})
	rewritten, err := rule.ApplyExpr(expr)
	require.NoError(t, err)
	require.Equal(t, int64(7), rewritten.GetF().Args[0].GetLit().GetI64Val())
	require.Equal(t, []byte{1, 2, 3}, rewritten.GetF().AggConfig)
	require.Equal(
		t,
		planpb.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		rewritten.GetF().AggConfigType,
	)

	rewritten.GetF().AggConfig[0] = 9
	require.Equal(t, byte(1), expr.GetF().AggConfig[0])
}

func TestResetParamRefRuleRebindsTypedAncestors(t *testing.T) {
	ctx := context.Background()
	param := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	inner, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
		param,
		makePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	outer, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{inner})
	require.NoError(t, err)

	rule := NewResetParamRefRule(ctx, []*planpb.Expr{
		makePlan2Float64ConstExprWithType(-1.5),
	})
	rewritten, err := rule.ApplyExpr(outer)
	require.NoError(t, err)
	require.Equal(t, types.T_float64, types.T(rewritten.Typ.Id))
	require.Equal(t, types.T_float64, types.T(rewritten.GetF().Args[0].Typ.Id))
}

func TestFillValuesOfParamsInPlanUsesBinaryRuntimeType(t *testing.T) {
	ctx := context.Background()
	param := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
		}
	}

	selectParam, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{param()})
	require.NoError(t, err)
	query := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{selectParam},
		}},
	}}}

	decimal := types.New(types.T_decimal64, 2, 1)
	filled, err := FillValuesOfParamsInPlan(ctx, query, []any{ParamValue{
		Value:          "-1.5",
		RuntimeType:    decimal,
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	result := filled.GetQuery().Nodes[0].ProjectList[0]
	require.Equal(t, int32(types.T_decimal64), result.Typ.Id)
	boundArg := result.GetF().Args[0]
	require.Equal(t, int64(-15), boundArg.GetLit().GetDecimal64Val().A)
	require.Nil(t, boundArg.GetF())
	require.Equal(t, int32(types.T_decimal64), boundArg.Typ.Id)

	stringFilled, err := FillValuesOfParamsInPlan(ctx, query, []any{ParamValue{
		Value:          "-1.5",
		RuntimeType:    types.T_text.ToType(),
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	stringResult := stringFilled.GetQuery().Nodes[0].ProjectList[0]
	require.Equal(t, int32(types.T_decimal64), stringResult.Typ.Id)
	require.Nil(t, stringResult.GetF().Args[0].GetF())
	require.Equal(t, int64(-15), stringResult.GetF().Args[0].GetLit().GetDecimal64Val().A)

	sleepParam, err := BindFuncExprImplByPlanExpr(ctx, "sleep", []*planpb.Expr{param()})
	require.NoError(t, err)
	sleepQuery := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{sleepParam},
		}},
	}}}
	sleepFilled, err := FillValuesOfParamsInPlan(ctx, sleepQuery, []any{ParamValue{
		Value:          "0.05",
		RuntimeType:    types.T_float64.ToType(),
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	sleepResult := sleepFilled.GetQuery().Nodes[0].ProjectList[0]
	require.Equal(t, int32(types.T_uint8), sleepResult.Typ.Id)
	require.Equal(t, int32(types.T_float64), sleepResult.GetF().Args[0].Typ.Id)
	require.Nil(t, sleepResult.GetF().Args[0].GetF())

	sleepFloat, err := FillValuesOfParamsInPlan(ctx, sleepQuery, []any{ParamValue{
		Value:          "0.05",
		RuntimeType:    types.T_float32.ToType(),
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	sleepFloatArg := sleepFloat.GetQuery().Nodes[0].ProjectList[0].GetF().Args[0]
	require.Equal(t, int32(types.T_float64), sleepFloatArg.Typ.Id)
	require.Equal(t, "cast", sleepFloatArg.GetF().Func.GetObjName())
	require.Equal(t, int32(types.T_float32), sleepFloatArg.GetF().Args[0].Typ.Id)

	sleepDecimal, err := FillValuesOfParamsInPlan(ctx, sleepQuery, []any{ParamValue{
		Value:          "0.05",
		RuntimeType:    types.New(types.T_decimal64, 3, 2),
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	sleepDecimalArg := sleepDecimal.GetQuery().Nodes[0].ProjectList[0].GetF().Args[0]
	require.Equal(t, int32(types.T_float64), sleepDecimalArg.Typ.Id)
	require.Equal(t, "cast", sleepDecimalArg.GetF().Func.GetObjName())
	require.Equal(t, int32(types.T_decimal64), sleepDecimalArg.GetF().Args[0].Typ.Id)

	direct := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{param()},
		}},
	}}}
	filled, err = FillValuesOfParamsInPlan(ctx, direct, []any{ParamValue{
		Value:          "-12345678901234567890.123456789",
		RuntimeType:    types.New(types.T_decimal128, 29, 9),
		HasRuntimeType: true,
	}})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), filled.GetQuery().Nodes[0].ProjectList[0].Typ.Id)
	require.Equal(t, int32(29), filled.GetQuery().Nodes[0].ProjectList[0].Typ.Width)
	require.Equal(t, int32(9), filled.GetQuery().Nodes[0].ProjectList[0].Typ.Scale)
}

func TestFillValuesOfParamsMaterializesInferredTextNumericLiteral(t *testing.T) {
	ctx := context.Background()
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	absExpr, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{param(0)})
	require.NoError(t, err)
	eqExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
		makePlan2Int64ConstExprWithType(1),
		param(1),
	})
	require.NoError(t, err)
	query := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{absExpr, eqExpr},
		}},
	}}}

	filled, err := FillValuesOfParamsInPlan(ctx, query, []any{
		ParamValue{Value: "-1.5", RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
		ParamValue{Value: "1"},
	})
	require.NoError(t, err)
	bound := filled.GetQuery().Nodes[0].ProjectList[1].GetF().Args[1]
	require.Equal(t, int64(1), bound.GetLit().GetI64Val())
}

func TestFillValuesOfParamsSpecializationTracksBinaryExecutionDomains(t *testing.T) {
	ctx := context.Background()
	param := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
		}
	}
	absExpr, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{param()})
	require.NoError(t, err)
	query := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{absExpr},
		}},
	}}}

	_, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
		ParamValue{Value: "-1.5", IsBinaryProtocol: true},
	})
	require.NoError(t, err)
	require.True(t, specialized, "COM_STMT text numeric values must rebind ABS")

	sleepExpr, err := BindFuncExprImplByPlanExpr(ctx, "sleep", []*planpb.Expr{param()})
	require.NoError(t, err)
	sleepQuery := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{sleepExpr},
		}},
	}}}
	sleepFilled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, sleepQuery, []any{
		ParamValue{Value: "0.05", IsBinaryProtocol: true},
	})
	require.NoError(t, err)
	require.True(t, specialized, "COM_STMT text fractional values must rebind SLEEP")
	require.NotNil(t, sleepFilled.GetQuery().Nodes[0].ProjectList[0].GetF())

	_, specialized, err = FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
		ParamValue{Value: "-1.5", RuntimeType: types.T_float64.ToType(), HasRuntimeType: true},
	})
	require.NoError(t, err)
	require.True(t, specialized)

	direct := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_VALUE_SCAN,
			ProjectList: []*planpb.Expr{param()},
		}},
	}}}
	_, specialized, err = FillValuesOfParamsInPlanWithSpecialization(ctx, direct, []any{
		ParamValue{Value: "text", IsBinaryProtocol: true},
	})
	require.NoError(t, err)
	require.False(t, specialized, "same-domain text execution should reuse the cached plan")

	_, specialized, err = FillValuesOfParamsInPlanWithSpecialization(ctx, direct, []any{
		ParamValue{Value: "5", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
	})
	require.NoError(t, err)
	require.True(t, specialized, "direct numeric result metadata must be specialized")
}

func TestFillValuesOfParamsUsesNumericPrefixForPreparedCommonType(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal128, 20, 4)
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	decimalColumn := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&decimalType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
		}
	}
	makeQuery := func(t *testing.T, name string, args []*planpb.Expr) *planpb.Plan {
		t.Helper()
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
			StmtType: planpb.Query_SELECT,
			Steps:    []int32{0},
			Nodes: []*planpb.Node{{
				NodeType:    planpb.Node_VALUE_SCAN,
				ProjectList: []*planpb.Expr{expr},
			}},
		}}}
	}
	findNumericPrefixCast := func(expr *planpb.Expr) *planpb.Expr {
		var visit func(*planpb.Expr) *planpb.Expr
		visit = func(current *planpb.Expr) *planpb.Expr {
			if current == nil {
				return nil
			}
			if fn := current.GetF(); fn != nil {
				if fn.Func.GetObjName() == "cast" && current.Typ.Charset == 255 {
					return current
				}
				for _, arg := range fn.Args {
					if found := visit(arg); found != nil {
						return found
					}
				}
			}
			if list := current.GetList(); list != nil {
				for _, item := range list.List {
					if found := visit(item); found != nil {
						return found
					}
				}
			}
			return nil
		}
		return visit(expr)
	}
	collectNumericOperandTypes := func(expr *planpb.Expr) []types.T {
		fn := expr.GetF()
		require.NotNil(t, fn)
		result := make([]types.T, 0, len(fn.Args))
		for _, arg := range fn.Args {
			if list := arg.GetList(); list != nil {
				for _, item := range list.List {
					result = append(result, types.T(item.Typ.Id))
				}
				continue
			}
			result = append(result, types.T(arg.Typ.Id))
		}
		return result
	}

	for _, name := range []string{"coalesce", "greatest", "least"} {
		t.Run(name, func(t *testing.T) {
			query := makeQuery(t, name, []*planpb.Expr{param(0), decimalColumn()})
			cached, err := query.Marshal()
			require.NoError(t, err)
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
				ParamValue{
					Value:               "9007199254740992.0001tail",
					IsBinaryProtocol:    true,
					EnableNumericPrefix: true,
				},
			})
			require.NoError(t, err)
			require.True(t, specialized, filled.String())
			after, err := query.Marshal()
			require.NoError(t, err)
			require.Equal(t, cached, after, "execute-time specialization must not mutate the cached plan")

			result := filled.GetQuery().Nodes[0].ProjectList[0]
			require.True(t, types.T(result.Typ.Id).IsDecimal(), result.String())
			prefixCast := findNumericPrefixCast(result)
			require.NotNil(t, prefixCast, result.String())
			require.Equal(t, "9007199254740992.0001tail", prefixCast.GetF().Args[0].GetLit().GetSval())
		})
	}

	t.Run("nested common value keeps exact outer comparison", func(t *testing.T) {
		common, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", []*planpb.Expr{param(0), decimalColumn()})
		require.NoError(t, err)
		query := makeQuery(t, "=", []*planpb.Expr{common, decimalColumn()})
		values := []any{ParamValue{
			Value: "9007199254740992.0001tail", IsBinaryProtocol: true, EnableNumericPrefix: true,
		}}
		require.True(t, PreparedPlanNeedsNumericPrefixSpecialization(query, values))

		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, values)
		require.NoError(t, err)
		require.True(t, specialized, filled.String())
		predicate := filled.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "=", predicate.GetF().GetFunc().GetObjName(), predicate.String())
		require.Equal(t, []types.T{types.T_decimal128, types.T_decimal128},
			collectNumericOperandTypes(predicate), predicate.String())
		require.NotNil(t, findNumericPrefixCast(predicate), predicate.String())
	})

	t.Run("SQL eligibility excludes float peer with decimal variable kind", func(t *testing.T) {
		floatType := types.T_float32.ToType()
		floatColumn := &planpb.Expr{
			Typ:  makePlan2Type(&floatType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2}},
		}
		query := makeQuery(t, "=", []*planpb.Expr{floatColumn, param(0)})
		require.False(t, PreparedPlanNeedsNumericPrefixSpecialization(query, []any{ParamValue{
			Value: "1.2345678", PrepareParamKind: vector.PrepareParamDecimal, EnableNumericPrefix: true,
		}}))
	})

	for _, test := range []struct {
		name   string
		fnName string
		args   func() []*planpb.Expr
		values []any
	}{
		{
			name: "comparison", fnName: "=",
			args: func() []*planpb.Expr { return []*planpb.Expr{decimalColumn(), param(0)} },
			values: []any{ParamValue{
				Value: "9007199254740992.0001tail", IsBinaryProtocol: true, EnableNumericPrefix: true,
			}},
		},
		{
			name: "between", fnName: "between",
			args: func() []*planpb.Expr { return []*planpb.Expr{decimalColumn(), param(0), param(1)} },
			values: []any{
				ParamValue{Value: "9007199254740992.0000low", IsBinaryProtocol: true, EnableNumericPrefix: true},
				ParamValue{Value: "9007199254740992.0002high", IsBinaryProtocol: true, EnableNumericPrefix: true},
			},
		},
		{
			name: "in list", fnName: "in",
			args: func() []*planpb.Expr {
				return []*planpb.Expr{decimalColumn(), {
					Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{param(0), param(1)}}},
				}}
			},
			values: []any{
				ParamValue{Value: "9007199254740992.0001first", IsBinaryProtocol: true, EnableNumericPrefix: true},
				ParamValue{Value: "9007199254740992.0002second", IsBinaryProtocol: true, EnableNumericPrefix: true},
			},
		},
		{
			name: "not in list", fnName: "not_in",
			args: func() []*planpb.Expr {
				return []*planpb.Expr{decimalColumn(), {
					Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{param(0), param(1)}}},
				}}
			},
			values: []any{
				ParamValue{Value: "9007199254740992.0001first", IsBinaryProtocol: true, EnableNumericPrefix: true},
				ParamValue{Value: "9007199254740992.0002second", IsBinaryProtocol: true, EnableNumericPrefix: true},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := makeQuery(t, test.fnName, test.args())
			cached, err := query.Marshal()
			require.NoError(t, err)
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, test.values)
			require.NoError(t, err)
			require.True(t, specialized, filled.String())
			after, err := query.Marshal()
			require.NoError(t, err)
			require.Equal(t, cached, after)
			predicate := filled.GetQuery().Nodes[0].ProjectList[0]
			require.Equal(t, int32(types.T_bool), predicate.Typ.Id, predicate.String())
			require.NotNil(t, findNumericPrefixCast(predicate), predicate.String())
		})
	}

	t.Run("parameter on in left keeps typed list", func(t *testing.T) {
		first, err := makePlan2DecimalExprWithType(ctx, "1.25")
		require.NoError(t, err)
		second, err := makePlan2DecimalExprWithType(ctx, "2.50")
		require.NoError(t, err)
		list := &planpb.Expr{
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{first, second}}},
		}
		query := makeQuery(t, "in", []*planpb.Expr{param(0), list})
		preparedPredicate := query.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "in", preparedPredicate.GetF().GetFunc().GetObjName(), preparedPredicate.String())
		require.True(t, isImplicitPreparedParamCast(preparedPredicate.GetF().Args[0]), preparedPredicate.String())

		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "1.50tail", IsBinaryProtocol: true, EnableNumericPrefix: true},
		})
		require.NoError(t, err)
		require.True(t, specialized, filled.String())
		predicate := filled.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "in", predicate.GetF().GetFunc().GetObjName(), predicate.String())
		require.NotNil(t, findNumericPrefixCast(predicate), predicate.String())
		require.Len(t, collectNumericOperandTypes(predicate), 3)
	})

	t.Run("parameter on between left keeps decimal bounds", func(t *testing.T) {
		lower := decimalColumn()
		upper := decimalColumn()
		upper.GetCol().ColPos = 1
		query := makeQuery(t, "between", []*planpb.Expr{param(0), lower, upper})
		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{
				Value: "9007199254740992.0001tail", IsBinaryProtocol: true, EnableNumericPrefix: true,
			},
		})
		require.NoError(t, err)
		require.True(t, specialized, filled.String())
		predicate := filled.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "between", predicate.GetF().GetFunc().GetObjName(), predicate.String())
		require.Equal(t, []types.T{types.T_decimal128, types.T_decimal128, types.T_decimal128},
			collectNumericOperandTypes(predicate), predicate.String())
		require.NotNil(t, findNumericPrefixCast(predicate), predicate.String())
	})

	t.Run("all strings stay text", func(t *testing.T) {
		query := makeQuery(t, "greatest", []*planpb.Expr{param(0), param(1)})
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "10", IsBinaryProtocol: true, EnableNumericPrefix: true},
			ParamValue{Value: "2", IsBinaryProtocol: true, EnableNumericPrefix: true},
		})
		require.NoError(t, err)
		result := filled.GetQuery().Nodes[0].ProjectList[0]
		require.True(t, types.T(result.Typ.Id).IsMySQLString(), result.String())
		require.Nil(t, findNumericPrefixCast(result))
	})

	t.Run("string peer keeps common value function in text domain", func(t *testing.T) {
		stringType := types.T_varchar.ToType()
		stringPeer := &planpb.Expr{
			Typ:  makePlan2Type(&stringType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1}},
		}
		query := makeQuery(t, "greatest", []*planpb.Expr{param(0), decimalColumn(), stringPeer})
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "12.5tail", IsBinaryProtocol: true, EnableNumericPrefix: true},
		})
		require.NoError(t, err)
		result := filled.GetQuery().Nodes[0].ProjectList[0]
		require.True(t, types.T(result.Typ.Id).IsMySQLString(), result.String())
		require.Nil(t, findNumericPrefixCast(result), result.String())
	})

	t.Run("explicit string cast remains a string boundary", func(t *testing.T) {
		varcharType := types.T_varchar.ToType()
		explicitString, err := appendExplicitCastBeforeExpr(ctx, param(0), makePlan2Type(&varcharType))
		require.NoError(t, err)
		query := makeQuery(t, "greatest", []*planpb.Expr{explicitString, decimalColumn()})
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "12.5tail", IsBinaryProtocol: true, EnableNumericPrefix: true},
		})
		require.NoError(t, err)
		result := filled.GetQuery().Nodes[0].ProjectList[0]
		require.True(t, types.T(result.Typ.Id).IsMySQLString(), result.String())
		require.Nil(t, findNumericPrefixCast(result), result.String())
	})

	t.Run("decimal parameter kind establishes numeric context", func(t *testing.T) {
		query := makeQuery(t, "coalesce", []*planpb.Expr{param(0), param(1)})
		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{
				Value: "12.5", PrepareParamKind: vector.PrepareParamDecimal, EnableNumericPrefix: true,
			},
			ParamValue{
				Value: int64(2), PrepareParamKind: vector.PrepareParamInteger, EnableNumericPrefix: true,
			},
		})
		require.NoError(t, err)
		require.True(t, specialized, filled.String())
		result := filled.GetQuery().Nodes[0].ProjectList[0]
		require.True(t, types.T(result.Typ.Id).IsDecimal(), result.String())
		require.Nil(t, findNumericPrefixCast(result),
			"native decimal and integer parameters should be materialized without a text-prefix cast")
	})

	t.Run("in list uses one float domain", func(t *testing.T) {
		list := &planpb.Expr{
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{param(0), param(1)}}},
		}
		query := makeQuery(t, "in", []*planpb.Expr{decimalColumn(), list})
		preparedPredicate := query.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "in", preparedPredicate.GetF().GetFunc().GetObjName(), preparedPredicate.String())
		require.Len(t, preparedPredicate.GetF().Args[1].GetList().List, 2)

		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "9007199254740992.0001tail", EnableNumericPrefix: true},
			ParamValue{
				Value: "9007199254740992.0002", PrepareParamKind: vector.PrepareParamFloat,
				EnableNumericPrefix: true,
			},
		})
		require.NoError(t, err)
		require.True(t, specialized, filled.String())
		predicate := filled.GetQuery().Nodes[0].ProjectList[0]
		require.Equal(t, "in", predicate.GetF().GetFunc().GetObjName(), predicate.String())
		require.Equal(t, []types.T{types.T_float64, types.T_float64, types.T_float64},
			collectNumericOperandTypes(predicate))
	})

	t.Run("runtime source categories", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			value   any
			kind    vector.PrepareParamKind
			wantOID types.T
		}{
			{name: "boolean", value: true, kind: vector.PrepareParamBoolean, wantOID: types.T_decimal128},
			{name: "integer", value: int64(42), kind: vector.PrepareParamInteger, wantOID: types.T_decimal128},
			{name: "decimal", value: "42.25", kind: vector.PrepareParamDecimal, wantOID: types.T_decimal128},
			{name: "float", value: "42.25", kind: vector.PrepareParamFloat, wantOID: types.T_float64},
		} {
			t.Run(test.name, func(t *testing.T) {
				query := makeQuery(t, "coalesce", []*planpb.Expr{param(0), decimalColumn()})
				filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
					ParamValue{
						Value: test.value, PrepareParamKind: test.kind, EnableNumericPrefix: true,
					},
				})
				require.NoError(t, err)
				require.True(t, specialized)
				result := filled.GetQuery().Nodes[0].ProjectList[0]
				require.Equal(t, int32(test.wantOID), result.Typ.Id, result.String())
			})
		}
	})

	t.Run("protocol gate", func(t *testing.T) {
		query := makeQuery(t, "coalesce", []*planpb.Expr{param(0), decimalColumn()})
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, query, []any{
			ParamValue{Value: "12.5tail", IsBinaryProtocol: true},
		})
		require.NoError(t, err)
		require.Nil(t, findNumericPrefixCast(filled.GetQuery().Nodes[0].ProjectList[0]))
	})
}

func TestPreparedNumericCommonType(t *testing.T) {
	expr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{Typ: makePlan2Type(&typ)}
	}
	for _, test := range []struct {
		name      string
		operands  []types.Type
		wantOID   types.T
		wantWidth int32
		wantScale int32
		wantOK    bool
	}{
		{
			name: "uint64 keeps full integral capacity",
			operands: []types.Type{
				types.New(types.T_decimal128, 20, 4), types.T_uint64.ToType(),
			},
			wantOID: types.T_decimal128, wantWidth: 24, wantScale: 4, wantOK: true,
		},
		{
			name: "decimal256 combines independent integral and scale maxima",
			operands: []types.Type{
				types.New(types.T_decimal256, 38, 0), types.New(types.T_decimal256, 38, 38),
			},
			wantOID: types.T_decimal256, wantWidth: 76, wantScale: 38, wantOK: true,
		},
		{
			name: "decimal256 physical overflow uses float",
			operands: []types.Type{
				types.New(types.T_decimal256, 76, 0), types.New(types.T_decimal256, 76, 76),
			},
			wantOID: types.T_float64, wantWidth: types.T_float64.ToType().Width, wantOK: true,
		},
		{
			name: "real operand is approximate boundary",
			operands: []types.Type{
				types.New(types.T_decimal64, 8, 2), types.T_float32.ToType(),
			},
			wantOID: types.T_float64, wantWidth: types.T_float64.ToType().Width, wantOK: true,
		},
		{
			name: "string blocks numeric common domain",
			operands: []types.Type{
				types.New(types.T_decimal64, 8, 2), types.T_varchar.ToType(),
			},
			wantOK: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			operands := make([]*planpb.Expr, len(test.operands))
			for i := range test.operands {
				operands[i] = expr(test.operands[i])
			}
			got, ok := preparedNumericCommonType(operands)
			require.Equal(t, test.wantOK, ok)
			if !test.wantOK {
				return
			}
			require.Equal(t, test.wantOID, got.Oid)
			require.Equal(t, test.wantWidth, got.Width)
			require.Equal(t, test.wantScale, got.Scale)
		})
	}
}

func TestVisitPlanDeduplicatesAliasedWindowPartitionExpr(t *testing.T) {
	newPlan := func(t *testing.T) (*planpb.Plan, *planpb.WindowSpec, *planpb.Node) {
		t.Helper()
		paramExpr := func(pos int32) *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.GenNewBindTag()
		inputID := builder.appendNode(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}, bindCtx)
		window := &planpb.WindowSpec{
			WindowFunc:  paramExpr(1),
			PartitionBy: []*planpb.Expr{paramExpr(3)},
		}
		bindCtx.windows = []*planpb.Expr{{Expr: &planpb.Expr_W{W: window}}}
		windowID, err := builder.appendWindowNode(bindCtx, inputID, nil)
		require.NoError(t, err)
		windowNode := builder.qry.Nodes[windowID]
		partitionNode := builder.qry.Nodes[windowNode.Children[0]]
		require.Equal(t, planpb.Node_PARTITION, partitionNode.NodeType)
		require.Same(t, window.PartitionBy[0], partitionNode.OrderBy[0].Expr)
		return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
			Steps: []int32{windowID},
			Nodes: builder.qry.Nodes,
		}}}, window, partitionNode
	}

	t.Run("collects once", func(t *testing.T) {
		queryPlan, _, _ := newPlan(t)
		rule := NewGetParamRule()
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		rule.SetParamOrder()
		require.Equal(t, map[int]int{1: 0, 3: 1}, rule.params)
	})

	t.Run("resets the shared partition expression once", func(t *testing.T) {
		queryPlan, window, partitionNode := newPlan(t)
		rule := NewResetParamOrderRule(map[int]int{1: 0, 3: 1})
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		require.Equal(t, int32(0), window.WindowFunc.GetP().Pos)
		require.Equal(t, int32(1), window.PartitionBy[0].GetP().Pos)
		require.Equal(t, int32(1), partitionNode.OrderBy[0].Expr.GetP().Pos)
	})

	t.Run("replaces the shared partition expression once", func(t *testing.T) {
		queryPlan, window, partitionNode := newPlan(t)
		rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
			makePlan2Int64ConstExprWithType(7),
			makePlan2Int64ConstExprWithType(11),
			nil,
			makePlan2Int64ConstExprWithType(13),
		})
		require.NoError(t, NewVisitPlan(queryPlan, []VisitPlanRule{rule}).Visit(context.Background()))
		require.Equal(t, int64(11), window.WindowFunc.GetLit().GetI64Val())
		require.Equal(t, int64(13), window.PartitionBy[0].GetLit().GetI64Val())
		require.Same(t, partitionNode.OrderBy[0].Expr, window.PartitionBy[0])
	})
}
