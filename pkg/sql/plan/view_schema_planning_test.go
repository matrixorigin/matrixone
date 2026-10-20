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

package plan

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func buildViewSchemaTestPlan(t *testing.T, request *ViewSchemaRequest, query string, prepared bool) (*Plan, error) {
	t.Helper()
	statements, err := mysql.Parse(t.Context(), query, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	return request.BuildPlan(statements[0], prepared)
}

func TestViewSchemaPlanningUsesRealSubtreesAfterDescriptionMemo(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "middle_v", "select n_name as label from nation")
	f.addDefinition(t, "root_v", "create view root_v (explicit_name) as select label from middle_v")
	request := f.request(t)
	viewSchemaTestResult(t, request, "root_v").Release()
	viewSchemaTestResult(t, request, "root_v").Release()
	require.NotEmpty(t, request.nestedMemo)
	hits := request.hits
	for _, prepared := range []bool{false, true} {
		p, err := buildViewSchemaTestPlan(t, request, "select explicit_name as result_name from root_v", prepared)
		require.NoError(t, err)
		require.Equal(t, prepared, p.IsPrepare)
		columns := GetResultColumnsFromPlan(p)
		require.Len(t, columns, 1)
		require.Equal(t, "result_name", columns[0].Name)
		require.EqualValues(t, types.T_varchar, columns[0].Typ.Id)
		var scans []string
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_TABLE_SCAN {
				require.NotEqual(t, catalog.SystemViewRel, node.TableDef.TableType)
				scans = append(scans, node.TableDef.Name)
			}
		}
		require.Equal(t, []string{"nation"}, scans, "a description-only View scan is not executable")
		var dependencies []string
		for _, ref := range p.GetQuery().CatalogDependencies {
			dependencies = append(dependencies, ref.ObjName)
		}
		require.ElementsMatch(t, []string{"root_v", "middle_v", "nation"}, dependencies)
		require.Equal(t, hits, request.hits, "planning must not consume description memo")
		if prepared {
			schemas, _, err := ResetPreparePlan(f.compiler, p)
			require.NoError(t, err)
			var names []string
			for _, ref := range schemas {
				names = append(names, ref.ObjName)
			}
			require.ElementsMatch(t, dependencies, names)
		}
		// Independent metadata copies do not alias the physical catalog.
		columns[0].Typ.Width++
		require.Equal(t, "stale", f.compiler.tables["root_v"].Cols[0].Name)
	}
	request.Close()
	require.Zero(t, f.generation.Used())
	require.False(t, f.generation.Closed())
}

func TestViewSchemaPlanningPreservesCorrelatedMainOutput(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addDefinition(t, "root_v", `create view root_v (outer_key, inner_key) as
		select n1.n_nationkey,
			(select (select n3.n_regionkey from nation n3 where n3.n_nationkey = n1.n_nationkey)
			 from nation n2 where n2.n_nationkey = n1.n_nationkey)
		from nation n1`)
	request := f.request(t)
	p, err := buildViewSchemaTestPlan(t, request, "select outer_key, inner_key from root_v", false)
	require.NoError(t, err)
	request.Close()
	require.NotNil(t, p.GetQuery().Correlation)
	require.NoError(t, p.GetQuery().ValidateCorrelation())
	copied := &Plan{Plan: &planpb.Plan_Query{Query: DeepCopyQuery(p.GetQuery())}}
	require.NoError(t, copied.GetQuery().ValidateCorrelation())
	for _, plan := range []*Plan{p, copied} {
		columns := GetResultColumnsFromPlan(plan)
		require.Len(t, columns, 2)
		require.Equal(t, "outer_key", columns[0].Name)
		require.Equal(t, "inner_key", columns[1].Name)
		for _, column := range columns {
			require.EqualValues(t, types.T_int32, column.Typ.Id)
		}
	}
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaPlanningExecutionObjectAccount(t *testing.T) {
	for _, tc := range []struct {
		name         string
		account      uint32
		pub          *planpb.PubInfo
		view         bool
		subscription string
	}{
		{"local", 42, nil, false, ""},
		{"publisher", 7, nil, false, ""},
		{"system publisher", 0, nil, false, ""},
		{"existing publisher", 7, &planpb.PubInfo{TenantId: 7, XXX_unrecognized: []byte{0x10, 0x07}}, false, ""},
		{"publisher intermediate view", 7, nil, true, ""},
		{"subscription entry view", 7, &planpb.PubInfo{TenantId: 7}, true, "sub"},
		{"invalid subscription entry view", 7, &planpb.PubInfo{TenantId: 7}, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			name := "nation"
			def := f.compiler.tables[name]
			if tc.view {
				name = "inner_v"
				def = f.addView(t, name, "select n_nationkey from nation")
			}
			catalogObject := f.compiler.objects[name]
			catalogObject.PubInfo = tc.pub
			catalogObject.SubscriptionName = tc.subscription
			before := DeepCopyObjectRef(catalogObject)
			request := f.request(t)
			require.NoError(t, request.open())
			state := newViewSchemaDerivation(request)
			defer state.close()
			state.planning = true
			obj, _, err := state.compiler.record(catalogObject, def,
				&Snapshot{Tenant: &SnapshotTenant{TenantID: tc.account}}, "tpch", name)
			require.NoError(t, err)
			require.Equal(t, before, catalogObject, "catalog-owned identity must not be mutated")
			require.NotSame(t, catalogObject, obj)
			require.Len(t, state.planDependencies, 1)
			require.Equal(t, tc.subscription, obj.SubscriptionName)
			if tc.view && tc.pub != nil {
				_, _, err := viewSchemaSubscriptionContext(t.Context(), obj, nil, "tpch")
				if tc.subscription == "" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				} else {
					require.NoError(t, err)
				}
			}
			if tc.account != 42 {
				require.NotNil(t, state.planDependencies[0].PubInfo)
				require.EqualValues(t, tc.account, state.planDependencies[0].PubInfo.TenantId)
			}
			if tc.account == 42 || tc.view && tc.pub == nil {
				require.Nil(t, obj.PubInfo, "a publisher-owned intermediate View is not a subscription entry")
			} else {
				require.NotNil(t, obj.PubInfo, "execution needs the same account as dependency validation")
				require.EqualValues(t, tc.account, obj.PubInfo.TenantId)
				require.Equal(t, obj.PubInfo, state.planDependencies[0].PubInfo)
				require.NotSame(t, obj.PubInfo, state.planDependencies[0].PubInfo)
				if tc.pub != nil {
					require.Equal(t, tc.pub.XXX_unrecognized, obj.PubInfo.XXX_unrecognized)
				}
			}
		})
	}
}

func TestViewSchemaPlanningNumericProbeDatabase(t *testing.T) {
	for _, tc := range []struct{ name, scope, qualified, want string }{
		{"view default", "view_db", "", "view_db"},
		{"explicit qualifier", "view_db", "explicit_db", "explicit_db"},
		{"statement default", "", "", "statement_db"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			statementDB := "statement_db"
			f.compiler.defaultDatabase = &statementDB
			var lookups []string
			f.compiler.resolve = func(_ context.Context, database, _ string, _ *Snapshot) error {
				lookups = append(lookups, database)
				return nil
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, f.compiler, true, true)
			ctx := NewBindContext(builder, nil)
			ctx.defaultDatabase = tc.scope
			sources := []numericProjectionSourceInfo{{sourceName: "nation", sourceSchema: tc.qualified}}
			resolveNumericProjectionSourceOutputs(builder, &tree.Select{}, ctx, sources, make(map[*tree.Select]bool))
			require.Equal(t, []string{tc.want}, lookups)
			require.True(t, sources[0].outputKnown)
			require.Contains(t, sources[0].outputNames, "n_nationkey")
			require.Equal(t, tc.qualified, sources[0].sourceSchema)
		})
	}
}

func TestViewSchemaPlanningInternalSQLContext(t *testing.T) {
	for _, mode := range []string{"legacy", "describe", "planning"} {
		t.Run(mode, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			proc := f.compiler.GetProcess()
			type preservedKey struct{}
			ctx, cancel := context.WithCancelCause(defines.AttachAccountId(t.Context(), 42))
			defer cancel(nil)
			ctx = context.WithValue(ctx, preservedKey{}, "preserved")
			var state *viewSchemaDerivation
			if mode != "legacy" {
				state = &viewSchemaDerivation{planning: mode == "planning"}
				ctx = context.WithValue(ctx, viewSchemaContextKey{}, state)
			}
			previous := proc.GetTopContext()
			proc.ReplaceTopCtx(ctx)
			defer proc.ReplaceTopCtx(previous)
			cause := errors.New("catalog read cancelled")
			calls := 0
			setPlanTestGlobalVariable(t, proc.GetService(), moruntime.InternalSQLExecutor,
				&captureSQLExecutor{exec: func(execCtx context.Context, _ string, opts executor.Options) (executor.Result, error) {
					calls++
					require.Nil(t, execCtx.Value(viewSchemaContextKey{}), "internal SQL is not another user root")
					require.Equal(t, "preserved", execCtx.Value(preservedKey{}))
					account, err := defines.GetAccountId(execCtx)
					require.NoError(t, err)
					require.EqualValues(t, 7, account)
					require.EqualValues(t, 7, opts.AccountID())
					cancel(cause)
					require.ErrorIs(t, context.Cause(execCtx), cause)
					return executor.Result{}, cause
				}},
			)
			// The interface wrapper deliberately selects the runtime executor,
			// not MockCompilerContext's private in-memory executor.
			compiler := struct{ CompilerContext }{f.compiler}
			_, err := runSqlWithSnapshot(compiler, "select 1", &Snapshot{Tenant: &SnapshotTenant{TenantID: 7}})
			require.ErrorIs(t, err, cause)
			require.Equal(t, 1, calls)
			require.Same(t, ctx, proc.GetTopContext())
			if state != nil {
				require.Same(t, state, ctx.Value(viewSchemaContextKey{}), "the outer binder keeps its marker")
			}
		})
	}
}

func TestViewSchemaPlanningCTASPreservesBoundaryDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, body, defaultSQL, wantDefault string
		nullable                            bool
	}{
		{"literal", "select n_nationkey as k from nation", "7", "0", false},
		{"expression default", "select n_nationkey as k from nation", "(7)", "(7)", false},
		{"expression column", "select n_nationkey + 1 as k from nation", "7", "", false},
		{"outer join", "select n.n_nationkey as k from region r left join nation n on r.r_regionkey=n.n_regionkey", "7", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			source := f.compiler.tables["nation"].Cols[0]
			source.Typ.NotNullable = true
			source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: tc.defaultSQL}
			f.addView(t, "middle_v", tc.body)
			f.addDefinition(t, "root_v", "create view root_v (explicit_name) as select k from middle_v")
			request := f.request(t)
			// A completed schema description is the metadata contract, while CTAS
			// deliberately applies its independent default policy to that output.
			result := viewSchemaTestResult(t, request, "root_v")
			columns, err := result.Columns()
			require.NoError(t, err)
			result.Release()
			p, err := buildViewSchemaTestPlan(t, request, "create table derived_target as select explicit_name from root_v", false)
			require.NoError(t, err)
			column := p.GetDdl().GetCreateTable().TableDef.Cols[0]
			require.Equal(t, "explicit_name", column.Name)
			require.Equal(t, columns[0].Typ.Id, column.Typ.Id)
			require.Equal(t, tc.nullable, column.Default.NullAbility)
			require.Equal(t, tc.wantDefault, column.Default.OriginString)
			require.NotEmpty(t, p.GetDdl().GetCreateTable().CreateAsSelectSql, "ordinary CTAS executes its existing follow-up INSERT")
			prepared, err := buildViewSchemaTestPlan(t, request, "create table derived_target as select explicit_name from root_v", true)
			require.NoError(t, err)
			require.NotNil(t, prepared.GetDdl().Query, "prepared CTAS retains its source query for reuse validation")
			dependencyNames := make([]string, 0, len(prepared.GetDdl().Query.CatalogDependencies))
			for _, ref := range prepared.GetDdl().Query.CatalogDependencies {
				dependencyNames = append(dependencyNames, ref.ObjName)
			}
			wantDependencies := []string{"root_v", "middle_v", "nation"}
			if tc.nullable {
				wantDependencies = append(wantDependencies, "region")
			}
			require.ElementsMatch(t, wantDependencies, dependencyNames)
			request.Close()
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaPlanningPrepareCapturesTransitiveSchemas(t *testing.T) {
	for _, textPrepare := range []bool{false, true} {
		t.Run(strconv.FormatBool(textPrepare), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "middle_v", "select n_nationkey as k from nation")
			f.addView(t, "root_v", "select k from middle_v")
			const sourceSQL = "select k from root_v where k > ?"
			statements, err := mysql.Parse(t.Context(), sourceSQL, 1)
			require.NoError(t, err)
			defer statements[0].Free()
			var input tree.Statement = &tree.PrepareStmt{Name: "derived_plan", Stmt: statements[0]}
			if textPrepare {
				input = &tree.PrepareString{Name: "derived_plan", Sql: sourceSQL}
			}
			request := f.request(t)
			p, err := request.BuildPlan(input, false)
			require.NoError(t, err)
			request.Close()
			prepared := p.GetDcl().GetPrepare()
			require.NotNil(t, prepared)
			require.True(t, prepared.Plan.IsPrepare)
			require.Len(t, prepared.ParamTypes, 1)
			require.Equal(t, "k", GetResultColumnsFromPlan(prepared.Plan)[0].Name)
			names := make([]string, 0, len(prepared.Schemas))
			for _, ref := range prepared.Schemas {
				names = append(names, ref.ObjName)
				def := f.compiler.tables[ref.ObjName]
				require.EqualValues(t, def.TblId, ref.Obj)
				require.EqualValues(t, def.Version, ref.Server)
			}
			require.ElementsMatch(t, []string{"root_v", "middle_v", "nation"}, names)
			require.Len(t, prepared.Plan.GetQuery().CatalogDependencies, 3)
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaPlanningDatabaseProbePreservesErrorsAndSnapshot(t *testing.T) {
	for _, failure := range []error{nil, moerr.NewBadDB(t.Context(), "source_db"), context.Canceled} {
		f := newViewSchemaTestFixture(t)
		f.compiler.snapshot = &Snapshot{Tenant: &planpb.SnapshotTenant{TenantID: 42}}
		f.compiler.GetDatabaseIdFunc = func(name string, snapshot *Snapshot) (uint64, error) {
			require.Equal(t, "source_db", name)
			require.Same(t, f.compiler.snapshot, snapshot)
			return 7, failure
		}
		f.compiler.DatabaseExistsFunc = func(string, *Snapshot) bool {
			t.Fatal("the bool-only probe loses publisher identity and error causes")
			return false
		}
		request := f.request(t)
		require.NoError(t, request.open())
		state := newViewSchemaDerivation(request)
		exists, err := state.compiler.CheckViewDatabase("source_db", f.compiler.snapshot)
		state.close()
		if moerr.IsMoErrCode(failure, moerr.ErrBadDB) {
			require.False(t, exists)
			require.NoError(t, err)
		} else {
			require.Equal(t, failure == nil, exists)
			require.ErrorIs(t, err, failure)
		}
	}
}

func TestViewSchemaPlanningPublisherIdentityCopiesAreIndependent(t *testing.T) {
	original := &ObjectRef{PubInfo: &planpb.PubInfo{TenantId: 42, XXX_unrecognized: []byte{0x10, 0x01}}}
	copy := DeepCopyObjectRef(original)
	copy.PubInfo.TenantId = 99
	copy.PubInfo.XXX_unrecognized[1] = 2
	require.EqualValues(t, 42, original.PubInfo.TenantId)
	require.Equal(t, []byte{0x10, 0x01}, original.PubInfo.XXX_unrecognized)
}

func TestViewSchemaPlanningRejectsInvalidDefinitionsWithoutFallback(t *testing.T) {
	for _, kind := range []string{"missing context", "unstable star", "missing source", "broken definition"} {
		t.Run(kind, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			view := f.addView(t, "root_v", "select n_name from nation")
			switch kind {
			case "missing context":
				view.ViewSql.View = `{"Stmt":"create view root_v as select n_name from nation","DefaultDatabase":"tpch"}`
			case "unstable star":
				view = f.addView(t, "root_v", "select * from nation")
			case "missing source":
				delete(f.compiler.tables, "nation")
			case "broken definition":
				view.ViewSql.View = "{"
			}
			require.NotEmpty(t, view.Cols, "the stale fallback would otherwise be available")
			request := f.request(t)
			p, err := buildViewSchemaTestPlan(t, request, "select * from root_v limit 0", false)
			require.Error(t, err)
			require.Nil(t, p)
			require.Zero(t, f.generation.Used())
			require.Nil(t, f.compiler.GetContext().Value(viewSchemaContextKey{}))
		})
	}
}

type viewSchemaAuthorizationCompiler struct {
	*viewSchemaTestCompiler
	subscriptionProbes int
}

func (c *viewSchemaAuthorizationCompiler) GetSubscriptionMeta(database string, snapshot *Snapshot) (*SubscriptionMeta, error) {
	c.subscriptionProbes++
	return c.viewSchemaTestCompiler.GetSubscriptionMeta(database, snapshot)
}

func buildViewSchemaAuthorizationPlan(t *testing.T, request *ViewSchemaRequest, mode string, queries ...string) (*Plan, error) {
	t.Helper()
	query := "select id from tpch.root_v"
	if len(queries) != 0 {
		query = queries[0]
	}
	return buildViewSchemaAuthorizationPlanWithLower(t, request, mode, query, 1)
}

func buildViewSchemaAuthorizationPlanWithLower(t *testing.T, request *ViewSchemaRequest, mode, query string, lower int64) (*Plan, error) {
	t.Helper()
	statements, err := mysql.Parse(t.Context(), query, lower)
	require.NoError(t, err)
	defer statements[0].Free()
	var input tree.Statement = statements[0]
	switch mode {
	case "text":
		input = &tree.PrepareString{Name: "auth_plan", Sql: query}
	case "typed":
		input = &tree.PrepareStmt{Name: "auth_plan", Stmt: statements[0]}
	}
	return request.BuildPlan(input, mode == "direct")
}

func TestViewSchemaPlanningAuthorizesRootsBeforeLookup(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		t.Run(mode, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name as id from nation")
			failure := errors.New("denied root")
			f.authErr = failure
			databaseProbes := 0
			getDatabaseID := f.compiler.GetDatabaseIdFunc
			f.compiler.GetDatabaseIdFunc = func(database string, snapshot *Snapshot) (uint64, error) {
				databaseProbes++
				return getDatabaseID(database, snapshot)
			}
			authorize := f.binding.Authorize
			f.binding.Authorize = func(ctx context.Context, database, name string, snapshot *Snapshot) error {
				require.Nil(t, ctx.Value(viewSchemaContextKey{}), "authorization catalog SQL must not inherit the private planning mode")
				require.Equal(t, "tpch", database)
				require.Equal(t, "root_v", name, "nested execution authorization belongs to ViewReferences")
				err := authorize(ctx, database, name, snapshot)
				// Only the first check denies: optional metadata inference must
				// not swallow it and retry the policy into an allow decision.
				f.authErr = nil
				return err
			}
			compiler := &viewSchemaAuthorizationCompiler{viewSchemaTestCompiler: f.compiler}
			f.binding.Compiler = compiler
			request := f.request(t)
			p, err := buildViewSchemaAuthorizationPlan(t, request, mode)
			require.ErrorIs(t, err, failure)
			require.Nil(t, p)
			require.Zero(t, f.compiler.lookups.Load())
			require.Zero(t, databaseProbes)
			require.Zero(t, compiler.subscriptionProbes)
			require.EqualValues(t, 1, f.authorizes.Load())
			f.authErr = nil
			p, err = buildViewSchemaAuthorizationPlan(t, request, mode)
			require.NoError(t, err)
			require.NotNil(t, p)
			if mode == "text" || mode == "typed" {
				p = p.GetDcl().GetPrepare().Plan
			}
			require.NotEmpty(t, p.GetQuery().ViewReferences)
		})
	}
}

func TestViewSchemaPlanningAuthorizationSnapshot(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, historicalAllowed := range []bool{true, false} {
			for _, shape := range []struct {
				name, query string
				aggregate   bool
			}{
				{"view", "select id from tpch.root_v", false},
				{"cte", "with q as (select id from tpch.root_v) select id from q", false},
				{"source hint", "select id from tpch.root_v {snapshot='daily'}", false},
				{"cte hint", "with q as (select id from tpch.root_v) select id from q {snapshot='daily'}", false},
				{"cte sum", "with q as (select id from tpch.root_v) select sum(id) from q", true},
				{"cte avg", "with q as (select id from tpch.root_v) select avg(id) from q", true},
				{"cte chain sum", "with q as (select id from root_v), r as (select id from q) select sum(id) from r", true},
				{"ctas", "create table historical_target as select id from tpch.root_v", false},
				{"ctas sum", "create table historical_target as with q as (select id from tpch.root_v) select sum(id) from q", true},
				{"ctas hint", "create table historical_target as select id from tpch.root_v {snapshot='daily'}", false},
			} {
				t.Run(mode+"/"+strconv.FormatBool(historicalAllowed)+"/"+shape.name, func(t *testing.T) {
					f := newViewSchemaTestFixture(t)
					if shape.aggregate {
						f.addView(t, "root_v", "select n_nationkey as id from nation")
					} else {
						f.addView(t, "root_v", "select n_name as id from nation")
					}
					snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1234}, Tenant: &SnapshotTenant{TenantID: 42}}
					f.compiler.snapshot = snapshot
					if shape.name == "source hint" || shape.name == "ctas hint" {
						f.compiler.snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 4321}, Tenant: &SnapshotTenant{TenantID: 42}}
						f.compiler.namedSnapshot = snapshot
					} else if shape.name == "cte hint" {
						// The CTE's declaration snapshot has precedence in the
						// existing binder; optional inference must defer to it.
						f.compiler.namedSnapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 4321}, Tenant: &SnapshotTenant{TenantID: 42}}
					}
					failure := errors.New("snapshot domain denied")
					authorizations := 0
					f.binding.Authorize = func(_ context.Context, database, name string, actual *Snapshot) error {
						authorizations++
						require.Equal(t, "tpch", database)
						require.Equal(t, "root_v", name)
						if !historicalAllowed {
							if actual == nil {
								return nil
							}
							return failure
						}
						if actual == nil {
							return failure
						}
						require.Equal(t, snapshot, actual)
						return nil
					}
					f.compiler.resolve = func(_ context.Context, _, name string, actual *Snapshot) error {
						if name == "historical_target" || name == catalog.MOForeignKeys {
							require.Nil(t, actual, "CTAS target existence check stays in the current catalog")
							return nil
						}
						require.True(t, historicalAllowed, "denied historical lookup must not reach the catalog")
						require.Equal(t, snapshot, actual, "authorization and lookup must share the same snapshot tenant/TS")
						return nil
					}
					p, err := buildViewSchemaAuthorizationPlan(t, f.request(t), mode, shape.query)
					if !historicalAllowed {
						require.ErrorIs(t, err, failure)
						require.Nil(t, p)
						require.Zero(t, f.compiler.lookups.Load())
						return
					}
					require.NoError(t, err)
					require.NotNil(t, p)
					require.Positive(t, authorizations)
					require.Positive(t, f.compiler.lookups.Load())
					if mode == "text" || mode == "typed" {
						for _, ref := range p.GetDcl().GetPrepare().Schemas {
							if p.GetDcl().GetPrepare().Plan.GetDdl() != nil && ref.ObjName == "" && ref.SchemaName == "" {
								require.Equal(t, &ObjectRef{}, ref, "preserve the existing account-wide DDL invalidation marker")
								continue
							}
							if ref.ObjName == "historical_target" || ref.SchemaName == catalog.MO_CATALOG && ref.ObjName == catalog.MOForeignKeys {
								require.Nil(t, ref.Snapshot, "CTAS target/catalog negotiation stays current-domain")
								continue
							}
							require.Equal(t, snapshot, ref.Snapshot, "source PREPARE reference %s.%s", ref.SchemaName, ref.ObjName)
						}
						p = p.GetDcl().GetPrepare().Plan
					}
					query := p.GetQuery()
					if p.GetDdl() != nil {
						query = p.GetDdl().Query
						if mode == "ordinary" {
							// Ordinary CTAS executes its follow-up INSERT separately.
							require.NotEmpty(t, p.GetDdl().GetCreateTable().CreateAsSelectSql)
							return
						}
					}
					for _, ref := range query.CatalogDependencies {
						require.Equal(t, snapshot, ref.Snapshot, "historical-only query must not depend on current DDL")
					}
					columns := GetResultColumnsFromPlan(&Plan{Plan: &planpb.Plan_Query{Query: query}})
					require.Len(t, columns, 1)
					if !shape.aggregate {
						require.EqualValues(t, types.T_varchar, columns[0].Typ.Id)
					}
				})
			}
		}
	}
}

func TestViewSchemaPlanningCteDeclarationNames(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, tc := range []struct{ name, query string }{
			{"outer cte", "with q as (select id from tpch.root_v), r as (select * from q) select sum(id) from (with q as (select id from tpch.blocked_v) select * from r) d"},
			{"qualified control", "with q as (select id from tpch.root_v), r as (select * from tpch.root_v) select sum(id) from (with q as (select id from tpch.blocked_v) select * from r) d"},
			{"chain", "with q as (select id from tpch.root_v), r as (select * from q), s as (select * from r) select sum(id) from (with q as (select id from tpch.blocked_v), r as (select * from q) select * from s) d"},
			{"body with", "with q as (select id from tpch.blocked_v), r as (with q as (select id from tpch.root_v) select * from q) select sum(id) from r"},
			{"scalar", "with q as (select id from tpch.root_v), r as (select * from q) select (with q as (select id from tpch.blocked_v) select sum(id) from r) + 1"},
			{"later declaration", "with r as (select * from root_v), root_v as (select id from tpch.blocked_v) select sum(id) from r"},
			{"later chain", "with r as (select * from root_v), s as (select * from r), root_v as (select id from tpch.blocked_v) select sum(id) from s"},
			{"later scalar", "with r as (select * from root_v), root_v as (select id from tpch.blocked_v) select (select sum(id) from r) + 1"},
			{"later qualified control", "with r as (select * from tpch.root_v), root_v as (select id from tpch.blocked_v) select sum(id) from r"},
			{"ancestor mask", "with r as (with s as (select * from root_v) select * from s), root_v as (select id from tpch.blocked_v) select sum(id) from r"},
			{"ancestor scalar", "with r as (with s as (select * from root_v) select * from s), root_v as (select id from tpch.blocked_v) select (select sum(id) from r) + 1"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newViewSchemaTestFixture(t)
				f.addView(t, "root_v", "select n_nationkey as id from nation")
				f.addView(t, "blocked_v", "select n_nationkey as id from nation")
				authorizations := 0
				f.binding.Authorize = func(_ context.Context, database, name string, _ *Snapshot) error {
					authorizations++
					if database != "tpch" || name != "root_v" {
						require.Equal(t, "root_v", name)
						return errors.New("unreferenced root denied: " + database + "." + name)
					}
					return nil
				}
				f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
					require.NotEqual(t, "blocked_v", name, "unused inner CTE must not be probed")
					return nil
				}
				p, err := buildViewSchemaAuthorizationPlan(t, f.request(t), mode, tc.query)
				require.NoError(t, err)
				require.Positive(t, authorizations)
				if mode == "text" || mode == "typed" {
					p = p.GetDcl().GetPrepare().Plan
				}
				require.Len(t, GetResultColumnsFromPlan(p), 1)
				for _, ref := range p.GetQuery().CatalogDependencies {
					require.NotEqual(t, "blocked_v", ref.ObjName)
				}
			})
		}
	}
}

func TestViewSchemaPlanningDuplicateCteDoesNotProbe(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, query := range []string{
			"with q as (select id from tpch.blocked_v), q as (select 1) select * from q",
			"select * from (with q as (select id from tpch.blocked_v), q as (select 1) select * from q) d",
		} {
			t.Run(mode+"/"+query, func(t *testing.T) {
				f := newViewSchemaTestFixture(t)
				p, err := buildViewSchemaAuthorizationPlan(t, f.request(t), mode, query)
				require.Nil(t, p)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrSyntaxError), "%v", err)
				require.Zero(t, f.authorizes.Load())
				require.Zero(t, f.compiler.lookups.Load())
			})
		}
	}
}

func TestViewSchemaPlanningCteNameCase(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, tc := range []struct {
			name, query string
			lower       int64
			denied      bool
		}{
			{"physical", "with root_v as (select id from tpch.blocked_v) select sum(id) from Root_V", 0, false},
			{"quoted physical", "with root_v as (select id from tpch.blocked_v) select sum(id) from `Root_V`", 0, false},
			{"qualified", "with root_v as (select id from tpch.blocked_v) select sum(id) from tpch.Root_V", 0, false},
			{"scalar", "with root_v as (select id from tpch.blocked_v) select (select sum(id) from Root_V) + 1", 0, false},
			{"cte", "with root_v as (select id from tpch.Root_V) select sum(id) from root_v", 0, false},
			{"uppercase declaration", "with Root_V as (select id from tpch.Root_V) select sum(id) from root_v", 0, false},
			{"folded control", "with root_v as (select id from tpch.blocked_v) select sum(id) from Root_V", 1, true},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newViewSchemaTestFixture(t)
				f.compiler.lower = tc.lower
				f.compiler.ResolveVariableFunc = func(name string, system, global bool) (interface{}, error) {
					if name == "lower_case_table_names" {
						return tc.lower, nil
					}
					if name == "sql_mode" {
						return "", nil
					}
					return nil, nil
				}
				f.addView(t, "Root_V", "select n_nationkey as id from nation")
				f.addView(t, "blocked_v", "select n_nationkey as id from nation")
				failure := errors.New("actual root denied")
				authorizations := 0
				f.binding.Authorize = func(_ context.Context, db, name string, _ *Snapshot) error {
					authorizations++
					if db != "tpch" || name != "Root_V" {
						return failure
					}
					return nil
				}
				f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
					require.NotEqual(t, "blocked_v", name)
					return nil
				}
				p, err := buildViewSchemaAuthorizationPlanWithLower(t, f.request(t), mode, tc.query, tc.lower)
				if tc.denied {
					require.ErrorIs(t, err, failure)
					require.Nil(t, p)
					require.Zero(t, f.compiler.lookups.Load())
					return
				}
				require.NoError(t, err)
				require.Positive(t, authorizations)
				if mode == "text" || mode == "typed" {
					p = p.GetDcl().GetPrepare().Plan
				}
				require.Len(t, GetResultColumnsFromPlan(p), 1)
				for _, ref := range p.GetQuery().CatalogDependencies {
					require.NotEqual(t, "blocked_v", ref.ObjName)
				}
			})
		}
	}
}

func TestViewSchemaPlanningNumericCteScope(t *testing.T) {
	for _, mode := range []string{"inherited", "declaration", "explicit snapshot"} {
		t.Run(mode, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			statementDB := "statement_db"
			f.compiler.defaultDatabase = &statementDB
			builder := NewQueryBuilder(planpb.Query_SELECT, f.compiler, true, true)
			ctx := NewBindContext(builder, nil)
			ctx.defaultDatabase = "view_db"
			ctx.lower = 1
			ctx.snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1234}, Tenant: &SnapshotTenant{TenantID: 42}}
			statements, err := mysql.Parse(t.Context(), "with q as (select n_nationkey from nation) select sum(n_nationkey) from q", 1)
			require.NoError(t, err)
			defer statements[0].Free()
			cte := statements[0].(*tree.Select).With.CTEs[0]
			ctx.numericCteByName = map[string]*CTERef{"q": {ast: cte}}
			ctx.cteByName = ctx.numericCteByName
			ctx.numericTableProjectionTypes = map[string][]Type{"q": {{Id: int32(types.T_int64)}}}
			wantDatabase, wantSnapshot := ctx.defaultDatabase, ctx.snapshot
			if mode != "inherited" {
				declaration := NewBindContext(builder, nil)
				declaration.defaultDatabase = "declaration_db"
				declaration.lower = 0
				ref := &CTERef{ast: cte, declarationCtx: declaration}
				ctx.cteByName = map[string]*CTERef{"q": ref}
				wantDatabase = declaration.defaultDatabase
				if mode == "explicit snapshot" {
					ref.snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 5678}, Tenant: &SnapshotTenant{TenantID: 17}}
					wantSnapshot = ref.snapshot
				}
			}
			f.compiler.resolve = func(_ context.Context, database, _ string, snapshot *Snapshot) error {
				require.Equal(t, wantDatabase, database)
				require.Equal(t, wantSnapshot, snapshot)
				return nil
			}
			seedNumericCteProjectionTypes(builder, ctx)
			require.Positive(t, f.compiler.lookups.Load(), "exercise the temporary numeric propagation context")
		})
	}
}

func TestViewSchemaPlanningNumericProbeUnknownSnapshot(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	builder := NewQueryBuilder(planpb.Query_SELECT, f.compiler, true, true)
	sources := []numericProjectionSourceInfo{{sourceName: "nation", sourceSchema: "tpch"}}
	resolveNumericProjectionSourceOutputs(builder, &tree.Select{}, nil, sources, make(map[*tree.Select]bool))
	require.False(t, sources[0].outputKnown)
	require.Zero(t, f.compiler.lookups.Load(), "no binding context means no known snapshot domain")
}

func TestViewSchemaPlanningPseudoTableAuthorization(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, tc := range []struct {
			name, query string
			physical    bool
		}{
			{"no from", "select 1 as one", false},
			{"dual", "select 1 as one from dual", false},
			{"upper dual", "select 1 as one from DUAL", false},
			{"quoted dual", "select 1 as one from `dual`", false},
			{"aliased dual", "select 1 as one from dual d", false},
			{"cte named dual", "with dual as (select id from tpch.root_v) select 1 as one from dual", false},
			{"qualified dual", "select 1 as one from tpch.dual", true},
			{"quoted qualified dual", "select 1 as one from tpch.`dual`", true},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				f := newViewSchemaTestFixture(t)
				f.addView(t, "root_v", "select n_nationkey as id from nation")
				failure := errors.New("physical roots denied")
				f.authErr = failure
				p, err := buildViewSchemaAuthorizationPlan(t, f.request(t), mode, tc.query)
				require.Zero(t, f.compiler.lookups.Load())
				if tc.physical {
					require.ErrorIs(t, err, failure)
					require.Nil(t, p)
					require.EqualValues(t, 1, f.authorizes.Load())
					return
				}
				require.NoError(t, err)
				require.Zero(t, f.authorizes.Load(), "a placeholder must not be authorized as a physical root")
				if mode == "text" || mode == "typed" {
					require.Empty(t, p.GetDcl().GetPrepare().Schemas)
					p = p.GetDcl().GetPrepare().Plan
				}
				require.Empty(t, p.GetQuery().CatalogDependencies)
				for _, node := range p.GetQuery().Nodes {
					require.NotEqual(t, planpb.Node_TABLE_SCAN, node.NodeType)
				}
				columns := GetResultColumnsFromPlan(p)
				require.Len(t, columns, 1)
				require.Equal(t, "one", columns[0].Name)
			})
		}
	}
}

func TestViewSchemaPlanningDeniedRootDoesNotWaitForCatalog(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		t.Run(mode, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name as id from nation")
			failure := errors.New("denied before blocked catalog")
			f.authErr = failure
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			entered := make(chan struct{}, 1)
			f.compiler.resolve = func(ctx context.Context, _, _ string, _ *Snapshot) error {
				entered <- struct{}{}
				<-ctx.Done()
				return context.Cause(ctx)
			}
			request := NewViewSchemaRequest(ctx, f.provider)
			defer request.Close()
			const query = "select id from tpch.root_v"
			statements, err := mysql.Parse(ctx, query, 1)
			require.NoError(t, err)
			defer statements[0].Free()
			var input tree.Statement = statements[0]
			if mode == "text" {
				input = &tree.PrepareString{Name: "blocked_auth", Sql: query}
			} else if mode == "typed" {
				input = &tree.PrepareStmt{Name: "blocked_auth", Stmt: statements[0]}
			}
			done := make(chan error, 1)
			go func() {
				p, err := request.BuildPlan(input, mode == "direct")
				if p != nil {
					err = errors.New("denied planning returned a plan")
				}
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, failure)
			case <-entered:
				cancel()
				<-done
				t.Fatal("denied user root entered the blocking catalog resolver")
			case <-ctx.Done():
				<-done
				t.Fatal("denied root failed to finish without catalog access")
			}
			require.Zero(t, f.compiler.lookups.Load())
		})
	}
}

func TestViewSchemaPlanningFailureRestoresOwnership(t *testing.T) {
	for _, panicFailure := range []bool{false, true} {
		t.Run(strconv.FormatBool(panicFailure), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name from nation")
			request := f.request(t)
			failure := errors.New("source catalog failure")
			f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
				if name == "nation" {
					if panicFailure {
						panic(failure)
					}
					return failure
				}
				return nil
			}
			if panicFailure {
				require.PanicsWithValue(t, failure, func() { _, _ = buildViewSchemaTestPlan(t, request, "select * from root_v", false) })
			} else {
				p, err := buildViewSchemaTestPlan(t, request, "select * from root_v", false)
				require.Nil(t, p)
				require.ErrorIs(t, err, failure)
			}
			require.Zero(t, f.generation.Used())
			require.Nil(t, f.compiler.GetContext().Value(viewSchemaContextKey{}))
			f.compiler.resolve = nil
			p, err := buildViewSchemaTestPlan(t, request, "select * from root_v", false)
			require.NoError(t, err)
			request.Close()
			require.NotEmpty(t, GetResultColumnsFromPlan(p))
			require.Zero(t, f.generation.Used())
			require.EqualValues(t, 1, f.closes.Load())
		})
	}
}

func TestViewSchemaPlanningFailureRestoresSnapshot(t *testing.T) {
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, panicFailure := range []bool{false, true} {
			for _, inherited := range []bool{false, true} {
				t.Run(mode+"/panic="+strconv.FormatBool(panicFailure)+"/inherited="+strconv.FormatBool(inherited), func(t *testing.T) {
					f := newViewSchemaTestFixture(t)
					f.addView(t, "root_v", "with q as (select n_name from nation) select n_name from q")
					f.addView(t, "current_v", "select n_name from nation")
					historical := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1234}, Tenant: &SnapshotTenant{TenantID: 42}}
					f.compiler.namedSnapshot = historical
					if inherited {
						f.compiler.snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 5678}, Tenant: &SnapshotTenant{TenantID: 42}}
					}
					before := f.compiler.snapshot
					request := f.request(t)
					failure := errors.New("CTE catalog failed")
					failures := 0
					f.compiler.resolve = func(_ context.Context, _, name string, actual *Snapshot) error {
						if name == "nation" && IsSnapshotValid(f.compiler.GetSnapshot()) && f.compiler.GetSnapshot().TS.Equal(*historical.TS) {
							failures++
							require.Equal(t, historical, actual)
							if panicFailure {
								panic(failure)
							}
							return failure
						}
						return nil
					}
					build := func() {
						p, err := buildViewSchemaAuthorizationPlan(t, request, mode, "select * from root_v {snapshot='daily'}")
						require.Nil(t, p)
						require.ErrorIs(t, err, failure)
					}
					if panicFailure {
						require.PanicsWithValue(t, failure, func() {
							_, _ = buildViewSchemaAuthorizationPlan(t, request, mode, "select * from root_v {snapshot='daily'}")
						})
					} else {
						build()
					}
					require.Positive(t, failures, "fail only after CTE binding publishes its compiler snapshot")
					if panicFailure {
						require.Equal(t, 1, failures)
					}
					require.Same(t, before, f.compiler.GetSnapshot(), "panic/error must restore the caller domain")
					require.Zero(t, f.generation.Used())
					require.Nil(t, f.compiler.GetContext().Value(viewSchemaContextKey{}))
					f.compiler.resolve = func(_ context.Context, _, _ string, actual *Snapshot) error {
						require.Equal(t, before, actual)
						return nil
					}
					authorizations := 0
					f.binding.Authorize = func(_ context.Context, _, name string, actual *Snapshot) error {
						authorizations++
						require.Equal(t, "current_v", name)
						require.Equal(t, before, actual)
						return nil
					}
					p, err := buildViewSchemaAuthorizationPlan(t, request, mode, "select * from current_v")
					require.NoError(t, err)
					require.Positive(t, authorizations)
					if mode == "text" || mode == "typed" {
						p = p.GetDcl().GetPrepare().Plan
					}
					for _, ref := range p.GetQuery().CatalogDependencies {
						require.Equal(t, before, ref.Snapshot)
					}
					require.Same(t, before, f.compiler.GetSnapshot())
					request.Close()
					require.Zero(t, f.generation.Used())
					require.EqualValues(t, 1, f.closes.Load())
				})
			}
		}
	}
}

func TestViewSchemaPlanningCancelAndClose(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	entered := make(chan struct{})
	f.compiler.resolve = func(ctx context.Context, _, name string, _ *Snapshot) error {
		if name == "nation" {
			close(entered)
			<-ctx.Done()
			return context.Cause(ctx)
		}
		return nil
	}
	statements, err := mysql.Parse(t.Context(), "select * from root_v", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	request := f.request(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p, err := request.BuildPlan(statements[0], false)
		require.Nil(t, p)
		require.ErrorIs(t, err, ErrViewSchemaClosed)
	}()
	viewSchemaTestWait(t, entered)
	p, err := request.BuildPlan(statements[0], false)
	require.Nil(t, p)
	require.ErrorIs(t, err, ErrViewSchemaBusy)
	request.Close()
	viewSchemaTestWait(t, done)
	require.Zero(t, f.generation.Used())
	require.False(t, f.generation.Closed())
	require.EqualValues(t, 1, f.closes.Load())
}

func TestViewSchemaPlanningRejectsUnsupportedStatements(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	request := f.request(t)
	for _, query := range []string{"drop table nation", "create table target (a int)", "prepare rejected from 'drop table nation'", "prepare rejected from 'create table target (a int)'", "prepare rejected from ''"} {
		p, err := buildViewSchemaTestPlan(t, request, query, false)
		require.Nil(t, p)
		require.Error(t, err)
	}
	require.EqualValues(t, 1, f.opens.Load(), "only parsing a PREPARE body needs the scoped binding")
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaPlanningDefaultEntryRetainsLegacyCompatibility(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	view := f.addView(t, "root_v", "select n_name from nation")
	view.ViewSql.View = `{"Stmt":"create view root_v as select n_name from nation","DefaultDatabase":"tpch"}`
	stmts, err := mysql.Parse(t.Context(), "select * from root_v", 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	_, err = BuildPlan(f.compiler, stmts[0], false)
	require.NoError(t, err, "this task must not activate strict legacy rejection by default")
	request := f.request(t)
	_, err = request.BuildPlan(stmts[0], false)
	require.Error(t, err)
}

func TestViewSchemaIgnoresPersistedColumnSnapshot(t *testing.T) {
	for _, count := range []int{0, 1, MaxViewMetadataColumns + 1} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			view := f.addView(t, "root_v", "select n_name as label from nation")
			view.Cols = make([]*ColDef, count)
			for i := range view.Cols {
				view.Cols[i] = &ColDef{Name: "old", Typ: Type{Id: int32(types.T_int8)}}
			}
			r := f.request(t)
			result, err := r.Describe("tpch", "root_v", nil)
			if result != nil {
				defer result.Release()
			}
			require.NoError(t, err)
			columns, err := result.Columns()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			require.Equal(t, "label", columns[0].Name)
			require.EqualValues(t, types.T_varchar, columns[0].Typ.Id)
			require.Len(t, view.Cols, count, "catalog-owned snapshot must remain unchanged")
		})
	}
}
