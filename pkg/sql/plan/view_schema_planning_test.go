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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		name    string
		account uint32
		pub     *planpb.PubInfo
	}{
		{"local", 42, nil},
		{"publisher", 7, nil},
		{"system publisher", 0, nil},
		{"existing publisher", 7, &planpb.PubInfo{TenantId: 7, XXX_unrecognized: []byte{0x10, 0x07}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			catalogObject := f.compiler.objects["nation"]
			catalogObject.PubInfo = tc.pub
			before := DeepCopyObjectRef(catalogObject)
			request := f.request(t)
			require.NoError(t, request.open())
			state := newViewSchemaDerivation(request)
			defer state.close()
			state.planning = true
			obj, _, err := state.compiler.record(catalogObject, f.compiler.tables["nation"],
				&Snapshot{Tenant: &SnapshotTenant{TenantID: tc.account}}, "tpch", "nation")
			require.NoError(t, err)
			require.Equal(t, before, catalogObject, "catalog-owned identity must not be mutated")
			require.NotSame(t, catalogObject, obj)
			require.Len(t, state.planDependencies, 1)
			if tc.account == 42 {
				require.Nil(t, obj.PubInfo)
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

func TestViewSchemaPlanningAuthorizesRootsBeforeLookup(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	failure := errors.New("denied root")
	f.authErr = failure
	authorize := f.binding.Authorize
	f.binding.Authorize = func(ctx context.Context, database, name string, snapshot *Snapshot) error {
		require.Nil(t, ctx.Value(viewSchemaContextKey{}), "authorization catalog SQL must not inherit the private planning mode")
		return authorize(ctx, database, name, snapshot)
	}
	request := f.request(t)
	p, err := buildViewSchemaTestPlan(t, request, "select * from root_v", false)
	require.ErrorIs(t, err, failure)
	require.Nil(t, p)
	require.Zero(t, f.compiler.lookups.Load())
	f.authErr = nil
	p, err = buildViewSchemaTestPlan(t, request, "select * from root_v", false)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.EqualValues(t, 2, f.authorizes.Load(), "nested execution authorization belongs to ViewReferences")
	require.NotEmpty(t, p.GetQuery().ViewReferences)
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
