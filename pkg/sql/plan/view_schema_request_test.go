// Copyright 2026 Matrix Origin
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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type viewSchemaTestProvider func(context.Context) (*ViewSchemaBinding, error)

func (p viewSchemaTestProvider) OpenViewSchemaBinding(ctx context.Context) (*ViewSchemaBinding, error) {
	return p(ctx)
}

// Unlike MockCompilerContext.Resolve, this fixture returns catalog-owned
// objects. The request itself must protect them from binder and caller writes.
type viewSchemaTestCompiler struct {
	*MockCompilerContext
	snapshot *Snapshot
	resolve  func(context.Context, string, string, *Snapshot) error
	lookups  atomic.Int64
}

func (c *viewSchemaTestCompiler) GetSnapshot() *Snapshot { return c.snapshot }
func (c *viewSchemaTestCompiler) SetSnapshot(snapshot *Snapshot) {
	c.snapshot = snapshot
}
func (c *viewSchemaTestCompiler) Resolve(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	c.lookups.Add(1)
	if c.resolve != nil {
		if err := c.resolve(c.GetContext(), database, name, snapshot); err != nil {
			return nil, nil, err
		}
	}
	return c.objects[name], c.tables[name], nil
}

type viewSchemaTestFixture struct {
	compiler   *viewSchemaTestCompiler
	binding    *ViewSchemaBinding
	provider   viewSchemaTestProvider
	generation *process.ExecutionResourceGeneration
	opens      atomic.Int64
	closes     atomic.Int64
	authorizes atomic.Int64
	checkErr   error
	authErr    error
	nextID     uint64
}

func newViewSchemaTestFixture(t testing.TB) *viewSchemaTestFixture {
	t.Helper()
	mock := NewMockCompilerContext(false)
	mock.SetContext(t.Context())
	mock.GetAccountIdFunc = func() (uint32, error) { return 42, nil }
	mock.GetDatabaseIdFunc = func(string, *Snapshot) (uint64, error) { return 7, nil }
	f := &viewSchemaTestFixture{compiler: &viewSchemaTestCompiler{MockCompilerContext: mock}, nextID: 1000}
	for i, name := range []string{"nation", "region"} {
		def := mock.tables[name]
		def.DbId, def.DbName = 7, "tpch"
		def.TblId, def.LogicalId, def.Version = uint64(10+i), uint64(100+i), 3
		mock.objects[name].Obj = int64(def.TblId)
		mock.objects[name].Db = 7
	}
	budget := process.MustNewExecutionResourceBudget(64<<20, 64<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	f.generation = generation
	f.binding = &ViewSchemaBinding{
		Compiler: f.compiler, Generation: generation,
		Check: func() error { return f.checkErr },
		Authorize: func(context.Context, string, string, *Snapshot) error {
			f.authorizes.Add(1)
			return f.authErr
		},
		Close: func() { f.closes.Add(1) },
	}
	f.provider = func(ctx context.Context) (*ViewSchemaBinding, error) {
		f.opens.Add(1)
		f.compiler.SetContext(ctx)
		return f.binding, nil
	}
	proc := mock.GetProcess()
	t.Cleanup(func() {
		generation.Close()
		budget.Close()
		proc.Free()
		proc.GetFileService().Close(context.Background())
	})
	return f
}

func (f *viewSchemaTestFixture) addView(t testing.TB, name, selectSQL string) *TableDef {
	t.Helper()
	return f.addDefinition(t, name, "create view "+name+" as "+selectSQL)
}

func (f *viewSchemaTestFixture) addDefinition(t testing.TB, name, statement string) *TableDef {
	t.Helper()
	f.nextID++
	data, err := json.Marshal(ViewData{Stmt: statement, DefaultDatabase: "tpch"})
	require.NoError(t, err)
	def := &TableDef{
		Name: name, DbName: "tpch", DbId: 7, TblId: f.nextID, LogicalId: f.nextID + 10000,
		Version: 5, TableType: catalog.SystemViewRel,
		ViewSql: &planpb.ViewDef{View: string(data)},
		// A stale physical schema must never determine the returned columns.
		Cols: []*ColDef{{Name: "stale", Typ: planpb.Type{Id: int32(types.T_int8)}}},
	}
	f.compiler.tables[name] = def
	f.compiler.objects[name] = &ObjectRef{Db: 7, Obj: int64(def.TblId), SchemaName: "tpch", ObjName: name}
	return def
}

func (f *viewSchemaTestFixture) request(t *testing.T) *ViewSchemaRequest {
	t.Helper()
	r := NewViewSchemaRequest(t.Context(), f.provider)
	t.Cleanup(r.Close)
	return r
}

func viewSchemaTestResult(t *testing.T, request *ViewSchemaRequest, name string) *ViewSchemaResult {
	t.Helper()
	result, err := request.Describe("tpch", name, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	t.Cleanup(result.Release)
	return result
}

func viewSchemaTestWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("view schema lifecycle barrier did not finish")
	}
}

func TestViewSchemaRequestMatchesAuthoritativeSchema(t *testing.T) {
	for _, tc := range []struct {
		name, statement string
		dependencies    []string
	}{
		{"source defaults", "create view root_v (Label, KeyValue) as select n_name, n_nationkey from nation", []string{"root_v", "nation"}},
		{"computed output", "create view root_v as select n_nationkey + 1 as k, cast(n_name as char(8)) as label from nation", []string{"root_v", "nation"}},
		{"outer join", "create view root_v as select n.n_nationkey as k from region r left join nation n on r.r_regionkey=n.n_regionkey", []string{"root_v", "nation", "region"}},
		{"nested", "create view root_v as select k, label from middle_v", []string{"root_v", "middle_v", "nation"}},
		{"pure null boundary", "create view root_v as select x from null_v union all select 1", []string{"root_v", "null_v"}},
		{"enum and set boundary", "create view root_v as select n_enum, n_set from special_v", []string{"root_v", "special_v", "nation"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			source := f.compiler.tables["nation"].Cols[0]
			source.Typ.NotNullable = true
			source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
			f.compiler.tables["nation"].Cols[1].Typ.Width = 77
			f.compiler.tables["nation"].Cols = append(f.compiler.tables["nation"].Cols,
				&ColDef{Name: "n_enum", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,A,"}, Default: &planpb.Default{NullAbility: true}},
				&ColDef{Name: "n_set", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}, Default: &planpb.Default{NullAbility: true}},
			)
			f.addView(t, "middle_v", "select n_nationkey as k, n_name as label from nation")
			f.addView(t, "null_v", "select null as x")
			f.addView(t, "special_v", "select distinct n_enum, n_set from nation")
			root := f.addDefinition(t, "root_v", tc.statement)
			beforeSource, beforeRoot := proto.Clone(f.compiler.tables["nation"]), proto.Clone(root)
			oracle, err := RegenerateViewDefinition(f.compiler, root.ViewSql.View)
			require.NoError(t, err)
			r := f.request(t)
			r.memoDisabled = true
			result := viewSchemaTestResult(t, r, "root_v")
			columns, err := result.Columns()
			require.NoError(t, err)
			require.Equal(t, oracle.TableDef.Cols, columns)
			dependencies, err := result.Dependencies()
			require.NoError(t, err)
			names := make([]string, len(dependencies))
			for i, dependency := range dependencies {
				names[i] = dependency.RelationName
				require.Equal(t, uint32(42), dependency.AccountID)
				require.Equal(t, uint64(7), dependency.DatabaseID)
				require.Equal(t, f.compiler.tables[dependency.RelationName].TblId, dependency.RelationID)
				require.Equal(t, f.compiler.tables[dependency.RelationName].Version, dependency.Version)
			}
			require.ElementsMatch(t, tc.dependencies, names)
			require.Equal(t, beforeSource, f.compiler.tables["nation"])
			require.Equal(t, beforeRoot, root)
		})
	}
}

func TestViewSchemaRequestOwnsResultsAndMemo(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.compiler.tables["nation"].Cols[0].Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
	f.addView(t, "root_v", "select n_nationkey as k from nation")
	r := f.request(t)
	first := viewSchemaTestResult(t, r, "root_v")
	columns, err := first.Columns()
	require.NoError(t, err)
	dependencies, err := first.Dependencies()
	require.NoError(t, err)
	origins, err := first.Provenance()
	require.NoError(t, err)
	require.Len(t, origins, 1)
	require.Equal(t, ProvenanceSingleSource, origins[0].State)
	require.Equal(t, uint64(10), origins[0].SourceTableID)
	require.NotNil(t, origins[0].Source)
	require.NotNil(t, origins[0].Source.Default)
	originalSourceType := origins[0].Source.Typ
	origins[0].Source.Typ.Id = int32(types.T_text)
	origins[0].Source.Default.OriginString = "caller mutation"
	origins[0].Source.Default.Expr.Typ.Id = int32(types.T_text)
	origins[0].SourceTableID = 999
	againOrigins, err := first.Provenance()
	require.NoError(t, err)
	require.Equal(t, originalSourceType, againOrigins[0].Source.Typ)
	require.Equal(t, "7", againOrigins[0].Source.Default.OriginString)
	require.Equal(t, int32(types.T_int32), againOrigins[0].Source.Default.Expr.Typ.Id)
	require.Equal(t, uint64(10), againOrigins[0].SourceTableID)
	protocol, err := first.RequiredProtocolVersion()
	require.NoError(t, err)
	require.Zero(t, protocol)
	columns[0].Name, columns[0].Default.OriginString = "changed", "changed"
	columns[0].Default.Expr.Typ.Id = int32(types.T_text)
	dependencies[0].RelationName = "changed"
	again, err := first.Columns()
	require.NoError(t, err)
	require.Equal(t, "k", again[0].Name)
	require.Equal(t, "7", again[0].Default.OriginString)
	require.Equal(t, int32(types.T_int32), again[0].Default.Expr.Typ.Id)
	second := viewSchemaTestResult(t, r, "root_v")
	warm, err := second.Columns()
	require.NoError(t, err)
	require.Equal(t, again, warm)
	warmDependencies, err := second.Dependencies()
	require.NoError(t, err)
	warmOrigins, err := second.Provenance()
	require.NoError(t, err)
	require.Equal(t, againOrigins, warmOrigins)
	warmProtocol, err := second.RequiredProtocolVersion()
	require.NoError(t, err)
	require.Equal(t, protocol, warmProtocol)
	for _, dependency := range warmDependencies {
		require.NotEqual(t, "changed", dependency.RelationName)
	}
	require.Equal(t, uint64(1), r.binds)
	require.Equal(t, uint64(1), r.hits)
	require.Equal(t, int64(2), f.authorizes.Load())
	first.Release()
	first.Release()
	_, err = first.Columns()
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	_, err = first.Dependencies()
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	_, err = first.Provenance()
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	_, err = first.RequiredProtocolVersion()
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	second.Release()
	r.Close()
	require.Zero(t, f.generation.Used())
	require.False(t, f.generation.Closed(), "the request borrows the execution generation")
}

func TestViewSchemaRequestProvenanceAtCompletedViewBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, selectSQL, defaultSQL string
		state                       ProvenanceState
		policy                      CTASDefaultPolicy
		nullable                    bool
	}{
		{"source literal default", "select n_nationkey as k from nation", "7", ProvenanceSingleSource, CTASDefaultUseTypeDefault, false},
		{"source generated default", "select n_nationkey as k from nation", "(7)", ProvenanceSingleSource, CTASDefaultInheritViewSource, false},
		{"source no default", "select n_nationkey as k from nation", "", ProvenanceSingleSource, CTASDefaultNone, false},
		{"outer join source", "select n.n_nationkey as k from region r left join nation n on r.r_regionkey=n.n_regionkey", "7", ProvenanceSingleSource, CTASDefaultUseTypeDefault, true},
		{"computed", "select n_nationkey + 1 as k from nation", "7", ProvenanceNone, CTASDefaultNone, false},
		{"pure null", "select null as k", "", ProvenancePureNull, CTASDefaultNone, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			source := f.compiler.tables["nation"].Cols[0]
			source.Typ.NotNullable = true
			source.Default = nil
			if tc.defaultSQL != "" {
				source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: tc.defaultSQL}
			}
			f.addView(t, "root_v", tc.selectSQL)
			r := f.request(t)
			for i := 0; i < 2; i++ {
				result := viewSchemaTestResult(t, r, "root_v")
				origins, err := result.Provenance()
				require.NoError(t, err)
				require.Len(t, origins, 1)
				require.Equal(t, tc.state, origins[0].State)
				require.Equal(t, tc.policy, origins[0].CTASDefaultPolicy)
				if tc.state == ProvenanceSingleSource {
					require.Equal(t, uint64(10), origins[0].SourceTableID)
					require.NotNil(t, origins[0].Source)
					require.Equal(t, tc.nullable, origins[0].Source.NullAbility)
					if tc.defaultSQL == "" {
						require.Nil(t, origins[0].Source.Default)
					} else {
						require.Equal(t, tc.defaultSQL, origins[0].Source.Default.OriginString)
					}
				} else {
					require.Nil(t, origins[0].Source)
				}
				result.Release()
			}
			require.Equal(t, uint64(1), r.hits)
		})
	}
}

func TestViewSchemaRequestRequiredProtocolSurvivesFoldingAndMemo(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	rt := moruntime.ServiceRuntime(f.compiler.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor} {
		previous, present := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if present {
				rt.SetGlobalVariables(key, previous)
			} else if current, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, current)
			}
		})
		rt.SetGlobalVariables(key, int64(defines.MORPCLatestVersion))
	}
	f.addView(t, "root_v", "select inet_ntoa('1.6') as ip")
	mark := func(name, sql string, protocol int64) {
		def := f.addView(t, name, sql)
		var data ViewData
		require.NoError(t, json.Unmarshal([]byte(def.ViewSql.View), &data))
		data.RequiredProtocolVersion = &protocol
		encoded, err := json.Marshal(data)
		require.NoError(t, err)
		def.ViewSql.View = string(encoded)
	}
	mark("marker_leaf", "select n_name as label from nation", defines.MORPCVersion97)
	f.addView(t, "marker_middle", "select label from marker_leaf")
	mark("marker_high", "select label from marker_middle", defines.MORPCVersion98)
	mark("marker_low", "select label from marker_middle", defines.MORPCVersion86)
	mark("marker_leaf_root", "select label from marker_leaf", defines.MORPCVersion86)
	mark("marker_only", "select n_name as label from nation", defines.MORPCVersion98)
	for _, disabled := range []bool{false, true} {
		r := f.request(t)
		r.memoDisabled = disabled
		for i := 0; i < 2; i++ {
			result := viewSchemaTestResult(t, r, "root_v")
			required, err := result.RequiredProtocolVersion()
			require.NoError(t, err)
			require.Equal(t, int64(defines.MORPCVersion86), required)
			result.Release()
		}
		for _, tc := range []struct {
			name     string
			required int64
		}{
			{"marker_high", defines.MORPCVersion98},
			{"marker_low", defines.MORPCVersion97},
			{"marker_leaf_root", defines.MORPCVersion97},
			{"marker_only", defines.MORPCVersion98},
			{"marker_only", defines.MORPCVersion98},
		} {
			result := viewSchemaTestResult(t, r, tc.name)
			required, err := result.RequiredProtocolVersion()
			require.NoError(t, err)
			require.Equal(t, tc.required, required, "memo disabled=%t, root=%s; an ancestor's marker must not contaminate a shared child", disabled, tc.name)
			result.Release()
		}
		r.Close()
	}
}

func TestViewSchemaRequestMemoOffIsOracle(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			def := f.addView(t, "root_v", "select n_name as label from nation")
			oracle, err := DescribeViewColumns(f.compiler, def.ViewSql.View)
			require.NoError(t, err)
			r := f.request(t)
			r.memoDisabled = disabled
			var previous []ViewDependency
			for i := 0; i < 2; i++ {
				result := viewSchemaTestResult(t, r, "root_v")
				columns, err := result.Columns()
				require.NoError(t, err)
				require.Equal(t, oracle, columns)
				dependencies, err := result.Dependencies()
				require.NoError(t, err)
				if i > 0 {
					require.Equal(t, previous, dependencies)
				}
				previous = dependencies
				result.Release()
			}
			if disabled {
				require.Equal(t, uint64(2), r.binds)
				require.Zero(t, r.hits)
				require.Empty(t, r.memo)
			} else {
				require.Equal(t, uint64(1), r.binds)
				require.Equal(t, uint64(1), r.hits)
			}
		})
	}
}

func TestViewSchemaRequestSharedProjectionMemoMatchesCacheOff(t *testing.T) {
	for _, tc := range []struct {
		name, middleName, middleSQL, firstSQL, secondSQL string
		catalogName                                      string
	}{
		{"unqualified star", "middle_v", "select * from leaf_v", "select * from middle_v", "select * from middle_v", ""},
		{"qualified star", "middle_v", "select leaf_v.* from leaf_v", "select * from middle_v", "select middle_v.* from middle_v", ""},
		{"reference name differs from catalog", "mixed", "select * from leaf_v", "select * from mixed", "select mixed.k as key_value, mixed.label as Label from mixed", "Mixed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			source := f.compiler.tables["nation"].Cols[0]
			source.Typ.NotNullable = true
			source.Default = &planpb.Default{Expr: makePlan2Int32ConstExprWithType(7), OriginString: "7"}
			f.compiler.tables["nation"].Cols[1].Typ.Width = 77
			f.addView(t, "leaf_v", "select n_nationkey as k, n_name as label from nation")
			middle := f.addView(t, tc.middleName, tc.middleSQL)
			if tc.catalogName != "" {
				middle.Name = tc.catalogName
				f.compiler.objects[tc.middleName].ObjName = tc.catalogName
			}
			f.addView(t, "first_v", tc.firstSQL)
			f.addView(t, "second_v", tc.secondSQL)
			beforeSource, beforeMiddle := proto.Clone(f.compiler.tables["nation"]), proto.Clone(middle)

			oracle := f.request(t)
			oracle.memoDisabled = true
			viewSchemaTestResult(t, oracle, "first_v").Release()
			plain := viewSchemaTestResult(t, oracle, "second_v")
			wantColumns, err := plain.Columns()
			require.NoError(t, err)
			wantDependencies, err := plain.Dependencies()
			require.NoError(t, err)
			wantOrigins, err := plain.Provenance()
			require.NoError(t, err)
			wantProtocol, err := plain.RequiredProtocolVersion()
			require.NoError(t, err)
			plain.Release()
			oracle.Close()
			require.Zero(t, f.generation.Used())

			cached := f.request(t)
			viewSchemaTestResult(t, cached, "first_v").Release()
			require.NotEmpty(t, cached.nestedMemo, "a shared transparent chain must retain a reusable boundary")
			beforeHits := cached.hits
			var resolved []string
			f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
				resolved = append(resolved, name)
				return nil
			}
			warm := viewSchemaTestResult(t, cached, "second_v")
			gotColumns, err := warm.Columns()
			require.NoError(t, err)
			gotDependencies, err := warm.Dependencies()
			require.NoError(t, err)
			gotOrigins, err := warm.Provenance()
			require.NoError(t, err)
			gotProtocol, err := warm.RequiredProtocolVersion()
			require.NoError(t, err)
			require.Equal(t, wantColumns, gotColumns, "type, nullability, defaults and heading metadata must survive nested reuse")
			require.Equal(t, wantDependencies, gotDependencies, "cache hits must replay intermediate and base identities")
			require.Equal(t, wantOrigins, gotOrigins)
			require.Equal(t, wantProtocol, gotProtocol)
			require.Equal(t, beforeHits+1, cached.hits, "the second root has never been described, so this must be a nested hit")
			require.Equal(t, uint64(2), cached.binds)
			require.Equal(t, oracle.work, cached.work, "cached nested work remains charged to the statement")
			require.Equal(t, []string{"second_v", tc.middleName}, resolved, "shared descendants must not be rebound")
			require.Equal(t, beforeSource, f.compiler.tables["nation"])
			require.Equal(t, beforeMiddle, middle)
			warm.Release()
			cached.Close()
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestNestedMemoUsesEffectiveLowerCaseMode(t *testing.T) {
	for _, upperTableExists := range []bool{false, true} {
		t.Run(fmt.Sprintf("uppercase table exists %t", upperTableExists), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			if upperTableExists {
				upper := proto.Clone(f.compiler.tables["nation"]).(*TableDef)
				upper.Name, upper.TblId, upper.LogicalId = "NATION", 99, 199
				upper.Cols[1].Typ = planpb.Type{Id: int32(types.T_int64)}
				f.compiler.tables["NATION"] = upper
				f.compiler.objects["NATION"] = &ObjectRef{Db: 7, Obj: 99, SchemaName: "tpch", ObjName: "NATION"}
			}
			// A legacy nested definition inherits each parent's persisted mode.
			// The statement-local session mode remains 1 throughout this test.
			f.addView(t, "shared_v", "select n_name as label from NATION")
			for i, rootName := range []string{"folded_v", "sensitive_v"} {
				root := f.addView(t, rootName, "select label from shared_v")
				var data ViewData
				require.NoError(t, json.Unmarshal([]byte(root.ViewSql.View), &data))
				lower := int64(1 - i)
				data.LowerCaseTableNames = &lower
				encoded, err := json.Marshal(data)
				require.NoError(t, err)
				root.ViewSql.View = string(encoded)
			}

			oracle := f.request(t)
			oracle.memoDisabled = true
			plain, wantErr := oracle.Describe("tpch", "sensitive_v", nil)
			var wantColumns []*ColDef
			var wantDependencies []ViewDependency
			if plain != nil {
				t.Cleanup(plain.Release)
				var err error
				wantColumns, err = plain.Columns()
				require.NoError(t, err)
				wantDependencies, err = plain.Dependencies()
				require.NoError(t, err)
				plain.Release()
			}
			if upperTableExists {
				require.NoError(t, wantErr)
				require.Equal(t, int32(types.T_int64), wantColumns[0].Typ.Id)
				for _, dependency := range wantDependencies {
					wantLower := int64(0)
					if dependency.RelationName == "sensitive_v" {
						wantLower = 1 // The Describe root is resolved in the caller's mode.
					}
					require.Equal(t, wantLower, dependency.LowerCaseTableNames, dependency.RelationName)
				}
			} else {
				require.Error(t, wantErr)
			}
			oracle.Close()

			cached := f.request(t)
			viewSchemaTestResult(t, cached, "folded_v").Release()
			require.NotEmpty(t, cached.nestedMemo)
			warm, gotErr := cached.Describe("tpch", "sensitive_v", nil)
			if warm != nil {
				t.Cleanup(warm.Release)
			}
			if wantErr != nil {
				require.Nil(t, warm)
				require.EqualError(t, gotErr, wantErr.Error())
			} else {
				require.NoError(t, gotErr)
				gotColumns, err := warm.Columns()
				require.NoError(t, err)
				gotDependencies, err := warm.Dependencies()
				require.NoError(t, err)
				require.Equal(t, wantColumns, gotColumns)
				require.Equal(t, wantDependencies, gotDependencies)
				warm.Release()
			}
			require.Zero(t, cached.hits, "legacy nested results from lower=1 cannot answer a lower=0 root")
			cached.Close()
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestNestedMemoReplaysDepthBoundary(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("memo disabled %t", disabled), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			for i := viewSchemaDepthLimit - 2; i >= 0; i-- {
				sql := "select n_name as x from nation"
				if i < viewSchemaDepthLimit-2 {
					sql = fmt.Sprintf("select x from depth_%d", i+1)
				}
				f.addView(t, fmt.Sprintf("depth_%d", i), sql)
			}
			f.addView(t, "warm_v", "select x from depth_0")
			f.addView(t, "edge_v", "select x from depth_0")
			f.addView(t, "extra_v", "select x from depth_0")
			f.addView(t, "overflow_v", "select x from extra_v")
			r := f.request(t)
			r.memoDisabled = disabled
			viewSchemaTestResult(t, r, "warm_v").Release()
			viewSchemaTestResult(t, r, "edge_v").Release()
			if !disabled {
				require.Equal(t, uint64(1), r.hits)
			}
			result, err := r.Describe("tpch", "overflow_v", nil)
			if result != nil {
				t.Cleanup(result.Release)
			}
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrViewSchemaLimit, "a memo hit cannot shorten logical dependency depth")
			require.Empty(t, r.memo)
			require.Empty(t, r.nestedMemo)
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestNestedMemoBypassesSemanticBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, nestedSQL, rootSQL string }{
		{"computed ancestor", "select n_nationkey as k from nation", "select k + 1 as k from middle_v"},
		{"filter ancestor", "select n_nationkey as k from nation", "select k from middle_v where k > 0"},
		{"filter nested", "select n_nationkey as k from nation where n_nationkey > 0", "select k from middle_v"},
		{"enum boundary", "select n_enum as k from nation", "select k from middle_v"},
		{"set boundary", "select n_set as k from nation", "select k from middle_v"},
		{"pure null boundary", "select null as k", "select k from middle_v"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.compiler.tables["nation"].Cols = append(f.compiler.tables["nation"].Cols,
				&ColDef{Name: "n_enum", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,A,"}},
				&ColDef{Name: "n_set", Typ: planpb.Type{Id: int32(types.T_uint64), Enumvalues: ",a"}},
			)
			f.addView(t, "middle_v", tc.nestedSQL)
			f.addView(t, "warm_v", "select k from middle_v")
			f.addView(t, "root_v", tc.rootSQL)
			oracle := f.request(t)
			oracle.memoDisabled = true
			plain := viewSchemaTestResult(t, oracle, "root_v")
			wantColumns, err := plain.Columns()
			require.NoError(t, err)
			wantOrigins, err := plain.Provenance()
			require.NoError(t, err)
			wantDependencies, err := plain.Dependencies()
			require.NoError(t, err)
			plain.Release()
			oracle.Close()
			cached := f.request(t)
			viewSchemaTestResult(t, cached, "warm_v").Release()
			beforeHits := cached.hits
			warm := viewSchemaTestResult(t, cached, "root_v")
			gotColumns, err := warm.Columns()
			require.NoError(t, err)
			gotOrigins, err := warm.Provenance()
			require.NoError(t, err)
			gotDependencies, err := warm.Dependencies()
			require.NoError(t, err)
			require.Equal(t, wantColumns, gotColumns)
			require.Equal(t, wantOrigins, gotOrigins)
			require.Equal(t, wantDependencies, gotDependencies)
			require.Equal(t, beforeHits, cached.hits, "shape outside the transparent subset must bind normally")
			warm.Release()
		})
	}
}

func TestViewSchemaRequestRejectsInvalidNestedDefinitionsWithOrWithoutMemo(t *testing.T) {
	for _, kind := range []string{"nil", "empty", "json", "empty statement", "comment only", "multiple statements", "not a view statement"} {
		t.Run(kind, func(t *testing.T) {
			var firstError string
			for _, disabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("memo disabled %t", disabled), func(t *testing.T) {
					f := newViewSchemaTestFixture(t)
					middle := f.addView(t, "middle_v", "select n_nationkey as k from nation")
					// The stale schema would make the parent appear valid if an
					// empty persisted definition fell back to a physical scan.
					middle.Cols[0].Name = "k"
					f.addView(t, "root_v", "select k from middle_v")
					switch kind {
					case "nil":
						middle.ViewSql = nil
					case "empty":
						middle.ViewSql.View = ""
					case "json":
						middle.ViewSql.View = "{"
					default:
						statement := map[string]string{
							"empty statement":      "",
							"comment only":         "/* persisted definition is missing */",
							"multiple statements":  "create view middle_v as select n_nationkey as k from nation; select 1",
							"not a view statement": "select n_nationkey as k from nation",
						}[kind]
						encoded, err := json.Marshal(ViewData{Stmt: statement, DefaultDatabase: "tpch"})
						require.NoError(t, err)
						middle.ViewSql.View = string(encoded)
					}
					before := proto.Clone(middle)
					r := f.request(t)
					r.memoDisabled = disabled
					var result *ViewSchemaResult
					var err error
					require.NotPanics(t, func() { result, err = r.Describe("tpch", "root_v", nil) })
					if result != nil {
						t.Cleanup(result.Release)
					}
					require.Nil(t, result)
					require.Error(t, err)
					if !disabled {
						firstError = err.Error()
					} else {
						require.Equal(t, firstError, err.Error())
					}
					require.Empty(t, r.memo)
					require.Empty(t, r.nestedMemo)
					require.Zero(t, f.generation.Used())
					require.Equal(t, before, middle)
				})
			}
		})
	}
}

func TestViewSchemaRequestAuthorizesBeforeEveryRootLookup(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	r := f.request(t)
	viewSchemaTestResult(t, r, "root_v").Release()
	viewSchemaTestResult(t, r, "root_v").Release()
	require.Equal(t, int64(2), f.authorizes.Load())
	require.Equal(t, uint64(1), r.hits)
	before := f.compiler.lookups.Load()
	f.authErr = errors.New("root privilege revoked")
	result, err := r.Describe("tpch", "root_v", nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, f.authErr)
	require.Equal(t, int64(3), f.authorizes.Load())
	require.Equal(t, before, f.compiler.lookups.Load(), "denied roots cannot resolve through a warm memo")
	require.Empty(t, r.memo)
}

func TestViewSchemaRequestOpenIsLazyAndClosesOnce(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	r := f.request(t)
	require.Zero(t, f.opens.Load())
	r.Close()
	r.Close()
	require.Zero(t, f.opens.Load())
	require.Zero(t, f.closes.Load())
	result, err := r.Describe("tpch", "missing", nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrViewSchemaClosed)
}

func TestViewSchemaRequestReleasesPartialOpen(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		t.Run(fmt.Sprint(incomplete), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			openErr := errors.New("catalog read domain unavailable")
			provider := viewSchemaTestProvider(func(context.Context) (*ViewSchemaBinding, error) {
				if incomplete {
					return &ViewSchemaBinding{Close: f.binding.Close}, nil
				}
				return f.binding, openErr
			})
			r := NewViewSchemaRequest(t.Context(), provider)
			t.Cleanup(r.Close)
			result, err := r.Describe("tpch", "missing", nil)
			require.Nil(t, result)
			if incomplete {
				require.ErrorContains(t, err, "incomplete view schema binding")
			} else {
				require.ErrorIs(t, err, openErr)
			}
			require.Equal(t, int64(1), f.closes.Load())
			r.Close()
			require.Equal(t, int64(1), f.closes.Load())
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestFailureAndPanicReturnBinderOwnership(t *testing.T) {
	for _, panicFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(panicFailure), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name from nation")
			r := f.request(t)
			failure := errors.New("catalog read failed")
			f.compiler.resolve = func(context.Context, string, string, *Snapshot) error {
				if panicFailure {
					panic(failure)
				}
				return failure
			}
			if panicFailure {
				require.PanicsWithValue(t, failure, func() { _, _ = r.Describe("tpch", "root_v", nil) })
			} else {
				result, err := r.Describe("tpch", "root_v", nil)
				require.Nil(t, result)
				require.ErrorIs(t, err, failure)
			}
			require.Zero(t, f.generation.Used())
			require.Empty(t, r.memo)
			f.compiler.resolve = nil
			viewSchemaTestResult(t, r, "root_v").Release()
			require.Equal(t, int64(1), f.opens.Load())
			r.Close()
			require.Equal(t, int64(1), f.closes.Load())
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestReleasesUnpublishedResultOnFinalCheckFailure(t *testing.T) {
	for _, panicFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(panicFailure), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name from nation")
			r := f.request(t)
			r.memoDisabled = true
			failure := errors.New("final result visibility check failed")
			visited := false
			f.binding.Check = func() error {
				// Input leases exist only while the derivation context is installed.
				// With memo disabled, retained bytes after restoration belong to the
				// admitted result that Describe has not yet published.
				if f.compiler.GetContext().Value(viewSchemaContextKey{}) == nil && f.generation.Used() > 0 {
					visited = true
					if panicFailure {
						panic(failure)
					}
					return failure
				}
				return nil
			}
			if panicFailure {
				require.PanicsWithValue(t, failure, func() { _, _ = r.Describe("tpch", "root_v", nil) })
			} else {
				result, err := r.Describe("tpch", "root_v", nil)
				require.Nil(t, result)
				require.ErrorIs(t, err, failure)
			}
			require.True(t, visited)
			require.Zero(t, f.generation.Used(), "unpublished results must return their reservation on error and panic")
			require.Empty(t, r.memo)
			f.binding.Check = func() error { return nil }
			viewSchemaTestResult(t, r, "root_v").Release()
			r.Close()
			require.Equal(t, int64(1), f.closes.Load())
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewSchemaRequestRejectsInvalidRootsAndRecovers(t *testing.T) {
	for _, kind := range []string{"missing", "table", "json", "sql"} {
		t.Run(kind, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			root := f.addView(t, "root_v", "select n_name from nation")
			saved := root.ViewSql.View
			switch kind {
			case "missing":
				delete(f.compiler.tables, "root_v")
			case "table":
				root.TableType = catalog.SystemOrdinaryRel
			case "json":
				root.ViewSql.View = "{"
			case "sql":
				data, err := json.Marshal(ViewData{Stmt: "create view root_v as select from", DefaultDatabase: "tpch"})
				require.NoError(t, err)
				root.ViewSql.View = string(data)
			}
			r := f.request(t)
			result, err := r.Describe("tpch", "root_v", nil)
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, f.generation.Used())
			require.Empty(t, r.memo)
			f.compiler.tables["root_v"] = root
			root.TableType, root.ViewSql.View = catalog.SystemViewRel, saved
			viewSchemaTestResult(t, r, "root_v").Release()
		})
	}
}

func TestViewSchemaRequestVisibilityAndGenerationInvalidateMemo(t *testing.T) {
	for _, closeGeneration := range []bool{false, true} {
		t.Run(fmt.Sprint(closeGeneration), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "root_v", "select n_name from nation")
			r := f.request(t)
			viewSchemaTestResult(t, r, "root_v").Release()
			if closeGeneration {
				f.generation.Close()
			} else {
				f.checkErr = errors.New("statement visibility changed")
			}
			before := f.compiler.lookups.Load()
			result, err := r.Describe("tpch", "root_v", nil)
			require.Nil(t, result)
			if closeGeneration {
				require.ErrorIs(t, err, ErrViewSchemaClosed)
			} else {
				require.ErrorIs(t, err, f.checkErr)
			}
			require.Equal(t, before, f.compiler.lookups.Load())
			require.Zero(t, f.generation.Used())
			require.Empty(t, r.memo)
		})
	}
}

func TestViewSchemaRequestCancelUnblocksCatalogAndClose(t *testing.T) {
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
	r := f.request(t)
	described := make(chan struct{})
	var result *ViewSchemaResult
	var describeErr error
	go func() {
		defer close(described)
		result, describeErr = r.Describe("tpch", "root_v", nil)
	}()
	viewSchemaTestWait(t, entered)
	_, err := r.Describe("tpch", "root_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaBusy)
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	viewSchemaTestWait(t, described)
	viewSchemaTestWait(t, closed)
	require.Nil(t, result)
	require.ErrorIs(t, describeErr, ErrViewSchemaClosed)
	require.Equal(t, int64(1), f.closes.Load())
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaResultReleaseAndRequestCloseBarriers(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	r := f.request(t)
	result := viewSchemaTestResult(t, r, "root_v")
	closed := make(chan struct{})
	go func() { r.Close(); close(closed) }()
	viewSchemaTestWait(t, r.ctx.Done())
	require.Zero(t, f.closes.Load(), "the provider stays live while the result has an owner")
	columns, err := result.Columns()
	require.NoError(t, err, "request cancellation does not invalidate an outstanding result")
	require.Len(t, columns, 1)

	// Stop an existing reader at the same lock held throughout decoding.
	// Release cannot reclaim the payload until this reader has left.
	result.mu.RLock()
	released, releasing := make(chan struct{}), make(chan struct{})
	go func() { close(releasing); result.Release(); close(released) }()
	viewSchemaTestWait(t, releasing)
	select {
	case <-released:
		result.mu.RUnlock()
		t.Fatal("Release reclaimed an active reader's payload")
	default:
	}
	result.mu.RUnlock()
	viewSchemaTestWait(t, released)
	viewSchemaTestWait(t, closed)
	require.Equal(t, int64(1), f.closes.Load())
	require.Zero(t, f.generation.Used())
	require.False(t, f.generation.Closed())
}

func TestViewSchemaRequestParentCancellationWins(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	failure := errors.New("parent statement cancelled")
	r := NewViewSchemaRequest(ctx, f.provider)
	t.Cleanup(r.Close)
	cancel(failure)
	result, err := r.Describe("tpch", "missing", nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, failure)
	require.Zero(t, f.opens.Load())
}

func TestViewSchemaRequestDepthBoundaryAndCycle(t *testing.T) {
	for _, depth := range []int{viewSchemaDepthLimit, viewSchemaDepthLimit + 1} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			for i := depth - 1; i >= 0; i-- {
				sql := "select n_nationkey as x from nation"
				if i+1 < depth {
					sql = fmt.Sprintf("select x from chain_%d", i+1)
				}
				f.addView(t, fmt.Sprintf("chain_%d", i), sql)
			}
			r := f.request(t)
			r.memoDisabled = true
			result, err := r.Describe("tpch", "chain_0", nil)
			if depth > viewSchemaDepthLimit {
				require.Nil(t, result)
				require.ErrorIs(t, err, ErrViewSchemaLimit)
				require.Zero(t, f.generation.Used())
			} else {
				require.NoError(t, err)
				t.Cleanup(result.Release)
				dependencies, err := result.Dependencies()
				require.NoError(t, err)
				require.Len(t, dependencies, depth+1)
				result.Release()
			}
		})
	}
	t.Run("cycle is not a cache hit", func(t *testing.T) {
		f := newViewSchemaTestFixture(t)
		f.addView(t, "cycle_a", "select x from cycle_b")
		f.addView(t, "cycle_b", "select x from cycle_a")
		r := f.request(t)
		result, err := r.Describe("tpch", "cycle_a", nil)
		require.Nil(t, result)
		require.ErrorContains(t, err, "cyclic view")
		require.Empty(t, r.memo)
		require.Zero(t, f.generation.Used())
	})
}

func TestViewSchemaRequestColumnSlotBoundary(t *testing.T) {
	for _, count := range []int{MaxViewMetadataColumns, MaxViewMetadataColumns + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			items := make([]string, count)
			for i := range items {
				items[i] = fmt.Sprintf("n_nationkey as c%d", i)
			}
			f.addView(t, "root_v", "select "+strings.Join(items, ",")+" from nation")
			r := f.request(t)
			r.memoDisabled = true
			result, err := r.Describe("tpch", "root_v", nil)
			if count > MaxViewMetadataColumns {
				require.Nil(t, result)
				require.ErrorIs(t, err, ErrViewSchemaLimit)
				require.Zero(t, f.generation.Used())
			} else {
				require.NoError(t, err)
				t.Cleanup(result.Release)
				columns, err := result.Columns()
				require.NoError(t, err)
				require.Len(t, columns, count)
				result.Release()
			}
		})
	}
}

func TestViewSchemaRequestInputBoundary(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			root := f.addView(t, "root_v", "select n_name from nation")
			root.ViewSql.View += strings.Repeat(" ", viewSchemaInputLimit+extra-len(root.ViewSql.View))
			r := f.request(t)
			r.memoDisabled = true
			result, err := r.Describe("tpch", "root_v", nil)
			if extra != 0 {
				require.Nil(t, result)
				require.ErrorIs(t, err, ErrViewSchemaLimit)
				require.Zero(t, f.generation.Used())
			} else {
				require.NoError(t, err)
				t.Cleanup(result.Release)
				columns, err := result.Columns()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				result.Release()
			}
		})
	}
}

func TestViewSchemaRequestWorkLimitAlsoChargesMemoHits(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	r := f.request(t)
	viewSchemaTestResult(t, r, "root_v").Release()
	// Place a previously admitted statement at N-1; exercise both the final
	// successful operation and the first rejection through the public boundary.
	r.work = viewSchemaWorkLimit - 1
	viewSchemaTestResult(t, r, "root_v").Release()
	require.Equal(t, viewSchemaWorkLimit, r.work)
	require.Equal(t, uint64(1), r.hits)
	result, err := r.Describe("tpch", "root_v", nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	require.Equal(t, viewSchemaWorkLimit, r.work)
	require.Empty(t, r.memo)
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaRequestRootAttemptLimit(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	r := f.request(t)
	r.roots = viewSchemaWorkLimit - 1
	viewSchemaTestResult(t, r, "root_v").Release()
	require.Equal(t, viewSchemaWorkLimit, r.roots)
	before := f.authorizes.Load()
	result, err := r.Describe("tpch", "root_v", nil)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	require.Equal(t, before, f.authorizes.Load(), "the rejected attempt does not acquire another root")
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaRequestMemoEntryLimitFallsBackToBinding(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	r := f.request(t)
	for i := 0; i <= viewSchemaMemoEntries; i++ {
		name := fmt.Sprintf("root_%04d", i)
		f.addView(t, name, "select 1 as k")
		result, err := r.Describe("tpch", name, nil)
		if result != nil {
			result.Release()
		}
		require.NoError(t, err, "optional memo saturation must not reject root %d", i)
		if i >= viewSchemaMemoEntries-1 {
			require.Len(t, r.memo, viewSchemaMemoEntries)
		}
	}
	require.Empty(t, r.nestedMemo)
	require.LessOrEqual(t, r.memoBytes, viewSchemaMemoLimit)
	viewSchemaTestResult(t, r, "root_0000").Release()
	require.Equal(t, uint64(1), r.hits)
	viewSchemaTestResult(t, r, fmt.Sprintf("root_%04d", viewSchemaMemoEntries)).Release()
	require.Equal(t, uint64(1), r.hits, "the first unretained root binds again")
	r.Close()
	require.Zero(t, f.generation.Used())
}

func TestViewSchemaRequestResultAdmissionAndOptionalMemo(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	probe := f.request(t)
	probe.memoDisabled = true
	baseline := viewSchemaTestResult(t, probe, "root_v")
	bytes := uint64(len(baseline.columns) + len(baseline.dependencies) + len(baseline.provenance))
	// The cold path also owns catalog input copies. Measure the necessary
	// peak without optional memo; output encoding alone is not that peak.
	peak := f.generation.Peak()
	baseline.Release()
	probe.Close()
	require.Positive(t, bytes)
	require.GreaterOrEqual(t, peak, bytes)

	tooSmall := process.MustNewExecutionResourceBudget(peak-1, peak-1)
	rejectedGeneration, err := tooSmall.OpenGeneration(2)
	require.NoError(t, err)
	t.Cleanup(func() { rejectedGeneration.Close(); tooSmall.Close() })
	f.binding.Generation = rejectedGeneration
	rejected := f.request(t)
	rejected.memoDisabled = true
	result, err := rejected.Describe("tpch", "root_v", nil)
	if result != nil {
		t.Cleanup(result.Release)
	}
	require.Nil(t, result)
	require.ErrorIs(t, err, process.ErrExecutionResourceAdmission)
	require.Zero(t, rejectedGeneration.Used(), "the first insufficient byte must unwind every input lease")
	rejected.Close()

	budget := process.MustNewExecutionResourceBudget(peak, peak)
	generation, err := budget.OpenGeneration(2)
	require.NoError(t, err)
	t.Cleanup(func() { generation.Close(); budget.Close() })
	f.binding.Generation = generation
	r := f.request(t)
	first := viewSchemaTestResult(t, r, "root_v")
	require.Equal(t, bytes, generation.Used())
	require.Empty(t, r.memo, "optional memo cannot exclude a result that fits the query ceiling")
	result, err = r.Describe("tpch", "root_v", nil)
	if result != nil {
		t.Cleanup(result.Release)
	}
	require.Nil(t, result)
	require.ErrorIs(t, err, process.ErrExecutionResourceAdmission)
	require.Equal(t, bytes, generation.Used(), "failed admission must not release the first result")
	columns, err := first.Columns()
	require.NoError(t, err)
	require.Len(t, columns, 1)
	first.Release()
	require.Zero(t, generation.Used())
	viewSchemaTestResult(t, r, "root_v").Release()
	r.Close()
	require.Zero(t, generation.Used())
	require.False(t, generation.Closed())
}

func TestViewSchemaRequestSnapshotEvidenceIsOwned(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	r := f.request(t)
	snapshot := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1234}, Tenant: &planpb.SnapshotTenant{TenantID: 42}}
	before := DeepCopySnapshot(snapshot)
	result, err := r.Describe("tpch", "root_v", snapshot)
	require.NoError(t, err)
	t.Cleanup(result.Release)
	require.Equal(t, before, snapshot)
	dependencies, err := result.Dependencies()
	require.NoError(t, err)
	require.Len(t, dependencies, 2)
	for _, dependency := range dependencies {
		require.Equal(t, before, dependency.Snapshot)
		dependency.Snapshot.TS.PhysicalTime = 9999
	}
	again, err := result.Dependencies()
	require.NoError(t, err)
	for _, dependency := range again {
		require.Equal(t, before, dependency.Snapshot)
	}
	require.Nil(t, f.compiler.snapshot, "the borrowed compiler snapshot is restored")
}

func BenchmarkViewSchemaRequest(b *testing.B) {
	for _, disabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("memo_disabled=%t", disabled), func(b *testing.B) {
			f := newViewSchemaTestFixture(b)
			f.addView(b, "leaf_v", "select n_nationkey as k, n_name as label from nation")
			f.addView(b, "middle_v", "select * from leaf_v")
			f.addView(b, "root_v", "select k, label from middle_v")
			var request *ViewSchemaRequest
			defer func() {
				if request != nil {
					request.Close()
				}
			}()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%256 == 0 {
					if request != nil {
						request.Close()
					}
					request = NewViewSchemaRequest(b.Context(), f.provider)
					request.memoDisabled = disabled
				}
				result, err := request.Describe("tpch", "root_v", nil)
				require.NoError(b, err)
				columns, columnErr := result.Columns()
				origins, originErr := result.Provenance()
				dependencies, dependencyErr := result.Dependencies()
				_, protocolErr := result.RequiredProtocolVersion()
				result.Release()
				require.NoError(b, columnErr)
				require.NoError(b, originErr)
				require.NoError(b, dependencyErr)
				require.NoError(b, protocolErr)
				require.Len(b, columns, 2)
				require.Len(b, origins, 2)
				require.Len(b, dependencies, 4)
			}
			b.StopTimer()
			if request != nil {
				request.Close()
				request = nil
			}
			require.Zero(b, f.generation.Used())
		})
	}
}
