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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type viewMetadataTestCompiler struct {
	*viewSchemaTestCompiler
	ctx context.Context
}

func (c *viewMetadataTestCompiler) GetContext() context.Context    { return c.ctx }
func (c *viewMetadataTestCompiler) SetContext(ctx context.Context) { c.ctx = ctx }

func TestViewMetadataRequestSharesBudgetAndAuthorizesMemoHits(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_nationkey from nation")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	require.False(t, HasViewMetadataRequest(t.Context()))
	require.True(t, HasViewMetadataRequest(r.Context()))
	for i := 0; i < 2; i++ {
		func() {
			result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
			require.NoError(t, err)
			defer result.Release()
			columns, err := result.Columns()
			require.NoError(t, err)
			require.Equal(t, "n_nationkey", columns[0].Name)
			require.Equal(t, int32(types.T_int32), columns[0].Typ.Id)
		}()
	}
	require.Equal(t, int64(2), f.authorizes.Load())
	require.Equal(t, uint64(1), r.schema.hits)
	r.schema.roots = viewSchemaWorkLimit
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	r.Close()
	require.Zero(t, f.generation.Used())
	require.Equal(t, int64(1), f.closes.Load())
	_, err = DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	_, err = DescribeViewMetadata(t.Context(), "tpch", "current_v", nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
}

func TestViewMetadataRequestCancellationWhileWaitingDoesNotOwnBinder(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_nationkey from nation")
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.binding.Authorize = func(ctx context.Context, _, _ string, _ *Snapshot) error {
		close(entered)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-resume:
			return nil
		}
	}
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	var firstErr error
	go func() {
		defer close(done)
		result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
		firstErr = err
		result.Release()
	}()
	viewSchemaTestWait(t, entered)
	ctx, cancel := context.WithCancel(r.Context())
	cancel()
	_, err := DescribeViewMetadata(ctx, "tpch", "current_v", nil)
	require.ErrorIs(t, err, context.Canceled)
	close(resume)
	viewSchemaTestWait(t, done)
	require.NoError(t, firstErr)
}

func TestViewMetadataRequestCloseCancelsActiveBinderAndWaiters(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	entered, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.binding.Authorize = func(ctx context.Context, _, _ string, _ *Snapshot) error {
		close(entered)
		<-ctx.Done()
		return context.Cause(ctx)
	}
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	var firstErr error
	go func() {
		defer close(done)
		result, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
		firstErr = err
		result.Release()
	}()
	viewSchemaTestWait(t, entered)
	go func() { r.Close(); close(closed) }()
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaClosed)
	viewSchemaTestWait(t, done)
	viewSchemaTestWait(t, closed)
	require.ErrorIs(t, firstErr, ErrViewSchemaClosed)
	require.Zero(t, f.generation.Used())
}

func TestViewMetadataShowUsesCommonRequest(t *testing.T) {
	for _, sql := range []string{"describe current_v", "show columns from current_v", "show full columns from current_v", "show column_number from current_v", "show table_values from current_v"} {
		t.Run(sql, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			def := f.addView(t, "current_v", "select n_nationkey from nation")
			r := NewViewMetadataRequest(t.Context(), f.provider)
			defer r.Close()
			ctx := &viewMetadataTestCompiler{viewSchemaTestCompiler: f.compiler, ctx: r.Context()}
			p, err := buildViewMetadataConsumerPlan(t, ctx, sql, false)
			require.NoError(t, err)
			require.Positive(t, f.authorizes.Load())
			deps := p.GetQuery().CatalogDependencies
			require.True(t, p.GetQuery().ViewMetadataDependsOnUdf, "untracked non-relation dependencies force SHOW rebind")
			ids := make(map[int64]bool)
			for _, dep := range deps {
				ids[dep.Obj] = true
			}
			require.True(t, ids[int64(def.TblId)])
			require.True(t, ids[10], "the VALUES plan must retain the base relation identity")
			require.Equal(t, "stale", def.Cols[0].Name, "description cannot publish into the physical catalog")
			f.authErr = errors.New("denied")
			_, err = buildViewMetadataConsumerPlan(t, ctx, sql, false)
			require.ErrorIs(t, err, f.authErr)
		})
	}
}

func TestViewMetadataDependencyRefsPreserveDomain(t *testing.T) {
	deps := []ViewDependency{
		{AccountID: 7, DatabaseID: 3, RelationID: 9, Version: 4, DatabaseName: "pub", RelationName: "v", SubscriptionName: "sub"},
		{AccountID: 7, DatabaseID: 3, RelationID: 10, Version: 5, DatabaseName: "pub", RelationName: "t"},
		{AccountID: 42, DatabaseID: 4, RelationID: 11, Version: 6, DatabaseName: "local", RelationName: "t"},
		{AccountID: 7, DatabaseID: 3, RelationID: 9, Version: 4, DatabaseName: "pub", RelationName: "v", SubscriptionName: "another_sub"},
	}
	refs := viewMetadataDependencyRefs(deps, 42)
	require.Len(t, refs, 4)
	require.Equal(t, "another_sub", refs[3].SubscriptionName)
	require.Equal(t, int32(7), refs[0].PubInfo.TenantId)
	require.Equal(t, "sub", refs[0].SubscriptionName)
	require.Equal(t, int32(7), refs[1].PubInfo.TenantId)
	require.Nil(t, refs[2].PubInfo)
	require.Equal(t, int64(6), refs[2].Server)
}

func TestViewMetadataRejectsChangedConsumerIdentity(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_name from nation")
	r := NewViewMetadataRequest(defines.AttachAccount(t.Context(), 42, 7, 9), f.provider)
	defer r.Close()
	for _, ids := range [][3]uint32{{43, 7, 9}, {42, 8, 9}, {42, 7, 10}} {
		_, err := DescribeViewMetadata(defines.AttachAccount(r.Context(), ids[0], ids[1], ids[2]), "tpch", "current_v", nil)
		require.ErrorIs(t, err, ErrViewSchemaChanged)
	}
	require.Zero(t, f.authorizes.Load())
	result, err := DescribeViewMetadata(defines.AttachAccount(r.Context(), 42, 7, 9), "tpch", "current_v", nil)
	require.NoError(t, err)
	result.Release()
}

func TestViewMetadataBranchesShareWorkAndReleaseCancellation(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_name from nation")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	for i := 0; i < 2; i++ {
		branch, cancel := context.WithCancel(r.Context())
		result, err := DescribeViewMetadata(branch, "tpch", "current_v", nil)
		require.NoError(t, err)
		result.Release()
		cancel() // completed branch cleanup must not cancel later sibling work
		if i == 0 {
			r.schema.work = viewSchemaWorkLimit - 1
		}
	}
	require.Equal(t, viewSchemaWorkLimit, r.schema.work)
	_, err := DescribeViewMetadata(r.Context(), "tpch", "current_v", nil)
	require.ErrorIs(t, err, ErrViewSchemaLimit)
	require.Zero(t, f.generation.Used())
}

// collectNodeScanTables returns the lowercased names of every physical table
// scanned by the query, so tests can distinguish the logical description path
// from a persisted-column fallback.
func collectNodeScanTables(query *Query) map[string]bool {
	tables := make(map[string]bool)
	if query == nil {
		return tables
	}
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != planpb.Node_TABLE_SCAN || node.TableDef == nil {
			continue
		}
		tables[strings.ToLower(node.TableDef.Name)] = true
	}
	return tables
}

// The SHOW rewrite must never fall back to the persisted View column snapshot:
// user Views are described into a statement-local row source, while ordinary
// tables keep their physical mo_catalog.mo_columns read. Losing either side
// breaks the S2/S4 authority boundary documented for #30668.
func TestShowColumnsSplitsPhysicalAndLogicalAuthority(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "current_v", "select n_nationkey from nation")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	ctx := &viewMetadataTestCompiler{viewSchemaTestCompiler: f.compiler, ctx: r.Context()}

	for _, stmt := range []string{
		"describe current_v",
		"show columns from current_v",
		"show full columns from current_v",
		"show column_number from current_v",
	} {
		t.Run(stmt, func(t *testing.T) {
			p, err := buildViewMetadataConsumerPlan(t, ctx, stmt, false)
			require.NoError(t, err, stmt)
			require.False(t, collectNodeScanTables(p.GetQuery())["mo_columns"],
				stmt+": a described View must not scan the persisted column snapshot")
		})
	}
	// Ordinary tables keep reading the physical catalog. The plain mock is used
	// here on purpose: the authority split must not depend on an active derived
	// metadata request.
	for _, stmt := range []string{
		"describe nation",
		"show columns from nation",
		"show full columns from nation",
		"show column_number from nation",
	} {
		t.Run(stmt, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, stmt)
			require.NoError(t, err, stmt)
			require.True(t, collectNodeScanTables(p.GetQuery())["mo_columns"],
				stmt+": ordinary tables keep their physical mo_columns read")
		})
	}
}

func TestViewMetadataCandidateFailureProvenance(t *testing.T) {
	for _, stage := range []string{"authorize", "open", "root", "definition"} {
		t.Run(stage, func(t *testing.T) {
			f := newViewSchemaTestFixture(t)
			f.addView(t, "current_v", "select n_nationkey from nation")
			missing := moerr.NewNoSuchTable(t.Context(), "tpch", "missing")
			switch stage {
			case "authorize":
				f.authErr = missing
			case "open":
				f.provider = func(context.Context) (*ViewSchemaBinding, error) { return nil, missing }
			default:
				f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
					if (stage == "root" && name == "current_v") || (stage == "definition" && name == "nation") {
						return missing
					}
					return nil
				}
			}
			r := NewViewMetadataRequest(t.Context(), f.provider)
			defer r.Close()
			result, invalid, err := DescribeViewMetadataCandidate(r.Context(), "tpch", "current_v", nil)
			require.Nil(t, result)
			require.ErrorIs(t, err, missing)
			require.Equal(t, stage == "definition", invalid)
			_, err = r.schema.Describe("tpch", "current_v", nil)
			require.Same(t, missing, err, "existing S2 callers retain the original error identity/code")
			r.Close()
			require.Zero(t, f.generation.Used())
		})
	}
}

func TestViewMetadataWarmMemoFailureRecovery(t *testing.T) {
	for _, mode := range []string{"memo", "cache off"} {
		for _, fault := range []string{"root missing", "transient resolution", "authorization revoked"} {
			t.Run(mode+"/"+fault, func(t *testing.T) {
				f := newViewSchemaTestFixture(t)
				f.addView(t, "middle_v", "select n_name from nation")
				f.addView(t, "root_v", "select n_name from middle_v")
				r := NewViewMetadataRequest(t.Context(), f.provider)
				defer r.Close()
				r.schema.memoDisabled = mode == "cache off"
				for range 2 {
					result, err := DescribeViewMetadata(r.Context(), "tpch", "root_v", nil)
					require.NoError(t, err)
					result.Release()
				}
				if mode == "memo" {
					require.Positive(t, r.schema.hits)
				}
				failure := errors.New("temporary catalog failure")
				if fault == "root missing" {
					failure = moerr.NewNoSuchTable(t.Context(), "tpch", "root_v")
				}
				if fault == "authorization revoked" {
					f.authErr = failure
				} else {
					f.compiler.resolve = func(_ context.Context, _, name string, _ *Snapshot) error {
						if name == "root_v" {
							return failure
						}
						return nil
					}
				}
				result, invalid, err := DescribeViewMetadataCandidate(r.Context(), "tpch", "root_v", nil)
				result.Release()
				require.Nil(t, result, "预热结果不能遮蔽根对象解析或权限失败")
				require.ErrorIs(t, err, failure)
				require.False(t, invalid, "根查找失败不能伪装成 binder 确证的依赖缺失")
				require.Zero(t, f.generation.Used())
				f.authErr, f.compiler.resolve = nil, nil
				result, invalid, err = DescribeViewMetadataCandidate(r.Context(), "tpch", "root_v", nil)
				if result != nil {
					defer result.Release()
				}
				require.NoError(t, err)
				require.False(t, invalid)
				columns, err := result.Columns()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.Equal(t, "n_name", columns[0].Name)
				result.Release()
				r.Close()
				require.Zero(t, f.generation.Used())
			})
		}
	}
}

func TestViewMetadataWarmMemoCancellationDuringRootRead(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "middle_v", "select n_name from nation")
	f.addView(t, "root_v", "select n_name from middle_v")
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	result, err := DescribeViewMetadata(r.Context(), "tpch", "root_v", nil)
	require.NoError(t, err)
	result.Release()
	entered, done := make(chan struct{}), make(chan struct{})
	f.compiler.resolve = func(ctx context.Context, _, name string, _ *Snapshot) error {
		if name == "root_v" {
			close(entered)
			<-ctx.Done()
			// 取消与缺失同时可见时，取消必须获胜，不能被 bulk skip 吞掉。
			return moerr.NewNoSuchTable(ctx, "tpch", name)
		}
		return nil
	}
	ctx, cancel := context.WithCancelCause(r.Context())
	defer func() {
		cancel(nil)
		viewSchemaTestWait(t, done) // Join before the deferred request.Close, including assertion failures.
	}()
	var invalid bool
	go func() {
		defer close(done)
		result, invalid, err = DescribeViewMetadataCandidate(ctx, "tpch", "root_v", nil)
		result.Release()
	}()
	viewSchemaTestWait(t, entered)
	cancelled := errors.New("statement reader cancelled")
	cancel(cancelled)
	viewSchemaTestWait(t, done)
	require.Nil(t, result)
	require.False(t, invalid)
	require.ErrorIs(t, err, cancelled)
	r.Close()
	require.Zero(t, f.generation.Used())
	require.False(t, f.generation.Closed())

	// 失败只终结旧执行代；新请求不能继承失败或已取消的私有 compiler context。
	f.compiler.resolve = nil
	next := NewViewMetadataRequest(t.Context(), f.provider)
	defer next.Close()
	result, err = DescribeViewMetadata(next.Context(), "tpch", "root_v", nil)
	if result != nil {
		defer result.Release()
	}
	require.NoError(t, err)
	result.Release()
	next.Close()
	require.Zero(t, f.generation.Used())
}

// The actual public SHOW/view_columns metadata route drives the released
// extra-grouping fixture through DescribeViewMetadata: the request results
// carry the exact single visible BIGINT a and the seeded physical
// pg200.tw_src identity, the result and request are both released, and a
// marked full-binding control surfaces the genuine duplicate-window
// validation instead of a raw parse error.
func TestViewMetadataRequestDescribesReleasedExtraGroupingFixture(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	seedReleasePg200TwSrc(t, f.compiler.MockCompilerContext)
	seedQualified := mockQualifiedTableName("pg200", "tw_src")
	f.compiler.MockCompilerContext.tables["tw_src"] = f.compiler.MockCompilerContext.tablesByQualifiedName[seedQualified]
	f.compiler.MockCompilerContext.objects["tw_src"] = f.compiler.MockCompilerContext.objectsByQualifiedName[seedQualified]
	// The authentic release-written bytes: the release did not persist a
	// creation-time lower_case_table_names, so the typed metadata route
	// genuinely refuses them with the real legacy-context error.
	var authentic ViewData
	require.NoErrorf(t, json.Unmarshal([]byte(releasePg189VNested), &authentic), "the authentic fixture bytes must decode")
	annotated := func(marked bool) string {
		t.Helper()
		creationContext := int64(1)
		data, err := json.Marshal(ViewData{
			Stmt: authentic.Stmt, DefaultDatabase: "pg200",
			SQLMode: authentic.SQLMode, SecurityType: authentic.SecurityType,
			LowerCaseTableNames: &creationContext,
			CTEScopeProtocol: func() *int64 {
				if marked {
					one := int64(1)
					return &one
				}
				return nil
			}(),
		})
		require.NoErrorf(t, err, "the registered definition must marshal to valid JSON")
		return string(data)
	}
	registerNamedView := func(name, viewSQL string) {
		def := &TableDef{
			Name: name, DbName: "pg200", DbId: 7, TblId: 4203, LogicalId: 4203, Version: 5,
			TableType: catalog.SystemViewRel,
			ViewSql:   &planpb.ViewDef{View: viewSQL},
			// A stale physical schema must never determine the described columns.
			Cols: []*ColDef{{Name: "stale", Typ: planpb.Type{Id: int32(types.T_int8)}}},
		}
		qualified := mockQualifiedTableName("pg200", name)
		f.compiler.MockCompilerContext.tablesByQualifiedName[qualified] = def
		f.compiler.MockCompilerContext.objectsByQualifiedName[qualified] = &ObjectRef{DbName: "pg200", ObjName: name, Obj: 4203, SchemaName: "pg200"}
		f.compiler.MockCompilerContext.tables[name] = def
		f.compiler.MockCompilerContext.objects[name] = &ObjectRef{DbName: "pg200", ObjName: name, Obj: 4203, SchemaName: "pg200"}
	}
	registerNamedView("v_nested", releasePg189VNested)
	createSQL := "create view pg200.v_nested as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute))) interval(ts, 2, minute) limit 5"
	orig := f.provider
	f.provider = func(ctx context.Context) (*ViewSchemaBinding, error) {
		b, err := orig(ctx)
		if err != nil {
			return nil, err
		}
		b.Compiler.SetContext(WithRestoredViewDefinition(b.Compiler.GetContext(), "pg200", "v_nested", releasePg189VNested, createSQL))
		return b, nil
	}
	r := NewViewMetadataRequest(t.Context(), f.provider)
	defer r.Close()
	_, err := DescribeViewMetadata(r.Context(), "pg200", "v_nested", nil)
	require.ErrorContainsf(t, err, "LEGACY_CONTEXT_UNAVAILABLE", "the strict authentic release bytes carry no creation-time context and must be refused by the real creation-context guard")
	// The provider-backed pattern registers the view exactly the way this
	// fixture registers every described view: the authentic statement with
	// the creation-time context. The released inference then describes it.
	registerNamedView("v_nested_ctx", annotated(false))
	result, err := DescribeViewMetadata(r.Context(), "pg200", "v_nested_ctx", nil)
	require.NoErrorf(t, err, "the actual metadata route must describe the released extra-grouping fixture through the creation-context registration")
	require.NotNilf(t, result, "the metadata route must return a real result")
	defer result.Release()
	cols, err := result.Columns()
	require.NoErrorf(t, err, "the result columns decode")
	require.Lenf(t, cols, 1, "exactly one visible output column per the release SHOW COLUMNS readback")
	require.Truef(t, strings.EqualFold(cols[0].Name, "a"), "the single visible output column is a")
	require.Equalf(t, int32(types.T_int64), cols[0].Typ.Id, "the output column a is BIGINT per the release readback")
	deps, err := result.Dependencies()
	require.NoErrorf(t, err, "the result dependencies decode")
	var sawSrc bool
	for _, dep := range deps {
		if strings.EqualFold(dep.DatabaseName, "pg200") && strings.EqualFold(dep.RelationName, "tw_src") {
			require.Equalf(t, uint64(4202), dep.RelationID, "the physical dependency must resolve to the seeded RelationID")
			require.Equalf(t, uint64(4202), dep.LogicalID, "the physical dependency must carry the seeded LogicalID")
			sawSrc = true
		}
	}
	require.Truef(t, sawSrc, "the real result must carry the seeded physical pg200.tw_src dependency (refuse nil/empty)")
	// The marked full-binding control on the same actual route: the genuine
	// duplicate-window validation, not a parse error.
	marked := annotated(true)
	requireRecognizedMarker(t, marked)
	registerNamedView("v_nested_marked", marked)
	_, err = DescribeViewMetadata(r.Context(), "pg200", "v_nested_marked", nil)
	require.ErrorContainsf(t, err, "multiple time window clauses", "a marked definition keeps the complete binding and must genuinely surface the duplicate-window validation through the metadata route")
	// The reviewers' exact triple-window body through the SAME real route and
	// request/binding: the released inference boundary pins the immediate
	// middle SELECT and its two-minute ownership in the actual binding.
	tripleStmt := "create view pg200.v_triple as ((select count(*) as a from pg200.tw_src interval(ts, 1, minute)) interval(ts, 2, minute) order by a) interval(ts, 3, minute) limit 5"
	tripleContext := int64(1)
	tripleFree, mErr := json.Marshal(ViewData{Stmt: tripleStmt, DefaultDatabase: "pg200",
		SQLMode: authentic.SQLMode, SecurityType: authentic.SecurityType,
		LowerCaseTableNames: &tripleContext})
	require.NoErrorf(t, mErr, "the triple-window creation-context definition must marshal")
	registerNamedView("v_triple_ctx", string(tripleFree))
	tripleRes, tErr := DescribeViewMetadata(r.Context(), "pg200", "v_triple_ctx", nil)
	require.NoErrorf(t, tErr, "the reviewers' triple-window body describes through the same real route")
	// Acquire-time release: a fatal column/dependency assertion below must
	// not leave the acquired reader live while the deferred request Close
	// waits for readers. Release is idempotent, and this later-registered
	// defer runs before the deferred r.Close, while the explicit release
	// below still guards the same-request reuse ordering.
	defer tripleRes.Release()
	tripleCols, cErr := tripleRes.Columns()
	require.NoErrorf(t, cErr, "the triple-window describe returns columns")
	require.Lenf(t, tripleCols, 1, "the triple-window view exposes exactly one visible column")
	require.Truef(t, strings.EqualFold(tripleCols[0].GetName(), "a"), "the visible column is a")
	require.Equalf(t, int32(types.T_int64), tripleCols[0].Typ.Id, "the visible column a is BIGINT per the middle-window release binding")
	sawTripleSrc := false
	tripleDeps, dErr := tripleRes.Dependencies()
	require.NoErrorf(t, dErr, "the triple-window dependencies decode")
	for _, dep := range tripleDeps {
		if strings.EqualFold(dep.DatabaseName, "pg200") && strings.EqualFold(dep.RelationName, "tw_src") {
			require.Equalf(t, uint64(4202), dep.RelationID, "the decoded direct physical RelationID")
			require.Equalf(t, uint64(4202), dep.LogicalID, "the decoded direct physical LogicalID")
			sawTripleSrc = true
		}
	}
	require.Truef(t, sawTripleSrc, "the triple-window describe binds the seeded pg200.tw_src dependency")
	// Release explicitly BEFORE the same-request reuse describe below.
	tripleRes.Release()
	// The effective two-minute ownership observed from the ACTUAL bound
	// inference, not a separately parsed AST: regenerate the same persisted
	// marker-free definition through the production regeneration path. The
	// rewritten Definition renders the bound body - the immediate middle
	// child's own two-minute window is the effective interval, while the
	// outer wrapper's three-minute window never reaches the regenerated
	// catalog definition.
	prevCtx := f.compiler.GetContext()
	f.compiler.SetContext(WithRestoredViewDefinition(context.Background(), "pg200", "v_triple_ctx", string(tripleFree), tripleStmt))
	regen, regenErr := RegenerateViewDefinition(f.compiler, string(tripleFree))
	f.compiler.SetContext(prevCtx)
	require.NoErrorf(t, regenErr, "the production regeneration succeeds on the same persisted triple-window definition")
	var regenData ViewData
	require.NoErrorf(t, json.Unmarshal([]byte(regen.TableDef.ViewSql.View), &regenData), "the regenerated catalog definition must decode")
	t.Logf("regenerated triple-window definition: %s", regenData.Definition)
	require.Containsf(t, regenData.Definition, "interval(`tw_src`.`ts`, 2, minute)",
		"the effective window from the actual bound inference is the middle two-minute interval, qualified onto the resolved tw_src.ts column")
	require.NotContainsf(t, regenData.Definition, "interval(`tw_src`.`ts`, 1, minute)",
		"the peeled inner one-minute window is not the binding's own window")
	require.NotContainsf(t, regenData.Definition, "interval(`tw_src`.`ts`, 3, minute)",
		"the outer wrapper's three-minute window is not the binding's own window")
	// Release and reuse on the same request context: after the explicit release
	// above, the request gate must admit a second describe of the same
	// marker-free view.
	reuseRes, reuseErr := DescribeViewMetadata(r.Context(), "pg200", "v_triple_ctx", nil)
	require.NoErrorf(t, reuseErr, "the same request context must admit a second describe after release")
	reuseRes.Release()
	// The marked same-body rejection through the same request: the whole-
	// statement duplicate-window validation stays genuine.
	tripleMarked, mkErr := json.Marshal(ViewData{Stmt: tripleStmt, DefaultDatabase: "pg200",
		SQLMode: authentic.SQLMode, SecurityType: authentic.SecurityType,
		LowerCaseTableNames: &tripleContext, CTEScopeProtocol: &tripleContext})
	require.NoErrorf(t, mkErr, "the marked triple definition must marshal")
	registerNamedView("v_triple_marked", string(tripleMarked))
	_, err = DescribeViewMetadata(r.Context(), "pg200", "v_triple_marked", nil)
	require.ErrorContainsf(t, err, "multiple time window clauses",
		"the marked same-body rejection reaches the real whole-statement duplicate-window validation")

}
