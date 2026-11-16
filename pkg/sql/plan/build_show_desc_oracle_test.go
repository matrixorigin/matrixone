// Copyright 2025 Matrix Origin
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
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// descParseOne parses exactly one statement for the DESCRIBE shortcuts.
func descParseOne(t *testing.T, ctx CompilerContext, sql string) tree.Statement {
	t.Helper()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(stmts))
	return stmts[0]
}

func descShowColumns(t *testing.T, stmt tree.Statement) *tree.ShowColumns {
	t.Helper()
	sc, ok := stmt.(*tree.ShowColumns)
	require.True(t, ok, "DESCRIBE parses to SHOW COLUMNS")
	return sc
}

// descAddUserViewFixture installs a user view whose stored physical column
// snapshot is deliberately stale while its live description (ViewSql) emits
// the origin-case alias `livename`.
func descAddUserViewFixture(t *testing.T, mock *MockOptimizer, view, physicalName, physicalOrigin string) {
	t.Helper()
	def := DeepCopyTableDef(mock.ctxt.tables["nation"], true)
	require.NotNil(t, def)
	def.Name = view
	for _, col := range def.Cols {
		if col.Name == "n_name" {
			col.Name = physicalName
			col.OriginName = physicalOrigin
			break
		}
	}
	def.ViewSql = &plan.ViewDef{}
	def.ViewSql.View = `{"Stmt":"create view ` + view + ` as select n_name as livename from nation","DefaultDatabase":"tpch"}`
	objRef := &ObjectRef{SchemaName: "tpch", ObjName: view, Obj: int64(def.TblId)}
	qualified := mockQualifiedTableName("tpch", view)
	mock.ctxt.tables[view] = def
	mock.ctxt.objects[view] = objRef
	mock.ctxt.tablesByQualifiedName[qualified] = def
	mock.ctxt.objectsByQualifiedName[qualified] = objRef
	mock.ctxt.legacyTableOwners[view] = qualified
	mock.ctxt.legacyObjectOwners[view] = qualified
}

// descKeptByFilterPattern locates the bound predicate carrying the filter pattern in the built
// DESCRIBE/SHOW plan and executes it with the repository expression executor
// over the authoritative field values, returning the kept rows. A missing
// bound predicate fails instead of passing silently: removing the derived
// filter must be caught, not satisfied. Only the column references are remapped
// into a local batch; the function object, overload and pattern literal stay
// exactly as the planner bound them.
func descKeptByFilterPattern(t *testing.T, proc *process.Process, p *Plan, pattern string, fields []string) []string {
	t.Helper()
	// The plan keeps an exact-name pattern as its LIKE predicate, or folds it
	// where the composition allows; locate the predicate that actually carries
	// the pattern literal the planner produced.
	var predicate *plan.Expr
	present := queryContainsExpr(p.GetQuery(), func(expr *plan.Expr) bool {
		if fn := expr.GetF(); fn != nil && predicate == nil {
			for _, arg := range fn.Args {
				if exprContainsStringLiteral(arg, pattern) {
					predicate = expr
					return true
				}
			}
		}
		return false
	})
	require.True(t, present && predicate != nil,
		"the plan must carry the generated bound filter predicate for %q", pattern)
	bound := DeepCopyExpr(predicate)
	replacements := map[[2]int32]*plan.Expr{}
	var collect func(expr *plan.Expr)
	collect = func(expr *plan.Expr) {
		if col := expr.GetCol(); col != nil {
			key := [2]int32{col.RelPos, col.ColPos}
			if _, ok := replacements[key]; !ok {
				replacements[key] = &plan.Expr{
					Typ:  expr.Typ,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(len(replacements))}},
				}
			}
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				collect(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				collect(arg)
			}
		}
	}
	collect(bound)
	require.Len(t, replacements, 1, "the DESCRIBE LIKE predicate filters a single authoritative name column")
	bound = replaceColumnsForExpr(bound, replacements)
	in := batch.NewWithSize(1)
	in.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer in.Vecs[0].Free(proc.Mp())
	for _, f := range fields {
		require.NoError(t, vector.AppendBytes(in.Vecs[0], []byte(f), false, proc.Mp()))
	}
	in.SetRowCount(len(fields))
	executor, err := colexec.NewExpressionExecutor(proc, bound)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{in}, nil)
	require.NoError(t, err)
	kept := make([]string, 0, len(fields))
	for i := range fields {
		if !result.IsNull(uint64(i)) && vector.GetFixedAtWithTypeCheck[bool](result, i) {
			kept = append(kept, fields[i])
		}
	}
	return kept
}

// G1: a reused saved DESC AST must re-derive its evaluated filter from the
// CURRENT stored spelling, exactly like a fresh plan. The evaluated predicate
// runs first; the saved-AST pristine check comes afterwards (#32197 review).
func TestDescColNameReplanRefiltersAfterStoredSpellingChange(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	proc := newPlanTestProcess(t)
	ctx := mock.CurrentContext()
	stmt := descParseOne(t, ctx, "desc nation n_name")
	defer stmt.Free()
	sc := descShowColumns(t, stmt)
	nationCol := mock.ctxt.tables["nation"].Cols[1]
	require.Equal(t, "n_name", nationCol.Name)

	nationCol.OriginName = "N_Name"
	first, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"N_Name"},
		descKeptByFilterPattern(t, proc, first, "N_Name", []string{"N_Name", "n_PAME"}),
		"the first build filters the then-current stored spelling")
	require.True(t, queryContainsStringLiteral(first.GetQuery(), "N_Name"))

	nationCol.OriginName = "n_PAME"
	reused, err := BuildPlan(ctx, stmt, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"n_PAME"},
		descKeptByFilterPattern(t, proc, reused, "n_PAME", []string{"N_Name", "n_PAME"}),
		"a reused prepared DESC must filter by the current stored spelling")

	fresh, err := runOneStmt(mock, t, "desc nation n_name")
	require.NoError(t, err)
	require.Equal(t, []string{"n_PAME"},
		descKeptByFilterPattern(t, proc, fresh, "n_PAME", []string{"N_Name", "n_PAME"}),
		"a fresh DESC filters by the current stored spelling")
	require.True(t, queryContainsStringLiteral(reused.GetQuery(), "n_PAME"))
	require.False(t, queryContainsStringLiteral(reused.GetQuery(), "N_Name"))

	// Authored LIKE and the wildcard/unknown shortcuts execute their real
	// predicate over the current stored fields.
	likeStmt, err := runOneStmt(mock, t, "show columns from nation like 'n_na%'")
	require.NoError(t, err)
	// The repository LIKE honors the effective (case-insensitive) collation
	// (#31175), so the flipped-store spelling differs beyond letter case:
	// n_PAME matches an exact-name filter, not the authored lowercase prefix.
	require.Equal(t, []string{"n_nationkey"},
		descKeptByFilterPattern(t, proc, likeStmt, "n_na%", []string{"n_nationkey", "n_PAME", "n_regionkey", "n_comment"}))
	wild, err := runOneStmt(mock, t, "desc nation `n_na%`")
	require.NoError(t, err)
	// The repository LIKE honors the effective (case-insensitive) collation
	// (#31175), so the flipped-store spelling differs beyond letter case:
	// n_PAME matches an exact-name filter, not the authored lowercase prefix.
	require.Equal(t, []string{"n_nationkey"},
		descKeptByFilterPattern(t, proc, wild, "n_na%", []string{"n_nationkey", "n_PAME", "n_regionkey", "n_comment"}))
	miss, err := runOneStmt(mock, t, "desc nation nosuch")
	require.NoError(t, err)
	require.Empty(t, descKeptByFilterPattern(t, proc, miss, "nosuch", []string{"n_PAME", "n_regionkey"}))

	// Pristine saved AST AFTER the evaluated witnesses.
	require.Nil(t, sc.Like, "the derived predicate must not persist into the saved AST")
}

// G2: for user views the live description that produces the rows also owns the
// EVALUATED DESCRIBE filter, beating the stale map-hit physical origin and
// the map-miss stale physical snapshot alike.
func TestDescColNameUserViewFiltersByLiveDescriptionSpelling(t *testing.T) {
	cases := []struct {
		name           string
		physicalName   string
		physicalOrigin string
		stale          string
	}{
		{"map hit stale origin", "livename", "LiveName", "LiveName"},
		{"map miss stale snapshot", "oldthing", "OldThing", "LIVENAME"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := newPlanTestProcess(t)
			descAddUserViewFixture(t, mock, "v_hit", tc.physicalName, tc.physicalOrigin)
			stmt := descParseOne(t, mock.CurrentContext(), "desc v_hit LIVENAME")
			defer stmt.Free()
			sc := descShowColumns(t, stmt)
			got, err := BuildPlan(mock.CurrentContext(), stmt, false, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"livename"},
				descKeptByFilterPattern(t, proc, got, "livename", []string{"livename", "n_regionkey"}),
				"the live description that produces the rows must own the evaluated filter")
			require.False(t, queryContainsStringLiteral(got.GetQuery(), tc.stale),
				"stale spelling %q must not reach the planner filter", tc.stale)
			require.Nil(t, sc.Like, "the derived predicate must not persist into the saved AST")
		})
	}
	t.Run("current physical control", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		proc := newPlanTestProcess(t)
		got, err := runOneStmt(mock, t, "desc nation n_name")
		require.NoError(t, err)
		require.Equal(t, []string{"n_name"},
			descKeptByFilterPattern(t, proc, got, "n_name", []string{"n_name", "n_nationkey"}))
	})
	t.Run("lowercase authored argument", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		proc := newPlanTestProcess(t)
		descAddUserViewFixture(t, mock, "v_ctl", "livename", "LiveName")
		got, err := runOneStmt(mock, t, "desc v_ctl livename")
		require.NoError(t, err)
		require.Equal(t, []string{"livename"},
			descKeptByFilterPattern(t, proc, got, "livename", []string{"livename", "n_regionkey"}))
	})
	t.Run("authored wildcard keeps authored semantics", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		proc := newPlanTestProcess(t)
		descAddUserViewFixture(t, mock, "v_ctl", "livename", "LiveName")
		got, err := runOneStmt(mock, t, "desc v_ctl `live%`")
		require.NoError(t, err)
		require.Equal(t, []string{"livename"},
			descKeptByFilterPattern(t, proc, got, "live%", []string{"livename", "n_regionkey"}))
	})
	t.Run("unknown name filters to no rows", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		proc := newPlanTestProcess(t)
		descAddUserViewFixture(t, mock, "v_ctl", "livename", "LiveName")
		got, err := runOneStmt(mock, t, "desc v_ctl nosuchcol")
		require.NoError(t, err)
		require.Empty(t, descKeptByFilterPattern(t, proc, got, "nosuchcol", []string{"livename", "n_regionkey"}))
	})
}

type descViewProviderRecorder struct {
	MockCompilerContext
	openCount, closeCount int
	newCtxErr             error
	events                []string
	querying              *SubscriptionMeta
	childBoundAccounts    []uint32
	childBoundSubs        int
}

func (r *descViewProviderRecorder) SetQueryingSubscription(sub *SubscriptionMeta) {
	r.querying = sub
	if sub == nil {
		r.events = append(r.events, "parent-restore")
	} else {
		r.events = append(r.events, "parent-bind")
	}
}

func (r *descViewProviderRecorder) GetQueryingSubscription() *SubscriptionMeta {
	return r.querying
}

func (r *descViewProviderRecorder) NewViewDescriptionCompilerContext(context.Context) (CompilerContext, func(), error) {
	r.openCount++
	if r.newCtxErr != nil {
		return nil, nil, r.newCtxErr
	}
	child := &descViewProviderChild{MockCompilerContext: r.MockCompilerContext, rec: r}
	return child, func() { r.closeCount++; r.events = append(r.events, "cleanup") }, nil
}

type descViewProviderChild struct {
	MockCompilerContext
	rec *descViewProviderRecorder
}

func (c *descViewProviderChild) SetContext(ctx context.Context) {
	if id, acctErr := defines.GetAccountId(ctx); acctErr == nil {
		c.rec.childBoundAccounts = append(c.rec.childBoundAccounts, id)
	}
	c.rec.events = append(c.rec.events, "bind-account")
	c.MockCompilerContext.SetContext(ctx)
}

func (c *descViewProviderChild) SetQueryingSubscription(sub *SubscriptionMeta) {
	c.rec.childBoundSubs++
	if sub != nil {
		c.rec.events = append(c.rec.events, "bind-sub")
	}
	c.MockCompilerContext.SetQueryingSubscription(sub)
}

// descPostDescriptionBindFailContext fails the rewritten-sql binding (the
// mo_catalog lookups of the composed SHOW COLUMNS query) once the child live
// description has closed, so the build fails AFTER a successful description.
type descPostDescriptionBindFailContext struct {
	*descViewProviderRecorder
}

func (c *descPostDescriptionBindFailContext) Resolve(
	dbName string, tableName string, snapshot *Snapshot, borrowOutput *TableResolution,
) (*ObjectRef, *TableDef, error) {
	borrowOutput.Reset()
	if c.closeCount >= 1 && strings.EqualFold(dbName, MO_CATALOG_DB_NAME) {
		return nil, nil, errors.New("rewritten sql binding unavailable")
	}
	return c.descViewProviderRecorder.Resolve(dbName, tableName, snapshot, nil)
}

func (c *descPostDescriptionBindFailContext) GetAccountId() (uint32, error) {
	if c.closeCount >= 1 {
		return 0, errors.New("rewritten sql binding unavailable")
	}
	return c.descViewProviderRecorder.GetAccountId()
}

// G3: the DESCRIBE build path with an active subscription binds the publisher
// account and subscription BEFORE the live child description, shares that one
// authoritative description with rows and evaluated predicate, and restores
// the caller on success, on description failure, and on rewritten-sql failure.
func TestDescColNameSubscriptionViewBindsPublisherAndRestoresContext(t *testing.T) {
	newRecorder := func(t *testing.T) (*MockOptimizer, *descViewProviderRecorder) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		descAddUserViewFixture(t, mock, "v_sub", "livename", "LiveName")
		mock.ctxt.objects["v_sub"].PubInfo = &plan.PubInfo{TenantId: 17}
		rec := &descViewProviderRecorder{MockCompilerContext: mock.ctxt}
		return mock, rec
	}
	fullEventOrder := []string{"parent-bind", "bind-account", "bind-sub", "cleanup", "parent-restore"}

	t.Run("success binds publisher then cleans up", func(t *testing.T) {
		_, rec := newRecorder(t)
		proc := newPlanTestProcess(t)
		stmt := descParseOne(t, rec, "desc v_sub LIVENAME")
		defer stmt.Free()
		sc := descShowColumns(t, stmt)
		got, err := BuildPlan(rec, stmt, false, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"livename"},
			descKeptByFilterPattern(t, proc, got, "livename", []string{"livename", "n_regionkey"}),
			"the published view's live description owns the evaluated filter")
		require.False(t, queryContainsStringLiteral(got.GetQuery(), "LiveName"))
		require.Equal(t, 1, rec.openCount, "one authoritative description per build")
		require.Equal(t, 1, rec.closeCount, "the child description binding closes exactly once")
		require.Equal(t, []uint32{17}, rec.childBoundAccounts,
			"publisher account attaches before the live description")
		require.Equal(t, 1, rec.childBoundSubs,
			"publisher subscription attaches before the live description")
		require.Equal(t, fullEventOrder, rec.events)
		require.Nil(t, rec.querying)
		require.Nil(t, sc.Like)
	})

	t.Run("describe failure on the published target cleans up and restores", func(t *testing.T) {
		mock, rec := newRecorder(t)
		mock.ctxt.tables["v_sub"].ViewSql.View = `{"Stmt":"create view v_sub as NOSEL","DefaultDatabase":"tpch"}`
		stmt := descParseOne(t, rec, "desc v_sub LIVENAME")
		defer stmt.Free()
		sc := descShowColumns(t, stmt)
		_, err := BuildPlan(rec, stmt, false, nil)
		require.Error(t, err)
		require.Equal(t, 1, rec.openCount)
		require.Equal(t, 1, rec.closeCount, "the created child closes exactly once when the description fails")
		require.Equal(t, []uint32{17}, rec.childBoundAccounts,
			"publisher binding precedes the failed description")
		require.Equal(t, 1, rec.childBoundSubs)
		require.Equal(t, fullEventOrder, rec.events,
			"caller subscription is restored on the description failure path")
		require.Nil(t, rec.querying)
		require.Nil(t, sc.Like, "the saved AST stays original after the failed description")
	})

	t.Run("rewritten sql fails after a successful description", func(t *testing.T) {
		_, rec := newRecorder(t)
		failCtx := &descPostDescriptionBindFailContext{descViewProviderRecorder: rec}
		stmt := descParseOne(t, failCtx, "desc v_sub LIVENAME")
		defer stmt.Free()
		sc := descShowColumns(t, stmt)
		got, err := BuildPlan(failCtx, stmt, false, nil)
		require.Error(t, err, "the rewritten sql must fail to bind after the successful child description")
		require.Nil(t, got)
		require.Equal(t, 1, rec.openCount)
		require.Equal(t, 1, rec.closeCount,
			"the successful child description closes exactly once before the binding failure")
		require.Equal(t, []uint32{17}, rec.childBoundAccounts)
		require.Equal(t, 1, rec.childBoundSubs)
		require.Equal(t, fullEventOrder, rec.events,
			"caller subscription is restored after the rewritten-sql failure")
		require.Nil(t, rec.querying)
		require.Nil(t, sc.Like, "the saved AST stays original after the binding failure")
	})

	t.Run("child creation failure restores caller", func(t *testing.T) {
		_, rec := newRecorder(t)
		rec.newCtxErr = errors.New("binding context unavailable")
		stmt := descParseOne(t, rec, "desc v_sub LIVENAME")
		defer stmt.Free()
		sc := descShowColumns(t, stmt)
		_, err := BuildPlan(rec, stmt, false, nil)
		require.Error(t, err)
		require.Equal(t, 1, rec.openCount)
		require.Equal(t, 0, rec.closeCount, "failed creation must not invoke cleanup")
		require.Empty(t, rec.childBoundAccounts, "no publisher binding without a created child")
		require.Zero(t, rec.childBoundSubs)
		require.Equal(t, []string{"parent-bind", "parent-restore"}, rec.events)
		require.Nil(t, rec.querying, "caller subscription restored after failure")
		require.Nil(t, sc.Like)
	})

	t.Run("subscription view without provider fails closed", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		descAddUserViewFixture(t, mock, "v_nop", "livename", "LiveName")
		mock.ctxt.objects["v_nop"].PubInfo = &plan.PubInfo{TenantId: 17}
		wrapper := &descNoProviderSubscribedContext{
			MockCompilerContext: &mock.ctxt,
			sub:                 &SubscriptionMeta{AccountId: 17},
		}
		stmt := descParseOne(t, wrapper, "desc v_nop LIVENAME")
		defer stmt.Free()
		_, err := BuildPlan(wrapper, stmt, false, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "isolated binding context")
	})
}

type descNoProviderSubscribedContext struct {
	*MockCompilerContext
	sub *SubscriptionMeta
}

func (c *descNoProviderSubscribedContext) GetQueryingSubscription() *SubscriptionMeta {
	return c.sub
}

// The quoted wildcard argument is the SHOW COLUMNS LIKE filter: the
// DESCRIBE form derives the identical bound predicate and the repository
// expression executor keeps exactly the genuinely matching current
// columns, with the escaped forms behaving verbatim like the authored
// LIKE comparison.
func TestDescQuotedWildcardFiltersThroughRealLikeEvaluation(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	proc := newPlanTestProcess(t)
	ctx := mock.CurrentContext()
	fields := []string{"n_nationkey", "n_name", "n_regionkey", "n_comment"}

	// Start from the parse: the quoted argument must reach the planner as
	// a LIKE comparison, never as a named column.
	stmt := descParseOne(t, ctx, "desc nation 'n_na%'")
	defer stmt.Free()
	sc := descShowColumns(t, stmt)
	require.NotNil(t, sc.Like)
	require.Nil(t, sc.ColName)

	descStmt, err := runOneStmt(mock, t, "desc nation 'n_na%'")
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name"},
		descKeptByFilterPattern(t, proc, descStmt, "n_na%", fields),
		"the quoted prefix filter keeps the genuinely matching columns")

	showStmt, err := runOneStmt(mock, t, "show columns from nation like 'n_na%'")
	require.NoError(t, err)
	require.Equal(t, []string{"n_nationkey", "n_name"},
		descKeptByFilterPattern(t, proc, showStmt, "n_na%", fields),
		"the authored SHOW COLUMNS LIKE form keeps the same rows")

	allStmt, err := runOneStmt(mock, t, "desc nation '%'")
	require.NoError(t, err)
	require.Equal(t, fields,
		descKeptByFilterPattern(t, proc, allStmt, "%", fields),
		"the bare wildcard keeps every column")

	missStmt, err := runOneStmt(mock, t, "desc nation 'nosuch%'")
	require.NoError(t, err)
	require.Empty(t,
		descKeptByFilterPattern(t, proc, missStmt, "nosuch%", fields),
		"a non-matching pattern keeps zero rows")

	escStmt, err := runOneStmt(mock, t, "desc nation 'n\\_name'")
	require.NoError(t, err)
	require.Equal(t, []string{"n_name"},
		descKeptByFilterPattern(t, proc, escStmt, `n\_name`, fields),
		"the escaped underscore matches the literal column name only")

	escShowStmt, err := runOneStmt(mock, t, "show columns from nation like 'n\\_name'")
	require.NoError(t, err)
	require.Equal(t, []string{"n_name"},
		descKeptByFilterPattern(t, proc, escShowStmt, `n\_name`, fields),
		"the authored escaped LIKE form keeps the same single row")
}
