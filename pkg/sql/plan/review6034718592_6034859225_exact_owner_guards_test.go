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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// The exact reviewer witnesses for the three corrected owner seams: the
// INSERT source keeps both WITH-owner shapes, the pagination wrap exempts
// exactly an explicitly threaded executable Ep, and the restored legacy
// inference selects the inner window only for the exact released dual-window
// shape that owns no other authored state.

func p132ParseOne(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
}

// The INSERT source owner: a root WITH preceding the parenthesized body puts
// the WITH on the enclosing statement-level select (rowsForInsert), while a
// WITH inside the parentheses puts it on the body. The guard must keep the
// complete source in the first case and only the body in the second.
func TestInsertParenSourceOwnerKeepsBothWithShapes(t *testing.T) {
	root := p132ParseOne(t, "insert into t(a) with w as (select 9 as a) (select a from w)")
	ins := root.(*tree.Insert)
	rows := ins.Rows
	if rows == nil {
		t.Fatalf("the root-owned-WITH source must bind a *tree.Select")
	}
	if rows.With == nil {
		t.Fatalf("the root-owned-WITH shape must carry its WITH on the enclosing source select")
	}
	paren, ok := rows.Select.(*tree.ParenSelect)
	if !ok || paren.Select == nil {
		t.Fatalf("the root-owned-WITH shape must wrap a parenthesized body")
	}
	if paren.Select.With != nil {
		t.Fatalf("in the root-owned shape the body itself must not own a second WITH")
	}

	inner := p132ParseOne(t, "insert into t(a) (with w as (select 9 as a) (select a from w))")
	ins2 := inner.(*tree.Insert)
	rows2 := ins2.Rows
	if rows2 == nil {
		t.Fatalf("the body-owned-WITH source must bind a *tree.Select")
	}
	if rows2.With != nil {
		t.Fatalf("in the body-owned shape the enclosing select must not own the WITH")
	}
	paren2, ok := rows2.Select.(*tree.ParenSelect)
	if !ok || paren2.Select == nil {
		t.Fatalf("the body-owned-WITH shape must wrap a parenthesized body")
	}
	if paren2.Select.With == nil {
		t.Fatalf("the body-owned shape must carry its WITH on the parenthesized body")
	}
	// The no-WITH nested separator shape: the enclosing select owns no WITH and
	// the body owns none either, so the historical body-only binding applies.
	nested := p132ParseOne(t, "insert into t(a) (select a from s limit 2) limit 0")
	ins3 := nested.(*tree.Insert)
	rows3 := ins3.Rows
	if rows3 == nil {
		t.Fatalf("the no-WITH separator source must bind a *tree.Select")
	}
	if rows3.With != nil {
		t.Fatalf("the no-WITH separator source must not own a WITH")
	}
	// The plain reviewed nested form keeps its authored inner LIMIT on the body.
	plain := p132ParseOne(t, "insert into t(a) (select 7 limit 1) limit 1")
	insPlain := plain.(*tree.Insert)
	rowsPlain := insPlain.Rows
	if rowsPlain == nil {
		t.Fatalf("the reviewed nested form must bind a *tree.Select")
	}
	parenPlain, ok := rowsPlain.Select.(*tree.ParenSelect)
	if !ok || parenPlain.Select == nil || parenPlain.Select.Limit == nil {
		t.Fatalf("the reviewed nested form must keep its authored inner LIMIT on the body")
	}
	if rowsPlain.Limit == nil {
		t.Fatalf("the reviewed nested form keeps its authored outer LIMIT on the enclosing source")
	}
}

// The pagination wrap's export exemption is bound to the explicitly threaded
// executable owner only: a nil owner (a nested binder entry such as the CTE
// body route) declines an Ep even when it sits at the walk root, and the
// threading of the real executable statement composes the pages with the Ep
// statement left as the frontend's single export owner.
func TestOrdinaryPaginationWrapExemptsOnlyExplicitExportOwner(t *testing.T) {
	// s owns the WITH and its inner page; an Ep rides on s itself.
	getWrapped := func() *tree.Select {
		stmt := p132ParseOne(t,
			"with q as (select 1 as a union all select 2 as a) (select a from q limit 2) order by a limit 1 offset 1 into outfile '/tmp/p132-guard.csv'")
		return stmt.(*tree.Select)
	}

	sNil := getWrapped()
	if _, _, _, ok := ordinaryPaginationWrap(sNil, false, nil); ok {
		t.Fatalf("a nil export owner must decline the composition when the Ep sits on the walk root")
	}

	sOwner := getWrapped()
	wrapped, _, _, ok := ordinaryPaginationWrap(sOwner, false, sOwner)
	if !ok {
		t.Fatalf("the explicitly threaded executable Ep statement must compose its pages")
	}
	if wrapped == nil {
		t.Fatalf("the composition must return a non-nil wrapped select")
	}
	if sOwner.Ep == nil {
		t.Fatalf("the original executable statement must retain its authored Ep for the frontend executor")
	}
}

// The restored legacy inner-window inference input accepts exactly the
// witnessed released dual-window decoration (a direct outer window plus the
// authored outer LIMIT) and excludes every other authored state: outer
// ORDER BY / BY RANK / WITH, and a competing inner LIMIT.
func TestRestoredLegacyInnerWindowInferenceShapeIsExact(t *testing.T) {
	proc := newPlanTestProcess(t)
	mc := NewMockCompilerContext(false, proc)
	bindDefinition := func(definition string) {
		mc.SetContext(WithRestoredViewDefinition(context.Background(), "pg", "v", definition, "create view v as x"))
	}
	stmtOf := func(sql string) *tree.Select {
		t.Helper()
		stmt := p132ParseOne(t, sql)
		sel, ok := stmt.(*tree.Select)
		if !ok {
			t.Fatalf("expected *tree.Select for %q", sql)
		}
		return sel
	}
	markerFree := `{"Stmt":"shape","DefaultDatabase":"pg","sql_mode":""}`

	// The exact reviewed shape: outer window + authored outer ORDER BY and
	// LIMIT (both pagination carriers), inner window, no other authored state.
	// The outer ORDER BY is projection ordering over the inner window: the
	// released schema inference binds paren.Select only, so it is eligible.
	shape := "(select count(*) as a from tw_src interval(ts, 1, minute)) interval(ts, 2, minute) order by a limit 5"
	bindDefinition(markerFree)
	stmt := stmtOf(shape)
	innerGood := stmt.Select.(*tree.ParenSelect).Select
	if innerGood == nil || innerGood.TimeWindow == nil || innerGood.Limit != nil {
		t.Fatalf("the witness shape's inner body must own the window and no LIMIT")
	}
	if stmt.TimeWindow == nil || stmt.Limit == nil {
		t.Fatalf("the witness shape's outer level must own both the window and the authored LIMIT")
	}
	inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmt)
	if !ok || inner == nil {
		t.Fatalf("the exact witnessed dual-window shape must select the inner inference input")
	}

	// The adjacent inner-LIMIT-without-ORDER marker-free shape is inside the
	// released boundary: v4.2.9 authors it (CREATE rc 0, marker-free ViewDef,
	// SHOW COLUMNS a BIGINT NOT NULL) while its own reader rejects the
	// duplicate-clause combination at query time - exactly the released
	// semantics the restored inference now defers to (see the release witness
	// receipt in this review's phase evidence).
	innerLimit := "(select count(*) as a from tw_src interval(ts, 1, minute) limit 3) interval(ts, 2, minute) limit 5"
	bindDefinition(markerFree)
	if inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmtOf(innerLimit)); !ok || inner == nil {
		t.Fatalf("the released-witnessed inner-LIMIT shape must select the inner inference input")
	}
	// Excluded shapes keep the complete binding: a body without its own
	// window is outside the released dual-window boundary.
	excluded := []string{
		"(select count(*) as a from tw_src) interval(ts, 2, minute) limit 5",
	}
	for _, sql := range excluded {
		bindDefinition(markerFree)
		if inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmtOf(sql)); ok || inner != nil {
			t.Fatalf("an unrelated authored state must keep the previous complete binding: %q", sql)
		}
	}

	// A current-generation definition (the marker present) never enters the
	// legacy branch regardless of shape.
	bindDefinition(`{"Stmt":"shape","DefaultDatabase":"pg","sql_mode":"","cte_scope_protocol":1}`)
	if inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmtOf(shape)); ok || inner != nil {
		t.Fatalf("a current-generation definition must not use the legacy inner-window inference")
	}
	// A metadata payload that fails to decode also keeps the complete binding,
	// and a context without any restored definition keeps it too.
	bindDefinition("{not-json")
	if inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmtOf(shape)); ok || inner != nil {
		t.Fatalf("an undecodable payload must keep the previous complete binding")
	}
	mc.SetContext(context.Background())
	if inner, ok := restoredLegacyInnerWindow(mc, "pg", "v", stmtOf(shape)); ok || inner != nil {
		t.Fatalf("a context without any restored definition must keep the previous complete binding")
	}
}

// The real caller-level boundary discriminator: the same authored paginated
// WITH+OUTFILE statement composed through the ownership-neutral public
// BuildPlan (the recursive diagnostic / internal routes) must NOT claim the
// executable export exemption, while BuildPlanWithExecutionOwner - the
// frontend execution and prepared routes' entry - composes the statement's
// pages exactly once.
func TestBuildPlanExecutionOwnerBoundaryDiscriminator(t *testing.T) {
	stmt := p132ParseOne(t,
		"with q as (select 1 as a union all select 2 as a) (select a from q limit 2) order by a limit 1 offset 1 into outfile '/tmp/p135-owner-guard.csv'")
	sel, ok := stmt.(*tree.Select)
	if !ok {
		t.Fatalf("the probe statement must bind as *tree.Select")
	}
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	if _, err := BuildPlan(ctx, sel, false, nil); err == nil {
		// A neutral diagnostic route (EXPLAIN's recursive child, an internal
		// helper) that plans this statement must not silently compose with
		// an unscoped Ep exemption: the duplicate-clause contract holds.
		t.Fatalf("the neutral public BuildPlan must reject the duplicate pagination clauses without an execution owner")
	}
	if _, err := BuildPlanWithExecutionOwner(ctx, sel, false, nil, sel); err != nil {
		t.Fatalf("the execution-owner route must compose the statement's pages: %v", err)
	}
}

func TestEnclosingAuthoredPagesComposeAtTheWithTerminal(t *testing.T) {
	// The genuine reviewed shape: the enclosing outer LIMIT/OFFSET sits
	// OUTSIDE the closing parentheses, so its stage is peeled with the
	// wrapper and must still compose as the outermost authored page.
	const sql = "(with q as (select 1 as a union all select 2 as a union all select 3 as a union all select 4 as a) ((select a from q limit 4) order by a limit 3 offset 1)) limit 1 offset 2"
	sel := p132ParseOne(t, sql).(*tree.Select)

	var enclosing []*tree.Select
	withStmt := sel
	for withStmt.With == nil {
		paren, ok := withStmt.Select.(*tree.ParenSelect)
		if !ok || paren.Select == nil {
			t.Fatalf("the witness must peel onto its WITH-bearing stage")
		}
		enclosing = append(enclosing, withStmt)
		withStmt = paren.Select
	}
	innerAuthored := withStmt.Select.(*tree.ParenSelect).Select
	wrapped, pages, _, rewritten := ordinaryPaginationWrap(withStmt, false, nil, enclosing...)
	if !rewritten || wrapped == nil {
		t.Fatalf("the enclosing authored pages must compose at the WITH terminal")
	}
	if len(pages) != 3 {
		t.Fatalf("the composition must be the inner page, the WITH-hosting carrier and the enclosing page, got %d pages", len(pages))
	}
	if pages[1].Limit != nil || len(pages[1].OrderBy) > 0 || pages[1].With != withStmt.With {
		t.Fatalf("the middle page must be the clause-free WITH-hosting carrier carrying the authored WITH pointer")
	}
	// AST-owner assertions: the syntheses carry the authored clause
	// pointers unchanged, so the enclosing root's Limit must land on the
	// outermost composed page and the inner authored page must keep its own.
	if pages[len(pages)-1].Limit != sel.Limit {
		t.Fatalf("the outermost composed page must carry the enclosing root's authored Limit pointer")
	}
	if pages[0].Limit != innerAuthored.Limit {
		t.Fatalf("the inner authored page must keep its own authored Limit pointer")
	}
	// Without the recorded enclosing stages the composition loses the
	// genuine outer page: this is the discriminating pre-fix behavior, so
	// the assertion keeps the test honest about which input owns the stage.
	_, pagesWithout, _, withoutRewritten := ordinaryPaginationWrap(withStmt, false, nil)
	if withoutRewritten && len(pagesWithout) > 0 && pagesWithout[len(pagesWithout)-1].Limit == sel.Limit {
		t.Fatalf("the peeled enclosing stage must not appear without the recorded enclosing input")
	}
}

func TestWrapDeclinesANestedExportWithADifferentAction(t *testing.T) {
	const sql = "with q as (select 1 as a union all select 2 as a) ((select a from q limit 2) order by a limit 1 offset 1 into outfile '/tmp/patrol147-should-not-exist.csv')"
	sel := p132ParseOne(t, sql).(*tree.Select)
	if sel.Ep == nil {
		t.Fatalf("the parser copies the one authored export onto the root")
	}
	// The production anchor: the WITH sits on the root itself, so the
	// caller binds s = the rooted statement and threads the same statement
	// as the export owner.
	if _, _, _, rewritten := ordinaryPaginationWrap(sel, false, sel); !rewritten {
		t.Fatalf("the one shared action must stay exempt as the root's own export")
	}
	paren, ok := sel.Select.(*tree.ParenSelect)
	if !ok || paren.Select == nil {
		t.Fatalf("the witness body must descend to the walk's first inner level")
	}
	walked := paren.Select
	// A genuinely different second action on a level the walk visits must
	// decline: pointer identity is the only authority here.
	walked.Ep = &tree.ExportParam{}
	if _, _, _, rewritten := ordinaryPaginationWrap(sel, false, sel); rewritten {
		t.Fatalf("a distinct nested export action must not gain the root's exemption")
	}
	// The same pointer copied onto the walked level stays the one action.
	walked.Ep = sel.Ep
	if _, _, _, rewritten := ordinaryPaginationWrap(sel, false, sel); !rewritten {
		t.Fatalf("the same shared pointer must stay exempt on the walked level")
	}
	walked.Ep = nil
	if _, _, _, rewritten := ordinaryPaginationWrap(sel, false, sel); !rewritten {
		t.Fatalf("the neutral no-export level must compose as before")
	}
}

func TestEnclosingStagesStayOutsideTheWithRegion(t *testing.T) {
	// An enclosing stage is authored outside the WITH's region, so it must
	// not acquire inner CTE visibility: the outermost composed page carries
	// the enclosing authored clauses, and a clause-free carrier inside the
	// composition hosts the WITH.
	const sql = "(with k147 as (select 9 as a) (select a from k147 limit 5)) order by a limit 1"
	sel := p132ParseOne(t, sql).(*tree.Select)
	var enclosing []*tree.Select
	withStmt := sel
	for withStmt.With == nil {
		paren, ok := withStmt.Select.(*tree.ParenSelect)
		if !ok || paren.Select == nil {
			t.Fatalf("the witness must peel onto its WITH-bearing stage")
		}
		enclosing = append(enclosing, withStmt)
		withStmt = paren.Select
	}
	wrapped, pages, _, rewritten := ordinaryPaginationWrap(withStmt, false, nil, enclosing...)
	if !rewritten || wrapped == nil {
		t.Fatalf("the enclosing authored stages must compose at the WITH terminal")
	}
	if wrapped.With != nil {
		t.Fatalf("the outermost composed page is an enclosing stage: it must not own the WITH")
	}
	if len(wrapped.OrderBy) != len(sel.OrderBy) || wrapped.Limit != sel.Limit {
		t.Fatalf("the enclosing stage's authored ORDER BY and LIMIT pointers must land on the outermost page")
	}
	hosted := false
	for _, page := range pages {
		if page.With == withStmt.With {
			hosted = true
		}
	}
	if !hosted {
		t.Fatalf("the authored WITH pointer must stay hosted inside the composition")
	}
	if wrapped.With == withStmt.With {
		t.Fatalf("no enclosing page may re-home the WITH onto the outermost stage")
	}
}

func TestSameActionEnclosingChainComposesOneAction(t *testing.T) {
	// The parser distributes the one authored export action as an identical
	// Ep pointer onto the executable root, the WITH-bearing node and the
	// inner page. The enclosing chain of exactly that action composes like
	// its clause-free siblings; any different action still declines.
	const sql = "(with q as (select 1 as a union all select 2 as a) ((select a from q limit 2) order by a limit 1 offset 1 into outfile '/tmp/patrol147-guard-none.csv') ) limit 1"
	sel := p132ParseOne(t, sql).(*tree.Select)
	if sel.Ep == nil {
		t.Fatalf("the parser copies the authored action onto the initial root")
	}
	var enclosing []*tree.Select
	withStmt := sel
	for withStmt.With == nil {
		paren, ok := withStmt.Select.(*tree.ParenSelect)
		if !ok || paren.Select == nil {
			t.Fatalf("the witness must peel onto its WITH-bearing stage")
		}
		enclosing = append(enclosing, withStmt)
		withStmt = paren.Select
	}
	wrapped, pages, _, rewritten := ordinaryPaginationWrap(withStmt, false, sel, enclosing...)
	if !rewritten || wrapped == nil {
		t.Fatalf("the one shared action's contiguous enclosing chain must compose")
	}
	if len(pages) != 3 {
		t.Fatalf("the composition must record the inner page, the WITH-hosting carrier and the enclosing page, got %d", len(pages))
	}
	innerAuthored := withStmt.Select.(*tree.ParenSelect).Select
	if pages[0].Limit != innerAuthored.Limit {
		t.Fatalf("the inner authored page must keep its own Limit pointer")
	}
	if pages[1].Limit != nil || len(pages[1].OrderBy) > 0 || pages[1].With != withStmt.With || pages[1].Ep != nil {
		t.Fatalf("the middle page must be the clause-free WITH-hosting carrier with no action")
	}
	if wrapped.Limit != sel.Limit || wrapped.Ep != nil {
		t.Fatalf("the outermost composed page must carry the enclosing root's Limit and no action")
	}
	for _, p := range pages {
		if p.Ep != nil {
			t.Fatalf("the composed pages must carry no export action: the frontend keeps the one authored writer")
		}
	}
	enclosing[0].Ep = &tree.ExportParam{}
	if _, _, _, declined := ordinaryPaginationWrap(withStmt, false, sel, enclosing...); declined {
		t.Fatalf("a different action on the enclosing chain must still decline")
	}
}
