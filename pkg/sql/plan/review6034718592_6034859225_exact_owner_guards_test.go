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

	// The exact witnessed shape: outer window + outer LIMIT, inner window, no other state.
	shape := "(select count(*) as a from tw_src interval(ts, 1, minute)) interval(ts, 2, minute) limit 5"
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

	// Excluded shapes keep the previous complete binding.
	excluded := []string{
		"(select count(*) as a from tw_src interval(ts, 1, minute)) interval(ts, 2, minute) order by a limit 5",
		"(select count(*) as a from tw_src interval(ts, 1, minute) limit 3) interval(ts, 2, minute) limit 5",
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
