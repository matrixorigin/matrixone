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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// PlanCallsAnyFunc reports whether any expression REACHABLE from the query's roots calls
// one of the named functions. Names are matched against plan.Function.Func.ObjName.
//
// Reachability is the whole point, and the reason this is shared rather than reimplemented
// per plugin. query.Nodes is an append-only arena of every node the builder ever created,
// not the live plan: when a rewrite succeeds it leaves its pre-rewrite nodes behind in
// there. The fulltext rewrite, for instance, re-parents the scan under a join with the
// index scan and abandons the original FILTER node, which still holds the original
// fulltext_match. Scanning the arena finds that corpse and condemns a view the index can
// serve perfectly well -- measured against a real FULLTEXT(body) index, it rejected every
// CREATE VIEW carrying a WHERE MATCH.
//
// The walk is bounded: each node is visited once, and out-of-range child ids are skipped,
// so a malformed graph cannot hang or panic DDL.
func PlanCallsAnyFunc(query *plan.Query, names ...string) bool {
	if query == nil || len(names) == 0 {
		return false
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}

	found := false
	// Declared up front: checkExpr follows a subquery's NodeId edge back into walk.
	var walk func(nodeID int32)
	var checkExpr func(expr *plan.Expr)
	checkExpr = func(expr *plan.Expr) {
		if expr == nil || found {
			return
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func != nil {
				if _, ok := wanted[fn.Func.ObjName]; ok {
					found = true
					return
				}
			}
			for _, arg := range fn.Args {
				checkExpr(arg)
			}
			return
		}
		if list := expr.GetList(); list != nil {
			for _, sub := range list.List {
				checkExpr(sub)
			}
			return
		}
		// A window spec is an Expr in its own right, carrying the window function plus its
		// PARTITION BY and ORDER BY. Stopping at Expr_F/Expr_List leaves those unread, and
		// `OVER (ORDER BY MATCH(...))` hides there -- visiting the node's WinSpecList is not
		// enough if the walk will not descend into the spec it finds.
		if w := expr.GetW(); w != nil {
			checkExpr(w.WindowFunc)
			for _, part := range w.PartitionBy {
				checkExpr(part)
			}
			for _, order := range w.OrderBy {
				if order != nil {
					checkExpr(order.Expr)
				}
			}
			return
		}
		// A subquery reference nests in two directions at once: Child is an expression, and
		// NodeId points at a whole subtree that Children does not list. Flattening normally
		// removes these before the optimized plan, but "normally" is not a guarantee this
		// function can rely on -- if one survives, a MATCH inside it would be invisible and
		// the unusable view would persist. Following both edges costs nothing when there are
		// none.
		if sub := expr.GetSub(); sub != nil {
			checkExpr(sub.Child)
			walk(sub.NodeId)
		}
	}
	checkExprs := func(exprs []*plan.Expr) {
		for _, expr := range exprs {
			checkExpr(expr)
		}
	}

	visited := make(map[int32]bool, len(query.Nodes))
	walk = func(nodeID int32) {
		if found || nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
			return
		}
		visited[nodeID] = true
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		// Every expression-bearing field, not the obvious few. Missing one does not
		// degrade the check, it silently defeats it: the placeholder rides through in
		// the unwalked list and the unusable view is persisted exactly as before. A
		// window's OVER (ORDER BY MATCH(...)) hides in WinSpecList and did precisely
		// that. Kept in step with the field list the planner's own whole-node walkers
		// use (increaseRefCntForNode / replaceColumnsForNode in pkg/sql/plan), widened
		// with the remaining Expr fields on plan.Node so an unlisted one cannot leak.
		checkExprs(node.ProjectList)
		checkExprs(node.FilterList)
		checkExprs(node.OnList)
		checkExprs(node.GroupBy)
		checkExprs(node.AggList)
		checkExprs(node.WinSpecList)
		checkExprs(node.BlockFilterList)
		checkExprs(node.TimeWindowPartitionBy)
		checkExprs(node.TblFuncExprList)
		checkExprs(node.FillVal)
		checkExprs(node.OnUpdateExprs)
		checkExpr(node.Limit)
		checkExpr(node.Offset)
		checkExpr(node.Interval)
		checkExpr(node.Sliding)
		checkExpr(node.Timestamp)
		checkExpr(node.WEnd)
		for _, order := range node.OrderBy {
			if order != nil {
				checkExpr(order.Expr)
			}
		}
		for _, child := range node.Children {
			walk(child)
		}
	}
	for _, rootID := range query.Steps {
		walk(rootID)
		if found {
			return true
		}
	}
	return found
}

// MatchPlaceholderFuncs are the plan functions MATCH() AGAINST() binds to. Neither has an
// evaluable implementation: pkg/sql/plan/function/func_fulltext.go raises "MATCH() AGAINST()
// function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is
// not supported yet" for both. They are placeholders the fulltext rewrite is expected to
// replace with an index scan.
//
// fulltext_match is what a WHERE MATCH binds to and what survives when no index matches;
// fulltext_match_score is what an unmatched SELECT MATCH is converted into
// (getFullTextMatchFromProject). Either one surviving optimization means the query throws.
var MatchPlaceholderFuncs = []string{"fulltext_match", "fulltext_match_score"}

// RefuseUnservableMatch is the shared body of the fulltext family's ValidateViewDefinition.
//
// Classic fulltext and fulltext2 bind MATCH to the same placeholders and are resolved by
// the same findMatchFullTextIndex, so their policy is identical by construction; each
// plugin still declares the hook so the registry stays the single source of truth for what
// an algorithm does, but the body lives here rather than being copied and left to drift.
//
// SCOPE, and why it stops here. This guards the statements that CREATE a view definition:
// CREATE VIEW, ALTER VIEW, CREATE OR REPLACE VIEW. What it establishes is narrow, and worth
// stating precisely: "view DDL does not persist a definition that cannot run ON ITS OWN".
// It is NOT "a persisted view is always runnable", for two independent reasons.
//
// One, the definition is validated in isolation, but a query through the view inlines it
// into the surrounding statement and re-plans the whole thing, which can reach a shape the
// rewrite does not cover. Measured: a view over `MATCH(body)` with the score projected is
// accepted and works under an outer ORDER BY, a join, an aggregate, a window, a nested
// view, a subquery and a union -- but `WHERE sc > 0` on the score column fails with the
// runtime 20105. That is a planner gap, not a hole in this check: the same shape fails
// identically as a bare derived table with no view involved.
//
// Two, it deliberately does not guard the statements that invalidate an existing view --
// DROP INDEX above all, but equally ALTER TABLE DROP COLUMN and DROP TABLE.
//
// That is deliberate, for three reasons:
//
//   - MySQL parity. MySQL checks only foreign-key dependencies when dropping an index;
//     views are not dependency-tracked, and MySQL has no full-scan fallback for MATCH, so
//     there too the view simply starts failing at query time. #27027 states the same from
//     MySQL 8.0.45 testing and explicitly scopes this lifecycle out.
//   - Guarding DROP INDEX alone would buy no invariant. Measured on MatrixOne: with a view
//     over MATCH(body), both `ALTER TABLE docs DROP COLUMN body` and `DROP TABLE docs`
//     succeed today and leave the view in the catalog failing on every query. Closing one
//     of three doors would be inconsistent rather than safe.
//   - It would break a normal workflow. Dropping a fulltext index, bulk loading, then
//     recreating it is standard practice; refusing the drop would force every dependent
//     view to be dropped and recreated around each reindex.
//
// The situation is also self-healing: recreating the index makes the stored views work
// again, verified end to end. If MatrixOne ever wants the stronger guarantee, it needs to
// cover DROP COLUMN and DROP TABLE in the same change, not DROP INDEX on its own.
func RefuseUnservableMatch(ctx CompilerContext, query *plan.Query) error {
	if !PlanCallsAnyFunc(query, MatchPlaceholderFuncs...) {
		return nil
	}
	// Its own moerr code, mapped to MySQL ER_FT_MATCHING_KEY_NOT_FOUND (1191): snapshot
	// restore / PITR / CLONE identify this refusal by code and skip the view rather than
	// aborting the whole restore.
	return moerr.NewFtMatchingKeyNotFound(ctx.GetContext())
}
