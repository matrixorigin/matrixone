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
		// enough if the walk will not descend into the spec it finds. cuVS-style nesting
		// aside, this is the only Expr variant that contains further expressions.
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
		}
	}
	checkExprs := func(exprs []*plan.Expr) {
		for _, expr := range exprs {
			checkExpr(expr)
		}
	}

	visited := make(map[int32]bool, len(query.Nodes))
	var walk func(nodeID int32)
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
