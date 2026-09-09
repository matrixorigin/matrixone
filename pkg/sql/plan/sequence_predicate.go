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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// ContainsSequenceFunction reports whether expr contains a bound sequence
// function.  The executable plan has already resolved function overloads, so
// use the function id rather than SQL text: a user-defined name or an
// unresolved/prepared placeholder must not accidentally change placement.
func ContainsSequenceFunction(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	found := false
	_ = planpb.VisitExprTree(expr, func(candidate *planpb.Expr) error {
		if found {
			return nil
		}
		fn := candidate.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		fid, overload := function.DecodeOverloadID(fn.Func.Obj)
		switch fid {
		case function.NEXTVAL, function.SETVAL, function.CURRVAL, function.LASTVAL:
			found = true
		case function.LAST_INSERT_ID:
			// LAST_INSERT_ID() is a pure session read and can be evaluated on
			// any worker.  Only the one-argument overload mutates the
			// initiating session's tentative state.
			found = overload == function.LastInsertIDExprOverload
		}
		return nil
	})
	return found
}

// QueryContainsSequenceFunction reports whether any expression in the bound
// query can evaluate a session sequence function. It walks only executable
// plan fields (including supplemental operator fields) and then lets
// VisitExprTree descend through nested function/list/window expressions.
func QueryContainsSequenceFunction(qry *planpb.Query) bool {
	if qry == nil {
		return false
	}
	if containsSequenceExpressions(qry.Params...) {
		return true
	}

	// Only nodes reachable from a query step execute.  A few tests and plan
	// fragments omit Steps while constructing a query by hand; in that case
	// conservatively inspect all nodes.  Do not use VisitExpressionsInOwner on
	// the whole Query here: it also descends into TableDef/ColDef metadata,
	// where a default expression is stored even when the scan only reads an
	// already-materialized value.
	if len(qry.Steps) == 0 {
		for _, node := range qry.Nodes {
			if containsSequenceNodeExpressions(node) {
				return true
			}
		}
	} else {
		seen := make(map[int32]struct{}, len(qry.Nodes))
		var visitNode func(int32) bool
		visitNode = func(nodeID int32) bool {
			if nodeID < 0 || int(nodeID) >= len(qry.Nodes) {
				return false
			}
			if _, ok := seen[nodeID]; ok {
				return false
			}
			seen[nodeID] = struct{}{}
			node := qry.Nodes[nodeID]
			if containsSequenceNodeExpressions(node) {
				return true
			}
			for _, childID := range node.GetChildren() {
				if visitNode(childID) {
					return true
				}
			}
			return false
		}
		for _, step := range qry.Steps {
			if visitNode(step) {
				return true
			}
		}
	}

	// Background queries are real executable plans owned by the same statement
	// (for example FK side effects), so include their expression roots without
	// treating their catalog metadata as executable.
	for _, background := range qry.BackgroundQueries {
		if QueryContainsSequenceFunction(background) {
			return true
		}
	}
	return false
}

func containsSequenceExpressions(exprs ...*planpb.Expr) bool {
	for _, expr := range exprs {
		if ContainsSequenceFunction(expr) {
			return true
		}
	}
	return false
}

func containsSequenceOrderBy(specs []*planpb.OrderBySpec) bool {
	for _, spec := range specs {
		if spec != nil && ContainsSequenceFunction(spec.Expr) {
			return true
		}
	}
	return false
}

// containsSequenceNodeExpressions enumerates expression fields consumed by
// physical operators.  TableDef, InsertCtx.TableDef, UpdateCtx.TableDef,
// DeleteCtx.TableDef, and vector-index hidden table definitions are schema
// metadata; their defaults/generated expressions are not evaluated by a
// scan. INSERT/UPDATE planners copy any evaluated default/generated expression
// into ProjectList/OnUpdateExprs (or one of the explicit execution fields
// below), so excluding metadata here does not hide a side effect.
func containsSequenceNodeExpressions(node *planpb.Node) bool {
	if node == nil {
		return false
	}
	if containsSequenceExpressions(node.ProjectList...) ||
		containsSequenceExpressions(node.OnList...) ||
		containsSequenceExpressions(node.FilterList...) ||
		containsSequenceExpressions(node.GroupBy...) ||
		containsSequenceExpressions(node.AggList...) ||
		containsSequenceExpressions(node.WinSpecList...) ||
		containsSequenceExpressions(node.TblFuncExprList...) ||
		containsSequenceExpressions(node.BlockFilterList...) ||
		containsSequenceExpressions(node.FillVal...) ||
		containsSequenceExpressions(node.OnUpdateExprs...) ||
		containsSequenceExpressions(node.TimeWindowPartitionBy...) ||
		containsSequenceExpressions(node.PhysicalEqualityKeyList...) {
		return true
	}
	if containsSequenceExpressions(node.Limit, node.Offset, node.Interval, node.Sliding,
		node.Timestamp, node.WEnd, node.GapFillStart, node.GapFillEnd) {
		return true
	}
	if containsSequenceOrderBy(node.OrderBy) {
		return true
	}
	if rowset := node.RowsetData; rowset != nil {
		for _, col := range rowset.Cols {
			if col == nil {
				continue
			}
			for _, value := range col.Data {
				if value != nil && ContainsSequenceFunction(value.Expr) {
					return true
				}
			}
		}
	}
	if reader := node.IndexReaderParam; reader != nil {
		if ContainsSequenceFunction(reader.Limit) || containsSequenceOrderBy(reader.OrderBy) {
			return true
		}
		if dist := reader.DistRange; dist != nil &&
			(ContainsSequenceFunction(dist.LowerBound) || ContainsSequenceFunction(dist.UpperBound)) {
			return true
		}
	}
	for _, target := range node.LockTargets {
		if target != nil && ContainsSequenceFunction(target.LockRows) {
			return true
		}
	}
	for _, spec := range node.RuntimeFilterProbeList {
		if spec != nil && (ContainsSequenceFunction(spec.Expr) || ContainsSequenceFunction(spec.BuildExpr)) {
			return true
		}
	}
	for _, spec := range node.RuntimeFilterBuildList {
		if spec != nil && (ContainsSequenceFunction(spec.Expr) || ContainsSequenceFunction(spec.BuildExpr)) {
			return true
		}
	}
	if dedup := node.DedupJoinCtx; dedup != nil && containsSequenceExpressions(dedup.UpdateColExprList...) {
		return true
	}
	if pre := node.PreInsertCtx; pre != nil &&
		containsSequenceExpressions(pre.CompPkeyExpr, pre.ClusterByExpr) {
		return true
	}
	if scan := node.VectorIndexScan; scan != nil {
		if ContainsSequenceFunction(scan.QueryVector) ||
			ContainsSequenceFunction(scan.CandidateLimit) ||
			containsSequenceExpressions(scan.PreFilters...) || ContainsSequenceFunction(scan.FirstRoundLimit) {
			return true
		}
		if dist := scan.DistanceRange; dist != nil &&
			(ContainsSequenceFunction(dist.LowerBound) || ContainsSequenceFunction(dist.UpperBound)) {
			return true
		}
	}
	return false
}
