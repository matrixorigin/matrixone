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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// reduceSetMembershipJoins removes multiplicity that a non-ALL set operation
// cannot observe. It deliberately does not propagate duplicate insensitivity
// through other consumers: DISTINCT(project_D(D JOIN F)) needs only existence
// of a matching F, whereas COUNT, LIMIT and ALL set operations need its count.
func (builder *QueryBuilder) reduceSetMembershipJoins(rootID int32, step int) {
	if builder.qry.Correlation != nil {
		return
	}
	owners := make(map[int32]int)
	seen := make(map[int32]bool)
	var count func(int32)
	count = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		for _, child := range builder.qry.Nodes[id].Children {
			// Count edges before deduplicating traversal: a shared Project can
			// have a uniquely referenced JOIN that is nevertheless not ours.
			owners[child]++
			count(child)
		}
	}
	for i, root := range builder.qry.Steps {
		if i == step {
			root = rootID // Earlier rewrites may have replaced this step's root.
		}
		owners[root]++
		count(root)
	}
	clear(seen)
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		for _, child := range node.Children {
			visit(child)
		}
		switch node.NodeType {
		case planpb.Node_UNION, planpb.Node_INTERSECT, planpb.Node_MINUS:
			for _, child := range node.Children {
				builder.reduceSetMembershipArm(child, owners)
			}
		}
	}
	visit(rootID)
}

func (builder *QueryBuilder) reduceSetMembershipArm(id int32, owners map[int32]int) {
	project := builder.qry.Nodes[id]
	if project.NodeType != planpb.Node_PROJECT || len(project.Children) != 1 ||
		len(project.ProjectList) == 0 || len(project.FilterList) != 0 || len(project.BlockFilterList) != 0 {
		return
	}
	join := builder.qry.Nodes[project.Children[0]]
	if join.NodeType != planpb.Node_JOIN || join.JoinType != planpb.Node_INNER ||
		len(join.Children) != 2 || len(join.OnList) == 0 || len(join.ProjectList) != 0 ||
		len(join.FilterList) != 0 || len(join.BlockFilterList) != 0 {
		return
	}
	if !builder.setMembershipInputIsTotal(id, owners, make(map[int32]bool)) {
		return
	}
	// The initial rule handles only column equalities. Whole-subtree totality
	// also checks their computed origins; determinism alone does not rule out
	// a text-to-integer cast failing on a row skipped by SEMI short-circuiting.
	for _, expr := range join.OnList {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 ||
			fn.Args[0].GetCol() == nil || fn.Args[1].GetCol() == nil {
			return
		}
		functionID, _ := function.DecodeOverloadID(fn.Func.Obj)
		if functionID != function.EQUAL {
			return
		}
	}
	if !builder.existentialJoinPredicatesAreTotal(join.OnList, join.Children...) {
		return
	}
	leftTags := make(map[int32]bool)
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(join.Children[0]) {
		leftTags[tag] = true
	}
	for _, tag := range builder.enumerateTags(join.Children[1]) {
		if leftTags[tag] {
			return
		}
		rightTags[tag] = true
	}
	var side int8
	for _, expr := range project.ProjectList {
		side |= getJoinSideWithOuterScope(expr, leftTags, rightTags, -1)
	}
	if side != JoinSideLeft && side != JoinSideRight {
		return
	}
	if side == JoinSideRight {
		join.Children[0], join.Children[1] = join.Children[1], join.Children[0]
	}
	// References still use logical binding tags. The normal subsequent stats,
	// build/probe and column-remapping passes choose physical orientation.
	join.JoinType = planpb.Node_SEMI
	join.IsRightJoin = false
}

// setMembershipInputIsTotal proves the complete evaluation domain, not just
// the projected columns. Never cross a materialization/physical dependency or
// a shared node; changing its demand could affect a multiplicity observer.
func (builder *QueryBuilder) setMembershipInputIsTotal(id int32, owners map[int32]int, seen map[int32]bool) bool {
	if owners[id] != 1 || seen[id] {
		return false
	}
	seen[id] = true
	node := builder.qry.Nodes[id]
	if node.Limit != nil || node.Offset != nil || node.FilterIsBarrier ||
		node.ExtraOptions != "" || node.RecursiveSink || node.RecursiveCte || node.EmitCompressedRowCount ||
		len(node.SourceStep) != 0 || len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 ||
		len(node.SendMsgList) != 0 || len(node.RecvMsgList) != 0 || len(node.LockTargets) != 0 ||
		node.IndexReaderParam != nil || node.IndexSearchScan != nil || node.SampleFunc != nil ||
		node.DedupJoinCtx != nil || node.ExternScan != nil {
		return false
	}
	switch node.NodeType {
	case planpb.Node_TABLE_SCAN:
		if len(node.Children) != 0 || len(node.BindingTags) != 1 || node.TableDef == nil {
			return false
		}
	case planpb.Node_PROJECT:
		if len(node.Children) != 1 || len(node.BindingTags) != 1 {
			return false
		}
	case planpb.Node_FILTER:
		if len(node.Children) != 1 || len(node.BindingTags) != 0 {
			return false
		}
	case planpb.Node_JOIN:
		if len(node.Children) != 2 || node.JoinType != planpb.Node_INNER || len(node.BindingTags) != 0 {
			return false
		}
	default:
		return false
	}
	for _, expr := range node.ProjectList {
		if !isTruncationSafeRowExpr(expr) {
			return false
		}
	}
	if !areTruncationSafePredicates(node.FilterList) || !areTruncationSafePredicates(node.BlockFilterList) ||
		!areTruncationSafePredicates(node.OnList) {
		return false
	}
	// Fail closed on additional executable expression owners. Catalog defaults
	// and generated-column definitions are not evaluated by a TABLE_SCAN.
	remaining := *node
	remaining.TableDef = nil
	remaining.ProjectList = nil
	remaining.FilterList = nil
	remaining.BlockFilterList = nil
	remaining.OnList = nil
	unknown := false
	_ = planpb.VisitExpressionsInOwner(&remaining, func(*planpb.Expr) error {
		unknown = true
		return nil
	})
	if unknown {
		return false
	}
	for _, child := range node.Children {
		if !builder.setMembershipInputIsTotal(child, owners, seen) {
			return false
		}
	}
	return true
}
