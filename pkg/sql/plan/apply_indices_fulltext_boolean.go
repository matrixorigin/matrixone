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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// fullTextBooleanCompositionExpr deliberately does not enter subqueries or
// windows: only the WHERE predicate owned by this scan can preserve its rows.
func fullTextBooleanCompositionExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		if (fn.Func.ObjName == "or" || fn.Func.ObjName == "not" || fn.Func.ObjName == "unary_mark" || fn.Func.ObjName == "xor") && exprCallsFunc(expr, "fulltext_match") {
			return true
		}
		for _, arg := range fn.Args {
			if fullTextBooleanCompositionExpr(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if fullTextBooleanCompositionExpr(arg) {
				return true
			}
		}
	}
	return false
}

func scanHasFullTextBooleanComposition(scan *plan.Node) bool {
	if scan == nil || scan.NodeType != plan.Node_TABLE_SCAN {
		return false
	}
	for _, expr := range scan.FilterList {
		if fullTextBooleanCompositionExpr(expr) {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) fullTextBooleanUnsupported() error {
	return moerr.NewNotSupported(builder.GetContext(), "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
}

// applyFullTextBooleanComposition preserves the base relation until its original
// Boolean predicates evaluate. Complete, unique score streams are LEFT attached;
// missing scores become FLOAT32 zero below all enclosing user joins. No legacy
// INNER-driver guard, candidate limit or membership reduction is valid here.
func (builder *QueryBuilder) applyFullTextBooleanComposition(nodeID int32, scan *plan.Node,
	idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	if !scanHasFullTextBooleanComposition(scan) {
		return nodeID, nil
	}
	// The pagination pushdown rule keeps query-block pagination above this path.
	// A pre-existing scan limit has no reliable ownership proof; refuse it.
	if scan.Limit != nil || scan.Offset != nil || len(scan.BindingTags) != 1 || scan.ObjRef == nil || scan.TableDef == nil || scan.TableDef.Pkey == nil {
		return -1, builder.fullTextBooleanUnsupported()
	}
	pkPos, ok := scan.TableDef.Name2ColIndex[scan.TableDef.Pkey.PkeyColName]
	if !ok || pkPos < 0 || int(pkPos) >= len(scan.TableDef.Cols) {
		return -1, builder.fullTextBooleanUnsupported()
	}
	var leaves []*plan.Expr
	var indexes []*plan.IndexDef
	var lifted, kept []*plan.Expr
	for _, expr := range scan.FilterList {
		matches := collectNestedFullTextMatches(expr, nil)
		if len(matches) == 0 {
			kept = append(kept, expr)
			continue
		}
		lifted = append(lifted, DeepCopyExpr(expr))
		for _, match := range matches {
			fn := match.GetF()
			idx := builder.findMatchFullTextIndex(fn, scan)
			if idx == nil || fn.Args[1].GetLit().GetI64Val() == fulltext2.JSONProbeMode {
				return -1, builder.fullTextBooleanUnsupported()
			}
			duplicate := false
			for _, leaf := range leaves {
				if builder.equalsFullTextMatchFuncSameTable(fn, leaf.GetF()) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				leaves = append(leaves, DeepCopyExpr(match))
				indexes = append(indexes, idx)
			}
		}
	}
	if len(leaves) == 0 {
		return -1, builder.fullTextBooleanUnsupported()
	}
	ctx := builder.ctxByNode[nodeID]
	pkType := scan.TableDef.Cols[pkPos].Typ
	baseTag := scan.BindingTags[0]
	basePK := &plan.Expr{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: baseTag, ColPos: pkPos}}}
	chain := nodeID
	projectTag := builder.genNewBindTag()
	projectList := make([]*plan.Expr, 0, len(scan.TableDef.Cols)+len(leaves))
	remap := make(map[[2]int32]*plan.Expr, len(scan.TableDef.Cols))
	for i, col := range scan.TableDef.Cols {
		projectList = append(projectList, &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: baseTag, ColPos: int32(i), Name: col.Name}}})
		remap[[2]int32{baseTag, int32(i)}] = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: int32(i), Name: col.Name}}}
		builder.nameByColRef[[2]int32{projectTag, int32(i)}] = builder.nameByColRef[[2]int32{baseTag, int32(i)}]
	}
	served := make([]fulltextServedMatch, 0, len(leaves))
	for i, leaf := range leaves {
		streamID, _, err := builder.buildFullTextMatchSearchScan(ctx, scan, indexes[i], leaf.GetF(), nil, "")
		if err != nil {
			return -1, err
		}
		stream := builder.qry.Nodes[streamID]
		// Preserve the complete PK encoding/type (including binary/composite keys).
		stream.TableDef.Cols[0].Typ = pkType
		streamTag := stream.BindingTags[0]
		streamPK := &plan.Expr{Typ: pkType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: streamTag, ColPos: 0}}}
		on, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{DeepCopyExpr(basePK), streamPK})
		if err != nil {
			return -1, err
		}
		chain = builder.appendNode(&plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT, Children: []int32{chain, streamID}, OnList: []*plan.Expr{on}}, ctx)
		score := builder.fullTextScoreColRef(streamID)
		score.Typ.NotNullable = false // LEFT absence, independent of provider non-NULL scores.
		normalized, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "coalesce", []*plan.Expr{score, makePlan2Float32ConstExprWithType(0)})
		if err != nil {
			return -1, err
		}
		scorePos := int32(len(projectList))
		projectList = append(projectList, normalized)
		replacement := &plan.Expr{Typ: normalized.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: scorePos}}}
		// Emitted scores are never NULL, so the raw score is NULL exactly when the index
		// did not return the row.
		rawScore := DeepCopyExpr(score)
		foundPos := int32(len(projectList))
		projectList = append(projectList, rawScore)
		found := &plan.Expr{Typ: rawScore.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: projectTag, ColPos: foundPos}}}
		served = append(served, fulltextServedMatch{fn: leaf.GetF(), nodeID: streamID, replacementExpr: replacement, foundExpr: found})
		builder.nameByColRef[[2]int32{projectTag, scorePos}] = fmt.Sprintf("fulltext_score_%d", i)
		builder.nameByColRef[[2]int32{projectTag, foundPos}] = fmt.Sprintf("fulltext_found_%d", i)
		for j, col := range stream.TableDef.Cols {
			builder.nameByColRef[[2]int32{streamTag, int32(j)}] = fmt.Sprintf("mo_fulltext_boolean_%d.%s", i, col.Name)
		}
	}
	normalizedID := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, Children: []int32{chain}, BindingTags: []int32{projectTag}, ProjectList: projectList}, ctx)
	for i, expr := range lifted {
		// The binder leaves a bare MATCH predicate numeric; as a predicate it is whether
		// the index returned the row.
		expr, err := fullTextPredicateAsBool(builder.GetContext(), expr)
		if err != nil {
			return -1, err
		}
		expr = replaceScoreFnInExprBy(expr, func(fn *plan.Function) *plan.Expr { return builder.servedFullTextScoreSameTable(fn, served) })
		expr = replaceColumnsForExpr(expr, remap)
		lifted[i], err = makePlan2CastExpr(builder.GetContext(), expr, plan.Type{Id: int32(types.T_bool)})
		if err != nil {
			return -1, err
		}
	}
	filterID := builder.appendNode(&plan.Node{NodeType: plan.Node_FILTER, Children: []int32{normalizedID}, FilterList: lifted}, ctx)
	// Publish only after every leaf, node and predicate has been constructed.
	scan.FilterList = kept
	scan.BlockFilterList = nil // no cached MATCH-bearing scan predicate can survive.
	builder.ftJoinServed = append(builder.ftJoinServed, served...)
	for key, value := range remap {
		idxColMap[key] = value
	}
	return filterID, nil
}

// isFullTextBooleanScoreProject keeps absence normalization below user outer
// joins even during the final simple-projection-removal pass.
func (builder *QueryBuilder) isFullTextBooleanScoreProject(node *plan.Node) bool {
	if node == nil || node.NodeType != plan.Node_PROJECT || len(node.BindingTags) != 1 {
		return false
	}
	for _, s := range builder.ftJoinServed {
		if s.replacementExpr != nil {
			if col := s.replacementExpr.GetCol(); col != nil && col.RelPos == node.BindingTags[0] {
				return true
			}
		}
	}
	return false
}

// Rewrite served composition leaves before replacing base-column bindings.
// This closes PROJECT/SORT/AGG/WINDOW/PARTITION and join-child consumers without
// reapplying COALESCE above a user join or silently serving a different leaf.
func (builder *QueryBuilder) rewriteFullTextBooleanConsumers(node *plan.Node) error {
	var composed []fulltextServedMatch
	for _, served := range builder.ftJoinServed {
		if served.replacementExpr != nil {
			composed = append(composed, served)
		}
	}
	if len(composed) == 0 {
		return nil
	}
	var rewriteErr error
	rewrite := func(fn *plan.Function) *plan.Expr {
		if builder.fullTextIfConditionAsBool(fn) {
			return nil
		}
		match := fn
		if inner := fullTextMatchCastToBool(fn); inner != nil {
			match = inner
		}
		if match == nil || match.Func == nil || match.Func.ObjName != "fulltext_match" {
			return nil
		}
		if len(match.Args) < 3 {
			rewriteErr = builder.fullTextBooleanUnsupported()
			return nil
		}
		if expr := builder.servedFullTextExpr(fn, composed, true); expr != nil {
			return expr
		}
		// A different served leaf on this binding is not evidence that this
		// leaf is unserved: search all exact identities before refusing it.
		for _, s := range composed {
			for _, arg := range match.Args[2:] {
				if c := arg.GetCol(); c != nil && c.RelPos == s.fn.Args[2].GetCol().RelPos {
					rewriteErr = builder.fullTextBooleanUnsupported()
				}
			}
		}
		return nil
	}
	for _, list := range [][]*plan.Expr{node.ProjectList, node.GroupBy, node.PhysicalEqualityKeyList, node.AggList, node.WinSpecList, node.TimeWindowPartitionBy, node.TblFuncExprList} {
		for i, expr := range list {
			list[i] = replaceScoreFnInExprBy(expr, rewrite)
		}
	}
	for _, predicates := range [][]*plan.Expr{node.FilterList, node.OnList} {
		for i, expr := range predicates {
			predicate, err := fullTextPredicateAsBool(builder.GetContext(), expr)
			if err != nil {
				return err
			}
			replaced := replaceScoreFnInExprBy(predicate, rewrite)
			if replaced == predicate {
				replaced = expr
			}
			if replaced != expr && replaced.Typ.Id != int32(types.T_bool) {
				replaced, err = makePlan2CastExpr(builder.GetContext(), replaced, plan.Type{Id: int32(types.T_bool)})
				if err != nil {
					return err
				}
			}
			predicates[i] = replaced
		}
	}
	for _, order := range node.OrderBy {
		if order != nil {
			order.Expr = replaceScoreFnInExprBy(order.Expr, rewrite)
		}
	}
	return rewriteErr
}
