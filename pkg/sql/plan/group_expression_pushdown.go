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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// pushdownGroupingExpressions runs on finalized, child-local references. A
// total expression of build columns commutes with INNER join multiplicity.
// Keeping the grouping ordinal and all old build slots avoids a new FD or
// column-pruning contract. Earlier projection inlining would undo this move.
func (builder *QueryBuilder) pushdownGroupingExpressions() {
	if builder.qry.StmtType != planpb.Query_SELECT || builder.isPrepareStatement || builder.qry.Correlation != nil ||
		len(builder.qry.Params) != 0 || len(builder.qry.BackgroundQueries) != 0 {
		return
	}
	owners := make(map[int32]int)
	state := make(map[int32]uint8)
	var count func(int32, int) bool
	count = func(id int32, depth int) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || builder.qry.Nodes[id] == nil ||
			depth >= 64 || len(state) > 4096 || state[id] == 1 {
			return false
		}
		owners[id]++
		if state[id] == 2 {
			return true
		}
		state[id] = 1
		for _, child := range builder.qry.Nodes[id].Children {
			if !count(child, depth+1) {
				return false
			}
		}
		state[id] = 2
		return true
	}
	for _, root := range builder.qry.Steps {
		if !count(root, 0) {
			return
		}
	}
	if GetExecType(builder.qry, false, false) == ExecTypeTP {
		return
	}
	var visit func(int32)
	visit = func(id int32) {
		// A uniquely referenced descendant of a shared ancestor is shared too.
		if owners[id] != 1 {
			return
		}
		node := builder.qry.Nodes[id]
		for _, child := range node.Children {
			visit(child)
		}
		if node.NodeType == planpb.Node_AGG && len(node.Children) == 1 && owners[node.Children[0]] == 1 {
			builder.pushdownGroupingExpression(node)
		}
	}
	for _, root := range builder.qry.Steps {
		visit(root)
	}
}

func (builder *QueryBuilder) pushdownGroupingExpression(agg *planpb.Node) {
	if !groupExpressionOrdinaryNode(agg) || len(agg.GroupBy) == 0 ||
		(len(agg.GroupingFlag) != 0 && len(agg.GroupingFlag) != len(agg.GroupBy)) ||
		hasInactiveGroupingColumn(agg.GroupingFlag) {
		return
	}
	join := builder.qry.Nodes[agg.Children[0]]
	if join.NodeType != planpb.Node_JOIN || join.JoinType != planpb.Node_INNER || join.IsRightJoin ||
		len(join.Children) != 2 || !groupExpressionOrdinaryNode(join) ||
		join.Stats == nil || join.Stats.HashmapStats == nil || join.Stats.HashmapStats.Shuffle ||
		!groupExpressionHasHashKey(join.OnList) {
		return
	}
	scan := builder.qry.Nodes[join.Children[1]]
	if scan.NodeType != planpb.Node_TABLE_SCAN || len(scan.Children) != 0 || scan.TableDef == nil ||
		!groupExpressionOrdinaryNode(scan) || scan.Stats == nil ||
		!finitePositive(join.Stats.Outcnt) || !finitePositive(scan.Stats.Outcnt) ||
		join.Stats.Outcnt/scan.Stats.Outcnt < 4 || !finitePositive(scan.Stats.Rowsize) {
		return
	}
	for _, projected := range scan.ProjectList {
		if projected.GetCol() == nil || projected.PreparedNumeric != nil {
			return
		}
	}
	for _, projected := range join.ProjectList {
		if projected.GetCol() == nil || projected.PreparedNumeric != nil {
			return
		}
	}
	for groupPos, expr := range agg.GroupBy {
		if !groupExpressionSubstring(expr) {
			continue
		}
		input := expr.GetF().Args[0]
		col := input.GetCol()
		if col == nil || col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(join.ProjectList) {
			continue
		}
		output := join.ProjectList[col.ColPos]
		buildCol := output.GetCol()
		if buildCol.RelPos != 1 || buildCol.ColPos < 0 || int(buildCol.ColPos) >= len(scan.ProjectList) ||
			!proto.Equal(&input.Typ, &output.Typ) ||
			!proto.Equal(&output.Typ, &scan.ProjectList[buildCol.ColPos].Typ) {
			continue
		}
		usedByJoin, valid := groupExpressionReferences(join.OnList, 1, buildCol.ColPos)
		if !valid || usedByJoin {
			continue
		}
		// All AGG owners, including provenance, participate in the overwrite
		// proof. The candidate itself is the only reference being replaced.
		remaining := *agg
		remaining.GroupBy = append([]*planpb.Expr(nil), agg.GroupBy...)
		remaining.GroupBy[groupPos] = nil
		usedElsewhere, valid := groupExpressionReferences(&remaining, 0, col.ColPos)
		if !valid {
			continue
		}
		moved := DeepCopyExpr(expr)
		moved.GetF().Args[0].GetCol().ColPos = buildCol.ColPos
		project := &planpb.Node{
			NodeType: planpb.Node_PROJECT, NodeId: int32(len(builder.qry.Nodes)),
			Children: []int32{join.Children[1]},
			Stats: &planpb.Stats{Outcnt: scan.Stats.Outcnt, Cost: scan.Stats.Outcnt,
				Rowsize: scan.Stats.Rowsize + float64(types.VarlenaSize) + 4*float64(expr.Typ.Width)},
		}
		for pos, source := range scan.ProjectList {
			project.ProjectList = append(project.ProjectList, GetColExpr(source.Typ, 0, int32(pos)))
		}
		project.ProjectList = append(project.ProjectList, moved)
		projected := DeepCopyExpr(expr)
		projected.AuxId = 0
		projected.Expr = &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: int32(len(scan.ProjectList))}}
		outputPos := col.ColPos
		join.ProjectList = append([]*planpb.Expr(nil), join.ProjectList...)
		if usedElsewhere {
			outputPos = int32(len(join.ProjectList))
			join.ProjectList = append(join.ProjectList, projected)
		} else {
			join.ProjectList[outputPos] = projected
		}
		replacement := DeepCopyExpr(expr)
		replacement.AuxId = 0
		replacement.Expr = &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: outputPos}}
		agg.GroupBy = append([]*planpb.Expr(nil), agg.GroupBy...)
		agg.GroupBy[groupPos] = replacement
		join.Children = append([]int32(nil), join.Children...)
		join.Children[1] = project.NodeId
		builder.qry.Nodes = append(builder.qry.Nodes, project)
		builder.ctxByNode = append(builder.ctxByNode, nil)
		return // Bound retained build payload: at most one moved key per JOIN.
	}
}

// IsEquiJoin2 also reorients its arguments. This finalized-plan pass must not
// mutate ON expressions, including when a later guard rejects the candidate.
func groupExpressionHasHashKey(conditions []*planpb.Expr) bool {
	for _, condition := range conditions {
		fn := condition.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 || !IsEqualFunc(fn.Func.Obj) || isJSONNumericComparisonArgs(fn.Args) {
			continue
		}
		left, right := fn.Args[0].GetCol(), fn.Args[1].GetCol()
		if left != nil && right != nil && left.RelPos == 0 && right.RelPos == 1 {
			return true
		}
	}
	return false
}

// Only the known scalar expression owners and ordinary join-map message may
// participate. Unknown executable owners require their own coordinate proof.
func groupExpressionOrdinaryNode(node *planpb.Node) bool {
	if node.Limit != nil || node.Offset != nil || node.RankOption != nil || node.FilterIsBarrier ||
		node.ExtraOptions != "" || node.RollupFilter || node.EmitCompressedRowCount ||
		len(node.SourceStep) != 0 || node.RecursiveSink || node.RecursiveCte ||
		len(node.LockTargets) != 0 || len(node.RecvMsgList) != 0 ||
		node.IndexReaderParam != nil || node.IndexSearchScan != nil || node.SampleFunc != nil ||
		node.DedupJoinCtx != nil || node.ExternScan != nil || node.RowsetData != nil ||
		node.InsertCtx != nil || node.DeleteCtx != nil || node.PreInsertCtx != nil ||
		node.PreInsertUkCtx != nil || node.PreInsertSkCtx != nil || node.PostDmlCtx != nil ||
		len(node.UpdateCtxList) != 0 || node.Fuzzymessage != nil ||
		len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 {
		return false
	}
	for _, header := range node.SendMsgList {
		if node.NodeType != planpb.Node_JOIN || header.MsgType != int32(message.MsgJoinMap) {
			return false
		}
	}
	remaining := *node
	remaining.TableDef = nil
	remaining.ProjectList = nil
	switch node.NodeType {
	case planpb.Node_AGG:
		// HAVING remains on the aggregate. Its finalized group/aggregate
		// coordinates do not move, and it participates in the slot-use proof.
		remaining.GroupBy, remaining.AggList, remaining.FilterList = nil, nil, nil
	case planpb.Node_JOIN:
		remaining.OnList = nil
	case planpb.Node_TABLE_SCAN:
		remaining.FilterList, remaining.BlockFilterList = nil, nil
	default:
		return false
	}
	unknown := false
	_ = planpb.VisitExpressionsInOwner(&remaining, func(*planpb.Expr) error {
		unknown = true
		return nil
	})
	return !unknown
}

func groupExpressionSubstring(expr *planpb.Expr) bool {
	if expr == nil || expr.AuxId < 0 || expr.Typ.Id != int32(types.T_varchar) || expr.Typ.Width <= 0 ||
		expr.Typ.Width > types.MaxVarcharLen {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 3 || len(fn.AggConfig) != 0 || fn.AggConfigType != 0 || fn.SyntaxExplicitCast {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.SUBSTRING || overload != 2 {
		return false
	}
	if _, exists := function.GetFunctionByIdWithoutError(fn.Func.Obj); !exists {
		return false
	}
	input := fn.Args[0]
	if input == nil || input.AuxId < 0 || input.GetCol() == nil || input.PreparedNumeric != nil ||
		input.Typ.Id != int32(types.T_varchar) || input.Typ.Width < expr.Typ.Width ||
		input.Typ.Charset != expr.Typ.Charset || input.Typ.CollationVersion != expr.Typ.CollationVersion ||
		input.Typ.CollationMergeConflict != expr.Typ.CollationMergeConflict || input.Typ.PadSpace != expr.Typ.PadSpace {
		return false
	}
	// A bound function result has its own coercibility, unlike its source
	// column. Moving the unchanged expression preserves both declarations;
	// only the physical string domain must agree here, not their provenance.
	_, startOK := groupExpressionInt64(fn.Args[1])
	length, lengthOK := groupExpressionInt64(fn.Args[2])
	if !startOK || !lengthOK || length < 0 {
		return false
	}
	// substringBounds checks the offset before addition and only adds length
	// when it is smaller than the remaining input, including INT64 extremes.
	// The UTF-8 path emits at most four bytes per selected code point.
	if expr.PreparedNumeric == nil {
		return true
	}
	metadata := *expr.PreparedNumeric
	witness := metadata.StringDomainSource
	metadata.StringDomainSource = nil
	if !proto.Equal(&metadata, &planpb.PreparedNumericMetadata{}) || witness == nil ||
		witness.AuxId < 0 || witness.PreparedNumeric != nil || !proto.Equal(&witness.Typ, &expr.Typ) {
		return false
	}
	decl := witness.GetF()
	if decl == nil || decl.Func == nil || decl.Func.Obj != fn.Func.Obj || len(decl.Args) != 3 ||
		len(decl.AggConfig) != 0 || decl.AggConfigType != 0 || decl.SyntaxExplicitCast {
		return false
	}
	// Declaration witnesses describe an unknown value with a nullable typed
	// NULL, including when the real input column is declared NOT NULL.
	if !proto.Equal(decl.Args[0], stringDeclarationWitnessArg(input)) {
		return false
	}
	for i := 1; i < 3; i++ {
		actual, _ := groupExpressionInt64(fn.Args[i])
		declared, ok := groupExpressionInt64(decl.Args[i])
		if !ok || actual != declared {
			return false
		}
	}
	return true
}

func groupExpressionInt64(expr *planpb.Expr) (int64, bool) {
	if expr == nil || expr.AuxId < 0 || expr.Typ.Id != int32(types.T_int64) || expr.PreparedNumeric != nil {
		return 0, false
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull || lit.Src != nil || lit.IsBin || lit.IsSerialized || lit.LiteralForm != 0 ||
		lit.StringSource > uint32(types.StringSourceLiteral) {
		return 0, false
	}
	value, ok := lit.Value.(*planpb.Literal_I64Val)
	if !ok {
		return 0, false
	}
	return value.I64Val, true
}

// Include hidden declaration/provenance edges, which VisitExprTree deliberately
// leaves to its caller. The work cap also fails closed on cyclic metadata.
func groupExpressionReferences(owner any, rel, pos int32) (found, valid bool) {
	remaining := 4096
	stop := moerr.GetOkStopCurrRecur()
	var visit func(*planpb.Expr) error
	visit = func(expr *planpb.Expr) error {
		remaining--
		if remaining < 0 {
			return stop
		}
		switch expr.Expr.(type) {
		case *planpb.Expr_Col, *planpb.Expr_Lit, *planpb.Expr_F, *planpb.Expr_T, *planpb.Expr_P, *planpb.Expr_V, *planpb.Expr_Vec, *planpb.Expr_List:
		default:
			return stop
		}
		if col := expr.GetCol(); col != nil && col.RelPos == rel && col.ColPos == pos {
			found = true
		}
		if expr.PreparedNumeric != nil {
			metadata := *expr.PreparedNumeric
			source := metadata.StringDomainSource
			metadata.StringDomainSource = nil
			if !proto.Equal(&metadata, &planpb.PreparedNumericMetadata{}) {
				return stop
			}
			if source != nil {
				return planpb.VisitExprTree(source, visit)
			}
		}
		return nil
	}
	err := planpb.VisitExpressionsInOwner(owner, func(expr *planpb.Expr) error {
		return planpb.VisitExprTree(expr, visit)
	})
	return found, err == nil
}
