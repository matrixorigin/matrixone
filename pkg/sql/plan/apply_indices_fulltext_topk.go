// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// attachConditionalStreamTopK annotates only the complete retained
// SORT / PROJECT / positive-score FILTER / source-PK JOIN shape.
// No operators or pagination are removed and CandidateLimit stays nil.
func (b *QueryBuilder) attachConditionalStreamTopK(sortNode, project *plan.Node) {
	if b.sqlCalcFoundRows || sortNode == nil || project == nil || sortNode.RankOption != nil ||
		project.RankOption != nil || len(sortNode.Children) != 1 || len(project.Children) != 1 ||
		len(sortNode.OrderBy) != 2 || len(project.ProjectList) != 2 || len(project.BindingTags) != 1 ||
		len(sortNode.FilterList) != 0 || len(project.FilterList) != 0 || project.Limit != nil || project.Offset != nil {
		return
	}
	if project.NodeId >= 0 && int(project.NodeId) < len(b.ctxByNode) {
		if ctx := b.ctxByNode[project.NodeId]; ctx != nil && (ctx.isDistinct || len(ctx.windows) != 0 || len(ctx.groups) != 0 || len(ctx.aggregates) != 0) {
			return
		}
	}
	limit, ok := getLiteralUint64(sortNode.Limit)
	if !ok || limit == 0 {
		return
	}
	offset := uint64(0)
	if sortNode.Offset != nil {
		offset, ok = getLiteralUint64(sortNode.Offset)
		if !ok {
			return
		}
	}
	if limit > fulltext2.MaxConditionalStreamTopK || offset > fulltext2.MaxConditionalStreamTopK-limit {
		return
	}
	first, second := sortNode.OrderBy[0], sortNode.OrderBy[1]
	if first == nil || second == nil || first.Flag != plan.OrderBySpec_DESC ||
		(second.Flag != plan.OrderBySpec_INTERNAL && second.Flag != plan.OrderBySpec_ASC) {
		return
	}
	resolve := func(e *plan.Expr) *plan.Expr {
		if e == nil {
			return nil
		}
		col := e.GetCol()
		if col != nil && col.RelPos == project.BindingTags[0] && col.ColPos >= 0 && int(col.ColPos) < len(project.ProjectList) {
			return project.ProjectList[col.ColPos]
		}
		return e
	}
	input := b.qry.Nodes[sortNode.Children[0]]
	if input == project {
		input = b.qry.Nodes[project.Children[0]]
	}
	if input.NodeType != plan.Node_FILTER || len(input.Children) != 1 || len(input.FilterList) != 1 ||
		input.Limit != nil || input.Offset != nil || input.RankOption != nil {
		return
	}
	filter := input.FilterList[0]
	join := b.qry.Nodes[input.Children[0]]
	if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_INNER || len(join.Children) != 2 ||
		len(join.OnList) != 1 || len(join.FilterList) != 0 || join.Limit != nil || join.Offset != nil || join.RankOption != nil {
		return
	}
	source, search := b.qry.Nodes[join.Children[0]], b.qry.Nodes[join.Children[1]]
	if source.NodeType != plan.Node_TABLE_SCAN || search.NodeType != plan.Node_INDEX_SEARCH_SCAN {
		return
	}
	spec, def, ref := search.IndexSearchScan, source.TableDef, source.ObjRef
	if spec == nil || spec.CandidateLimit != nil || spec.ScanSnapshot != nil || source.ScanSnapshot != nil ||
		def == nil || ref == nil || ref.Obj <= 0 || uint64(ref.Obj) != def.TblId || def.Partition != nil ||
		def.IsTemporary || def.Hidden || ref.PubInfo != nil || ref.SubscriptionName != "" || ref.Snapshot != nil ||
		def.Pkey == nil || len(def.Pkey.Names) != 1 || def.Pkey.CompPkeyCol != nil ||
		def.Pkey.PkeyColName == catalog.FakePrimaryKeyColName || len(source.BindingTags) != 1 || len(search.BindingTags) != 1 ||
		len(source.FilterList) != 0 || len(search.FilterList) != 0 || source.Limit != nil || source.Offset != nil ||
		source.RankOption != nil || search.RankOption != nil || search.Limit != nil || search.Offset != nil ||
		len(source.RuntimeFilterProbeList) != 0 || len(search.RuntimeFilterProbeList) != 0 {
		return
	}
	// The visibility proof must describe this retained join's source, including
	// after a future planner rewrite reuses an already-built search node.
	searchRef, searchDef := spec.SourceTable, spec.SourceTableDef
	if searchRef == nil || searchDef == nil || searchRef.Obj != ref.Obj ||
		searchRef.SchemaName != ref.SchemaName || searchRef.ObjName != ref.ObjName ||
		searchDef.TblId != def.TblId || searchDef.Name != def.Name ||
		searchRef.PubInfo != nil || searchRef.SubscriptionName != "" || searchRef.Snapshot != nil {
		return
	}
	pkPos, exists := def.Name2ColIndex[def.Pkey.PkeyColName]
	if !exists || pkPos < 0 || int(pkPos) >= len(def.Cols) {
		return
	}
	pk := def.Cols[pkPos]
	if pk.Typ.Id != int32(types.T_varchar) || !pk.Typ.NotNullable || pk.Typ.Charset > uint32(types.CharsetUTF8MB4Bin) ||
		pk.Typ.CollationVersion != 0 {
		return
	}
	isCol := func(e *plan.Expr, tag, pos int32) bool {
		if e == nil {
			return false
		}
		c := e.GetCol()
		return c != nil && c.RelPos == tag && c.ColPos == pos
	}
	st, ft := source.BindingTags[0], search.BindingTags[0]
	if !isCol(resolve(first.Expr), ft, 1) || !isCol(resolve(second.Expr), st, pkPos) {
		return
	}
	havePK, haveScore := false, false
	for _, e := range project.ProjectList {
		if isCol(e, st, pkPos) {
			havePK = true
		} else if isCol(e, ft, 1) {
			haveScore = true
		} else {
			return
		}
	}
	if !havePK || !haveScore {
		return
	}
	on := join.OnList[0].GetF()
	if on == nil || on.Func.ObjName != "=" || len(on.Args) != 2 ||
		!(isCol(on.Args[0], st, pkPos) && isCol(on.Args[1], ft, 0) || isCol(on.Args[1], st, pkPos) && isCol(on.Args[0], ft, 0)) {
		return
	}
	pred := filter.GetF()
	if pred == nil || pred.Func.ObjName != ">" || len(pred.Args) != 2 || !fulltextTopKZero(pred.Args[1]) {
		return
	}
	score := pred.Args[0]
	if cast := score.GetF(); cast != nil && cast.Func.ObjName == "cast" && len(cast.Args) == 2 &&
		score.Typ.Id == int32(types.T_float64) && cast.Args[0].Typ.Id == int32(types.T_float32) {
		score = cast.Args[0]
	}
	if !isCol(score, ft, 1) || score.Typ.Id != int32(types.T_float32) {
		return
	}
	// This helper is called only from the existing fulltext construction path.
	// Classic fulltext options lack Config and cannot be admitted.
	opts, err := ft2plan.DecodeScanOptions(spec.AlgoOptions)
	if err != nil || opts.Config == "" || opts.Mode != int64(tree.FULLTEXT_BM25) || opts.IncludePreds != "" ||
		len(spec.Index.GetIncludedColumns()) != 0 || len(spec.AlgoExprs) != 0 {
		return
	}
	opts.ConditionalStreamTopK = &ft2plan.ConditionalStreamTopK{Semantics: ft2plan.ConditionalStreamTopKVersion, K: limit + offset}
	data, err := ft2plan.EncodeScanOptions(opts)
	if err == nil {
		spec.AlgoOptions = data
	}
}

func fulltextTopKZero(e *plan.Expr) bool {
	if e == nil || e.GetLit() == nil || e.GetLit().Isnull {
		return false
	}
	switch v := e.GetLit().Value.(type) {
	case *plan.Literal_I64Val:
		return v.I64Val == 0
	case *plan.Literal_U64Val:
		return v.U64Val == 0
	case *plan.Literal_Fval:
		return v.Fval == 0
	case *plan.Literal_Dval:
		return v.Dval == 0
	}
	return false
}
