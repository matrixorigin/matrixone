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
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Only the normal late index pass follows the last scan-statistics refresh.
// Its later passes do not revisit leaves, but may add compound-key filters;
// compilation can also complete parameter-diagnostic block filters. Exclude
// both sources before treating the current block list as a complete selector.
// The caller separately proves the full unary reader shape and original owner.
func temporalBaseBlockFiltersFinal(ctx context.Context, scan *plan.Node, lateBaseStatsFinalized bool, budget *int) bool {
	if !lateBaseStatsFinalized || scan == nil || scan.NodeType != plan.Node_TABLE_SCAN || scan.TableDef == nil || scan.TableDef.Pkey == nil ||
		len(scan.FilterList) == 0 || len(scan.FilterList) > 64 || scan.TableDef.Pkey.PkeyColName == "" ||
		len(scan.TableDef.Pkey.Names) > 1 || scan.TableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName ||
		(scan.TableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(scan.TableDef.ClusterBy.Name)) {
		return false
	}
	var nodes int
	for _, expr := range scan.FilterList {
		if !temporalCostExpressionFits(ctx, expr, 0, &nodes, budget) {
			return false
		}
	}
	if !spendTemporalCostWork(ctx, budget, nodes) {
		return false
	}
	for _, expr := range scan.FilterList {
		if containsStatementParameterDiagnostic(expr) {
			return false
		}
	}
	return true
}

// This proof is local to ordinary index costing. It does not change SQL or the
// existing candidate correctness/admission rules. The supported reader fully
// drains a normal persisted scan and has no pre-materialization row selector.
// Later composite-key filters cannot strengthen this subset: none of their
// key parts is filtered and no physical column occurs in two predicate leaves.
func temporalBaseReaderShapeSafe(ctx context.Context, query *plan.Query, scanID int32, budget *int) bool {
	if ctx == nil || query == nil || len(query.Nodes) > 64 || scanID < 0 || int(scanID) >= len(query.Nodes) || query.StmtType != plan.Query_SELECT || len(query.Steps) != 1 || len(query.Params) != 0 {
		return false
	}
	scan := query.Nodes[scanID]
	if scan == nil || scan.TableDef == nil || len(scan.TableDef.Cols) > engine.MaxTemporalTableColumns || len(scan.ProjectList) != 0 || len(scan.FilterList) == 0 || len(scan.FilterList) > 64 || len(scan.BlockFilterList) > 64 || scan.ExtraOptions == PreparedBlockFilterDisabledScanOption {
		return false
	}
	// Full ancestor validation precedes any clone or optional scalar evaluation.
	for id, steps := query.Steps[0], 0; ; steps++ {
		if steps >= 64 || !spendTemporalCostWork(ctx, budget, 1) {
			return false
		}
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return false
		}
		node := query.Nodes[id]
		if node.Limit != nil || node.Offset != nil || node.RankOption != nil || len(node.OrderBy) != 0 {
			return false
		}
		if node.IndexReaderParam != nil || len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 || len(node.SendMsgList) != 0 || len(node.RecvMsgList) != 0 || node.SampleFunc != nil || node.Fuzzymessage != nil || node.IndexSearchScan != nil {
			return false
		}
		if id == scanID {
			break
		}
		if len(node.Children) != 1 || (node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER) || len(node.ProjectList) > 64 || len(node.FilterList) != 0 || len(node.BlockFilterList) != 0 {
			return false
		}
		for _, expr := range node.ProjectList {
			if expr == nil || (expr.GetCol() == nil && expr.GetLit() == nil) || !spendTemporalCostWork(ctx, budget, 1) {
				return false
			}
		}
		id = node.Children[0]
	}
	var expressionNodes int
	for _, list := range [][]*plan.Expr{scan.FilterList, scan.BlockFilterList} {
		for _, expr := range list {
			if !temporalCostExpressionFits(ctx, expr, 0, &expressionNodes, budget) {
				return false
			}
		}
	}
	if !temporalCandidateSimplePathSafe(query, scanID) {
		return false
	}
	forbidden := make(map[string]bool)
	if pk := scan.TableDef.Pkey; pk != nil {
		if len(pk.Names) > engine.MaxTemporalIndexParts || len(pk.PkeyColName) > engine.MaxTemporalNameBytes {
			return false
		}
		forbidden[pk.PkeyColName] = true
		for _, name := range pk.Names {
			if len(name) > engine.MaxTemporalNameBytes {
				return false
			}
			forbidden[name] = true
		}
	}
	if cb := scan.TableDef.ClusterBy; cb != nil {
		if len(cb.Name) > engine.MaxTemporalNameBytes {
			return false
		}
		forbidden[cb.Name] = true
		if util.JudgeIsCompositeClusterByColumn(cb.Name) {
			// The shared decoder assumes a well-formed catalog name. Check its
			// length-prefixed envelope before optional costing invokes it.
			for at := len(catalog.PrefixCBColName); at < len(cb.Name); {
				if len(cb.Name)-at < 3 {
					return false
				}
				n, err := strconv.Atoi(cb.Name[at : at+3])
				if err != nil || n <= 0 || n > len(cb.Name)-at-3 {
					return false
				}
				at += 3 + n
			}
			for _, name := range util.SplitCompositeClusterByColumnName(cb.Name) {
				forbidden[name] = true
			}
		}
	}
	var used [maxTemporalFilterColumns]uint32
	usedCount, nodes := 0, 0
	var visit func(*plan.Expr, int) bool
	visit = func(expr *plan.Expr, depth int) bool {
		nodes++
		if nodes > 64 || depth > 64 || expr == nil || !spendTemporalCostWork(ctx, budget, 1) {
			return false
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
			return false
		}
		if fn.Func.ObjName == "and" || fn.Func.ObjName == "or" {
			return visit(fn.Args[0], depth+1) && visit(fn.Args[1], depth+1)
		}
		switch fn.Func.ObjName {
		case "=", "<", "<=", ">", ">=":
		default:
			return false
		}
		column, bound := fn.Args[0], fn.Args[1]
		if column == nil || bound == nil {
			return false
		}
		if column.GetCol() == nil {
			column, bound = bound, column
		}
		ref := column.GetCol()
		if ref == nil || len(scan.BindingTags) != 1 || ref.RelPos != scan.BindingTags[0] || ref.ColPos < 0 || int(ref.ColPos) >= len(scan.TableDef.Cols) || !temporalCandidateBoundSafe(bound) {
			return false
		}
		def := scan.TableDef.Cols[ref.ColPos]
		if def == nil || forbidden[def.Name] || def.Primary || def.ClusterBy || types.IsUnicodeCollation(uint8(def.Typ.Charset)) || def.Typ.Id != column.Typ.Id || def.Typ.Scale != column.Typ.Scale {
			return false
		}
		for _, seq := range used[:usedCount] {
			if seq == def.Seqnum {
				return false
			}
		}
		if usedCount == maxTemporalFilterColumns {
			return false
		}
		used[usedCount] = def.Seqnum
		usedCount++
		return true
	}
	for _, expr := range scan.FilterList {
		if !visit(expr, 0) {
			return false
		}
	}
	// calcScanStats copies filters and changes their statistics. Compare every
	// other field, including original source/provenance, collation and markers.
	for _, block := range scan.BlockFilterList {
		matched := false
		for _, filter := range scan.FilterList {
			if temporalCostFiltersEqual(ctx, block, filter, budget) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return budget != nil && *budget > 0
}

func temporalCostFiltersEqual(ctx context.Context, a, b *plan.Expr, budget *int) bool {
	// Positive shape validation bounds the scan tree, but an extra block tree
	// has not been proved yet. Bound both trees before copying either one.
	var ca, cb int
	if !temporalCostExpressionFits(ctx, a, 0, &ca, budget) || !temporalCostExpressionFits(ctx, b, 0, &cb, budget) {
		return false
	}
	bytes := a.ProtoSize() + b.ProtoSize()
	if bytes > 32768 || !spendTemporalCostWork(ctx, budget, bytes) {
		return false
	}

	x, y := proto.Clone(a).(*plan.Expr), proto.Clone(b).(*plan.Expr)
	var clearStats func(*plan.Expr)
	clearStats = func(expr *plan.Expr) {
		expr.Selectivity, expr.Ndv = 0, 0
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				clearStats(arg)
			}
		}
	}
	clearStats(x)
	clearStats(y)
	return proto.Equal(x, y)
}

// Only scalar leaves and functions are traversable here. Opaque vectors,
// folds, window specs and dependent trees can retain large or recursive
// payloads outside Function.Args and must be rejected before cloning.
func temporalCostExpressionFits(ctx context.Context, expr *plan.Expr, depth int, count, budget *int) bool {
	*count++
	if expr == nil || depth > 64 || *count > 64 || expr.PreparedNumeric != nil || !spendTemporalCostWork(ctx, budget, 1) {
		return false
	}
	switch value := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_T:
	case *plan.Expr_Lit:
		if !temporalCostLiteralFits(expr) {
			return false
		}
	case *plan.Expr_F:
		fn := value.F
		if fn == nil || fn.Func == nil || len(fn.Args) > 64 || len(fn.AggConfig) != 0 || fn.Func.PubInfo != nil || fn.Func.Snapshot != nil {
			return false
		}
		for _, arg := range fn.Args {
			if !temporalCostExpressionFits(ctx, arg, depth+1, count, budget) {
				return false
			}
		}
	default:
		return false
	}
	// Size calculation is now over a bounded positive tree; it reads lengths
	// rather than copying retained strings/unknown fields.
	return expr.ProtoSize() <= 16384
}

const maxTemporalFilterColumns = 4
