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
	"math"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type repeatedUpdateStatsKind uint8

const (
	repeatedStatsExpand repeatedUpdateStatsKind = iota
	repeatedStatsRoles
	repeatedStatsRole
	repeatedStatsParticipants
	repeatedStatsCapacityFilter
)

// These hints are planner-local: they describe the cardinality of the role
// expansion and RowID groups, whose generated columns have no catalog NDV.
type repeatedUpdateStatsSource struct {
	apply  int32
	scans  []int32
	tables []uint64
}

type repeatedUpdateStatsHint struct {
	kind    repeatedUpdateStatsKind
	source  *repeatedUpdateStatsSource
	role    int
	roles   []int
	filters []*planpb.Expr
}

func newRepeatedUpdateStatsSource(builder *QueryBuilder, match, apply int32, rowIDs []int, plans []*dmlPlanCtx) *repeatedUpdateStatsSource {
	if len(rowIDs) != len(plans) {
		return nil
	}
	source := &repeatedUpdateStatsSource{apply: apply, scans: make([]int32, len(plans)), tables: make([]uint64, len(plans))}
	for i, pos := range rowIDs {
		col := builder.qry.Nodes[match].ProjectList[pos].GetCol()
		if col == nil {
			return nil
		}
		scanID := builder.findScanNodeByTag(match, col.RelPos)
		if scanID < 0 {
			return nil
		}
		scan := builder.qry.Nodes[scanID]
		if scan.TableDef == nil || scan.TableDef.TblId != plans[i].tableDef.TblId {
			return nil
		}
		source.scans[i], source.tables[i] = scanID, scan.TableDef.TblId
	}
	return source
}

func (builder *QueryBuilder) setRepeatedUpdateStats(node int32, hint repeatedUpdateStatsHint) {
	if builder.repeatedUpdateStats == nil {
		builder.repeatedUpdateStats = make(map[int32]repeatedUpdateStatsHint)
	}
	builder.repeatedUpdateStats[node] = hint
}

func (builder *QueryBuilder) markRepeatedUpdateCapacityFilter(node int32) {
	filters := builder.qry.Nodes[node].FilterList
	builder.setRepeatedUpdateStats(node, repeatedUpdateStatsHint{
		kind: repeatedStatsCapacityFilter, filters: append([]*planpb.Expr(nil), filters...),
	})
}

func validRepeatedEstimate(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// estimates bounds each role's distinct rows by both joined matches and that
// role's filtered target scan. This avoids turning join fanout into writes.
func (source *repeatedUpdateStatsSource) estimates(builder *QueryBuilder) (float64, []float64, bool) {
	if source == nil || source.apply < 0 || int(source.apply) >= len(builder.qry.Nodes) {
		return 0, nil, false
	}
	apply := builder.qry.Nodes[source.apply]
	if apply.NodeType != planpb.Node_APPLY || len(apply.Children) == 0 {
		return 0, nil, false
	}
	match := builder.qry.Nodes[apply.Children[0]].Stats
	if match == nil || !validRepeatedEstimate(match.Outcnt) {
		return 0, nil, false
	}
	estimates := make([]float64, len(source.scans))
	for i, id := range source.scans {
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return 0, nil, false
		}
		scan := builder.qry.Nodes[id]
		if scan.NodeType != planpb.Node_TABLE_SCAN || scan.TableDef == nil || scan.TableDef.TblId != source.tables[i] ||
			scan.Stats == nil || !validRepeatedEstimate(scan.Stats.Outcnt) || !validRepeatedEstimate(scan.Stats.TableCnt) {
			return 0, nil, false
		}
		estimates[i] = math.Min(match.Outcnt, scan.Stats.Outcnt)
	}
	return match.Outcnt, estimates, true
}

func sameRepeatedFilters(node *planpb.Node, filters []*planpb.Expr) bool {
	if len(filters) == 0 || len(node.FilterList) != len(filters) {
		return false
	}
	for i, filter := range filters {
		if node.FilterList[i] != filter {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) applyRepeatedUpdateStats(nodeID int32) {
	hint, ok := builder.repeatedUpdateStats[nodeID]
	if !ok {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if node.Stats == nil {
		return
	}
	if hint.kind == repeatedStatsCapacityFilter {
		if node.NodeType == planpb.Node_FILTER && len(node.Children) == 1 && sameRepeatedFilters(node, hint.filters) {
			child := builder.qry.Nodes[node.Children[0]].Stats
			if child != nil {
				node.Stats.Outcnt, node.Stats.Cost = child.Outcnt, child.Cost
				node.Stats.Selectivity, node.Stats.BlockNum = 1, child.BlockNum
			}
		}
		return
	}
	match, roles, ok := hint.source.estimates(builder)
	if !ok {
		return
	}
	var out float64
	switch hint.kind {
	case repeatedStatsExpand:
		if node.NodeType != planpb.Node_APPLY {
			return
		}
		out = match * float64(len(roles))
	case repeatedStatsRoles:
		if node.NodeType != planpb.Node_AGG {
			return
		}
		for _, n := range roles {
			out += n
		}
	case repeatedStatsRole:
		if node.NodeType != planpb.Node_FILTER || !sameRepeatedFilters(node, hint.filters) || hint.role < 0 || hint.role >= len(roles) {
			return
		}
		out = roles[hint.role]
	case repeatedStatsParticipants:
		if node.NodeType != planpb.Node_AGG || len(hint.roles) == 0 {
			return
		}
		for _, role := range hint.roles {
			if role < 0 || role >= len(roles) {
				return
			}
			out += roles[role]
		}
		scan := builder.qry.Nodes[hint.source.scans[hint.roles[0]]]
		out = math.Min(out, scan.Stats.TableCnt)
	default:
		return
	}
	if !validRepeatedEstimate(out) {
		return
	}
	node.Stats.Outcnt = out
	if node.NodeType == planpb.Node_AGG {
		input := builder.qry.Nodes[node.Children[0]].Stats
		node.Stats.Cost = input.Outcnt + out
		if node.Stats.HashmapStats != nil {
			node.Stats.HashmapStats.HashmapSize = out
		}
	} else if node.NodeType == planpb.Node_APPLY {
		node.Stats.Cost = out
	} else if len(node.Children) == 1 {
		input := builder.qry.Nodes[node.Children[0]].Stats
		node.Stats.Selectivity = 1
		if input.Outcnt > 0 {
			node.Stats.Selectivity = math.Min(1, out/input.Outcnt)
		}
	}
}
