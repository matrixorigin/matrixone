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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// scalarCTERowSource stages native identity forwarding. Analysis is read-only;
// the range-aggregate owner publishes these private columns after admission.
// Public binding columns and captured CTE transport schemas stay unchanged.
type scalarCTERowSource struct {
	root        int32
	marker      *plan.Expr
	columns     []*plan.Expr
	aliases     [][2]int32
	projections []scalarCTEProjection
}

type scalarCTEProjection struct {
	node    *plan.Node
	columns []*plan.Expr
}

func (source *scalarCTERowSource) publish() {
	if source != nil {
		for _, projection := range source.projections {
			projection.node.ProjectList = projection.columns
		}
	}
}

// analyzeScalarCTERowSource admits only inline CTE projections that preserve
// individual native table rows. A visible primary key or a row-id-looking name
// is not a substitute for the scan's authoritative hidden Row_ID. privatePos,
// when nonnegative, reserves the outer binding's first private group position.
func (builder *QueryBuilder) analyzeScalarCTERowSource(root int32, privatePos int) *scalarCTERowSource {
	if builder.isForUpdate {
		return nil
	}
	var projects []*plan.Node
	hasCTE := false
	id := root
	var scan *plan.Node
	for remaining := len(builder.qry.Nodes); remaining > 0; remaining-- {
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return nil
		}
		node := builder.qry.Nodes[id]
		if node == nil || node.Limit != nil || node.Offset != nil || node.RankOption != nil {
			return nil
		}
		if int(id) < len(builder.ctxByNode) {
			if ctx := builder.ctxByNode[id]; ctx != nil && (len(ctx.viewPath) > 0 || len(ctx.views) > 0) {
				return nil
			}
		}
		hasCTE = hasCTE || builder.localCTERoots[id]
		switch node.NodeType {
		case plan.Node_TABLE_SCAN:
			if len(node.Children) != 0 || len(node.BindingTags) != 1 || node.TableDef == nil || node.TableDef.ViewSql != nil || node.TableDef.TableType == catalog.SystemViewRel {
				return nil
			}
			scan = node
		case plan.Node_PROJECT:
			if len(node.Children) != 1 || len(node.BindingTags) != 1 || len(node.ProjectList) == 0 {
				return nil
			}
			projects = append(projects, node)
		case plan.Node_FILTER:
			if len(node.Children) != 1 {
				return nil
			}
		case plan.Node_SORT:
			if len(node.Children) != 1 {
				return nil
			}
			for _, order := range node.OrderBy {
				if order == nil || order.Expr == nil || order.Expr.GetCol() == nil {
					return nil
				}
			}
		default:
			return nil
		}
		if scan != nil {
			break
		}
		id = node.Children[0]
	}
	if scan == nil || !hasCTE || len(projects) == 0 ||
		!builder.cteSubtreeIsDeterministic(root, make(map[int32]bool)) {
		return nil
	}
	pos, ok := scan.TableDef.Name2ColIndex[catalog.Row_ID]
	if !ok || pos < 0 || int(pos) >= len(scan.TableDef.Cols) {
		return nil
	}
	identity := scan.TableDef.Cols[pos]
	if identity == nil || !identity.Hidden || identity.Name != catalog.Row_ID || identity.Typ.Id != int32(types.T_Rowid) {
		return nil
	}
	source := &scalarCTERowSource{root: root, marker: GetColExpr(identity.Typ, scan.BindingTags[0], pos)}
	source.aliases = append(source.aliases, [2]int32{scan.BindingTags[0], pos})
	source.columns = make([]*plan.Expr, len(scan.TableDef.Cols))
	for i, col := range scan.TableDef.Cols {
		if col == nil {
			return nil
		}
		source.columns[i] = GetColExpr(col.Typ, scan.BindingTags[0], int32(i))
	}
	for i := len(projects) - 1; i >= 0; i-- {
		node := projects[i]
		markerCol := source.marker.GetCol()
		markerPos := -1
		for j, expr := range node.ProjectList {
			if expr == nil {
				return nil
			}
			col := expr.GetCol()
			if col == nil || col.RelPos != markerCol.RelPos || col.ColPos < 0 || int(col.ColPos) >= len(source.columns) {
				return nil
			}
			if col.ColPos == markerCol.ColPos {
				markerPos = j
			}
		}
		columns := node.ProjectList
		if i == 0 && privatePos >= 0 {
			switch len(columns) {
			case privatePos:
				markerPos = -1
			case privatePos + 1:
				if markerPos != privatePos {
					return nil
				}
			default:
				return nil
			}
		}
		if markerPos < 0 {
			markerPos = len(columns)
			forwarded := make([]*plan.Expr, len(columns)+1)
			copy(forwarded, columns)
			forwarded[len(columns)] = DeepCopyExpr(source.marker)
			columns = forwarded
			source.projections = append(source.projections, scalarCTEProjection{node: node, columns: columns})
		}
		source.columns = columns
		source.marker = GetColExpr(identity.Typ, node.BindingTags[0], int32(markerPos))
		source.aliases = append(source.aliases, [2]int32{node.BindingTags[0], int32(markerPos)})
	}
	return source
}

// Only the established one-row-per-left-row compositions may carry a CTE
// identity into another reaggregation. Its private slot must remain positional.
func (builder *QueryBuilder) preservesScalarCTEIdentity(id int32, source *scalarCTERowSource) bool {
	for remaining := len(builder.qry.Nodes); remaining > 0; remaining-- {
		if id == source.root {
			return true
		}
		if id < 0 || int(id) >= len(builder.qry.Nodes) {
			return false
		}
		node := builder.qry.Nodes[id]
		if node == nil {
			return false
		}
		switch node.NodeType {
		case plan.Node_FILTER, plan.Node_SORT:
			if len(node.Children) != 1 {
				return false
			}
			id = node.Children[0]
		case plan.Node_JOIN:
			if len(node.Children) != 2 || (node.JoinType != plan.Node_SINGLE && node.JoinType != plan.Node_MARK && node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI) {
				return false
			}
			id = node.Children[0]
		case plan.Node_AGG:
			if _, ok := builder.scalarReaggAliases[id]; !ok || len(node.Children) != 1 {
				return false
			}
			marker := source.marker.GetCol()
			if marker.ColPos < 0 || int(marker.ColPos) >= len(node.GroupBy) {
				return false
			}
			group := node.GroupBy[marker.ColPos].GetCol()
			if group == nil || group.RelPos != marker.RelPos || group.ColPos != marker.ColPos {
				return false
			}
			join := builder.qry.Nodes[node.Children[0]]
			if join.NodeType != plan.Node_JOIN || join.JoinType != plan.Node_LEFT || len(join.Children) != 2 {
				return false
			}
			id = join.Children[0]
		default:
			return false
		}
	}
	return false
}
