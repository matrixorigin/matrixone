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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// A UNION ALL of the same ENUM domain can carry its storage ordinal without
// changing its displayed output. Prove both the complete relational path and
// every input domain before adding any hidden columns; DISTINCT, grouping and
// other set operations remain display-value boundaries.
func (builder *QueryBuilder) materializeEnumUnionAllForInsert(expr *Expr) (*Expr, bool, error) {
	if expr == nil || expr.GetCol() == nil {
		return expr, false, nil
	}
	rootID, ok := builder.tag2NodeID[expr.GetCol().RelPos]
	if !ok {
		return expr, false, nil
	}
	hasUnion, safe := builder.enumUnionAllRowPreserving(rootID, make(map[int32]bool))
	if !safe || !hasUnion {
		return expr, false, nil
	}
	var domain *planpb.Type
	compatible := func(display *Expr) bool {
		raw, found := storedMySQLSpecialTypeExpr(display)
		if !found || !isEnumPlanType(&raw.Typ) {
			return false
		}
		if domain == nil {
			domain = DeepCopyType(&raw.Typ)
			return true
		}
		return raw.Typ.Enumvalues == domain.Enumvalues && raw.Typ.EnumCollation == domain.EnumCollation
	}
	if !builder.isProjectedDisplayValueExpr(expr, compatible, true, nil) || domain == nil {
		return expr, false, nil
	}
	if builder.enumOrdinalByDisplayNode == nil {
		builder.enumOrdinalByDisplayNode = make(map[[2]int32]int32)
	}
	raw, ok := builder.materializeEnumUnionAllValue(expr, domain, make(map[[2]int32]bool))
	if !ok {
		return nil, false, moerr.NewInternalError(builder.GetContext(), "failed to materialize proven ENUM UNION ALL source")
	}
	return raw, true, nil
}

func (builder *QueryBuilder) enumUnionAllRowPreserving(nodeID int32, visiting map[int32]bool) (bool, bool) {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) || visiting[nodeID] {
		return false, false
	}
	node := builder.qry.Nodes[nodeID]
	if node.Limit != nil || node.Offset != nil || len(node.SourceStep) != 0 {
		return false, false
	}
	if int(nodeID) < len(builder.ctxByNode) {
		owner := builder.ctxByNode[nodeID]
		if owner != nil && (owner.isDistinct || len(owner.groups) != 0 || len(owner.aggregates) != 0) {
			return false, false
		}
	}
	switch node.NodeType {
	case planpb.Node_TABLE_SCAN, planpb.Node_EXTERNAL_SCAN, planpb.Node_VALUE_SCAN:
		return false, true
	case planpb.Node_PROJECT, planpb.Node_FILTER, planpb.Node_SORT:
		if len(node.Children) != 1 {
			return false, false
		}
	case planpb.Node_UNION_ALL:
		if len(node.Children) < 2 {
			return false, false
		}
	default:
		return false, false
	}
	visiting[nodeID] = true
	defer delete(visiting, nodeID)
	hasUnion := node.NodeType == planpb.Node_UNION_ALL
	for _, child := range node.Children {
		childUnion, safe := builder.enumUnionAllRowPreserving(child, visiting)
		if !safe {
			return false, false
		}
		hasUnion = hasUnion || childUnion
	}
	return hasUnion, true
}

func (builder *QueryBuilder) materializeEnumUnionAllValue(expr *Expr, domain *planpb.Type, visiting map[[2]int32]bool) (*Expr, bool) {
	if raw, ok := storedMySQLSpecialTypeExpr(expr); ok && isEnumPlanType(&raw.Typ) {
		return raw, true
	}
	if expr == nil || expr.GetCol() == nil {
		return nil, false
	}
	col := expr.GetCol()
	nodeID, ok := builder.tag2NodeID[col.RelPos]
	if !ok {
		return nil, false
	}
	return builder.materializeEnumUnionAllAtNode(nodeID, col.ColPos, col.RelPos, domain, visiting)
}

func (builder *QueryBuilder) materializeEnumUnionAllAtNode(nodeID, colPos, outputTag int32, domain *planpb.Type, visiting map[[2]int32]bool) (*Expr, bool) {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nil, false
	}
	node := builder.qry.Nodes[nodeID]
	key := [2]int32{nodeID, colPos}
	if colPos < 0 || int(colPos) >= len(node.ProjectList) || visiting[key] {
		return nil, false
	}
	if pos, ok := builder.enumOrdinalByDisplayNode[key]; ok {
		return GetColExpr(node.ProjectList[pos].Typ, outputTag, pos), true
	}
	visiting[key] = true
	defer delete(visiting, key)
	pos := int32(len(node.ProjectList))
	if node.NodeType == planpb.Node_UNION_ALL {
		for _, childID := range node.Children {
			if childID < 0 || int(childID) >= len(builder.qry.Nodes) || len(builder.qry.Nodes[childID].BindingTags) == 0 {
				return nil, false
			}
			child := builder.qry.Nodes[childID]
			value, ok := builder.materializeEnumUnionAllAtNode(childID, colPos, child.BindingTags[0], domain, visiting)
			if !ok || value.GetCol() == nil || value.GetCol().ColPos != pos {
				return nil, false
			}
		}
		typ := *domain
		typ.NotNullable = false
		leftTag := builder.qry.Nodes[node.Children[0]].BindingTags[0]
		node.ProjectList = append(node.ProjectList, GetColExpr(typ, leftTag, pos))
	} else if isNullLiteralExpr(node.ProjectList[colPos]) {
		typ := *domain
		typ.NotNullable = false
		node.ProjectList = append(node.ProjectList, &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}})
	} else {
		value, ok := builder.materializeEnumUnionAllValue(node.ProjectList[colPos], domain, visiting)
		if !ok {
			return nil, false
		}
		node.ProjectList = append(node.ProjectList, value)
	}
	builder.enumOrdinalByDisplayNode[key] = pos
	return GetColExpr(node.ProjectList[pos].Typ, outputTag, pos), true
}
