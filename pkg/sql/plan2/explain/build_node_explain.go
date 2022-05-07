// Copyright 2021 Matrix Origin
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

package explain

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

var _ NodeDescribe = &NodeDescribeImpl{}

type NodeDescribeImpl struct {
	Node *plan.Node
}

func NewNodeDescriptionImpl(node *plan.Node) *NodeDescribeImpl {
	return &NodeDescribeImpl{
		Node: node,
	}
}

func (ndesc NodeDescribeImpl) GetNodeBasicInfo(options *ExplainOptions) string {
	var result string
	var pname string /* node type name for text output */
	//var sname string /* node type name for non-text output */

	switch ndesc.Node.NodeType {
	case plan.Node_UNKNOWN:
		pname = "UnKnow Node"
	case plan.Node_VALUE_SCAN:
		pname = "Value Scan"
	case plan.Node_TABLE_SCAN:
		pname = "Table Scan"
	case plan.Node_FUNCTION_SCAN:
		pname = "Function Scan"
	case plan.Node_EXTERNAL_SCAN:
		pname = "External Scan"
	case plan.Node_PROJECT:
		pname = "Project"
	case plan.Node_EXTERNAL_FUNCTION:
		pname = "External Function"
	case plan.Node_MATERIAL:
		pname = "Material"
	case plan.Node_RECURSIVE_CTE:
		pname = "Recursive etc"
	case plan.Node_SINK:
		pname = "Sink"
	case plan.Node_SINK_SCAN:
		pname = "Sink Scan"
	case plan.Node_AGG:
		pname = "Aggregate"
	case plan.Node_JOIN:
		pname = "Join"
	case plan.Node_SAMPLE:
		pname = "Sample"
	case plan.Node_SORT:
		pname = "Sort"
	case plan.Node_UNION:
		pname = "Union"
	case plan.Node_UNION_ALL:
		pname = "Union All"
	case plan.Node_UNIQUE:
		pname = "Unique"
	case plan.Node_WINDOW:
		pname = "Window"
	case plan.Node_BROADCAST:
		pname = "Broadcast"
	case plan.Node_SPLIT:
		pname = "Split"
	case plan.Node_GATHER:
		pname = "Gather"
	case plan.Node_ASSERT:
		pname = "Assert"
	default:
		pname = "???"
	}

	if options.Format == EXPLAIN_FORMAT_TEXT {
		result += pname

		switch ndesc.Node.NodeType {
		case plan.Node_VALUE_SCAN:
			fallthrough
		case plan.Node_TABLE_SCAN:
			fallthrough
		case plan.Node_FUNCTION_SCAN:
			fallthrough
		case plan.Node_EXTERNAL_SCAN:
			result += " on "
			if ndesc.Node.ObjRef != nil {
				objRefImpl := NewObjRefDescribeImpl(ndesc.Node.ObjRef)
				result += " " + objRefImpl.GetDescription(options)
			} else if ndesc.Node.TableDef != nil {
				tableDefImpl := NewTableDefDescribeImpl(ndesc.Node.TableDef)
				result += " " + tableDefImpl.GetDescription(options)
			}
		case plan.Node_PROJECT:
			pname = "Project"
		case plan.Node_EXTERNAL_FUNCTION:
			pname = "External Function"
		case plan.Node_MATERIAL:
			pname = "Material"
		case plan.Node_RECURSIVE_CTE:
			pname = "Recursive etc"
		case plan.Node_SINK:
			pname = "Sink"
		case plan.Node_SINK_SCAN:
			pname = "Sink Scan"
		case plan.Node_AGG:
			pname = "Aggregate"
		case plan.Node_JOIN:
			pname = "Join"
		case plan.Node_SAMPLE:
			pname = "Sample"
		case plan.Node_SORT:
			pname = "Sort"
		case plan.Node_UNION:
			pname = "Union"
		case plan.Node_UNION_ALL:
			pname = "Union All"
		case plan.Node_UNIQUE:
			pname = "Unique"
		case plan.Node_WINDOW:
			pname = "Window"
		case plan.Node_BROADCAST:
			pname = "Broadcast"
		case plan.Node_SPLIT:
			pname = "Split"
		case plan.Node_GATHER:
			pname = "Gather"
		case plan.Node_ASSERT:
			pname = "Assert"
		case plan.Node_UNKNOWN:
			pname = "UnKnow Node"
		default:
			pname = "???"
		}
	}

	if options.Format == EXPLAIN_FORMAT_TEXT {
		result += "  (cost=%.2f..%.2f rows=%.0f width=%f)"
		//result += fmt.Sprintf("  (cost=%.2f..%.2f rows=%.0f width=%f)",
		//	ndesc.Node.Cost.Start,
		//	ndesc.Node.Cost.Total,
		//	ndesc.Node.Cost.Rowsize,
		//	ndesc.Node.Cost.Card)
	} else {
		//TODO implement me
	}
	return result
	//TODO implement me
	//panic("implement me")
}

func (ndesc NodeDescribeImpl) GetExtraInfo(options *ExplainOptions) []string {
	lines := make([]string, 0)
	if ndesc.Node.OrderBy != nil {
		orderByInfo := ndesc.GetOrderByInfo(options)
		lines = append(lines, orderByInfo)
	}

	if ndesc.Node.OnList != nil {
		joinOnInfo := ndesc.GetJoinConditionInfo(options)
		lines = append(lines, joinOnInfo)
	}

	if ndesc.Node.GroupBy != nil {
		groupByInfo := ndesc.GetGroupByInfo(options)
		lines = append(lines, groupByInfo)
	}

	if ndesc.Node.WhereList != nil {
		filterInfo := ndesc.GetWhereConditionInfo(options)
		lines = append(lines, filterInfo)
	}

	if ndesc.Node.Limit != nil {
		var temp string
		limitInfo := DescribeExpr(ndesc.Node.Limit)
		temp += "Limit: " + limitInfo
		if ndesc.Node.Offset != nil {
			offsetInfo := DescribeExpr(ndesc.Node.Offset)
			temp += ", Offset: " + offsetInfo
		}
		lines = append(lines, temp)
	}

	return lines
}

func (ndesc NodeDescribeImpl) GetProjectListInfo(options *ExplainOptions) string {
	var result string = "Output: "
	exprs := NewExprListDescribeImpl(ndesc.Node.ProjectList)
	result += exprs.GetDescription(options)
	return result
}

func (ndesc NodeDescribeImpl) GetJoinConditionInfo(options *ExplainOptions) string {
	//TODO implement me
	panic("implement me")
}

func (ndesc NodeDescribeImpl) GetWhereConditionInfo(options *ExplainOptions) string {
	var result string = "Filter: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.WhereList {
			if !first {
				result += " AND "
			}
			first = false
			result += DescribeExpr(v)
		}
	} else {
		panic("implement me")
	}
	return result
}

func (ndesc NodeDescribeImpl) GetGroupByInfo(options *ExplainOptions) string {
	var result string = "Group Key: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.GetGroupBy() {
			if !first {
				result += ", "
			}
			first = false
			result += DescribeExpr(v)
		}
	} else {
		panic("implement me")
	}
	return result
}

func (ndesc NodeDescribeImpl) GetOrderByInfo(options *ExplainOptions) string {
	var result string = "Sort Key: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.GetOrderBy() {
			if !first {
				result += ", "
			}
			first = false
			orderByDescImpl := NewOrderByDescribeImpl(v)
			result += orderByDescImpl.GetDescription(options)
		}
	} else {
		panic("implement me")
	}
	return result
}

//------------------------------------------------------------------------------------------
type NodeElemDescribe interface {
	GetDescription(options *ExplainOptions) string
}

var _ NodeElemDescribe = &CostDescribeImpl{}
var _ NodeElemDescribe = &ExprListDescribeImpl{}
var _ NodeElemDescribe = &OrderByDescribeImpl{}
var _ NodeElemDescribe = &WinSpecDescribeImpl{}
var _ NodeElemDescribe = &TableDefDescribeImpl{}
var _ NodeElemDescribe = &ObjRefDescribeImpl{}
var _ NodeElemDescribe = &RowsetDataDescribeImpl{}

type CostDescribeImpl struct {
	Cost *Cost
}

func (c *CostDescribeImpl) GetDescription(options *ExplainOptions) string {
	//TODO implement me
	panic("implement me")
}

type ExprListDescribeImpl struct {
	ExprList []*plan.Expr // ProjectList,OnList,WhereList,GroupBy,GroupingSet and so on
}

func NewExprListDescribeImpl(ExprList []*plan.Expr) *ExprListDescribeImpl {
	return &ExprListDescribeImpl{
		ExprList: ExprList,
	}
}

func (e *ExprListDescribeImpl) GetDescription(options *ExplainOptions) string {
	var first bool = true
	var result string = " "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		for _, v := range e.ExprList {
			if !first {
				result += ", "
			}
			first = false
			result += DescribeExpr(v)
		}
	} else {
		panic("implement me")
	}
	return result
}

type OrderByDescribeImpl struct {
	OrderBy *plan.OrderBySpec
}

func NewOrderByDescribeImpl(OrderBy *plan.OrderBySpec) *OrderByDescribeImpl {
	return &OrderByDescribeImpl{
		OrderBy: OrderBy,
	}
}

func (o *OrderByDescribeImpl) GetDescription(options *ExplainOptions) string {
	var result string = " "
	result += DescribeExpr(o.OrderBy.GetOrderBy())

	flagKey := int32(o.OrderBy.GetOrderByFlags())
	orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
	result += " " + orderbyFlag
	return result
}

type WinSpecDescribeImpl struct {
	WinSpec *plan.WindowSpec
}

func (w *WinSpecDescribeImpl) GetDescription(options *ExplainOptions) string {
	//TODO implement me
	panic("implement me")
}

type TableDefDescribeImpl struct {
	TableDef *plan.TableDef
}

func NewTableDefDescribeImpl(TableDef *plan.TableDef) *TableDefDescribeImpl {
	return &TableDefDescribeImpl{
		TableDef: TableDef,
	}
}
func (t *TableDefDescribeImpl) GetDescription(options *ExplainOptions) string {
	if t.TableDef != nil {
		return t.TableDef.GetName()
	}
	return ""
}

type ObjRefDescribeImpl struct {
	ObjRef *plan.ObjectRef
}

func NewObjRefDescribeImpl(ObjRef *plan.ObjectRef) *ObjRefDescribeImpl {
	return &ObjRefDescribeImpl{
		ObjRef: ObjRef,
	}
}

func (o *ObjRefDescribeImpl) GetDescription(options *ExplainOptions) string {
	if o.ObjRef != nil {
		return o.ObjRef.DbName + "." + o.ObjRef.GetObjName()
	}
	return ""
}

type RowsetDataDescribeImpl struct {
	RowsetData *RowsetData
}

func (r *RowsetDataDescribeImpl) GetDescription(options *ExplainOptions) string {
	//TODO implement me
	panic("implement me")
}
