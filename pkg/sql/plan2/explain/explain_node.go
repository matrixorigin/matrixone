// Copyright 2021 - 2022 Matrix Origin
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

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

var _ NodeDescribe = &NodeDescribeImpl{}

type NodeDescribeImpl struct {
	Node *plan.Node
}

func (ndesc *NodeDescribeImpl) GetTableDefine(options *ExplainOptions) (string, error) {
	var result string = "Table Define:"
	if ndesc.Node.NodeType == plan.Node_TABLE_SCAN {
		tableDef := ndesc.Node.TableDef
		result += "TABLE '" + tableDef.Name + "'("
		var first bool = true
		for i, col := range tableDef.Cols {
			if !first {
				result += ", "
			}
			first = false
			//result += "'" + col.Name + "':" + col.Typ.Id.String()
			if col.IsPrune {
				result += "'" + col.Name + "'#[0," + strconv.Itoa(i) + "](*)"
			} else {
				result += "'" + col.Name + "'#[0," + strconv.Itoa(i) + "]"
			}
		}
		result += ")"
	} else {
		panic("implement me")
	}
	return result, nil
}

func NewNodeDescriptionImpl(node *plan.Node) *NodeDescribeImpl {
	return &NodeDescribeImpl{
		Node: node,
	}
}

func (ndesc *NodeDescribeImpl) GetNodeBasicInfo(options *ExplainOptions) (string, error) {
	var result string
	var pname string /* node type name for text output */

	// Get the Node Name
	switch ndesc.Node.NodeType {
	case plan.Node_UNKNOWN:
		pname = "UnKnow Node"
	case plan.Node_VALUE_SCAN:
		pname = "Values Scan"
	case plan.Node_TABLE_SCAN:
		pname = "Table Scan"
	case plan.Node_FUNCTION_SCAN:
		pname = "Function Scan"
	case plan.Node_EXTERNAL_SCAN:
		pname = "External Scan"
	case plan.Node_MATERIAL_SCAN:
		pname = "Material Scan"
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
	case plan.Node_DISTINCT:
		pname = "Distinct"
	case plan.Node_FILTER:
		pname = "Filter"
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
	case plan.Node_INSERT:
		pname = "Insert"
	case plan.Node_UPDATE:
		pname = "Update"
	case plan.Node_DELETE:
		pname = "Delete"
	default:
		panic("error node type")
	}

	// Get Node's operator object info ,such as table, view
	if options.Format == EXPLAIN_FORMAT_TEXT {
		result += pname
		switch ndesc.Node.NodeType {
		case plan.Node_VALUE_SCAN:
			result += " \"*VALUES*\" "
		case plan.Node_TABLE_SCAN:
			fallthrough
		case plan.Node_FUNCTION_SCAN:
			fallthrough
		case plan.Node_EXTERNAL_SCAN:
			fallthrough
		case plan.Node_MATERIAL_SCAN:
			fallthrough
		case plan.Node_INSERT:
			fallthrough
		case plan.Node_UPDATE:
			fallthrough
		case plan.Node_DELETE:
			result += " on "
			if ndesc.Node.ObjRef != nil {
				result += ndesc.Node.ObjRef.GetSchemaName() + "." + ndesc.Node.ObjRef.GetObjName()
			} else if ndesc.Node.TableDef != nil {
				result += ndesc.Node.TableDef.GetName()
			}
		case plan.Node_PROJECT:
			fallthrough
		case plan.Node_EXTERNAL_FUNCTION:
			fallthrough
		case plan.Node_MATERIAL:
			fallthrough
		case plan.Node_RECURSIVE_CTE:
			fallthrough
		case plan.Node_SINK:
			fallthrough
		case plan.Node_SINK_SCAN:
			fallthrough
		case plan.Node_AGG:
			fallthrough
		case plan.Node_DISTINCT:
			fallthrough
		case plan.Node_FILTER:
			fallthrough
		case plan.Node_JOIN:
			fallthrough
		case plan.Node_SAMPLE:
			fallthrough
		case plan.Node_SORT:
			fallthrough
		case plan.Node_UNION:
			fallthrough
		case plan.Node_UNION_ALL:
			fallthrough
		case plan.Node_UNIQUE:
			fallthrough
		case plan.Node_WINDOW:
			fallthrough
		case plan.Node_BROADCAST:
			fallthrough
		case plan.Node_SPLIT:
			fallthrough
		case plan.Node_GATHER:
			fallthrough
		case plan.Node_ASSERT:
			fallthrough
		case plan.Node_UNKNOWN:
			fallthrough
		default:

		}
	}

	// Get Costs info of Node
	if options.Format == EXPLAIN_FORMAT_TEXT {
		costDescImpl := &CostDescribeImpl{
			Cost: ndesc.Node.GetCost(),
		}
		costInfo, err := costDescImpl.GetDescription(options)
		if err != nil {
			return result, err
		}
		result += costInfo

		//result += " (cost=%.2f..%.2f rows=%.0f width=%f)"
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetExtraInfo(options *ExplainOptions) ([]string, error) {
	lines := make([]string, 0)
	// Get Sort list info
	if ndesc.Node.OrderBy != nil {
		orderByInfo, err := ndesc.GetOrderByInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, orderByInfo)
	}

	// Get Join type info
	if ndesc.Node.NodeType == plan.Node_JOIN {
		joinTypeInfo, err := ndesc.GetJoinTypeInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, joinTypeInfo)
	}

	// Get Join Condition info
	if ndesc.Node.OnList != nil {
		joinOnInfo, err := ndesc.GetJoinConditionInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, joinOnInfo)
	}

	// Get Group key info
	if ndesc.Node.GroupBy != nil {
		groupByInfo, err := ndesc.GetGroupByInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, groupByInfo)
	}

	// Get Aggregate function info
	if ndesc.Node.AggList != nil {
		aggListInfo, err := ndesc.GetAggregationInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, aggListInfo)
	}

	// Get Filter list info
	if ndesc.Node.FilterList != nil && len(ndesc.Node.FilterList) != 0 {
		filterInfo, err := ndesc.GetFilterConditionInfo(options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, filterInfo)
	}

	// Get Limit And Offset info
	if ndesc.Node.Limit != nil {
		var temp string
		limitInfo, err := describeExpr(ndesc.Node.Limit, options)
		if err != nil {
			return nil, err
		}
		temp += "Limit: " + limitInfo
		if ndesc.Node.Offset != nil {
			offsetInfo, err := describeExpr(ndesc.Node.Offset, options)
			if err != nil {
				return nil, err
			}
			temp += ", Offset: " + offsetInfo
		}
		lines = append(lines, temp)
	}

	//if ndesc.Node.UpdateList != nil {
	//	updateListDesc := &UpdateListDescribeImpl{
	//		UpdateList: ndesc.Node.UpdateList,
	//	}
	//	updatedesc, err := updateListDesc.GetDescription(options)
	//	if err != nil {
	//		return nil, err
	//	}
	//	lines = append(lines, "Set columns with("+updatedesc+")")
	//}
	return lines, nil
}

func (ndesc *NodeDescribeImpl) GetProjectListInfo(options *ExplainOptions) (string, error) {
	var result string = "Output:"
	exprs := NewExprListDescribeImpl(ndesc.Node.ProjectList)
	describe, err := exprs.GetDescription(options)
	if err != nil {
		return result, err
	}
	result += describe
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetJoinTypeInfo(options *ExplainOptions) (string, error) {
	var result string = "Join Type: " + ndesc.Node.JoinType.String()
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetJoinConditionInfo(options *ExplainOptions) (string, error) {
	var result string = "Join Cond:"
	exprs := NewExprListDescribeImpl(ndesc.Node.OnList)
	describe, err := exprs.GetDescription(options)
	if err != nil {
		return result, err
	}
	result += describe
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetFilterConditionInfo(options *ExplainOptions) (string, error) {
	var result string = "Filter Cond: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.FilterList {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetGroupByInfo(options *ExplainOptions) (string, error) {
	var result string = "Group Key:"
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.GetGroupBy() {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetAggregationInfo(options *ExplainOptions) (string, error) {
	var result string = "Aggregate Functions: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.GetAggList() {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetOrderByInfo(options *ExplainOptions) (string, error) {
	var result string = "Sort Key:"
	if options.Format == EXPLAIN_FORMAT_TEXT {
		var first bool = true
		for _, v := range ndesc.Node.GetOrderBy() {
			if !first {
				result += ", "
			}
			first = false
			orderByDescImpl := NewOrderByDescribeImpl(v)
			describe, err := orderByDescImpl.GetDescription(options)
			if err != nil {
				return result, err
			}
			result += describe
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

var _ NodeElemDescribe = &CostDescribeImpl{}
var _ NodeElemDescribe = &ExprListDescribeImpl{}
var _ NodeElemDescribe = &OrderByDescribeImpl{}
var _ NodeElemDescribe = &WinSpecDescribeImpl{}
var _ NodeElemDescribe = &RowsetDataDescribeImpl{}
var _ NodeElemDescribe = &UpdateListDescribeImpl{}

type CostDescribeImpl struct {
	Cost *plan.Cost
}

func (c *CostDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	//(cost=11.75..13.15 rows=140 width=4)
	var result string
	if c.Cost == nil {
		result = " (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)"
		//result = " (cost=%.2f..%.2f rows=%.2f ndv=%.2f rowsize=%.f)"
	} else {
		result = "(cost=" +
			strconv.FormatFloat(c.Cost.Start, 'f', 2, 64) +
			".." + strconv.FormatFloat(c.Cost.Total, 'f', 2, 64) +
			" card=" + strconv.FormatFloat(c.Cost.Card, 'f', 2, 64) +
			" ndv=" + strconv.FormatFloat(c.Cost.Ndv, 'f', 2, 64) +
			" rowsize=" + strconv.FormatFloat(c.Cost.Rowsize, 'f', 0, 64)
	}

	return result, nil
}

type ExprListDescribeImpl struct {
	ExprList []*plan.Expr // ProjectList,OnList,FilterList,GroupBy,GroupingSet and so on
}

func NewExprListDescribeImpl(ExprList []*plan.Expr) *ExprListDescribeImpl {
	return &ExprListDescribeImpl{
		ExprList: ExprList,
	}
}

func (e *ExprListDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	var first bool = true
	var result string = " "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		for _, v := range e.ExprList {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, errors.New(errno.FeatureNotSupported, "unimplement explain format dot")
	}
	return result, nil
}

type OrderByDescribeImpl struct {
	OrderBy *plan.OrderBySpec
}

func NewOrderByDescribeImpl(OrderBy *plan.OrderBySpec) *OrderByDescribeImpl {
	return &OrderByDescribeImpl{
		OrderBy: OrderBy,
	}
}

func (o *OrderByDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	var result string = " "
	descExpr, err := describeExpr(o.OrderBy.Expr, options)
	if err != nil {
		return result, err
	}
	result += descExpr

	flagKey := int32(o.OrderBy.Flag)
	orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
	result += " " + orderbyFlag
	return result, nil
}

type WinSpecDescribeImpl struct {
	WinSpec *plan.WindowSpec
}

func (w *WinSpecDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	// TODO implement me
	panic("implement me")
}

type RowsetDataDescribeImpl struct {
	RowsetData *plan.RowsetData
}

func (r *RowsetDataDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	var result string
	if r.RowsetData == nil {
		return result, nil
	}

	var first bool = true
	for index := range r.RowsetData.Cols {
		if !first {
			result += ", "
		}
		first = false
		result += "\"*VALUES*\".column" + strconv.Itoa(index+1)
	}
	return result, nil
}

type UpdateListDescribeImpl struct {
	//UpdateList *plan.UpdateList
}

func (u *UpdateListDescribeImpl) GetDescription(options *ExplainOptions) (string, error) {
	// u.UpdateList.Columns
	var result string
	//var first bool = true
	//if len(u.UpdateList.Columns) != len(u.UpdateList.Values) {
	//	panic("update node number of columns and values is not equal")
	//}
	//for i, columnExpr := range u.UpdateList.Columns {
	//	if !first {
	//		result += ", "
	//	}
	//	colstr, err := describeExpr(columnExpr, options)
	//	if err != nil {
	//		return result, err
	//	}
	//	valstr, err := describeExpr(u.UpdateList.Values[i], options)
	//	if err != nil {
	//		return result, err
	//	}
	//	result += colstr + " = " + valstr
	//	first = false
	//}
	return result, nil
}
