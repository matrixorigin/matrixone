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
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ NodeDescribe = &NodeDescribeImpl{}

type NodeDescribeImpl struct {
	Node *plan.Node
}

func NewNodeDescriptionImpl(node *plan.Node) *NodeDescribeImpl {
	return &NodeDescribeImpl{
		Node: node,
	}
}

const TableScan = "Table Scan"
const ExternalScan = "External Scan"

func (ndesc *NodeDescribeImpl) GetNodeBasicInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	var result string
	var pname string /* node type name for text output */

	// Get the Node Name
	switch ndesc.Node.NodeType {
	case plan.Node_UNKNOWN:
		pname = "UnKnow Node"
	case plan.Node_VALUE_SCAN:
		pname = "Values Scan"
	case plan.Node_TABLE_SCAN:
		pname = TableScan
	case plan.Node_EXTERNAL_SCAN:
		pname = ExternalScan
	case plan.Node_MATERIAL_SCAN:
		pname = "Material Scan"
	case plan.Node_PROJECT:
		pname = "Project"
	case plan.Node_EXTERNAL_FUNCTION:
		pname = "External Function"
	case plan.Node_MATERIAL:
		pname = "Material"
	case plan.Node_RECURSIVE_CTE:
		pname = "Recursive CTE"
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
	case plan.Node_INTERSECT:
		pname = "Intersect"
	case plan.Node_INTERSECT_ALL:
		pname = "Intersect All"
	case plan.Node_MINUS:
		pname = "Minus"
	case plan.Node_MINUS_ALL:
		pname = "Minus All"
	case plan.Node_FUNCTION_SCAN:
		pname = ndesc.Node.TableDef.TblFunc.Name
	default:
		panic("error node type")
	}

	// Get Node's operator object info ,such as table, view
	if options.Format == EXPLAIN_FORMAT_TEXT {
		result += pname
		switch ndesc.Node.NodeType {
		case plan.Node_VALUE_SCAN:
			result += " \"*VALUES*\" "
		case plan.Node_TABLE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_INSERT:
			result += " on "
			if ndesc.Node.ObjRef != nil {
				result += ndesc.Node.ObjRef.GetSchemaName() + "." + ndesc.Node.ObjRef.GetObjName()
			} else if ndesc.Node.TableDef != nil {
				result += ndesc.Node.TableDef.GetName()
			}
		case plan.Node_UPDATE:
			result += " on "
			if ndesc.Node.UpdateCtxs != nil {
				first := true
				for _, ctx := range ndesc.Node.UpdateCtxs {
					if !first {
						result += ", "
					}
					result += ctx.DbName + "." + ctx.TblName
					if first {
						first = false
					}
				}
			}
		case plan.Node_DELETE:
			result += " on "
			if ndesc.Node.DeleteTablesCtx != nil {
				first := true
				for _, ctx := range ndesc.Node.DeleteTablesCtx {
					if !first {
						result += ", "
					}
					result += ctx.DbName + "." + ctx.TblName
					if first {
						first = false
					}
				}
			}
		}
	}

	// Get Costs info of Node
	if options.Format == EXPLAIN_FORMAT_TEXT {
		//result += " (cost=%.2f..%.2f rows=%.0f width=%f)"

		if options.Verbose {
			costDescImpl := &CostDescribeImpl{
				Stats: ndesc.Node.GetStats(),
			}
			costInfo, err := costDescImpl.GetDescription(ctx, options)
			if err != nil {
				return result, err
			}
			result += " " + costInfo
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetActualAnalyzeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Analyze: "
	if ndesc.Node.AnalyzeInfo != nil {
		impl := NewAnalyzeInfoDescribeImpl(ndesc.Node.AnalyzeInfo)
		describe, err := impl.GetDescription(ctx, options)
		if err != nil {
			return result, err
		}
		result += describe
	} else {
		result += "timeConsumed=0ms  inputRows=0  outputRows=0 inputSize=0 bytes outputSize:0 bytes, memorySize=0 bytes"
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetTableDef(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Table: "
	if ndesc.Node.NodeType == plan.Node_TABLE_SCAN {
		tableDef := ndesc.Node.TableDef
		result += "'" + tableDef.Name + "' ("
		first := true
		for i, col := range tableDef.Cols {
			if !first {
				result += ", "
			}
			first = false
			//result += "'" + col.Name + "':" + col.Typ.Id.String()
			result += strconv.Itoa(i) + ":'" + col.Name + "'"
		}
		result += ")"
	} else {
		panic("implement me")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetExtraInfo(ctx context.Context, options *ExplainOptions) ([]string, error) {
	lines := make([]string, 0)
	// Get Sort list info
	if len(ndesc.Node.OrderBy) > 0 {
		orderByInfo, err := ndesc.GetOrderByInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, orderByInfo)
	}

	// Get Join type info
	if ndesc.Node.NodeType == plan.Node_JOIN {
		joinTypeInfo, err := ndesc.GetJoinTypeInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, joinTypeInfo)
	}

	// Get Join Condition info
	if len(ndesc.Node.OnList) > 0 {
		joinOnInfo, err := ndesc.GetJoinConditionInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, joinOnInfo)
	}

	// Get Group key info
	if len(ndesc.Node.GroupBy) > 0 {
		groupByInfo, err := ndesc.GetGroupByInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, groupByInfo)
	}

	// Get Aggregate function info
	if len(ndesc.Node.AggList) > 0 {
		aggListInfo, err := ndesc.GetAggregationInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, aggListInfo)
	}

	// Get Filter list info
	if len(ndesc.Node.FilterList) > 0 {
		filterInfo, err := ndesc.GetFilterConditionInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, filterInfo)
	}

	// Get Limit And Offset info
	if ndesc.Node.Limit != nil {
		var temp string
		limitInfo, err := describeExpr(ctx, ndesc.Node.Limit, options)
		if err != nil {
			return nil, err
		}
		temp += "Limit: " + limitInfo
		if ndesc.Node.Offset != nil {
			offsetInfo, err := describeExpr(ctx, ndesc.Node.Offset, options)
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

func (ndesc *NodeDescribeImpl) GetProjectListInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Output: "
	exprs := NewExprListDescribeImpl(ndesc.Node.ProjectList)
	describe, err := exprs.GetDescription(ctx, options)
	if err != nil {
		return result, err
	}
	result += describe
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetJoinTypeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Join Type: " + ndesc.Node.JoinType.String()
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetJoinConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Join Cond: "
	exprs := NewExprListDescribeImpl(ndesc.Node.OnList)
	describe, err := exprs.GetDescription(ctx, options)
	if err != nil {
		return result, err
	}
	result += describe
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetFilterConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Filter Cond: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.FilterList {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(ctx, v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetGroupByInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Group Key: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.GetGroupBy() {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(ctx, v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetAggregationInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Aggregate Functions: "
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.GetAggList() {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(ctx, v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetOrderByInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	var result string
	if options.Format == EXPLAIN_FORMAT_TEXT {
		result = "Sort Key: "
		orderByDescImpl := NewOrderByDescribeImpl(ndesc.Node.OrderBy)
		describe, err := orderByDescImpl.GetDescription(ctx, options)
		if err != nil {
			return result, err
		}
		result += describe
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

var _ NodeElemDescribe = &CostDescribeImpl{}
var _ NodeElemDescribe = &ExprListDescribeImpl{}
var _ NodeElemDescribe = &OrderByDescribeImpl{}
var _ NodeElemDescribe = &WinSpecDescribeImpl{}
var _ NodeElemDescribe = &RowsetDataDescribeImpl{}
var _ NodeElemDescribe = &UpdateCtxsDescribeImpl{}
var _ NodeElemDescribe = &AnalyzeInfoDescribeImpl{}

type AnalyzeInfoDescribeImpl struct {
	AnalyzeInfo *plan.AnalyzeInfo
}

func NewAnalyzeInfoDescribeImpl(analyze *plan.AnalyzeInfo) *AnalyzeInfoDescribeImpl {
	return &AnalyzeInfoDescribeImpl{
		AnalyzeInfo: analyze,
	}
}

func (a AnalyzeInfoDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "timeConsumed=" + strconv.FormatInt(a.AnalyzeInfo.TimeConsumed, 10) + "us" +
		" inputRows=" + strconv.FormatInt(a.AnalyzeInfo.InputRows, 10) +
		" outputRows=" + strconv.FormatInt(a.AnalyzeInfo.OutputRows, 10) +
		" inputSize=" + strconv.FormatInt(a.AnalyzeInfo.InputSize, 10) + "bytes" +
		" outputSize=" + strconv.FormatInt(a.AnalyzeInfo.OutputSize, 10) + "bytes" +
		" memorySize=" + strconv.FormatInt(a.AnalyzeInfo.MemorySize, 10) + "bytes"
	return result, nil
}

type CostDescribeImpl struct {
	Stats *plan.Stats
}

func (c *CostDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	var result string
	if c.Stats == nil {
		result = " (cost=0)"
		//result = " (cost=%.2f..%.2f rows=%.2f ndv=%.2f rowsize=%.f)"
	} else {
		var blockNumStr, hashmapSizeStr string
		if c.Stats.BlockNum > 0 {
			blockNumStr = " blockNum=" + strconv.FormatInt(int64(c.Stats.BlockNum), 10)
		}
		if c.Stats.HashmapSize > 0 {
			hashmapSizeStr = " hashmapSize=" + strconv.FormatFloat(c.Stats.HashmapSize, 'f', 2, 64)
		}

		result = " (cost=" + strconv.FormatFloat(c.Stats.Cost, 'f', 2, 64) +
			" outcnt=" + strconv.FormatFloat(c.Stats.Outcnt, 'f', 2, 64) +
			blockNumStr + hashmapSizeStr + ")"
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

func (e *ExprListDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	first := true
	var result string
	if options.Format == EXPLAIN_FORMAT_TEXT {
		for _, v := range e.ExprList {
			if !first {
				result += ", "
			}
			first = false
			descV, err := describeExpr(ctx, v, options)
			if err != nil {
				return result, err
			}
			result += descV
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return result, moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return result, moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

type OrderByDescribeImpl struct {
	OrderBy []*plan.OrderBySpec
}

func NewOrderByDescribeImpl(OrderBy []*plan.OrderBySpec) *OrderByDescribeImpl {
	return &OrderByDescribeImpl{
		OrderBy: OrderBy,
	}
}

func (o *OrderByDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	var result string
	if options.Format == EXPLAIN_FORMAT_TEXT || options.Format == EXPLAIN_FORMAT_JSON {
		first := true
		for _, v := range o.OrderBy {
			if !first {
				result += ", "
			}
			first = false
			descExpr, err := describeExpr(ctx, v.Expr, options)
			if err != nil {
				return result, err
			}
			result += descExpr

			flagKey := int32(v.Flag)
			orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
			result += " " + orderbyFlag
		}
		return result, nil
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return result, nil
}

type WinSpecDescribeImpl struct {
	WinSpec *plan.WindowSpec
}

func (w *WinSpecDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	// TODO implement me
	panic("implement me")
}

type RowsetDataDescribeImpl struct {
	RowsetData *plan.RowsetData
}

func (r *RowsetDataDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Value:"
	if r.RowsetData == nil {
		return result, nil
	}

	first := true
	for index := range r.RowsetData.Cols {
		if !first {
			result += ", "
		}
		first = false
		result += "\"*VALUES*\".column" + strconv.Itoa(index+1)
	}
	return result, nil
}

type UpdateCtxsDescribeImpl struct {
	UpdateCtxs []*plan.UpdateCtx
}

func (u *UpdateCtxsDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Update Columns: "
	first := true
	for _, ctx := range u.UpdateCtxs {
		if ctx.UpdateCols != nil {
			for _, col := range ctx.UpdateCols {
				if !first {
					result += ", "
				} else {
					first = false
				}
				result += ctx.DbName + "." + ctx.TblName + "." + col.Name
			}
		}
	}
	return result, nil
}
