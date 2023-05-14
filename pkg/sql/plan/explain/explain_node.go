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
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"strconv"
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
	buf := bytes.NewBuffer(make([]byte, 0, 400))
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
		buf.WriteString(pname)
		switch ndesc.Node.NodeType {
		case plan.Node_VALUE_SCAN:
			buf.WriteString(" \"*VALUES*\" ")
		case plan.Node_TABLE_SCAN, plan.Node_FUNCTION_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_INSERT:
			buf.WriteString(" on ")
			if ndesc.Node.ObjRef != nil {
				buf.WriteString(ndesc.Node.ObjRef.GetSchemaName() + "." + ndesc.Node.ObjRef.GetObjName())
			} else if ndesc.Node.TableDef != nil {
				buf.WriteString(ndesc.Node.TableDef.GetName())
			}
		case plan.Node_UPDATE:
			buf.WriteString(" on ")
			if ndesc.Node.UpdateCtx != nil {
				first := true
				for _, ctx := range ndesc.Node.UpdateCtx.Ref {
					if !first {
						buf.WriteString(", ")
					}
					buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
					if first {
						first = false
					}
				}
			}
		case plan.Node_DELETE:
			buf.WriteString(" on ")
			if ndesc.Node.DeleteCtx != nil {
				first := true
				for _, ctx := range ndesc.Node.DeleteCtx.Ref {
					if !first {
						buf.WriteString(", ")
					}
					buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
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
			err := costDescImpl.GetDescription(ctx, options, buf)
			if err != nil {
				return "", err
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetActualAnalyzeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	buf.WriteString("Analyze: ")
	if ndesc.Node.AnalyzeInfo != nil {
		impl := NewAnalyzeInfoDescribeImpl(ndesc.Node.AnalyzeInfo)
		err := impl.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	} else {
		buf.WriteString("timeConsumed=0ns waitTime=0ns inputRows=0  outputRows=0 inputSize=0 bytes outputSize:0 bytes, memorySize=0 bytes")
	}
	return buf.String(), nil
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
		buf := bytes.NewBuffer(make([]byte, 0, 160))
		buf.WriteString("Limit: ")
		err := describeExpr(ctx, ndesc.Node.Limit, options, buf)
		if err != nil {
			return nil, err
		}
		if ndesc.Node.Offset != nil {
			buf.WriteString(", Offset: ")
			err := describeExpr(ctx, ndesc.Node.Offset, options, buf)
			if err != nil {
				return nil, err
			}
		}
		lines = append(lines, buf.String())
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
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	buf.WriteString("Output: ")
	exprs := NewExprListDescribeImpl(ndesc.Node.ProjectList)
	err := exprs.GetDescription(ctx, options, buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetJoinTypeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	result := "Join Type: " + ndesc.Node.JoinType.String()
	return result, nil
}

func (ndesc *NodeDescribeImpl) GetJoinConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Join Cond: ")
	exprs := NewExprListDescribeImpl(ndesc.Node.OnList)
	err := exprs.GetDescription(ctx, options, buf)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetFilterConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Filter Cond: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.FilterList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v, options, buf)
			if err != nil {
				return "", err
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetGroupByInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Group Key: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.GetGroupBy() {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v, options, buf)
			if err != nil {
				return "", err
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}

	if plan2.NeedShuffle(ndesc.Node) {
		buf.WriteString(" shuffle: true ")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetAggregationInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Aggregate Functions: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.GetAggList() {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v, options, buf)
			if err != nil {
				return "", err
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetOrderByInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	if options.Format == EXPLAIN_FORMAT_TEXT {
		buf.WriteString("Sort Key: ")
		orderByDescImpl := NewOrderByDescribeImpl(ndesc.Node.OrderBy)
		err := orderByDescImpl.GetDescription(ctx, options, buf)
		if err != nil {
			return "", err
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
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

func (a AnalyzeInfoDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	fmt.Fprintf(buf, "timeConsumed=%dms", a.AnalyzeInfo.TimeConsumed/1000000)
	fmt.Fprintf(buf, " waitTime=%dms", a.AnalyzeInfo.WaitTimeConsumed/1000000)
	fmt.Fprintf(buf, " inputRows=%d", a.AnalyzeInfo.InputRows)
	fmt.Fprintf(buf, " outputRows=%d", a.AnalyzeInfo.OutputRows)
	fmt.Fprintf(buf, " inputSize=%dbytes", a.AnalyzeInfo.InputSize)
	fmt.Fprintf(buf, " outputSize=%dbytes", a.AnalyzeInfo.OutputSize)
	fmt.Fprintf(buf, " memorySize=%dbytes", a.AnalyzeInfo.MemorySize)

	return nil
}

type CostDescribeImpl struct {
	Stats *plan.Stats
}

func (c *CostDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	//var result string
	if c.Stats == nil {
		buf.WriteString(" (cost=0)")
	} else {
		var blockNumStr, hashmapSizeStr string
		if c.Stats.BlockNum > 0 {
			blockNumStr = " blockNum=" + strconv.FormatInt(int64(c.Stats.BlockNum), 10)
		}
		if c.Stats.HashmapSize > 0 {
			hashmapSizeStr = " hashmapSize=" + strconv.FormatFloat(c.Stats.HashmapSize, 'f', 2, 64)
		}
		buf.WriteString(" (cost=" + strconv.FormatFloat(c.Stats.Cost, 'f', 2, 64) +
			" outcnt=" + strconv.FormatFloat(c.Stats.Outcnt, 'f', 2, 64) +
			" selectivity=" + strconv.FormatFloat(c.Stats.Selectivity, 'f', 4, 64) +
			blockNumStr + hashmapSizeStr + ")")
	}
	return nil
}

type ExprListDescribeImpl struct {
	ExprList []*plan.Expr // ProjectList,OnList,FilterList,GroupBy,GroupingSet and so on
}

func NewExprListDescribeImpl(ExprList []*plan.Expr) *ExprListDescribeImpl {
	return &ExprListDescribeImpl{
		ExprList: ExprList,
	}
}

func (e *ExprListDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	first := true
	if options.Format == EXPLAIN_FORMAT_TEXT {
		for _, v := range e.ExprList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v, options, buf)
			if err != nil {
				return err
				//return result, err
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return moerr.NewNYI(ctx, "explain format dot")
	}
	return nil
}

type OrderByDescribeImpl struct {
	OrderBy []*plan.OrderBySpec
}

func NewOrderByDescribeImpl(OrderBy []*plan.OrderBySpec) *OrderByDescribeImpl {
	return &OrderByDescribeImpl{
		OrderBy: OrderBy,
	}
}

func (o *OrderByDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	//var result string
	//buf := bytes.NewBuffer(make([]byte, 0, 120))
	if options.Format == EXPLAIN_FORMAT_TEXT || options.Format == EXPLAIN_FORMAT_JSON {
		first := true
		for _, v := range o.OrderBy {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v.Expr, options, buf)
			if err != nil {
				return err
			}

			flagKey := int32(v.Flag)
			orderbyFlag := plan.OrderBySpec_OrderByFlag_name[flagKey]
			buf.WriteString(" " + orderbyFlag)
		}
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return moerr.NewNYI(ctx, "explain format dot")
	}
	return nil
}

type WinSpecDescribeImpl struct {
	WinSpec *plan.WindowSpec
}

func (w *WinSpecDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	// TODO implement me
	panic("implement me")
}

type RowsetDataDescribeImpl struct {
	RowsetData *plan.RowsetData
}

func (r *RowsetDataDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	buf.WriteString("Value:")
	if r.RowsetData == nil {
		return nil
	}

	first := true
	for index := range r.RowsetData.Cols {
		if !first {
			buf.WriteString(", ")
		}
		first = false
		buf.WriteString("\"*VALUES*\".column" + strconv.Itoa(index+1))
	}
	return nil
}

type UpdateCtxsDescribeImpl struct {
	UpdateCtx *plan.UpdateCtx
}

func (u *UpdateCtxsDescribeImpl) GetDescription(ctx context.Context, options *ExplainOptions, buf *bytes.Buffer) error {
	buf.WriteString("Update Columns: ")
	first := true
	for i, ctx := range u.UpdateCtx.Ref {
		if u.UpdateCtx.UpdateCol[i] != nil {
			for colName := range u.UpdateCtx.UpdateCol[i].Map {
				if !first {
					buf.WriteString(", ")
				} else {
					first = false
				}
				buf.WriteString(ctx.SchemaName + "." + ctx.ObjName + "." + colName)
			}
		}
	}
	return nil
}
