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
	"sort"
	"strconv"

	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ NodeDescribe = &NodeDescribeImpl{}

const MB = 1024 * 1024
const GB = MB * 1024
const MILLION = 1000000

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
	case plan.Node_SOURCE_SCAN:
		pname = "Source Scan"
	case plan.Node_MATERIAL_SCAN:
		pname = "Material Scan"
	case plan.Node_PROJECT:
		pname = "Project"
	case plan.Node_EXTERNAL_FUNCTION:
		pname = "External Function"
	case plan.Node_MATERIAL:
		pname = "Material"
	case plan.Node_SINK:
		pname = "Sink"
	case plan.Node_SINK_SCAN:
		pname = "Sink Scan"
	case plan.Node_RECURSIVE_SCAN:
		pname = "Recursive Scan"
	case plan.Node_RECURSIVE_CTE:
		pname = "CTE Scan"
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
	case plan.Node_PARTITION:
		pname = "Partition"
	case plan.Node_UNION:
		pname = "Union"
	case plan.Node_UNION_ALL:
		pname = "Union All"
	case plan.Node_UNIQUE:
		pname = "Unique"
	case plan.Node_WINDOW:
		pname = "Window"
	case plan.Node_TIME_WINDOW:
		pname = "Time window"
	case plan.Node_FILL:
		pname = "Fill"
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
		pname = "Table Function"
	case plan.Node_PRE_INSERT:
		pname = "PreInsert"
	case plan.Node_PRE_INSERT_UK:
		pname = "PreInsert UniqueKey"
	case plan.Node_PRE_INSERT_SK:
		pname = "PreInsert SecondaryKey"
	case plan.Node_PRE_DELETE:
		pname = "PreDelete"
	case plan.Node_ON_DUPLICATE_KEY:
		pname = "On Duplicate Key"
	case plan.Node_FUZZY_FILTER:
		pname = "Fuzzy Filter for duplicate key"
	case plan.Node_LOCK_OP:
		pname = "Lock"
	default:
		panic("error node type")
	}

	// Get Node's operator object info ,such as table, view
	if options.Format == EXPLAIN_FORMAT_TEXT {
		buf.WriteString(pname)
		switch ndesc.Node.NodeType {
		case plan.Node_VALUE_SCAN:
			buf.WriteString(" \"*VALUES*\" ")
		case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_INSERT, plan.Node_SOURCE_SCAN:
			buf.WriteString(" on ")
			if ndesc.Node.ObjRef != nil {
				buf.WriteString(ndesc.Node.ObjRef.GetSchemaName() + "." + ndesc.Node.ObjRef.GetObjName())
			} else if ndesc.Node.TableDef != nil {
				buf.WriteString(ndesc.Node.TableDef.GetName())
			}
			if ndesc.Node.Stats.ForceOneCN {
				buf.WriteString(" [ForceOneCN]")
			}
		case plan.Node_FUNCTION_SCAN:
			buf.WriteString(" on ")
			if ndesc.Node.TableDef != nil && ndesc.Node.TableDef.TblFunc != nil {
				buf.WriteString(ndesc.Node.TableDef.TblFunc.Name)
			}
		case plan.Node_DELETE:
			buf.WriteString(" on ")
			if ndesc.Node.DeleteCtx != nil {
				ctx := ndesc.Node.DeleteCtx.Ref
				buf.WriteString(ctx.SchemaName + "." + ctx.ObjName)
			}
		case plan.Node_PRE_INSERT:
			buf.WriteString(" on ")
			if ndesc.Node.PreInsertCtx != nil {
				if ndesc.Node.PreInsertCtx.Ref != nil {
					buf.WriteString(ndesc.Node.PreInsertCtx.Ref.GetSchemaName() + "." + ndesc.Node.PreInsertCtx.Ref.GetObjName())
				} else if ndesc.Node.PreInsertCtx.TableDef != nil {
					buf.WriteString(ndesc.Node.TableDef.GetName())
				}
			}
		case plan.Node_PRE_DELETE:
			buf.WriteString(" on ")
			if ndesc.Node.ObjRef != nil {
				buf.WriteString(ndesc.Node.ObjRef.GetSchemaName() + "." + ndesc.Node.ObjRef.GetObjName())
			} else if ndesc.Node.TableDef != nil {
				buf.WriteString(ndesc.Node.TableDef.GetName())
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
		options.NodeType = ndesc.Node.NodeType
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

	// Get partition prune information
	if ndesc.Node.NodeType == plan.Node_TABLE_SCAN && ndesc.Node.TableDef.Partition != nil {
		partPruneInfo, err := ndesc.GetPartitionPruneInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, partPruneInfo)
	}

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

	if ndesc.Node.NodeType == plan.Node_FILL {
		fillCoslInfo, err := ndesc.GetFillColsInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, fillCoslInfo)
		fillModelInfo, err := ndesc.GetFillModeInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, fillModelInfo)
	}

	// Get Aggregate function info
	if len(ndesc.Node.AggList) > 0 && ndesc.Node.NodeType != plan.Node_FILL {
		var listInfo string
		var err error
		if ndesc.Node.NodeType == plan.Node_SAMPLE {
			listInfo, err = ndesc.GetSampleFuncInfo(ctx, options)
		} else {
			listInfo, err = ndesc.GetAggregationInfo(ctx, options)
		}
		if err != nil {
			return nil, err
		}
		lines = append(lines, listInfo)
	}

	// Get Window function info
	if ndesc.Node.NodeType == plan.Node_WINDOW {
		windowSpecListInfo, err := ndesc.GetWindowSpectListInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, windowSpecListInfo)
	}

	// Get Filter list info
	if len(ndesc.Node.FilterList) > 0 {
		filterInfo, err := ndesc.GetFilterConditionInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, filterInfo)
	}

	// Get Block Filter list info
	if len(ndesc.Node.BlockFilterList) > 0 {
		filterInfo, err := ndesc.GetBlockFilterConditionInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, filterInfo)
	}

	if len(ndesc.Node.RuntimeFilterProbeList) > 0 {
		filterInfo, err := ndesc.GetRuntimeFilteProbeInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, filterInfo)
	}

	if len(ndesc.Node.RuntimeFilterBuildList) > 0 {
		filterInfo, err := ndesc.GetRuntimeFilterBuildInfo(ctx, options)
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

	if len(ndesc.Node.SendMsgList) > 0 {
		msgInfo, err := ndesc.GetSendMessageInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, msgInfo)
	}

	if len(ndesc.Node.RecvMsgList) > 0 {
		msgInfo, err := ndesc.GetRecvMessageInfo(ctx, options)
		if err != nil {
			return nil, err
		}
		lines = append(lines, msgInfo)
	}
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
	if ndesc.Node.BuildOnLeft {
		if ndesc.Node.JoinType == plan.Node_SEMI || ndesc.Node.JoinType == plan.Node_ANTI {
			result = "Join Type: RIGHT " + ndesc.Node.JoinType.String()
		}
	}
	if ndesc.Node.Stats.HashmapStats != nil && ndesc.Node.Stats.HashmapStats.HashOnPK {
		result += "   hashOnPK"
	}
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

	if ndesc.Node.Stats.HashmapStats.Shuffle {
		idx := ndesc.Node.Stats.HashmapStats.ShuffleColIdx
		shuffleType := ndesc.Node.Stats.HashmapStats.ShuffleType
		var hashCol *plan.Expr
		switch exprImpl := ndesc.Node.OnList[idx].Expr.(type) {
		case *plan.Expr_F:
			hashCol = exprImpl.F.Args[0]
		}

		if shuffleType == plan.ShuffleType_Hash {
			buf.WriteString(" shuffle: hash(")
			err := describeExpr(ctx, hashCol, options, buf)
			if err != nil {
				return "", err
			}
			buf.WriteString(")")
		} else {
			buf.WriteString(" shuffle: range(")
			err := describeExpr(ctx, hashCol, options, buf)
			if err != nil {
				return "", err
			}
			buf.WriteString(")")
		}

		if ndesc.Node.Stats.HashmapStats.ShuffleTypeForMultiCN == plan.ShuffleTypeForMultiCN_Hybrid {
			buf.WriteString(" HYBRID ")
		}
	}

	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetPartitionPruneInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Hit Partition: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		if ndesc.Node.PartitionPrune != nil {
			first := true
			for _, v := range ndesc.Node.PartitionPrune.SelectedPartitions {
				if !first {
					buf.WriteString(", ")
				}
				first = false
				buf.WriteString(v.PartitionName)
			}
		} else {
			buf.WriteString("all partitions")
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetFilterConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 512))
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

func (ndesc *NodeDescribeImpl) GetBlockFilterConditionInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Block Filter Cond: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.BlockFilterList {
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

func (ndesc *NodeDescribeImpl) GetRuntimeFilteProbeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Runtime Filter Probe: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.RuntimeFilterProbeList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v.Expr, options, buf)
			if err != nil {
				return "", err
			}
			if v.MatchPrefix {
				buf.WriteString(" Match Prefix")
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetRuntimeFilterBuildInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Runtime Filter Build: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.RuntimeFilterBuildList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			err := describeExpr(ctx, v.Expr, options, buf)
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

func (ndesc *NodeDescribeImpl) GetSendMessageInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Send Message: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.SendMsgList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			describeMessage(v, buf)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetRecvMessageInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Recv Message: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.RecvMsgList {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			describeMessage(v, buf)
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

	if ndesc.Node.Stats.HashmapStats != nil && ndesc.Node.Stats.HashmapStats.Shuffle {
		idx := ndesc.Node.Stats.HashmapStats.ShuffleColIdx
		shuffleType := ndesc.Node.Stats.HashmapStats.ShuffleType
		if ndesc.Node.Stats.HashmapStats.ShuffleMethod != plan.ShuffleMethod_Reuse {
			if shuffleType == plan.ShuffleType_Hash {
				buf.WriteString(" shuffle: hash(")
				err := describeExpr(ctx, ndesc.Node.GroupBy[idx], options, buf)
				if err != nil {
					return "", err
				}
				buf.WriteString(")")
			} else {
				buf.WriteString(" shuffle: range(")
				err := describeExpr(ctx, ndesc.Node.GroupBy[idx], options, buf)
				if err != nil {
					return "", err
				}
				buf.WriteString(")")
			}
		}

		if ndesc.Node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
			buf.WriteString(" shuffle: REUSE ")
		} else if ndesc.Node.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reshuffle {
			buf.WriteString(" RESHUFFLE ")
		}
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

func (ndesc *NodeDescribeImpl) GetSampleFuncInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	if ndesc.Node.SampleFunc.Rows == plan2.NotSampleByRows {
		buf.WriteString(fmt.Sprintf("Sample %.2f Percent by: ", ndesc.Node.SampleFunc.Percent))
	} else {
		buf.WriteString(fmt.Sprintf("Sample %d Rows by: ", ndesc.Node.SampleFunc.Rows))
	}

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

func (ndesc *NodeDescribeImpl) GetFillColsInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Fill Columns: ")
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

func (ndesc *NodeDescribeImpl) GetFillModeInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Fill Mode: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		switch ndesc.Node.FillType {
		case plan.Node_NONE:
			buf.WriteString("None")
		case plan.Node_LINEAR:
			buf.WriteString("Linear")
		case plan.Node_NULL:
			buf.WriteString("Null")
		case plan.Node_VALUE:
			buf.WriteString("Value")
		case plan.Node_PREV:
			buf.WriteString("Prev")
		case plan.Node_NEXT:
			buf.WriteString("Next")
		}
		if len(ndesc.Node.FillVal) > 0 {
			buf.WriteString(" Fill Value: ")
			first := true
			for _, v := range ndesc.Node.GetFillVal() {
				if !first {
					buf.WriteString(", ")
				}
				first = false
				err := describeExpr(ctx, v, options, buf)
				if err != nil {
					return "", err
				}
			}
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return "", moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return "", moerr.NewNYI(ctx, "explain format dot")
	}
	return buf.String(), nil
}

func (ndesc *NodeDescribeImpl) GetWindowSpectListInfo(ctx context.Context, options *ExplainOptions) (string, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 300))
	buf.WriteString("Window Function: ")
	if options.Format == EXPLAIN_FORMAT_TEXT {
		first := true
		for _, v := range ndesc.Node.GetWinSpecList() {
			if !first {
				buf.WriteString("\n")
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

var _ NodeElemDescribe = (*CostDescribeImpl)(nil)
var _ NodeElemDescribe = (*ExprListDescribeImpl)(nil)
var _ NodeElemDescribe = (*OrderByDescribeImpl)(nil)
var _ NodeElemDescribe = (*WinSpecDescribeImpl)(nil)
var _ NodeElemDescribe = (*RowsetDataDescribeImpl)(nil)
var _ NodeElemDescribe = (*AnalyzeInfoDescribeImpl)(nil)

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

	var majorStr, minorStr string
	switch options.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
		majorStr = "scan"
		minorStr = "filter"
	case plan.Node_JOIN:
		majorStr = "probe"
		minorStr = "build"
	case plan.Node_AGG:
		majorStr = "group"
		minorStr = "mergegroup"
	case plan.Node_SORT:
		majorStr = "sort"
		minorStr = "mergesort"
	case plan.Node_FILTER:
		majorStr = ""
		minorStr = "filter"
	}

	majordop := len(a.AnalyzeInfo.TimeConsumedArrayMajor)
	if majordop > 1 {
		fmt.Fprintf(buf, " %v_time=[", majorStr)
		sort.Slice(a.AnalyzeInfo.TimeConsumedArrayMajor, func(i, j int) bool {
			return a.AnalyzeInfo.TimeConsumedArrayMajor[i] < a.AnalyzeInfo.TimeConsumedArrayMajor[j]
		})
		if majordop > 4 {
			var totalTime int64
			for i := range a.AnalyzeInfo.TimeConsumedArrayMajor {
				totalTime += a.AnalyzeInfo.TimeConsumedArrayMajor[i]
			}
			fmt.Fprintf(buf,
				"total=%vms,min=%vms,max=%vms,dop=%v]",
				totalTime/MILLION,
				a.AnalyzeInfo.TimeConsumedArrayMajor[0]/MILLION,
				a.AnalyzeInfo.TimeConsumedArrayMajor[len(a.AnalyzeInfo.TimeConsumedArrayMajor)-1]/MILLION,
				majordop)
		} else {
			for i := range a.AnalyzeInfo.TimeConsumedArrayMajor {
				if i != 0 {
					fmt.Fprintf(buf, ",")
				}
				fmt.Fprintf(buf, "%vms", a.AnalyzeInfo.TimeConsumedArrayMajor[i]/MILLION)
			}
			fmt.Fprintf(buf, "]")
		}

		minordop := len(a.AnalyzeInfo.TimeConsumedArrayMinor)
		if minordop > 0 {
			if minorStr == "mergegroup" || minorStr == "mergesort" {
				for i := range a.AnalyzeInfo.TimeConsumedArrayMinor {
					if i != 0 {
						a.AnalyzeInfo.TimeConsumedArrayMinor[0] += a.AnalyzeInfo.TimeConsumedArrayMinor[i]
					}
				}
				a.AnalyzeInfo.TimeConsumedArrayMinor = a.AnalyzeInfo.TimeConsumedArrayMinor[:1]
			}

			fmt.Fprintf(buf, " %v_time=[", minorStr)
			sort.Slice(a.AnalyzeInfo.TimeConsumedArrayMinor, func(i, j int) bool {
				return a.AnalyzeInfo.TimeConsumedArrayMinor[i] < a.AnalyzeInfo.TimeConsumedArrayMinor[j]
			})
			if minordop > 4 {
				var totalTime int64
				for i := range a.AnalyzeInfo.TimeConsumedArrayMinor {
					totalTime += a.AnalyzeInfo.TimeConsumedArrayMinor[i]
				}
				fmt.Fprintf(buf,
					"total=%vms,min=%vms,max=%vms,dop=%v]",
					totalTime/MILLION,
					a.AnalyzeInfo.TimeConsumedArrayMinor[0]/MILLION,
					a.AnalyzeInfo.TimeConsumedArrayMinor[len(a.AnalyzeInfo.TimeConsumedArrayMinor)-1]/MILLION,
					majordop)
			} else {
				for i := range a.AnalyzeInfo.TimeConsumedArrayMinor {
					if i != 0 {
						fmt.Fprintf(buf, ",")
					}
					fmt.Fprintf(buf, "%vms", a.AnalyzeInfo.TimeConsumedArrayMinor[i]/MILLION)
				}
				fmt.Fprintf(buf, "]")
			}
		}
	}

	fmt.Fprintf(buf, " waitTime=%dms", a.AnalyzeInfo.WaitTimeConsumed/MILLION)
	fmt.Fprintf(buf, " inputRows=%d", a.AnalyzeInfo.InputRows)
	fmt.Fprintf(buf, " outputRows=%d", a.AnalyzeInfo.OutputRows)
	if a.AnalyzeInfo.InputSize < MB {
		fmt.Fprintf(buf, " InputSize=%dbytes", a.AnalyzeInfo.InputSize)
	} else if a.AnalyzeInfo.InputSize < 10*GB {
		fmt.Fprintf(buf, " InputSize=%dmb", a.AnalyzeInfo.InputSize/MB)
	} else {
		fmt.Fprintf(buf, " InputSize=%dgb", a.AnalyzeInfo.InputSize/GB)
	}

	if a.AnalyzeInfo.OutputSize < MB {
		fmt.Fprintf(buf, " OutputSize=%dbytes", a.AnalyzeInfo.OutputSize)
	} else if a.AnalyzeInfo.OutputSize < 10*GB {
		fmt.Fprintf(buf, " OutputSize=%dmb", a.AnalyzeInfo.OutputSize/MB)
	} else {
		fmt.Fprintf(buf, " OutputSize=%dgb", a.AnalyzeInfo.OutputSize/GB)
	}

	if a.AnalyzeInfo.MemorySize < MB {
		fmt.Fprintf(buf, " MemorySize=%dbytes", a.AnalyzeInfo.MemorySize)
	} else if a.AnalyzeInfo.MemorySize < 10*GB {
		fmt.Fprintf(buf, " MemorySize=%dmb", a.AnalyzeInfo.MemorySize/MB)
	} else {
		fmt.Fprintf(buf, " MemorySize=%dgb", a.AnalyzeInfo.MemorySize/GB)
	}

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
		if c.Stats.HashmapStats != nil && c.Stats.HashmapStats.HashmapSize > 0 {
			hashmapSizeStr = " hashmapSize=" + strconv.FormatFloat(c.Stats.HashmapStats.HashmapSize, 'f', 2, 64)
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
