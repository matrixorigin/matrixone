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

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

const (
	Label_Table_Name                = "Full table name"
	Label_Table_Columns             = "Columns"
	Label_Total_Columns             = "Total columns"
	Label_Scan_Columns              = "Scan columns"
	Label_List_Expression           = "List of expressions"
	Label_Grouping_Keys             = "Grouping keys"
	Label_Agg_Functions             = "Aggregate functions"
	Label_Filter_Conditions         = "Filter conditions"
	Label_Block_Filter_Conditions   = "Block Filter conditions"
	Label_Join_Type                 = "Join type"
	Label_Join_Conditions           = "Join conditions"
	Label_Left_NodeId               = "Left node id"
	Label_Right_NodeId              = "Right node id"
	Label_Sort_Keys                 = "Sort keys"
	Label_List_Values               = "List of values"
	Label_Union_Expressions         = "Union expressions"
	Label_Union_All_Expressions     = "Union all expressions"
	Label_Intersect_Expressions     = "Intersect expressions"
	Label_Intersect_All_Expressions = "Intersect all expressions"
	Label_Minus_Expressions         = "Minus expressions"
	Label_Pre_Insert                = "Pre insert"
	Label_Pre_InsertUk              = "Pre insert uk"
	Label_Pre_InsertSk              = "Pre insert sk"
	Label_Pre_Delete                = "Pre delete"
	Label_Sink                      = "Sink"
	Label_Sink_Scan                 = "Sink scan"
	Label_Recursive_SCAN            = "Recursive scan"
	Label_Recursive_CTE             = "CTE scan"
	Label_Lock_Op                   = "Lock op"
	Label_Row_Number                = "Number of rows"
	Label_Offset                    = "Offset"

	Label_Time_Window       = "Time window"
	Label_Partition         = "Partition"
	Label_Fill              = "Fill"
	Label_Boardcast         = "Boardcast"
	Label_Split             = "Split"
	Label_Gather            = "Gather"
	Label_Assert            = "Assert"
	Label_On_Duplicate_Key  = "On duplicate key"
	Label_Fuzzy_Filter      = "Fuzzy filter"
	Label_External_Function = "External Function"
	Label_Distinct          = "Distinct"
	Label_Sample            = "Sample"
	Label_Window            = "Window"
	Label_Minus_All         = "Minus All"
	Label_Unique            = "Unique"
	Label_Replace           = "Replace"
	Label_Unknown           = "Unknown"
	Label_Meterial          = "Meterial"
	Label_Apply             = "Apply"
)

const (
	Statistic_Unit_ns    = "ns"
	Statistic_Unit_count = "count"
	Statistic_Unit_byte  = "byte"
)

var _ ExplainQuery = &ExplainQueryImpl{}

type ExplainQueryImpl struct {
	QueryPlan *plan.Query
}

func NewExplainQueryImpl(query *plan.Query) *ExplainQueryImpl {
	return &ExplainQueryImpl{
		QueryPlan: query,
	}
}

func (e *ExplainQueryImpl) ExplainPlan(ctx context.Context, buffer *ExplainDataBuffer, options *ExplainOptions) error {
	nodes := e.QueryPlan.Nodes

	isForest := false
	if len(e.QueryPlan.Steps) > 1 {
		isForest = true
	}

	for index, rootNodeID := range e.QueryPlan.Steps {
		logutil.Debugf("------------------------------------Query Plan-%v ---------------------------------------------", index)
		settings := FormatSettings{
			buffer: buffer,
			offset: 0,
			indent: 2,
			level:  0,
		}

		if isForest {
			title := fmt.Sprintf("Plan %v:", index)
			settings.buffer.PushPlanTitle(title)
		}

		err := traversalPlan(ctx, nodes[rootNodeID], nodes, &settings, options)
		if err != nil {
			return err
		}
	}
	return nil
}

func BuildJsonPlan(ctx context.Context, uuid uuid.UUID, options *ExplainOptions, query *plan.Query) *models.ExplainData {
	nodes := query.Nodes
	expdata := models.NewExplainData(uuid)
	for index, rootNodeId := range query.Steps {
		graphData := models.NewGraphData(len(nodes))
		err := PreOrderPlan(ctx, nodes[rootNodeId], nodes, graphData, options)
		if err != nil {
			var errdata *models.ExplainData
			if moErr, ok := err.(*moerr.Error); ok {
				errdata = models.NewExplainDataFail(uuid, moErr.MySQLCode(), moErr.Error())
			} else {
				newError := moerr.NewInternalError(ctx, "An error occurred when plan is serialized to json")
				errdata = models.NewExplainDataFail(uuid, newError.MySQLCode(), newError.Error())
			}
			return errdata
		}
		err = graphData.StatisticsGlobalResource(ctx)
		if err != nil {
			var errdata *models.ExplainData
			if moErr, ok := err.(*moerr.Error); ok {
				errdata = models.NewExplainDataFail(uuid, moErr.MySQLCode(), moErr.Error())
			} else {
				newError := moerr.NewInternalError(ctx, "An error occurred when plan is serialized to json")
				errdata = models.NewExplainDataFail(uuid, newError.MySQLCode(), newError.Error())
			}
			return errdata
		}

		step := models.NewStep(index)
		step.GraphData = *graphData

		expdata.Steps = append(expdata.Steps, *step)
	}
	if stats := statistic.StatsInfoFromContext(ctx); stats != nil {
		expdata.NewPlanStats = *stats
	}
	return expdata
}

func DebugPlan(pl *plan.Plan) string {
	if qry, ok := pl.Plan.(*plan.Plan_Query); ok {
		impl := NewExplainQueryImpl(qry.Query)
		buffer := NewExplainDataBuffer()
		opt := &ExplainOptions{
			Verbose: false,
			Analyze: false,
			Format:  EXPLAIN_FORMAT_TEXT,
		}
		err := impl.ExplainPlan(context.TODO(), buffer, opt)
		if err != nil {
			return fmt.Sprintf("debug plan error %s", err.Error())
		}
		return buffer.ToString()
	} else {
		return "only support to debug query plan"
	}
}

func explainStep(ctx context.Context, step *plan.Node, nodes []*plan.Node, settings *FormatSettings, options *ExplainOptions) error {
	nodedescImpl := NewNodeDescriptionImpl(step)

	if options.Format == EXPLAIN_FORMAT_TEXT {
		basicNodeInfo, err1 := nodedescImpl.GetNodeBasicInfo(ctx, options)
		if err1 != nil {
			return nil
		}
		settings.buffer.PushNewLine(basicNodeInfo, true, settings.level)

		if nodedescImpl.Node.NodeType == plan.Node_SINK_SCAN {
			msg := "DataSource: "
			for i, s := range nodedescImpl.Node.SourceStep {
				if i > 0 {
					msg += ", "
				}
				msg += fmt.Sprintf("Plan %v", s)
			}
			settings.buffer.PushNewLine(msg, false, settings.level)
		}

		if nodedescImpl.Node.NodeType == plan.Node_RECURSIVE_SCAN {
			msg := "DataSource: "
			for i, s := range nodedescImpl.Node.SourceStep {
				if i > 0 {
					msg += ", "
				}
				msg += fmt.Sprintf("Plan %v", s)
			}
			settings.buffer.PushNewLine(msg, false, settings.level)
		}

		if nodedescImpl.Node.NodeType == plan.Node_RECURSIVE_CTE {
			msg := "DataSource: "
			for i, s := range nodedescImpl.Node.SourceStep {
				if i > 0 {
					msg += ", "
				}
				msg += fmt.Sprintf("Plan %v", s)
			}
			settings.buffer.PushNewLine(msg, false, settings.level)
		}

		// Process verbose optioan information , "Output:"
		if options.Verbose {
			if nodedescImpl.Node.GetProjectList() != nil {
				projecrtInfo, err := nodedescImpl.GetProjectListInfo(ctx, options)
				if err != nil {
					return err
				}
				settings.buffer.PushNewLine(projecrtInfo, false, settings.level)
			}

			if nodedescImpl.Node.NodeType == plan.Node_TABLE_SCAN {
				if nodedescImpl.Node.TableDef != nil {
					tableDef, err := nodedescImpl.GetTableDef(ctx, options)
					if err != nil {
						return err
					}
					settings.buffer.PushNewLine(tableDef, false, settings.level)
				}
			}

			if nodedescImpl.Node.NodeType == plan.Node_VALUE_SCAN {
				if nodedescImpl.Node.RowsetData != nil {
					rowsetDataDescImpl := &RowsetDataDescribeImpl{
						RowsetData: nodedescImpl.Node.RowsetData,
					}
					// Provide a relatively balanced initial capacity [360] for byte slice to prevent multiple memory requests
					buf := bytes.NewBuffer(make([]byte, 0, 360))
					err := rowsetDataDescImpl.GetDescription(ctx, options, buf)
					if err != nil {
						return err
					}
					settings.buffer.PushNewLine(buf.String(), false, settings.level)
				}
			}

			if nodedescImpl.Node.NodeType == plan.Node_LOCK_OP {
				if nodedescImpl.Node.LockTargets != nil {
					buf := bytes.NewBuffer(make([]byte, 0, 360))
					buf.WriteString("Lock level: ")
					if nodedescImpl.Node.LockTargets[0].LockTable {
						buf.WriteString("Table level lock")
					} else {
						buf.WriteString("Row level lock")
					}
					settings.buffer.PushNewLine(buf.String(), false, settings.level)
				}
			}

			if nodedescImpl.Node.NodeType == plan.Node_FUZZY_FILTER {
				buf := bytes.NewBuffer(make([]byte, 0, 360))
				buf.WriteString("Build on: ")
				var sinkScan, tableScan *plan.Node
				for _, childNodeID := range step.Children {
					index, err := serachNodeIndex(ctx, childNodeID, nodes)
					if err != nil {
						return err
					}
					childNode := nodes[index]
					if childNode.NodeType == plan.Node_TABLE_SCAN {
						tableScan = childNode
					} else {
						sinkScan = childNode
					}
				}
				if (tableScan.Stats.Cost / sinkScan.Stats.Cost) < 0.5 {
					buf.WriteString("TableScan")
					if step.IfInsertFromUnique {
						buf.WriteString(" (InsertFromUnique)")
					}
				} else {
					buf.WriteString("SinkScan")
				}
				settings.buffer.PushNewLine(buf.String(), false, settings.level)
			}
		}

		// print out the actual operation information
		if options.Analyze {
			if nodedescImpl.Node.AnalyzeInfo != nil {
				analyze, err := nodedescImpl.GetActualAnalyzeInfo(ctx, options)
				if err != nil {
					return err
				}
				settings.buffer.PushNewLine(analyze, false, settings.level)
			}
		}

		// Get other node descriptions, such as "Filter:", "Group Key:", "Sort Key:"
		extraInfo, err := nodedescImpl.GetExtraInfo(ctx, options)
		if err != nil {
			return err
		}
		for _, line := range extraInfo {
			settings.buffer.PushNewLine(line, false, settings.level)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return moerr.NewNYI(ctx, "explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return moerr.NewNYI(ctx, "explain format dot")
	}
	return nil
}

func traversalPlan(ctx context.Context, node *plan.Node, Nodes []*plan.Node, settings *FormatSettings, options *ExplainOptions) error {
	if node == nil {
		return nil
	}
	err1 := explainStep(ctx, node, Nodes, settings, options)
	if err1 != nil {
		return err1
	}
	settings.level++
	// Recursive traversal Query Plan
	if len(node.Children) > 0 {
		for _, childNodeID := range node.Children {
			index, err := serachNodeIndex(ctx, childNodeID, Nodes)
			if err != nil {
				return err
			}
			err = traversalPlan(ctx, Nodes[index], Nodes, settings, options)
			if err != nil {
				return err
			}
		}
	}
	settings.level--
	return nil
}

// serach target node's index in Nodes slice
func serachNodeIndex(ctx context.Context, nodeID int32, Nodes []*plan.Node) (int32, error) {
	for i, node := range Nodes {
		if node.NodeId == nodeID {
			return int32(i), nil
		}
	}
	return -1, moerr.NewInvalidInputf(ctx, "invliad plan nodeID %d", nodeID)
}

func PreOrderPlan(ctx context.Context, node *plan.Node, Nodes []*plan.Node, graphData *models.GraphData, options *ExplainOptions) error {
	if node != nil {
		newNode, err := ConvertNode(ctx, node, options)
		if err != nil {
			return err
		}
		graphData.Nodes = append(graphData.Nodes, *newNode)
		if len(node.Children) > 0 {
			for _, childNodeID := range node.Children {
				index, err2 := serachNodeIndex(ctx, childNodeID, Nodes)
				if err2 != nil {
					return err2
				}

				edge := buildEdge(node, Nodes[index], index)
				graphData.Edges = append(graphData.Edges, *edge)

				err = PreOrderPlan(ctx, Nodes[index], Nodes, graphData, options)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
