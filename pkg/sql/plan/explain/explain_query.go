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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

func (e *ExplainQueryImpl) ExplainPlan(buffer *ExplainDataBuffer, options *ExplainOptions) error {
	nodes := e.QueryPlan.Nodes
	for index, rootNodeID := range e.QueryPlan.Steps {
		logutil.Infof("------------------------------------Query Plan-%v ---------------------------------------------", index)
		settings := FormatSettings{
			buffer: buffer,
			offset: 0,
			indent: 2,
			level:  0,
		}
		err := traversalPlan(nodes[rootNodeID], nodes, &settings, options)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *ExplainQueryImpl) BuildJsonPlan(uuid uuid.UUID, options *ExplainOptions) *ExplainData {
	nodes := e.QueryPlan.Nodes
	expdata := NewExplainData(uuid)
	for index, rootNodeId := range e.QueryPlan.Steps {
		graphData := NewGraphData()
		err := PreOrderPlan(nodes[rootNodeId], nodes, graphData, options)
		if err != nil {
			var errdata *ExplainData
			if moErr, ok := err.(*moerr.Error); ok {
				errdata = NewExplainDataFail(uuid, moErr.MySQLCode(), moErr.Error())
			} else {
				newError := moerr.NewInternalError("An error occurred when plan is serialized to json")
				errdata = NewExplainDataFail(uuid, newError.MySQLCode(), newError.Error())
			}
			return errdata
		}
		step := NewStep(index)
		step.GraphData = *graphData

		expdata.Steps = append(expdata.Steps, *step)
	}
	return expdata
}

func explainStep(step *plan.Node, settings *FormatSettings, options *ExplainOptions) error {
	nodedescImpl := NewNodeDescriptionImpl(step)

	if options.Format == EXPLAIN_FORMAT_TEXT {
		basicNodeInfo, err1 := nodedescImpl.GetNodeBasicInfo(options)
		if err1 != nil {
			return nil
		}
		settings.buffer.PushNewLine(basicNodeInfo, true, settings.level)

		// Process verbose optioan information , "Output:"
		if options.Verbose {
			if nodedescImpl.Node.GetProjectList() != nil {
				projecrtInfo, err := nodedescImpl.GetProjectListInfo(options)
				if err != nil {
					return err
				}
				settings.buffer.PushNewLine(projecrtInfo, false, settings.level)
			}

			if nodedescImpl.Node.NodeType == plan.Node_TABLE_SCAN {
				if nodedescImpl.Node.TableDef != nil {
					tableDef, err := nodedescImpl.GetTableDef(options)
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
					rowsetInfo, err := rowsetDataDescImpl.GetDescription(options)
					if err != nil {
						return err
					}
					settings.buffer.PushNewLine(rowsetInfo, false, settings.level)
				}
			}

			if nodedescImpl.Node.NodeType == plan.Node_UPDATE {
				if nodedescImpl.Node.UpdateCtxs != nil {
					updateCtxsDescImpl := &UpdateCtxsDescribeImpl{
						UpdateCtxs: nodedescImpl.Node.UpdateCtxs,
					}
					updateCols, err := updateCtxsDescImpl.GetDescription(options)
					if err != nil {
						return err
					}
					settings.buffer.PushNewLine(updateCols, false, settings.level)
				}
			}
		}

		// print out the actual operation information
		if options.Analyze {
			if nodedescImpl.Node.AnalyzeInfo != nil {
				analyze, err := nodedescImpl.GetActualAnalyzeInfo(options)
				if err != nil {
					return err
				}
				settings.buffer.PushNewLine(analyze, false, settings.level)
			}
		}

		// Get other node descriptions, such as "Filter:", "Group Key:", "Sort Key:"
		extraInfo, err := nodedescImpl.GetExtraInfo(options)
		if err != nil {
			return err
		}
		for _, line := range extraInfo {
			settings.buffer.PushNewLine(line, false, settings.level)
		}
	} else if options.Format == EXPLAIN_FORMAT_JSON {
		return moerr.NewNYI("explain format json")
	} else if options.Format == EXPLAIN_FORMAT_DOT {
		return moerr.NewNYI("explain format dot")
	}
	return nil
}

func traversalPlan(node *plan.Node, Nodes []*plan.Node, settings *FormatSettings, options *ExplainOptions) error {
	if node == nil {
		return nil
	}
	err1 := explainStep(node, settings, options)
	if err1 != nil {
		return err1
	}
	settings.level++
	// Recursive traversal Query Plan
	if len(node.Children) > 0 {
		for _, childNodeID := range node.Children {
			index, err := serachNodeIndex(childNodeID, Nodes)
			if err != nil {
				return err
			}
			err = traversalPlan(Nodes[index], Nodes, settings, options)
			if err != nil {
				return err
			}
		}
	}
	settings.level--
	return nil
}

// serach target node's index in Nodes slice
func serachNodeIndex(nodeID int32, Nodes []*plan.Node) (int32, error) {
	for i, node := range Nodes {
		if node.NodeId == nodeID {
			return int32(i), nil
		}
	}
	return -1, moerr.NewInvalidInput("invliad plan nodeID %d", nodeID)
}

func PreOrderPlan(node *plan.Node, Nodes []*plan.Node, graphData *GraphData, options *ExplainOptions) error {
	if node != nil {
		newNode, err := ConvertNode(node, options)
		if err != nil {
			return err
		}
		graphData.Nodes = append(graphData.Nodes, *newNode)
		if len(node.Children) > 0 {
			for _, childNodeID := range node.Children {
				index, err2 := serachNodeIndex(childNodeID, Nodes)
				if err2 != nil {
					return err2
				}

				edge := buildEdge(node, Nodes[index], index)
				graphData.Edges = append(graphData.Edges, *edge)

				err = PreOrderPlan(Nodes[index], Nodes, graphData, options)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// StatisticsRead statistics read rows, size in ExplainData
func (d *ExplainData) StatisticsRead() (rows int64, size int64) {
	for _, step := range d.Steps {
		for _, node := range step.GraphData.Nodes {
			if node.Name != TableScan && node.Name != ExternalScan {
				continue
			}
			for _, s := range node.Statistics.Throughput {
				switch s.Name {
				case InputRows:
					rows += s.Value
				case InputSize:
					size += s.Value
				}
			}
		}
	}
	return
}
