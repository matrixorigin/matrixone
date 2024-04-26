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
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
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

func BuildJsonPlan(ctx context.Context, uuid uuid.UUID, options *ExplainOptions, query *plan.Query) *ExplainData {
	nodes := query.Nodes
	expdata := NewExplainData(uuid)
	for index, rootNodeId := range query.Steps {
		graphData := NewGraphData(len(nodes))
		err := PreOrderPlan(ctx, nodes[rootNodeId], nodes, graphData, options)
		if err != nil {
			var errdata *ExplainData
			if moErr, ok := err.(*moerr.Error); ok {
				errdata = NewExplainDataFail(uuid, moErr.MySQLCode(), moErr.Error())
			} else {
				newError := moerr.NewInternalError(ctx, "An error occurred when plan is serialized to json")
				errdata = NewExplainDataFail(uuid, newError.MySQLCode(), newError.Error())
			}
			return errdata
		}
		err = graphData.StatisticsGlobalResource(ctx)
		if err != nil {
			var errdata *ExplainData
			if moErr, ok := err.(*moerr.Error); ok {
				errdata = NewExplainDataFail(uuid, moErr.MySQLCode(), moErr.Error())
			} else {
				newError := moerr.NewInternalError(ctx, "An error occurred when plan is serialized to json")
				errdata = NewExplainDataFail(uuid, newError.MySQLCode(), newError.Error())
			}
			return errdata
		}

		step := NewStep(index)
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

func explainStep(ctx context.Context, step *plan.Node, settings *FormatSettings, options *ExplainOptions) error {
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
	err1 := explainStep(ctx, node, settings, options)
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
	return -1, moerr.NewInvalidInput(ctx, "invliad plan nodeID %d", nodeID)
}

func PreOrderPlan(ctx context.Context, node *plan.Node, Nodes []*plan.Node, graphData *GraphData, options *ExplainOptions) error {
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

// StatisticsRead statistics read rows, size in ExplainData
//
// Deprecated: please use explain.GetInputRowsAndInputSize instead.
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

// Statistics of global resource usage, adding resources of all nodes
func (graphData *GraphData) StatisticsGlobalResource(ctx context.Context) error {
	if graphData == nil {
		return moerr.NewInternalError(ctx, "explain graphData data is null")
	} else {
		// time
		gtimeConsumed := NewStatisticValue(TimeConsumed, "ns")
		gwaitTime := NewStatisticValue(WaitTime, "ns")

		// Throughput
		ginputRows := NewStatisticValue(InputRows, "count")
		goutputRows := NewStatisticValue(OutputRows, "count")
		ginputSize := NewStatisticValue(InputSize, "byte")
		goutputSize := NewStatisticValue(OutputSize, "byte")

		// memory
		gMemorySize := NewStatisticValue(MemorySize, "byte")

		//io
		gDiskIO := NewStatisticValue(DiskIO, "byte")
		gS3IOByte := NewStatisticValue(S3IOByte, "byte")
		gS3IOInputCount := NewStatisticValue(S3IOInputCount, "count")
		gS3IOOutputCount := NewStatisticValue(S3IOOutputCount, "count")

		// network
		gNetwork := NewStatisticValue(Network, "byte")

		gtotalStats := TotalStats{
			Name:  "Time spent",
			Value: 0,
			Unit:  "ns",
		}

		for _, node := range graphData.Nodes {
			for _, timeStatValue := range node.Statistics.Time {
				if timeStatValue.Name == TimeConsumed {
					gtimeConsumed.Value += timeStatValue.Value
				}
				if timeStatValue.Name == WaitTime {
					gwaitTime.Value += timeStatValue.Value
				}
			}

			for _, throughputValue := range node.Statistics.Throughput {
				if throughputValue.Name == InputRows {
					ginputRows.Value += throughputValue.Value
				}
				if throughputValue.Name == OutputRows {
					goutputRows.Value += throughputValue.Value
				}
				if throughputValue.Name == InputSize {
					ginputSize.Value += throughputValue.Value
				}
				if throughputValue.Name == OutputSize {
					goutputSize.Value += throughputValue.Value
				}
			}

			for _, memoryValue := range node.Statistics.Memory {
				if memoryValue.Name == MemorySize {
					gMemorySize.Value += memoryValue.Value
				}
			}

			for _, ioValue := range node.Statistics.IO {
				if ioValue.Name == DiskIO {
					gDiskIO.Value += ioValue.Value
				}
				if ioValue.Name == S3IOByte {
					gS3IOByte.Value += ioValue.Value
				}
				if ioValue.Name == S3IOInputCount {
					gS3IOInputCount.Value += ioValue.Value
				}
				if ioValue.Name == S3IOOutputCount {
					gS3IOOutputCount.Value += ioValue.Value
				}
			}

			for _, networkValue := range node.Statistics.Network {
				if networkValue.Name == Network {
					gNetwork.Value += networkValue.Value
				}
			}
			gtotalStats.Value += node.TotalStats.Value
		}

		times := []StatisticValue{*gtimeConsumed, *gwaitTime}
		mbps := []StatisticValue{*ginputRows, *goutputRows, *ginputSize, *goutputSize}
		mems := []StatisticValue{*gMemorySize}
		io := []StatisticValue{*gDiskIO, *gS3IOByte, *gS3IOInputCount, *gS3IOOutputCount}
		nw := []StatisticValue{*gNetwork}

		graphData.Global.Statistics.Time = append(graphData.Global.Statistics.Time, times...)
		graphData.Global.Statistics.Throughput = append(graphData.Global.Statistics.Throughput, mbps...)
		graphData.Global.Statistics.Memory = append(graphData.Global.Statistics.Memory, mems...)
		graphData.Global.Statistics.IO = append(graphData.Global.Statistics.IO, io...)
		graphData.Global.Statistics.Network = append(graphData.Global.Statistics.Network, nw...)

		graphData.Global.TotalStats = gtotalStats
	}
	return nil
}
