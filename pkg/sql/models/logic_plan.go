// Copyright 2024 Matrix Origin
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

package models

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"

	"github.com/google/uuid"
)

const TimeConsumed = "Time Consumed"
const WaitTime = "Wait Time"
const ScanTime = "Scan Time"
const InsertTime = "Insert Time"

const InputRows = "Input Rows"
const OutputRows = "Output Rows"
const InputSize = "Input Size"
const OutputSize = "Output Size"
const MemorySize = "Memory Size"
const DiskIO = "Disk IO"
const S3IOByte = "S3 IO Byte"
const S3IOInputCount = "S3 IO Input Count"
const S3IOOutputCount = "S3 IO Output Count"
const Network = "Network"

type ExplainData struct {
	Steps        []Step `json:"steps"`
	Code         uint16 `json:"code"`
	Message      string `json:"message"`
	Uuid         string `json:"uuid"`
	PhyPlan      PhyPlan
	NewPlanStats statistic.StatsInfo
}

type Step struct {
	GraphData   GraphData `json:"graphData"`
	Step        int       `json:"step"`
	Description string    `json:"description"`
	State       string    `json:"state"`
	PlanStats   PlanStats `json:"stats"`
}

type GraphData struct {
	Nodes  []Node  `json:"nodes"`
	Edges  []Edge  `json:"edges"`
	Labels []Label `json:"labels"`
	Global Global  `json:"global"`
}

type PlanStats struct {
}

type Stats struct {
	BlockNum    int32   `json:"blocknum"`
	Outcnt      float64 `json:"outcnt"`
	Cost        float64 `json:"cost"`
	HashmapSize float64 `json:"hashmapsize"`
	Rowsize     float64 `json:"rowsize"`
}

type Node struct {
	NodeId     string     `json:"id"`
	Name       string     `json:"name"`
	Title      string     `json:"title"`
	Labels     []Label    `json:"labels"`
	Statistics Statistics `json:"statistics"`
	Stats      Stats      `json:"stats"`
	TotalStats TotalStats `json:"totalStats"`
}

type Edge struct {
	Id     string `json:"id"`
	Src    string `json:"src"`
	Dst    string `json:"dst"`
	Output int64  `json:"output"`
	Unit   string `json:"unit"`
}

type Label struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type TotalStats struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
	Unit  string `json:"unit"`
}

type Global struct {
	Statistics Statistics `json:"statistics"`
	TotalStats TotalStats `json:"totalStats"`
}

type Statistics struct {
	Time       []StatisticValue `json:"Time"`
	Memory     []StatisticValue `json:"Memory"`
	Throughput []StatisticValue `json:"Throughput"`
	IO         []StatisticValue `json:"IO"`
	Network    []StatisticValue `json:"Network"`
}

type StatisticValue struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
	Unit  string `json:"unit"`
}

func NewStatisticValue(name string, unit string) *StatisticValue {
	return &StatisticValue{
		Name: name,
		Unit: unit,
	}
}

func NewExplainData(uuid uuid.UUID) *ExplainData {
	return &ExplainData{
		Steps: make([]Step, 0),
		Uuid:  uuid.String(),
	}
}

func NewExplainDataFail(uuid uuid.UUID, code uint16, msg string) *ExplainData {
	return &ExplainData{
		Code:    code,
		Message: msg,
		Uuid:    uuid.String(),
	}
}

func NewStep(step int) *Step {
	return &Step{
		Step:        step,
		Description: "",
		State:       "success",
	}
}

func NewGraphData(nodeSize int) *GraphData {
	return &GraphData{
		Nodes:  make([]Node, 0, nodeSize),
		Edges:  make([]Edge, 0, nodeSize),
		Labels: make([]Label, 0),
		Global: *NewGlobal(),
	}
}

func NewGlobal() *Global {
	statistics := Statistics{
		Memory:     make([]StatisticValue, 0),
		Throughput: make([]StatisticValue, 0),
		IO:         make([]StatisticValue, 0),
		Network:    make([]StatisticValue, 0),
	}

	return &Global{
		Statistics: statistics,
		TotalStats: TotalStats{},
	}
}

func NewLabel(name string, value interface{}) *Label {
	return &Label{
		Name:  name,
		Value: value,
	}
}

func NewStatistics() *Statistics {
	return &Statistics{
		Memory:     make([]StatisticValue, 0),
		Throughput: make([]StatisticValue, 0),
		IO:         make([]StatisticValue, 0),
		Network:    make([]StatisticValue, 0),
	}
}

//----------------------------------------------------------------------------------------------------------------------

const TableScan = "Table Scan"
const ExternalScan = "External Scan"

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
