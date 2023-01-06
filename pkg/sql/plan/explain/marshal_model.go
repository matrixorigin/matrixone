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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type ExplainData struct {
	Steps   []Step `json:"steps"`
	Code    uint16 `json:"code"`
	Message string `json:"message"`
	Success bool   `json:"success"`
	Uuid    string `json:"uuid"`
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
		Steps:   make([]Step, 0),
		Success: true,
		Uuid:    uuid.String(),
	}
}

func NewExplainDataFail(uuid uuid.UUID, code uint16, msg string) *ExplainData {
	return &ExplainData{
		Code:    code,
		Message: msg,
		Success: false,
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

func NewGraphData() *GraphData {
	return &GraphData{
		Nodes:  make([]Node, 0),
		Edges:  make([]Edge, 0),
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

func buildEdge(parentNode *plan.Node, childNode *plan.Node, index int32) *Edge {
	edge := &Edge{
		Id:   "E" + strconv.Itoa(int(index)),
		Src:  strconv.FormatInt(int64(childNode.NodeId), 10),
		Dst:  strconv.FormatInt(int64(parentNode.NodeId), 10),
		Unit: "count",
	}
	if childNode.AnalyzeInfo != nil {
		edge.Output = childNode.AnalyzeInfo.OutputRows
	}
	return edge
}
