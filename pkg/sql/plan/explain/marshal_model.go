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

	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type ExplainData struct {
	Steps        []Step `json:"steps"`
	Code         uint16 `json:"code"`
	Message      string `json:"message"`
	Uuid         string `json:"uuid"`
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

var nodeTypeToNameMap = map[plan2.Node_NodeType]string{
	plan2.Node_UNKNOWN: "UnKnown Node",

	plan2.Node_VALUE_SCAN:    "Values Scan",
	plan2.Node_TABLE_SCAN:    "Table Scan",
	plan2.Node_FUNCTION_SCAN: "Function Scan",
	plan2.Node_EXTERNAL_SCAN: "External Scan",
	plan2.Node_MATERIAL_SCAN: "Material Scan",
	plan2.Node_SOURCE_SCAN:   "Source Scan",

	plan2.Node_PROJECT: "Project",

	plan2.Node_EXTERNAL_FUNCTION: "External Function",

	plan2.Node_MATERIAL:       "Material",
	plan2.Node_SINK:           "Sink",
	plan2.Node_SINK_SCAN:      "Sink Scan",
	plan2.Node_RECURSIVE_SCAN: "Recursive Scan",
	plan2.Node_RECURSIVE_CTE:  "CTE Scan",

	plan2.Node_AGG:       "Aggregate",
	plan2.Node_DISTINCT:  "Distinct",
	plan2.Node_FILTER:    "Filter",
	plan2.Node_JOIN:      "Join",
	plan2.Node_SAMPLE:    "Sample",
	plan2.Node_SORT:      "Sort",
	plan2.Node_UNION:     "Union",
	plan2.Node_UNION_ALL: "Union All",
	plan2.Node_UNIQUE:    "Unique",
	plan2.Node_WINDOW:    "Window",

	plan2.Node_BROADCAST: "Broadcast",
	plan2.Node_SPLIT:     "Split",
	plan2.Node_GATHER:    "Gather",

	plan2.Node_ASSERT: "Assert",

	plan2.Node_INSERT:  "Insert",
	plan2.Node_DELETE:  "Delete",
	plan2.Node_REPLACE: "Replace",

	plan2.Node_LOCK_OP: "Lock Operator",

	plan2.Node_INTERSECT:     "Intersect",
	plan2.Node_INTERSECT_ALL: "Intersect All",
	plan2.Node_MINUS:         "Minus",
	plan2.Node_MINUS_ALL:     "Minus All",

	plan2.Node_ON_DUPLICATE_KEY: "On Duplicate Key",
	plan2.Node_PRE_INSERT:       "Pre Insert",
	plan2.Node_PRE_DELETE:       "Pre Delete",
	plan2.Node_PRE_INSERT_UK:    "Pre Insert Unique",
	plan2.Node_PRE_INSERT_SK:    "Pre Insert 2nd Key",

	plan2.Node_TIME_WINDOW:  "Time window",
	plan2.Node_FILL:         "Fill",
	plan2.Node_PARTITION:    "Partition",
	plan2.Node_FUZZY_FILTER: "Fuzzy filter",
}

const (
	Label_Table_Name                = "Full table name"
	Label_Table_Columns             = "Columns"
	Label_Total_Columns             = "Total columns"
	Label_Scan_Columns              = "Scan columns"
	Label_List_Expression           = "List of expressions"
	Label_Grouping_Keys             = "Grouping keys"
	Label_Agg_Functions             = "Aggregate functions"
	Label_Filter_Conditions         = "Filter conditions"
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
)

const (
	Statistic_Unit_ns    = "ns"
	Statistic_Unit_count = "count"
	Statistic_Unit_byte  = "byte"
)
