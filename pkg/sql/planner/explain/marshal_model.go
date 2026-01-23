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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
)

func buildEdge(parentNode *plan.Node, childNode *plan.Node, index int32) *models.Edge {
	edge := &models.Edge{
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

var nodeTypeToNameMap = map[plan.Node_NodeType]string{
	plan.Node_UNKNOWN: "UnKnown Node",

	plan.Node_VALUE_SCAN:    "Values Scan",
	plan.Node_TABLE_SCAN:    "Table Scan",
	plan.Node_FUNCTION_SCAN: "Function Scan",
	plan.Node_EXTERNAL_SCAN: "External Scan",
	plan.Node_MATERIAL_SCAN: "Material Scan",
	plan.Node_SOURCE_SCAN:   "Source Scan",

	plan.Node_PROJECT: "Project",

	plan.Node_EXTERNAL_FUNCTION: "External Function",

	plan.Node_MATERIAL:       "Material",
	plan.Node_SINK:           "Sink",
	plan.Node_SINK_SCAN:      "Sink Scan",
	plan.Node_RECURSIVE_SCAN: "Recursive Scan",
	plan.Node_RECURSIVE_CTE:  "CTE Scan",

	plan.Node_AGG:       "Aggregate",
	plan.Node_DISTINCT:  "Distinct",
	plan.Node_FILTER:    "Filter",
	plan.Node_JOIN:      "Join",
	plan.Node_SAMPLE:    "Sample",
	plan.Node_SORT:      "Sort",
	plan.Node_UNION:     "Union",
	plan.Node_UNION_ALL: "Union All",
	plan.Node_UNIQUE:    "Unique",
	plan.Node_WINDOW:    "Window",

	plan.Node_BROADCAST: "Broadcast",
	plan.Node_SPLIT:     "Split",
	plan.Node_GATHER:    "Gather",

	plan.Node_ASSERT: "Assert",

	plan.Node_INSERT:       "Insert",
	plan.Node_DELETE:       "Delete",
	plan.Node_REPLACE:      "Replace",
	plan.Node_MULTI_UPDATE: "Multi Update",

	plan.Node_LOCK_OP: "Lock Operator",

	plan.Node_INTERSECT:     "Intersect",
	plan.Node_INTERSECT_ALL: "Intersect All",
	plan.Node_MINUS:         "Minus",
	plan.Node_MINUS_ALL:     "Minus All",

	plan.Node_ON_DUPLICATE_KEY: "On Duplicate Key",
	plan.Node_PRE_INSERT:       "Pre Insert",
	plan.Node_PRE_INSERT_UK:    "Pre Insert Unique",
	plan.Node_PRE_INSERT_SK:    "Pre Insert 2nd Key",

	plan.Node_TIME_WINDOW:  "Time window",
	plan.Node_FILL:         "Fill",
	plan.Node_PARTITION:    "Partition",
	plan.Node_FUZZY_FILTER: "Fuzzy filter",
}
