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
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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

	plan2.Node_AGG:        "Aggregate",
	plan2.Node_DISTINCT:   "Distinct",
	plan2.Node_FILTER:     "Filter",
	plan2.Node_JOIN:       "Join",
	plan2.Node_DEDUP_JOIN: "Dedup join",
	plan2.Node_SAMPLE:     "Sample",
	plan2.Node_SORT:       "Sort",
	plan2.Node_UNION:      "Union",
	plan2.Node_UNION_ALL:  "Union All",
	plan2.Node_UNIQUE:     "Unique",
	plan2.Node_WINDOW:     "Window",

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
