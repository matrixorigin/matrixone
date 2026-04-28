// Copyright 2022 Matrix Origin
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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestIvfSearchFilterInfo(t *testing.T) {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		TableDef: &plan.TableDef{
			TblFunc: &plan.TableFunction{Name: "ivf_search"},
		},
		TblFuncExprList: []*plan.Expr{
			plan2.MakePlan2StringConstExprWithType("tblcfg"),
			plan2.MakePlan2StringConstExprWithType("[1,2,3]"),
			plan2.MakePlan2StringConstExprWithType("`__mo_index_include_category` >= 20"),
		},
	}
	nodedesc := &NodeDescribeImpl{Node: node}
	lines, err := nodedesc.GetExtraInfo(context.TODO(), &ExplainOptions{})
	require.NoError(t, err)
	require.Contains(t, lines, "Filter Cond: `__mo_index_include_category` >= 20")
}

func TestExplainCollapsesIvfBackgroundQueriesByDefault(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   0,
				NodeType: plan.Node_FUNCTION_SCAN,
				TableDef: &plan.TableDef{
					TblFunc: &plan.TableFunction{Name: "ivf_search"},
				},
				AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: 7},
			},
		},
		BackgroundQueries: []*plan.Query{
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (1,2) LIMIT 40", 2),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (3,4) LIMIT 80", 0),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (5,6) LIMIT 160", 3),
		},
	}

	buffer := NewExplainDataBuffer()
	err := NewExplainQueryImpl(query).ExplainPlan(context.TODO(), buffer, &ExplainOptions{Analyze: true, Format: EXPLAIN_FORMAT_TEXT})
	require.NoError(t, err)

	planText := buffer.ToString()
	require.Contains(t, planText, "Background Queries: round_count=3")
	require.Contains(t, planText, "bucket_windows=0:2, 2:4, 4:6")
	require.Contains(t, planText, "round_limits=40, 80, 160")
	require.Contains(t, planText, "empty_rounds=1")
	require.Contains(t, planText, "dedup_output_rows=7")
}

func TestExplainExpandsSmallIvfBackgroundQueriesByDefault(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   0,
				NodeType: plan.Node_FUNCTION_SCAN,
				TableDef: &plan.TableDef{
					TblFunc: &plan.TableFunction{Name: "ivf_search"},
				},
				AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: 20},
			},
		},
		BackgroundQueries: []*plan.Query{
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (1,2) LIMIT 20", 11),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (3,4) LIMIT 20", 9),
		},
	}

	buffer := NewExplainDataBuffer()
	err := NewExplainQueryImpl(query).ExplainPlan(context.TODO(), buffer, &ExplainOptions{Analyze: true, Format: EXPLAIN_FORMAT_TEXT})
	require.NoError(t, err)

	planText := buffer.ToString()
	require.NotContains(t, planText, "Background Queries: round_count=")
	require.Contains(t, planText, "Project")
	require.GreaterOrEqual(t, strings.Count(planText, "Project"), 2)
}

func TestExplainVerboseCapsIvfBackgroundQueryExpansion(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:   0,
				NodeType: plan.Node_FUNCTION_SCAN,
				TableDef: &plan.TableDef{
					TblFunc: &plan.TableFunction{Name: "ivf_search"},
				},
			},
		},
		BackgroundQueries: []*plan.Query{
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (1,2) LIMIT 40", 1),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (3,4) LIMIT 80", 1),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (5,6) LIMIT 160", 1),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (7,8) LIMIT 320", 1),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (9,10) LIMIT 640", 1),
			makeIvfBackgroundQuery("SELECT * FROM t WHERE id IN (11,12) LIMIT 1280", 1),
		},
	}

	buffer := NewExplainDataBuffer()
	err := NewExplainQueryImpl(query).ExplainPlan(context.TODO(), buffer, &ExplainOptions{Verbose: true, Analyze: true, Format: EXPLAIN_FORMAT_TEXT})
	require.NoError(t, err)

	planText := buffer.ToString()
	require.Contains(t, planText, "Background Queries: skipped 1 middle plan(s)")
	require.NotContains(t, planText, "round_count=")
}

func makeIvfBackgroundQuery(sql string, outputRows int64) *plan.Query {
	return &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{
			{
				NodeId:      0,
				NodeType:    plan.Node_PROJECT,
				Stats:       &plan.Stats{Sql: sql},
				AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: outputRows},
			},
		},
	}
}
