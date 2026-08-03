// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestExplainBackgroundQueryHelpersFallbackToNodeScan(t *testing.T) {
	stepSQL := "SELECT * FROM t WHERE id IN (4,5) LIMIT 7"
	fallbackSQL := "SELECT * FROM t WHERE id IN (1,2,3) LIMIT 9"

	query := &plan.Query{
		Steps: []int32{5, 1},
		Nodes: []*plan.Node{
			{NodeId: 0, Stats: &plan.Stats{Sql: fallbackSQL}},
			{NodeId: 1, Stats: &plan.Stats{Sql: stepSQL}, AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: 11}},
		},
	}

	require.Equal(t, stepSQL, backgroundQuerySQL(query))

	limit, ok := extractSQLLimit(stepSQL)
	require.True(t, ok)
	require.Equal(t, 7, limit)

	inCount, ok := extractSQLInListCount(stepSQL)
	require.True(t, ok)
	require.Equal(t, 2, inCount)

	require.EqualValues(t, 11, queryOutputRows(query))

	fallbackQuery := &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{
			{NodeId: 0, Stats: &plan.Stats{Sql: fallbackSQL}},
			{NodeId: 1, Stats: &plan.Stats{}},
		},
	}
	require.Equal(t, fallbackSQL, backgroundQuerySQL(fallbackQuery))
}

func TestFindIvfSearchOutputRowsAndTruncateSummaryList(t *testing.T) {
	query := &plan.Query{
		Nodes: []*plan.Node{
			{
				NodeId:      0,
				NodeType:    plan.Node_FUNCTION_SCAN,
				TableDef:    &plan.TableDef{TblFunc: &plan.TableFunction{Name: "other_func"}},
				AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: 1},
			},
			{
				NodeId:      1,
				NodeType:    plan.Node_FUNCTION_SCAN,
				TableDef:    &plan.TableDef{TblFunc: &plan.TableFunction{Name: "ivf_search"}},
				AnalyzeInfo: &plan.AnalyzeInfo{OutputRows: 6},
			},
		},
	}

	rows, ok := findIvfSearchOutputRows(query)
	require.True(t, ok)
	require.EqualValues(t, 6, rows)

	_, ok = findIvfSearchOutputRows(nil)
	require.False(t, ok)

	require.Equal(t, "1, 2, 3", truncateSummaryList([]string{"1", "2", "3"}, 4))
	require.Equal(t, "1, 2, 3, 4, ...", truncateSummaryList([]string{"1", "2", "3", "4", "5"}, 4))
}
