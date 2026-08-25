// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTimeWindowHavingRepeatedAggregateStaysAfterFill(t *testing.T) {
	tests := []struct {
		name        string
		having      string
		filterCount int
	}{
		{
			name:        "repeated conjunct",
			having:      "max(sort_key) < 5 and max(sort_key) < 5",
			filterCount: 2,
		},
		{
			name:        "repeated expression operand",
			having:      "max(sort_key) + max(sort_key) < 10",
			filterCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			mock.ctxt.objects["tw_repeated_having"] = &plan.ObjectRef{DbName: "test", ObjName: "tw_repeated_having", Obj: 42}
			mock.ctxt.tables["tw_repeated_having"] = &plan.TableDef{
				Name: "tw_repeated_having",
				Cols: []*plan.ColDef{
					{Name: "series_id", Typ: plan.Type{Id: int32(types.T_int32)}},
					{Name: "ts", Typ: plan.Type{Id: int32(types.T_datetime)}},
					{Name: "sort_key", Typ: plan.Type{Id: int32(types.T_int64)}},
				},
			}

			queryPlan, err := runOneStmt(mock, t, "select series_id, _wstart\nfrom tw_repeated_having\ngroup by series_id\nhaving "+tc.having+"\ninterval(ts, 1, minute) gapfill(partition) fill(linear)\norder by series_id, _wstart")
			require.NoError(t, err)

			var aggNode, timeWindowNode, fillNode, havingFilter *plan.Node
			for _, node := range queryPlan.GetQuery().Nodes {
				switch node.NodeType {
				case plan.Node_AGG:
					aggNode = node
				case plan.Node_TIME_WINDOW:
					timeWindowNode = node
				case plan.Node_FILL:
					fillNode = node
				case plan.Node_FILTER:
					require.True(t, node.FilterIsBarrier)
					require.Nil(t, havingFilter, "repeated HAVING expressions must not split into multiple filters")
					havingFilter = node
				}
			}

			require.NotNil(t, aggNode)
			require.Empty(t, aggNode.FilterList)
			require.NotNil(t, timeWindowNode)
			require.Equal(t, []int32{aggNode.NodeId}, timeWindowNode.Children)
			require.NotNil(t, fillNode)
			require.Equal(t, []int32{timeWindowNode.NodeId}, fillNode.Children)
			require.NotNil(t, havingFilter)
			require.Equal(t, []int32{fillNode.NodeId}, havingFilter.Children)
			require.Len(t, havingFilter.FilterList, tc.filterCount)
		})
	}
}
