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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Filtered projections over a zonemap (RESULT_SCAN) file must map filter
// columns to physical file indexes, not prune-affected output positions.
// Regression: review of the #30571 fix showed `where filtered_col ...`
// pruned blocks by the wrong column's zone map/null count.
func Test_translateZonemapFilterColumnMap(t *testing.T) {
	t.Run("pruned single column maps to its physical index", func(t *testing.T) {
		// Saved schema [a, b]; the query projects only b: output position 0,
		// physical file position 1. A filter on b initially resolved to the
		// pruned schema (0), which must become the physical index (1).
		param := &ExternalParam{
			Attrs: []plan.ExternAttr{
				{ColName: "b", ColIndex: 0, ColFieldIndex: 1},
			},
			Filter: &FilterParam{
				columnMap: map[int]int{0: 0},
			},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{0: 1}, param.Filter.columnMap)
	})

	t.Run("full projection stays identity", func(t *testing.T) {
		param := &ExternalParam{
			Attrs: []plan.ExternAttr{
				{ColName: "a", ColIndex: 0, ColFieldIndex: 0},
				{ColName: "b", ColIndex: 1, ColFieldIndex: 1},
			},
			Filter: &FilterParam{
				columnMap: map[int]int{1: 1},
			},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{1: 1}, param.Filter.columnMap)
	})

	t.Run("nil filter and unmapped keys are untouched", func(t *testing.T) {
		// No Filter param at all.
		translateZonemapFilterColumnMap(&ExternalParam{})
		// No Filter.columnMap at all.
		translateZonemapFilterColumnMap(&ExternalParam{Filter: &FilterParam{}})
		// Keys that do not reference a projected attr are kept as-is.
		param := &ExternalParam{
			Attrs:  []plan.ExternAttr{{ColName: "b", ColIndex: 0, ColFieldIndex: 1}},
			Filter: &FilterParam{columnMap: map[int]int{5: 5}},
		}
		translateZonemapFilterColumnMap(param)
		require.Equal(t, map[int]int{5: 5}, param.Filter.columnMap)
	})
}
