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

package plan

import (
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNewQueryBuilderCapturesSpillThresholdBoundaries(t *testing.T) {
	for _, threshold := range []int64{0, 1, 9_999, 10_000, 100_000, 100_001, 1 << 30} {
		t.Run(fmt.Sprintf("%d", threshold), func(t *testing.T) {
			values := map[string]any{
				"sql_mode":       "ONLY_FULL_GROUP_BY",
				"agg_spill_mem":  threshold,
				"join_spill_mem": threshold,
				"sort_spill_mem": threshold,
				"max_dop":        int64(8),
			}
			seen := make(map[string]int)
			ctx := NewMockCompilerContext(true)
			ctx.ResolveVariableFunc = func(name string, system, global bool) (any, error) {
				require.True(t, system)
				require.False(t, global)
				seen[name]++
				return values[name], nil
			}

			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

			require.Equal(t, threshold, builder.aggSpillMem)
			require.Equal(t, threshold, builder.joinSpillMem)
			require.Equal(t, threshold, builder.sortSpillMem)
			require.Equal(t, int64(8), builder.qry.MaxDop)
			for _, name := range []string{
				"sql_mode", "agg_spill_mem", "join_spill_mem", "sort_spill_mem", "max_dop",
			} {
				require.Equal(t, 1, seen[name], name)
			}
		})
	}
}

func TestNewQueryBuilderRejectsNonInt64SpillThresholds(t *testing.T) {
	values := map[string]any{
		"sql_mode":       "ONLY_FULL_GROUP_BY",
		"agg_spill_mem":  "65536",
		"join_spill_mem": uint64(65_536),
		"sort_spill_mem": float64(65_536),
		"max_dop":        int(8),
	}
	ctx := NewMockCompilerContext(true)
	ctx.ResolveVariableFunc = func(name string, _, _ bool) (any, error) {
		return values[name], nil
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

	require.Zero(t, builder.aggSpillMem)
	require.Zero(t, builder.joinSpillMem)
	require.Zero(t, builder.sortSpillMem)
	require.Zero(t, builder.qry.MaxDop)
}
