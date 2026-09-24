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
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRecursiveCTEPrefixLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	for _, tc := range []struct {
		limit, offset, want uint64
	}{
		{3, 1, 4},
		{3, 0, 3},
		{0, 1, 0},
		{0, math.MaxUint64, 0},
		{math.MaxUint64, 1, math.MaxUint64},
		{1, math.MaxUint64, math.MaxUint64},
	} {
		t.Run(fmt.Sprintf("%d_%d", tc.limit, tc.offset), func(t *testing.T) {
			expr, err := recursiveCTEPrefixLimit(context.Background(), MakePlan2Uint64ConstExprWithType(tc.limit), MakePlan2Uint64ConstExprWithType(tc.offset))
			require.NoError(t, err)
			require.Equal(t, int32(types.T_uint64), expr.Typ.Id)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(func() { executor.Free() })
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[uint64](result, 0))
		})
	}
	limit := MakePlan2Uint64ConstExprWithType(3)
	got, err := recursiveCTEPrefixLimit(context.Background(), limit, nil)
	require.NoError(t, err)
	require.Same(t, limit, got)
	got, err = recursiveCTEPrefixLimit(context.Background(), nil, limit)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRecursiveCTEOffsetOutsideFeedback(t *testing.T) {
	for _, union := range []string{"union all", "union distinct"} {
		t.Run(union, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			t.Cleanup(optimizer.CurrentContext().GetProcess().Free)
			logicPlan, err := runOneStmt(optimizer, t, fmt.Sprintf(`
				with recursive c(n) as (
					select 1 %s select n + 1 from c where n < 10 limit 3 offset 1
				) select n from c`, union))
			require.NoError(t, err)
			var recursive, consumer *planpb.Node
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == planpb.Node_RECURSIVE_CTE {
					recursive = node
				}
				if node.NodeType == planpb.Node_SINK_SCAN && node.Offset != nil {
					consumer = node
				}
			}
			require.NotNil(t, recursive)
			require.Nil(t, recursive.Offset, "feedback must retain skipped rows")
			require.NotNil(t, recursive.Limit)
			require.NotNil(t, consumer, "only the result consumer may skip rows")
			require.Equal(t, uint64(1), consumer.Offset.GetLit().GetU64Val())
		})
	}
}
