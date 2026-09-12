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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestNumericDomainSurvivesFoldCopyAndWire(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, approximate := range []bool{false, true} {
		for _, optimizerFold := range []bool{false, true} {
			x := makePlan2Int64ConstExprWithType(5)
			if approximate {
				x = makePlan2Float64ConstExprWithType(5)
			}
			division, err := BindFuncExprImplByPlanExpr(t.Context(), "/", []*planpb.Expr{x, makePlan2Int64ConstExprWithType(2)})
			require.NoError(t, err)
			wrapped, err := BindFuncExprImplByPlanExpr(t.Context(), "abs", []*planpb.Expr{division})
			require.NoError(t, err)
			var folded *planpb.Expr
			if optimizerFold {
				node := &planpb.Node{ProjectList: []*planpb.Expr{wrapped}}
				rule.NewConstantFold(false).Apply(node, nil, proc)
				folded = node.ProjectList[0]
			} else {
				folded, err = ConstantFold(batch.EmptyForConstFoldBatch, wrapped, proc, false, true)
				require.NoError(t, err)
			}
			require.NotNil(t, folded.GetLit())
			require.Equal(t, !approximate, folded.GetPreparedNumeric().GetExactNumeric())
			copied := DeepCopyExpr(folded)
			wire, err := proto.Marshal(copied)
			require.NoError(t, err)
			var decoded planpb.Expr
			require.NoError(t, proto.Unmarshal(wire, &decoded))
			require.Equal(t, !approximate, rule.IsExactNumeric(&decoded, nil))
			target := types.T_int64.ToType()
			assignment, err := forceAssignmentCastExpr(t.Context(), &decoded, makePlan2Type(&target))
			require.NoError(t, err)
			result, err := ConstantFold(batch.EmptyForConstFoldBatch, assignment, proc, false, true)
			require.NoError(t, err)
			want := int64(3)
			if approximate {
				want = 2
			}
			require.Equal(t, want, result.GetLit().GetI64Val())
		}
	}
}
