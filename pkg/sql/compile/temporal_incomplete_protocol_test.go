// Copyright 2026 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestIncompleteTemporalWeekSessionSnapshot(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	source := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	for _, explicit := range []bool{false, true} {
		args := []*planpb.Expr{source}
		if explicit {
			args = append(args, plan2.MakePlan2Int64ConstExprWithType(3))
		}
		expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "week", args)
		require.NoError(t, err)
		features, err := planpb.RequiredRemoteExpressionFeatures(expr)
		require.NoError(t, err)
		require.Equal(t, !explicit, features.WeekSessionDefault)
		c.proc.Base.SessionInfo.DefaultWeekFormatSet = false
		err = validateRemoteExpressionFeatures(c.proc, features)
		if explicit {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "session snapshot")
		}
		c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
		require.NoError(t, validateRemoteExpressionFeatures(c.proc, features))
	}
}
