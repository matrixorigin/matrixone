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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestParenthesizedIntervalExpressionsBuild(t *testing.T) {
	mock := NewMockOptimizer(false)
	runTestShouldPass(mock, t, []string{
		"select date_sub('2026-05-07', interval (6) day)",
		"select date_sub('2026-05-07', interval (5+1) day)",
		"select date_sub('2026-05-07', interval (10%3) day)",
		"select date_sub('2026-05-07', interval (10%3)*2 day)",
		"select date_sub(current_date, interval ((dayofweek(current_date) + 5) % 7) day)",
		"select date_add('2026-05-07', interval (2*3) month)",
		"select '2026-05-07' - interval (6) day",
		"select '2026-05-07' + interval (3) day",
	}, false, false)
}

func TestResetDateFunctionArgsRejectsMalformedIntervalExpr(t *testing.T) {
	intervalExpr := &plan.Expr{
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*plan.Expr{
					makePlan2Int64ConstExprWithType(6),
				},
			},
		},
	}

	_, err := resetDateFunctionArgs(context.Background(), makePlan2Int64ConstExprWithType(1), intervalExpr)
	require.Error(t, err)
}
