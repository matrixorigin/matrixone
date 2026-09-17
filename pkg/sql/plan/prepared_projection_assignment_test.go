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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedProjectionPreservesAssignmentCast(t *testing.T) {
	for _, target := range []types.Type{types.T_int64.ToType(), types.New(types.T_decimal64, 10, 2), types.New(types.T_bit, 8, 0)} {
		t.Run(target.String(), func(t *testing.T) {
			source := &Expr{Typ: makePlan2Type(&types.Type{Oid: types.T_text}), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			assignment, err := forceAssignmentCastExpr(context.Background(), source, makePlan2Type(&target))
			require.NoError(t, err)
			require.NotNil(t, assignment.GetF())
			_, err = refreshPreparedPlanProjectionExprType(context.Background(), assignment, func(_ *planpb.ColRef, _ planpb.Type) (planpb.Type, bool) {
				return makePlan2Type(&types.Type{Oid: types.T_float64}), true
			})
			require.NoError(t, err)
			require.Equal(t, int32(target.Oid), assignment.Typ.Id, "write projection must retain destination vector ABI")
			require.NotNil(t, assignment.GetF(), "final write conversion must not become a raw producer column")
		})
	}
}
