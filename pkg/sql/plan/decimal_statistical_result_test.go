// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecimalStatisticalPersistedResultContract(t *testing.T) {
	expr := &planpb.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.VAR_POP, 0)}, Args: []*planpb.Expr{{Typ: types.New(types.T_decimal128, 38, 18).PlanType()}}}}}
	required, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion113, required)
	expr.Typ = types.New(types.T_decimal128, 38, 18).PlanType()
	_, err = RequiredPersistedExpressionProtocolVersion(expr)
	require.ErrorContains(t, err, "rebinding")
	expr.Typ = types.T_float64.ToType().PlanType()
	expr.GetF().Args[0].Typ = types.New(types.T_decimal256, 65, 30).PlanType()
	required, err = RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Zero(t, required)
}
