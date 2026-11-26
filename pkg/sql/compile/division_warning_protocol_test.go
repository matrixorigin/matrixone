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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Exercise the real arithmetic entry using a process reconstructed by the
// pipeline receiver, rather than only asserting its transported policy bit.
func requireRemoteDivisionWarning(t *testing.T, proc *process.Process) {
	t.Helper()
	previousSink := proc.WarningSink
	defer func() { proc.WarningSink = previousSink }()
	sink := &remoteWarningSession{}
	proc.WarningSink = sink
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "div", []*planpb.Expr{
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}}},
		{Typ: types.T_int64.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 0}}}},
	})
	require.NoError(t, err)
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.True(t, result.IsNull(0))
	require.Equal(t, uint64(1), sink.totalWarnings)
	require.Len(t, sink.warnings, 1)
	require.Equal(t, uint16(1365), sink.warnings[0].code)
	require.Equal(t, "Division by 0", sink.warnings[0].msg)
}
