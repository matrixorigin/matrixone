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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func incompleteTemporalStreamScope(t *testing.T, c *Compile, name string, typ types.T) *Scope {
	t.Helper()
	source := &planpb.Expr{Typ: typ.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	args := []*planpb.Expr{source}
	switch name {
	case "date_format":
		args = append(args, plan2.MakePlan2StringConstExprWithType("%Y-%m-%d"))
	case "datediff":
		args = append(args, source)
	case "week":
		args = append(args, plan2.MakePlan2Int64ConstExprWithType(3))
	case "week_default":
		name = "week"
		c.proc.Base.SessionInfo.DefaultWeekFormatSet = true
	case "date_add", "date_sub":
		args = append(args, &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_interval)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{plan2.MakePlan2StringConstExprWithType("1.5"), plan2.MakePlan2StringConstExprWithType("DAY")}}}})
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, name, args)
	require.NoError(t, err)
	op := projection.NewArgument()
	t.Cleanup(op.Release)
	op.ProjectList = []*planpb.Expr{expr}
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	return &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op, ScopeAnalyzer: NewScopeAnalyzer()}
}

func installIncompleteTemporalTestStream(t *testing.T, c *Compile, stream morpc.Stream) {
	t.Helper()
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.PipelineClient)
	client := &testPipelineClient{genStream: func(context.Context, string) (morpc.Stream, error) { return stream, nil }}
	rt.SetGlobalVariables(moruntime.PipelineClient, client)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.PipelineClient, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.PipelineClient, client)
		}
	})
}
