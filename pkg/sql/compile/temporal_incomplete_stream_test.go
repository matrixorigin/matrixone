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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
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

func TestIncompleteTemporalExecutionStream(t *testing.T) {
	for _, name := range []string{"date_format", "datediff", "to_days", "to_seconds", "dayofweek", "week", "week_default", "last_day", "date_add", "date_sub"} {
		for _, version := range []int64{defines.MORPCVersion117, defines.MORPCVersion118} {
			t.Run(fmt.Sprintf("%s/%d", name, version), func(t *testing.T) {
				c, queryClient := expressionProtocolTestCompile(t)
				queryClient.version = defines.MORPCVersion118
				scope := incompleteTemporalStreamScope(t, c, name, types.T_varchar)
				var required int64
				_, _, _, _, err := prepareRemoteRunSendingDataWithExecutionProtocol("", scope, c.proc, nil, uuid.Nil, &required)
				require.NoError(t, err)
				checks, payloads, closed := 0, 0, 0
				replies := make(chan morpc.Message, 1)
				stream := mock_morpc.NewMockStream(gomock.NewController(t))
				stream.EXPECT().ID().Return(uint64(17)).AnyTimes()
				stream.EXPECT().Receive().Return(replies, nil)
				stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, m morpc.Message) error {
					message := m.(*pipeline.Message)
					switch message.GetCmd() {
					case pipeline.Method_PipelineProtocolCheck:
						checks++
						require.Zero(t, payloads)
						require.Equal(t, defines.MORPCVersion118, message.ProtocolVersion)
						replies <- &pipeline.Message{Id: 17, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
					case pipeline.Method_PipelineMessage:
						payloads++
						if checks != 1 {
							t.Errorf("pipeline payload bypassed execution handshake: checks=%d", checks)
						}
						replies <- &pipeline.Message{Id: 17, Sid: pipeline.Status_MessageEnd}
					default:
						t.Fatalf("unexpected command %v", message.GetCmd())
					}
					return nil
				}).AnyTimes()
				stream.EXPECT().Close(true).DoAndReturn(func(bool) error { closed++; return nil })
				installIncompleteTemporalTestStream(t, c, stream)
				err = scope.RemoteRun(c)
				require.Equal(t, defines.MORPCVersion118, required)
				if version < defines.MORPCVersion118 {
					require.ErrorContains(t, err, "118")
					require.Zero(t, payloads)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, payloads)
				}
				require.Equal(t, 1, checks)
				require.Equal(t, 1, closed, "RemoteRun owns handshake-failure and successful cleanup")
				require.Positive(t, queryClient.calls)
				require.Equal(t, queryClient.calls, queryClient.releases)
			})
		}
	}
}

func TestIncompleteTemporalExecutionStreamFailures(t *testing.T) {
	for _, failure := range []string{"wrong stream", "wrong method", "missing version", "closed", "send error", "canceled"} {
		t.Run(failure, func(t *testing.T) {
			c, queryClient := expressionProtocolTestCompile(t)
			queryClient.version = defines.MORPCVersion118
			scope := incompleteTemporalStreamScope(t, c, "week", types.T_varchar)
			ctx, cancel := context.WithCancel(scope.Proc.Ctx)
			defer cancel()
			scope.Proc.Ctx = ctx
			replies := make(chan morpc.Message, 1)
			checks, payloads, closed := 0, 0, 0
			stream := mock_morpc.NewMockStream(gomock.NewController(t))
			stream.EXPECT().ID().Return(uint64(17)).AnyTimes()
			stream.EXPECT().Receive().Return(replies, nil)
			sendErr := errors.New("handshake send failed")
			stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, m morpc.Message) error {
				if m.(*pipeline.Message).GetCmd() != pipeline.Method_PipelineProtocolCheck {
					payloads++
					return sendErr
				}
				checks++
				response := &pipeline.Message{Id: 17, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion118}
				switch failure {
				case "wrong stream":
					response.Id++
				case "wrong method":
					response.Cmd = pipeline.Method_UnknownMethod
				case "missing version":
					response.ProtocolVersion = 0
				case "closed":
					close(replies)
					return nil
				case "send error":
					return sendErr
				case "canceled":
					cancel()
					return nil
				}
				replies <- response
				return nil
			}).AnyTimes()
			stream.EXPECT().Close(true).DoAndReturn(func(bool) error { closed++; return nil })
			installIncompleteTemporalTestStream(t, c, stream)
			// The low-level owner must retain the sender on rejection; close remains
			// idempotent even after cancellation. The public owner is exercised above.
			sender, err := scope.remoteRun(c)
			if sender != nil {
				t.Cleanup(func() { sender.close(context.Background()) })
			}
			require.Error(t, err)
			require.NotNil(t, sender)
			if failure == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if failure == "send error" {
				require.ErrorIs(t, err, sendErr)
			}
			require.False(t, sender.reportingRequestStarted)
			sender.close(context.Background())
			sender.close(context.Background())
			require.Equal(t, 1, checks)
			require.Zero(t, payloads)
			require.Equal(t, 1, closed)
		})
	}
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
