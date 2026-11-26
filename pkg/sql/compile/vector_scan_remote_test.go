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
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRemoteIVFScanSlowFirstResponse(t *testing.T) {
	const (
		responseDelay = 6 * time.Second
		queryTimeout  = 20 * time.Second
		accountID     = uint32(42)
		streamID      = uint64(3)
	)
	workers := engine.Nodes{
		{Id: "a", Addr: "a:6001", Mcpu: 2},
		{Id: "b", Addr: "b:6001", Mcpu: 2},
	}
	for _, tc := range []struct {
		name    string
		wantErr error
	}{
		{name: "late first response"},
		{name: "query deadline", wantErr: context.DeadlineExceeded},
		{name: "query cancel", wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, queryClient := vectorPlacementCompile(t, workers)
			rt := runtime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
			queryClient.version = defines.MORPCLatestVersion

			scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
			defer ReleaseScopes(scopes)
			require.NoError(t, err)
			require.Len(t, scopes, 2)
			peer := scopes[1]
			require.Equal(t, workers[1].Id, peer.NodeInfo.Id)
			c.proc.Base.TxnOperator = fakeTxnOperator{}
			c.proc.Base.SessionInfo.TimeZone = time.UTC

			synctest.Test(t, func(t *testing.T) {
				parent, cancelParent := context.WithTimeout(defines.AttachAccountId(context.Background(), accountID), queryTimeout)
				defer cancelParent()
				queryCtx := c.proc.Base.GetContextBase().BuildQueryCtx(parent)
				_, cancelQuery := process.GetQueryCtxFromProc(c.proc)
				defer cancelQuery()
				peer.Proc.BuildPipelineContext(queryCtx)
				defer peer.Proc.Cancel(nil)

				requests := make(chan pipeline.Message, 1)
				responses := make(chan morpc.Message, 1)
				stream := mock_morpc.NewMockStream(gomock.NewController(t))
				stream.EXPECT().ID().Return(streamID).AnyTimes()
				stream.EXPECT().Receive().Return(responses, nil)
				stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, msg morpc.Message) error {
						request := msg.(*pipeline.Message)
						requests <- *request
						cnclient.ReleaseMessage(request)
						return nil
					},
				)
				stream.EXPECT().Close(true).Return(nil)
				client := &testPipelineClient{
					genStream: func(_ context.Context, addr string) (morpc.Stream, error) {
						if addr != workers[1].Addr {
							return nil, fmt.Errorf("unexpected remote address %q", addr)
						}
						return stream, nil
					},
				}
				oldClient, hadClient := rt.GetGlobalVariables(runtime.PipelineClient)
				rt.SetGlobalVariables(runtime.PipelineClient, client)
				t.Cleanup(func() {
					if hadClient {
						rt.SetGlobalVariables(runtime.PipelineClient, oldClient)
					} else {
						rt.CompareAndDeleteGlobalVariables(runtime.PipelineClient, client)
					}
				})

				done := make(chan error, 1)
				go func() { done <- peer.RemoteRun(c) }()
				synctest.Wait()
				var request pipeline.Message
				select {
				case request = <-requests:
				default:
					select {
					case err := <-done:
						t.Fatalf("remote IVF returned before sending a request: %v", err)
					default:
						t.Fatal("remote IVF did not send a request")
					}
				}
				require.Equal(t, pipeline.Method_PipelineMessage, request.GetCmd())
				require.Equal(t, pipeline.Status_Last, request.GetSid())
				var wireScope pipeline.Pipeline
				require.NoError(t, wireScope.Unmarshal(request.GetData()))
				require.Equal(t, pipeline.Pipeline_Remote, wireScope.GetPipelineType())
				require.Equal(t, workers[1].Id, wireScope.GetNode().GetId())
				require.Equal(t, workers[1].Addr, wireScope.GetNode().GetAddr())
				require.Equal(t, int32(2), wireScope.GetNode().GetCnCnt())
				require.Equal(t, int32(1), wireScope.GetNode().GetCnIdx())
				source := wireScope.GetDataSource().GetNode()
				require.Equal(t, plan.Node_INDEX_SEARCH_SCAN, source.GetNodeType())
				require.Equal(t, catalog.MoIndexIvfFlatAlgo.ToString(), source.GetIndexSearchScan().GetIndex().GetIndexAlgo())
				var wireProc pipeline.ProcessInfo
				require.NoError(t, wireProc.Unmarshal(request.GetProcInfoData()))
				require.Equal(t, accountID, wireProc.GetAccountId())

				if tc.wantErr == nil {
					// The first reply arrives after the former five-second startup timeout.
					<-time.After(responseDelay)
				}
				select {
				case err := <-done:
					t.Fatalf("remote IVF returned before a response or query cancellation: %v", err)
				default:
				}
				require.NoError(t, queryCtx.Err())
				switch tc.wantErr {
				case nil:
					responses <- &pipeline.Message{Id: streamID, Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_MessageEnd}
					require.NoError(t, <-done)
					require.NoError(t, queryCtx.Err())
				case context.Canceled:
					cancelQuery()
					require.ErrorIs(t, <-done, tc.wantErr)
					require.ErrorIs(t, queryCtx.Err(), tc.wantErr)
				case context.DeadlineExceeded:
					require.ErrorIs(t, <-done, tc.wantErr)
					require.ErrorIs(t, queryCtx.Err(), tc.wantErr)
				}
			})
		})
	}
}
