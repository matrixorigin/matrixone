// Copyright 2021 - 2023 Matrix Origin
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

package queryservice

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
)

func testCreateQueryService(t *testing.T) QueryService {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{}}, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)
	address := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	err := os.RemoveAll(address[7:])
	assert.NoError(t, err)
	qs, err := NewQueryService("s1", address, morpc.Config{}, nil)
	assert.NoError(t, err)
	return qs
}

func TestNewQueryService(t *testing.T) {
	qs := testCreateQueryService(t)
	assert.NotNil(t, qs)
}

func TestUnwrapResponseError(t *testing.T) {
	qs := testCreateQueryService(t)
	assert.NotNil(t, qs)
	svc, ok := qs.(*queryService)
	assert.True(t, ok)
	resp1 := &pb.Response{Error: nil}
	resp2, err := svc.unwrapResponseError(resp1)
	assert.Nil(t, err)
	assert.Equal(t, resp2, resp1)

	e := moerr.NewInternalErrorNoCtx("test")
	moe, err := e.MarshalBinary()
	assert.NoError(t, err)
	resp1 = &pb.Response{Error: moe}
	resp2, err = svc.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Nil(t, resp2)
}

func TestQueryService(t *testing.T) {
	cn := metadata.CNService{
		ServiceID: "s1",
	}

	t.Run("sys tenant", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			sm.AddSession(&mockSession{id: "s1", tenant: "t1"})
			sm.AddSession(&mockSession{id: "s2", tenant: "t2"})
			sm.AddSession(&mockSession{id: "s3", tenant: "t3"})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			req.ShowProcessListRequest = &pb.ShowProcessListRequest{
				Tenant:    "sys",
				SysTenant: true,
			}
			resp, err := svc.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer svc.Release(resp)
			assert.NotNil(t, resp.ShowProcessListResponse)
			assert.Equal(t, 3, len(resp.ShowProcessListResponse.Sessions))
		})
	})

	t.Run("common tenant", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			sm.AddSession(&mockSession{id: "s1", tenant: "t1"})
			sm.AddSession(&mockSession{id: "s2", tenant: "t2"})
			sm.AddSession(&mockSession{id: "s3", tenant: "t3"})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			req.ShowProcessListRequest = &pb.ShowProcessListRequest{
				Tenant:    "t1",
				SysTenant: false,
			}
			resp, err := svc.SendMessage(ctx, addr, req)
			assert.NoError(t, err)
			defer svc.Release(resp)
			assert.NotNil(t, resp.ShowProcessListResponse)
			assert.Equal(t, 1, len(resp.ShowProcessListResponse.Sessions))
		})
	})

	t.Run("bad request", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod_ShowProcessList)
			_, err := svc.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "internal error: bad request", err.Error())
		})
	})

	t.Run("unsupported cmd", func(t *testing.T) {
		runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			req := svc.NewRequest(pb.CmdMethod(10))
			_, err := svc.SendMessage(ctx, addr, req)
			assert.Error(t, err)
			assert.Equal(t, "not supported: 10 not support in current service", err.Error())
		})
	})
}

func TestQueryServiceKillConn(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1"}
	runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		req := svc.NewRequest(pb.CmdMethod_KillConn)
		req.KillConnRequest = &pb.KillConnRequest{
			AccountID: 10,
			Version:   10,
		}
		resp, err := svc.SendMessage(ctx, addr, req)
		assert.NoError(t, err)
		defer svc.Release(resp)
		assert.NotNil(t, resp.KillConnResponse)
		assert.Equal(t, true, resp.KillConnResponse.Success)
	})
}

func runTestWithQueryService(t *testing.T, cn metadata.CNService,
	fn func(svc QueryService, addr string, sm *SessionManager)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	address := fmt.Sprintf("unix:///tmp/cn-%d-%s.sock",
		time.Now().Nanosecond(), cn.ServiceID)

	if err := os.RemoveAll(address[7:]); err != nil {
		panic(err)
	}
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices([]metadata.CNService{{
			ServiceID:    cn.ServiceID,
			QueryAddress: address,
		}}, nil))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	sm := NewSessionManager()
	qs, err := NewQueryService(cn.ServiceID, address, morpc.Config{}, sm)
	assert.NoError(t, err)
	qs.AddHandleFunc(pb.CmdMethod_KillConn, func(ctx context.Context, request *pb.Request, response *pb.Response) error {
		response.KillConnResponse = &pb.KillConnResponse{Success: true}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_AlterAccount, func(ctx context.Context, request *pb.Request, response *pb.Response) error {
		response.AlterAccountResponse = &pb.AlterAccountResponse{AlterSuccess: true}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_TraceSpan, func(ctx context.Context, request *pb.Request, resp *pb.Response) error {
		resp.TraceSpanResponse = &pb.TraceSpanResponse{
			Resp: "echo",
		}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_GetCacheInfo, func(ctx context.Context, request *pb.Request, resp *pb.Response) error {
		ci := &pb.CacheInfo{
			NodeType:  cn.ServiceID,
			NodeId:    "uuid",
			CacheType: "memory",
		}
		resp.GetCacheInfoResponse = &pb.GetCacheInfoResponse{
			CacheInfoList: []*pb.CacheInfo{ci},
		}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_GetTxnInfo, func(ctx context.Context, request *pb.Request, resp *pb.Response) error {
		ti := &pb.TxnInfo{
			CreateAt:  time.Now(),
			Meta:      nil,
			UserTxn:   true,
			WaitLocks: nil,
		}
		resp.GetTxnInfoResponse = &pb.GetTxnInfoResponse{
			CnId:        "uuid",
			TxnInfoList: []*pb.TxnInfo{ti},
		}
		return nil
	}, false)
	qs.AddHandleFunc(pb.CmdMethod_GetLockInfo, func(ctx context.Context, request *pb.Request, resp *pb.Response) error {
		li := &pb.LockInfo{
			TableId:     100,
			Keys:        nil,
			LockMode:    lock.LockMode_Shared,
			IsRangeLock: true,
			Holders:     nil,
			Waiters:     nil,
		}
		resp.GetLockInfoResponse = &pb.GetLockInfoResponse{
			CnId:         "uuid1",
			LockInfoList: []*pb.LockInfo{li},
		}
		return nil
	}, false)
	err = qs.Start()
	assert.NoError(t, err)

	fn(qs, address, sm)

	err = qs.Close()
	assert.NoError(t, err)
}

func TestQueryServiceAlterAccount(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1"}
	runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		req := svc.NewRequest(pb.CmdMethod_AlterAccount)
		req.AlterAccountRequest = &pb.AlterAccountRequest{
			TenantId: 10,
			Status:   "restricted",
		}
		resp, err := svc.SendMessage(ctx, addr, req)
		assert.NoError(t, err)
		defer svc.Release(resp)
		assert.NotNil(t, resp.AlterAccountResponse)
		assert.Equal(t, true, resp.AlterAccountResponse.AlterSuccess)
	})
}

func TestQueryServiceTraceSpan(t *testing.T) {
	cn := metadata.CNService{ServiceID: "s1"}
	runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		req := svc.NewRequest(pb.CmdMethod_TraceSpan)
		req.TraceSpanRequest = &pb.TraceSpanRequest{
			Cmd:   "cmd",
			Spans: "spans",
		}
		resp, err := svc.SendMessage(ctx, addr, req)
		assert.NoError(t, err)
		defer svc.Release(resp)
		assert.NotNil(t, resp.TraceSpanResponse)
		assert.Equal(t, "echo", resp.TraceSpanResponse.Resp)
	})
}

func TestRequestMultipleCn(t *testing.T) {
	type args struct {
		ctx                   context.Context
		nodes                 []string
		qs                    QueryService
		genRequest            func() *pb.Request
		handleValidResponse   func(string, *pb.Response)
		handleInvalidResponse func(string)
	}

	cn := metadata.CNService{ServiceID: "test_request_multi_cn"}
	runTestWithQueryService(t, cn, func(svc QueryService, addr string, sm *SessionManager) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		//////

		tests := []struct {
			name    string
			args    args
			wantErr assert.ErrorAssertionFunc
		}{
			{
				name: "genRequest is nil",
				args: args{
					ctx:                   context.Background(),
					nodes:                 []string{},
					qs:                    nil,
					genRequest:            nil,
					handleValidResponse:   nil,
					handleInvalidResponse: nil,
				},
				wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
					assert.NotNil(t, err)
					return true
				},
			},
			{
				name: "handleValidResponse is nil",
				args: args{
					ctx:                   context.Background(),
					nodes:                 []string{},
					qs:                    nil,
					genRequest:            func() *pb.Request { return nil },
					handleValidResponse:   nil,
					handleInvalidResponse: nil,
				},
				wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
					assert.NotNil(t, err)
					return true
				},
			},
			{
				name: "get cache info",
				args: args{
					ctx:   context.Background(),
					nodes: []string{},
					qs:    nil,
					genRequest: func() *pb.Request {
						req := svc.NewRequest(pb.CmdMethod_GetCacheInfo)
						req.GetCacheInfoRequest = &pb.GetCacheInfoRequest{}
						return req
					},
					handleValidResponse: func(nodeAddr string, rsp *pb.Response) {
						if rsp != nil && rsp.GetCacheInfoResponse != nil {
							assert.GreaterOrEqual(t, len(rsp.GetCacheInfoResponse.GetCacheInfoList()), 1)
						}
					},
					handleInvalidResponse: nil,
				},
				wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
					assert.Nil(t, err)
					return true
				},
			},
			{
				name: "get txn info",
				args: args{
					ctx:   context.Background(),
					nodes: []string{},
					qs:    nil,
					genRequest: func() *pb.Request {
						req := svc.NewRequest(pb.CmdMethod_GetTxnInfo)
						req.GetTxnInfoRequest = &pb.GetTxnInfoRequest{}
						return req
					},
					handleValidResponse: func(nodeAddr string, rsp *pb.Response) {
						if rsp != nil && rsp.GetTxnInfoResponse != nil {
							fmt.Printf("%v\n", rsp.GetTxnInfoResponse.TxnInfoList[0].UserTxn)
							assert.Equal(t, rsp.GetTxnInfoResponse.GetCnId(), "uuid")
							assert.True(t, rsp.GetTxnInfoResponse.TxnInfoList[0].UserTxn)
							assert.GreaterOrEqual(t, len(rsp.GetTxnInfoResponse.TxnInfoList), 1)
						}
					},
					handleInvalidResponse: nil,
				},
				wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
					assert.Nil(t, err)
					return true
				},
			},
			{
				name: "get lock info",
				args: args{
					ctx:   context.Background(),
					nodes: []string{},
					qs:    nil,
					genRequest: func() *pb.Request {
						req := svc.NewRequest(pb.CmdMethod_GetLockInfo)
						req.GetLockInfoRequest = &pb.GetLockInfoRequest{}
						return req
					},
					handleValidResponse: func(nodeAddr string, rsp *pb.Response) {
						if rsp != nil && rsp.GetLockInfoResponse != nil {
							li := rsp.GetLockInfoResponse.LockInfoList[0]
							fmt.Printf("%v %v %v %v\n", rsp.GetLockInfoResponse.GetCnId(), li.TableId, li.LockMode, li.IsRangeLock)
							assert.Equal(t, rsp.GetLockInfoResponse.GetCnId(), "uuid1")
							assert.Equal(t, li.TableId, uint64(100))
							assert.Equal(t, li.LockMode, lock.LockMode_Shared)
							assert.True(t, li.IsRangeLock)
							assert.GreaterOrEqual(t, len(rsp.GetLockInfoResponse.LockInfoList), 1)
						}
					},
					handleInvalidResponse: nil,
				},
				wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
					assert.Nil(t, err)
					return true
				},
			},
		}

		//////

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.wantErr(t,
					RequestMultipleCn(ctx,
						[]string{addr},
						svc,
						tt.args.genRequest,
						tt.args.handleValidResponse,
						tt.args.handleInvalidResponse),
					fmt.Sprintf("RequestMultipleCn(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.nodes, tt.args.qs, tt.args.genRequest != nil, tt.args.handleValidResponse != nil, tt.args.handleInvalidResponse != nil))
			})
		}
	})

}
