// Copyright 2021 - 2024 Matrix Origin
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

package client

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
)

var methodVersions = map[pb.CmdMethod]int64{
	pb.CmdMethod_ShowProcessList:          defines.MORPCVersion1,
	pb.CmdMethod_AlterAccount:             defines.MORPCVersion1,
	pb.CmdMethod_KillConn:                 defines.MORPCVersion1,
	pb.CmdMethod_TraceSpan:                defines.MORPCVersion1,
	pb.CmdMethod_GetLockInfo:              defines.MORPCVersion1,
	pb.CmdMethod_GetTxnInfo:               defines.MORPCVersion1,
	pb.CmdMethod_GetCacheInfo:             defines.MORPCVersion1,
	pb.CmdMethod_SyncCommit:               defines.MORPCVersion1,
	pb.CmdMethod_GetCommit:                defines.MORPCVersion1,
	pb.CmdMethod_RunTask:                  defines.MORPCVersion1,
	pb.CmdMethod_RemoveRemoteLockTable:    defines.MORPCVersion1,
	pb.CmdMethod_GetLatestBind:            defines.MORPCVersion1,
	pb.CmdMethod_UnsubscribeTable:         defines.MORPCVersion1,
	pb.CmdMethod_GetCacheData:             defines.MORPCVersion1,
	pb.CmdMethod_GetStatsInfo:             defines.MORPCVersion1,
	pb.CmdMethod_GetPipelineInfo:          defines.MORPCVersion1,
	pb.CmdMethod_GetProtocolVersion:       defines.MORPCMinVersion, // To make sure these methods are compatible with all versions.
	pb.CmdMethod_SetProtocolVersion:       defines.MORPCMinVersion,
	pb.CmdMethod_CoreDumpConfig:           defines.MORPCMinVersion,
	pb.CmdMethod_MigrateConnFrom:          defines.MORPCVersion1,
	pb.CmdMethod_MigrateConnTo:            defines.MORPCVersion1,
	pb.CmdMethod_ReloadAutoIncrementCache: defines.MORPCVersion1,
	pb.CmdMethod_CtlReader:                defines.MORPCVersion1,
}

type queryClient struct {
	serviceID string
	client    morpc.RPCClient
	pool      morpc.MessagePool[*pb.Request, *pb.Response]
}

type QueryClient interface {
	ServiceID() string
	// SendMessage send message to a cache server.
	SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error)
	// NewRequest creates a new request by cmd method.
	NewRequest(pb.CmdMethod) *pb.Request
	// Release releases the response.
	Release(*pb.Response)
	// Close closes the cache client.
	Close() error
}

func NewQueryClient(sid string, cfg morpc.Config) (QueryClient, error) {
	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.Response { return &pb.Response{} },
	)
	c, err := cfg.NewClient(
		sid,
		"query-client",
		func() morpc.Message {
			return pool.AcquireResponse()
		},
	)
	if err != nil {
		return nil, err
	}
	cc := &queryClient{
		serviceID: sid,
		client:    c,
		pool:      pool,
	}
	return cc, nil
}

func (c *queryClient) ServiceID() string {
	return c.serviceID
}

// SendMessage implements the QueryService interface.
func (c *queryClient) SendMessage(ctx context.Context, address string, req *pb.Request) (*pb.Response, error) {
	if address == "" {
		return nil, moerr.NewInternalError(ctx, "invalid CN query address %s", address)
	}
	if err := checkMethodVersion(ctx, c.serviceID, req); err != nil {
		return nil, err
	}
	f, err := c.client.Send(ctx, address, req)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	v, err := f.Get()
	if err != nil {
		return nil, err
	}
	resp := v.(*pb.Response)
	return c.unwrapResponseError(resp)
}

// NewRequest implements the QueryService interface.
func (c *queryClient) NewRequest(method pb.CmdMethod) *pb.Request {
	req := c.pool.AcquireRequest()
	req.CmdMethod = method
	return req
}

// Release implements the QueryService interface.
func (c *queryClient) Release(resp *pb.Response) {
	c.pool.ReleaseResponse(resp)
}

// Close implements the CacheService interface.
func (c *queryClient) Close() error {
	return c.client.Close()
}

func (c *queryClient) unwrapResponseError(resp *pb.Response) (*pb.Response, error) {
	if err := resp.UnwrapError(); err != nil {
		c.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}

func checkMethodVersion(ctx context.Context, service string, req *pb.Request) error {
	return runtime.CheckMethodVersion(ctx, service, methodVersions, req)
}
