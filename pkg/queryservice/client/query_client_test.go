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

package client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCreateQueryClient(t *testing.T) QueryClient {
	ct, err := NewQueryClient("", morpc.Config{})
	assert.NoError(t, err)
	return ct
}

func TestCountCachedObjectFilesReturnsWhileWriteIsPending(t *testing.T) {
	sid := t.Name()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })

	dir, err := os.MkdirTemp("/tmp", "mo-query-")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, os.RemoveAll(dir)) })
	address := "unix://" + dir + "/rpc"
	server, err := morpc.NewMessageHandler(sid, sid, address, morpc.Config{},
		morpc.NewMessagePool(
			func() *query.Request { return &query.Request{} },
			func() *query.Response { return &query.Response{} },
		))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, server.Close()) })
	server.RegisterMethod(uint32(query.CmdMethod_CountCachedObjectFiles),
		func(_ context.Context, req *query.Request, resp *query.Response, _ *morpc.Buffer) error {
			resp.CountCachedObjectFilesResponse = &query.CountCachedObjectFilesResponse{
				Hits: uint32(len(req.CountCachedObjectFilesRequest.ObjectIds)),
			}
			return nil
		}, false)
	require.NoError(t, server.Start())

	writerEntered := make(chan struct{})
	unblockWriter := make(chan struct{})
	defer close(unblockWriter)
	cfg := morpc.Config{BackendOptions: []morpc.BackendOption{
		morpc.WithBackendFilter(func(msg morpc.Message, _ string) bool {
			if isCountCachedObjectFilesRequest(msg) &&
				len(msg.(*query.Request).CountCachedObjectFilesRequest.ObjectIds) == 2 {
				close(writerEntered)
				<-unblockWriter
			}
			return true
		}),
	}}
	cli, err := NewQueryClient(sid, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cli.Close()) })

	guard, stopGuard := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopGuard()
	req := cli.NewRequest(query.CmdMethod_CountCachedObjectFiles)
	req.CountCachedObjectFilesRequest = &query.CountCachedObjectFilesRequest{ObjectIds: [][]byte{{1}}}
	resp, err := cli.SendMessage(guard, address, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	defer cli.Release(resp)
	require.EqualValues(t, 1, resp.CountCachedObjectFilesResponse.GetHits())

	ctx, cancel := context.WithCancel(guard)
	defer cancel()
	req = cli.NewRequest(query.CmdMethod_CountCachedObjectFiles)
	req.CountCachedObjectFilesRequest = &query.CountCachedObjectFilesRequest{ObjectIds: [][]byte{{1}, {2}}}
	done := make(chan error, 1)
	go func() {
		resp, err := cli.SendMessage(ctx, address, req)
		if resp != nil {
			cli.Release(resp)
		}
		done <- err
	}()
	select {
	case <-writerEntered:
	case <-guard.Done():
		t.Fatal("writer did not receive the request")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-guard.Done():
		t.Fatal("cancellation waited for the blocked writer")
	}
}

func TestCountCachedObjectFilesUnsentRequestIsReleased(t *testing.T) {
	sid := t.Name()
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	pool := &testQueryMessagePool{MessagePool: morpc.NewMessagePool(
		func() *query.Request { return &query.Request{} },
		func() *query.Response { return &query.Response{} },
	)}
	cli := &queryClient{serviceID: sid, pool: pool, client: unsentQueryRPCClient{}}

	for _, address := range []string{"", "unused"} {
		req := cli.NewRequest(query.CmdMethod_CountCachedObjectFiles)
		req.CountCachedObjectFilesRequest = &query.CountCachedObjectFilesRequest{ObjectIds: [][]byte{{1}}}
		resp, err := cli.SendMessage(t.Context(), address, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Equal(t, &query.Request{}, req)
	}
	require.Equal(t, 2, pool.releasedRequests)

	req := cli.NewRequest(query.CmdMethod_GetCommit)
	resp, err := cli.SendMessage(t.Context(), "unused", req)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, query.CmdMethod_GetCommit, req.CmdMethod)
	require.Equal(t, 2, pool.releasedRequests)

	cli.serviceID = sid + "-missing"
	req = cli.NewRequest(query.CmdMethod_CountCachedObjectFiles)
	resp, err = cli.SendMessage(t.Context(), "unused", req)
	require.ErrorContains(t, err, "service runtime is unavailable")
	require.Nil(t, resp)
	require.Equal(t, &query.Request{}, req)
	require.Equal(t, 3, pool.releasedRequests)
}

type testQueryMessagePool struct {
	morpc.MessagePool[*query.Request, *query.Response]
	releasedRequests int
}

func (p *testQueryMessagePool) ReleaseRequest(req *query.Request) {
	p.releasedRequests++
	p.MessagePool.ReleaseRequest(req)
}

type unsentQueryRPCClient struct {
	morpc.RPCClient
}

func (unsentQueryRPCClient) Send(context.Context, string, morpc.Message) (*morpc.Future, error) {
	return nil, context.Canceled
}

func TestMongoDBClientRetireRequiresProtocolVersion5(t *testing.T) {
	assert.Equal(t, defines.MORPCVersion5, methodVersions[query.CmdMethod_MongoDBClientRetire])
}

func TestGetPythonUdfStatusRequiresProtocolVersion118(t *testing.T) {
	assert.Equal(t, defines.MORPCVersion119, methodVersions[query.CmdMethod_GetPythonUdfStatus])
}

// The name says 54 because that is the contract: the protocol RefreshSessionAuth shipped with.
func TestRefreshSessionAuthRequiresProtocolVersion54(t *testing.T) {
	// A method's entry in methodVersions records the protocol it SHIPPED with, not whatever is
	// newest -- nearly every other entry is MORPCVersion1 for that reason. Gating a method on
	// the latest version would make it unusable until every peer in the cluster had upgraded,
	// which is the opposite of what the gate is for. So this stays at 54 however far the
	// protocol moves on.
	//
	// The assertion that pinned the latest version literally lived here too. Both this branch
	// and main had to edit it this cycle, for unrelated features, which is the argument
	// against it: TestMethodVersionsNeverExceedTheLatestProtocol checks the property it was
	// reaching for and needs no edit when the version moves.
	assert.Equal(t, defines.MORPCVersion54, methodVersions[query.CmdMethod_RefreshSessionAuth])
	assert.GreaterOrEqual(t, defines.MORPCLatestVersion, methodVersions[query.CmdMethod_RefreshSessionAuth])
}

// No method may require a protocol NEWER than the newest one that exists: no peer could ever
// satisfy such a gate, so the RPC would be permanently unreachable rather than merely gated.
//
// This replaces an assertion that pinned the literal latest version. That one fired on every
// bump, including bumps by features with nothing to do with session auth, and the edit it
// demanded was always the same: move the literal. Checking the relationship instead keeps the
// property it was reaching for -- a gate that cannot outrun the protocol -- without needing an
// edit each time, and covers every method rather than one.
func TestMethodVersionsNeverExceedTheLatestProtocol(t *testing.T) {
	for method, version := range methodVersions {
		assert.LessOrEqual(t, version, defines.MORPCLatestVersion,
			"%s is gated on protocol %d, above the latest %d: no peer can satisfy it",
			method, version, defines.MORPCLatestVersion)
	}
}

func TestNewCacheClient(t *testing.T) {
	ct := testCreateQueryClient(t)
	assert.NotNil(t, ct)
}

func TestUnwrapResponseError(t *testing.T) {
	ct := testCreateQueryClient(t)
	assert.NotNil(t, ct)
	client, ok := ct.(*queryClient)
	assert.True(t, ok)
	resp1 := &query.Response{Error: nil}
	resp2, err := client.unwrapResponseError(resp1)
	assert.Nil(t, err)
	assert.Equal(t, resp2, resp1)

	e := moerr.NewInternalErrorNoCtx("test")
	moe, err := e.MarshalBinary()
	assert.NoError(t, err)
	resp1 = &query.Response{Error: moe}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Nil(t, resp2)

	resp1 = &query.Response{
		CmdMethod: query.CmdMethod_RefreshSessionAuth,
		Error:     moe,
		RefreshSessionAuthResponse: &query.RefreshSessionAuthResponse{
			AuthenticationFailed: true,
		},
	}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Same(t, resp1, resp2)

	resp1 = &query.Response{
		CmdMethod: query.CmdMethod_RefreshSessionAuth,
		Error:     moe,
		RefreshSessionAuthResponse: &query.RefreshSessionAuthResponse{
			RequestRejected: true,
		},
	}
	resp2, err = client.unwrapResponseError(resp1)
	assert.Equal(t, "internal error: test", err.Error())
	assert.Same(t, resp1, resp2)
}

func TestSessionControlProtocol(t *testing.T) {
	sid := "session-control-version"
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(sid, rt) })
	for _, method := range []query.CmdMethod{query.CmdMethod_KillSession, query.CmdMethod_DisconnectSession} {
		req := &query.Request{CmdMethod: method}
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion119)
		require.Error(t, CheckMethodVersion(context.Background(), sid, req))
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion120)
		require.NoError(t, CheckMethodVersion(context.Background(), sid, req))
		rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion119)
		require.Error(t, CheckMethodVersion(context.Background(), sid, req))
	}
	request := &query.Request{CmdMethod: query.CmdMethod_KillSession, KillSessionRequest: &query.KillSessionRequest{ConnID: 1<<32 + 7, KillConnection: true, StatementID: "stmt", Principal: &query.KillSessionPrincipal{AccountID: 10, UserID: 20, Account: "acme", User: "alice", Role: "reader"}}}
	data, err := request.Marshal()
	require.NoError(t, err)
	var decoded query.Request
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, request, &decoded)
	request = &query.Request{CmdMethod: query.CmdMethod_DisconnectSession, DisconnectSessionRequest: &query.DisconnectSessionRequest{ConnID: 123}}
	data, err = request.Marshal()
	require.NoError(t, err)
	decoded = query.Request{}
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, request, &decoded)
	for _, response := range []*query.Response{{CmdMethod: query.CmdMethod_KillSession, KillSessionResponse: &query.KillSessionResponse{Found: true}}, {CmdMethod: query.CmdMethod_DisconnectSession, DisconnectSessionResponse: &query.DisconnectSessionResponse{}}} {
		data, err = response.Marshal()
		require.NoError(t, err)
		var got query.Response
		require.NoError(t, got.Unmarshal(data))
		require.Equal(t, response, &got)
	}
}
