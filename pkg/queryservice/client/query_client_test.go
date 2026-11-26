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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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

func TestMongoDBClientRetireRequiresProtocolVersion5(t *testing.T) {
	assert.Equal(t, defines.MORPCVersion5, methodVersions[query.CmdMethod_MongoDBClientRetire])
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
}

type cacheProbeTestPool struct {
	morpc.MessagePool[*query.Request, *query.Response]
	recycled chan *query.Request
}

func (p cacheProbeTestPool) AcquireRequest() *query.Request {
	select {
	case req := <-p.recycled:
		return req
	default:
		return p.MessagePool.AcquireRequest()
	}
}

func (p cacheProbeTestPool) ReleaseRequest(req *query.Request) {
	req.Reset()
	p.recycled <- req
}

type cacheProbeTestTransport struct {
	morpc.RPCClient
	enqueued chan *query.Request
}

func (c cacheProbeTestTransport) Send(ctx context.Context, addr string, req morpc.Message) (*morpc.Future, error) {
	pb := req.(*query.Request)
	isProbe := pb.CmdMethod == query.CmdMethod_CountCachedObjectFiles
	future, err := c.RPCClient.Send(ctx, addr, req)
	if isProbe && err == nil {
		c.enqueued <- pb
	}
	return future, err
}

func TestCacheProbeRequestOwnership(t *testing.T) {
	socketDir, err := os.MkdirTemp("", "mo-query-probe-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
	addr := "unix://" + filepath.Join(socketDir, "rpc.sock")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	blocker := &query.Request{CmdMethod: query.CmdMethod_GetProtocolVersion}
	writerEntered := make(chan struct{})
	unblock := make(chan struct{})
	unblockWriter := sync.OnceFunc(func() { close(unblock) })
	cfg := morpc.Config{
		MaxConnections: 1,
		BackendOptions: []morpc.BackendOption{
			morpc.WithBackendBatchSendSize(1),
			morpc.WithBackendFilter(func(m morpc.Message, _ string) bool {
				if m == blocker {
					close(writerEntered)
					<-unblock
				}
				req := m.(*query.Request)
				probe := req.CountCachedObjectFilesRequest
				return probe == nil || string(probe.ObjectIds[0]) != "writer-error"
			}),
		},
	}
	wire := make(chan string, 32)
	server, err := cfg.NewServer("", "query-test", addr, func() morpc.Message { return &query.Request{} }, func(morpc.Message) {})
	require.NoError(t, err)
	server.RegisterRequestHandler(func(ctx context.Context, msg morpc.RPCMessage, _ uint64, session morpc.ClientSession) error {
		req := msg.Message.(*query.Request)
		resp := &query.Response{RequestID: req.RequestID, CmdMethod: req.CmdMethod}
		if probe := req.CountCachedObjectFilesRequest; probe != nil {
			marker := string(probe.ObjectIds[0])
			wire <- marker
			if marker == "unsupported" {
				resp.WrapError(moerr.NewNotSupported(ctx, "cache probe"))
			} else {
				resp.CountCachedObjectFilesResponse = &query.CountCachedObjectFilesResponse{Hits: 1}
			}
		}
		return session.Write(ctx, resp)
	})
	require.NoError(t, server.Start())
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	qc, err := NewQueryClient("", cfg)
	require.NoError(t, err)
	closeClient := sync.OnceFunc(func() { require.NoError(t, qc.Close()) })
	t.Cleanup(closeClient)
	defer unblockWriter()
	c := qc.(*queryClient)
	pool := cacheProbeTestPool{MessagePool: c.pool, recycled: make(chan *query.Request, 32)}
	c.pool = pool
	transport := cacheProbeTestTransport{RPCClient: c.client, enqueued: make(chan *query.Request, 32)}
	c.client = transport

	newProbe := func(marker string) *query.Request {
		req := c.NewRequest(query.CmdMethod_CountCachedObjectFiles)
		require.Zero(t, req.RequestID, "an acquired request must have been reset")
		require.Nil(t, req.CountCachedObjectFilesRequest)
		req.CountCachedObjectFilesRequest = &query.CountCachedObjectFilesRequest{ObjectIds: [][]byte{[]byte(marker)}}
		return req
	}
	assertRecycled := func(req *query.Request) {
		t.Helper()
		select {
		case recycled := <-pool.recycled:
			require.Same(t, req, recycled)
			require.Equal(t, &query.Request{}, recycled)
			require.Empty(t, pool.recycled, "request must be released only once")
			pool.recycled <- recycled
		case <-ctx.Done():
			t.Fatal("request was not recycled")
		}
	}

	blocked, err := c.client.Send(ctx, addr, blocker)
	require.NoError(t, err)
	defer blocked.Close()
	select {
	case <-writerEntered:
	case <-ctx.Done():
		t.Fatal("writer did not stop at the barrier")
	}

	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	queued := newProbe("canceled")
	result := make(chan error, 1)
	go func() {
		_, err := c.SendMessage(probeCtx, addr, queued)
		result <- err
	}()
	select {
	case req := <-transport.enqueued:
		require.Same(t, queued, req)
	case <-ctx.Done():
		t.Fatal("cache probe was not enqueued")
	}
	stopProbe()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("canceled cache probe remained blocked on the writer")
	}
	require.Empty(t, pool.recycled, "writer still owns the canceled request")

	var rejected *query.Request
	for range 16 {
		req := newProbe("rejected")
		if rejected != nil {
			require.Same(t, rejected, req)
		}
		rejected = req
		_, err := c.SendMessage(ctx, addr, req)
		require.ErrorIs(t, err, errCacheProbeBusy)
	}
	require.Same(t, rejected, pool.AcquireRequest())
	require.Empty(t, transport.enqueued, "repeated probes must not grow the shared queue")
	unblockWriter()
	_, err = blocked.Get()
	require.NoError(t, err)
	assertRecycled(queued)

	for _, marker := range []string{"writer-error", "unsupported", "ok"} {
		req := newProbe(marker)
		require.Same(t, queued, req, "writer-released request must be reusable")
		resp, err := c.SendMessage(ctx, addr, req)
		switch marker {
		case "writer-error":
			require.Error(t, err)
			require.Nil(t, resp)
		case "unsupported":
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
			require.Nil(t, resp)
		default:
			require.NoError(t, err)
			require.Equal(t, uint32(1), resp.CountCachedObjectFilesResponse.Hits)
			c.Release(resp)
		}
		require.Len(t, transport.enqueued, 1)
		require.Same(t, req, <-transport.enqueued)
		assertRecycled(req)
	}
	require.Len(t, wire, 2, "canceled and filtered probes must never reach the server")
	require.Equal(t, "unsupported", <-wire)
	require.Equal(t, "ok", <-wire)

	closeClient()
	sendFailure := newProbe("failed")
	_, err = c.SendMessage(ctx, addr, sendFailure)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrClientClosed), "got %v", err)
	assertRecycled(sendFailure)
	require.Empty(t, transport.enqueued)

	_, err = c.SendMessage(probeCtx, addr, newProbe("expired"))
	require.ErrorIs(t, err, context.Canceled)
	assertRecycled(sendFailure)
}
