// Copyright 2021 Matrix Origin
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

package cnclient

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/require"
)

type testRPCClient struct {
	backend string
	lock    bool
}

func (c *testRPCClient) Send(ctx context.Context, backend string, request morpc.Message) (*morpc.Future, error) {
	return nil, nil
}

func (c *testRPCClient) NewStream(ctx context.Context, backend string, lock bool) (morpc.Stream, error) {
	c.backend = backend
	c.lock = lock
	return nil, nil
}

func (c *testRPCClient) Ping(ctx context.Context, backend string) error { return nil }
func (c *testRPCClient) Close() error                                   { return nil }
func (c *testRPCClient) CloseBackend() error                            { return nil }

func TestPipelineClient_NewStreamAllowsLocalBackend(t *testing.T) {
	rpcClient := &testRPCClient{}
	client := &pipelineClient{
		localServiceAddress: "127.0.0.1:1234",
		client:              rpcClient,
	}

	_, err := client.NewStream(context.Background(), "127.0.0.1:1234")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:1234", rpcClient.backend)
	require.True(t, rpcClient.lock)
}

func TestControlStreamUsesIndependentBackendPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	address := "unix://" + filepath.Join(t.TempDir(), "pipeline.sock")
	sessions := make(chan context.Context, 1)
	server, err := morpc.NewRPCServer(t.Name(), address, morpc.NewMessageCodec("", func() morpc.Message { return &pipeline.Message{} }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	server.RegisterRequestHandler(func(ctx context.Context, request morpc.RPCMessage, _ uint64, cs morpc.ClientSession) error {
		msg := request.Message.(*pipeline.Message)
		if msg.Cmd == pipeline.Method_StopSending {
			sessions <- cs.SessionCtx()
		}
		return cs.Write(ctx, &pipeline.Message{Id: msg.Id, Data: append([]byte(nil), msg.Data...)})
	})
	require.NoError(t, server.Start())
	cfg := &PipelineConfig{MaxSenderNumber: 1}
	cfg.RPC.MaxMessageSize = 1 << 20
	client, err := NewPipelineClient("", "", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	controls, ok := client.(ControlStreamClient)
	require.True(t, ok)
	control, err := controls.NewControlStream(ctx, address)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, control.Close(true)) })
	cancelled, cancelBusy := context.WithCancel(ctx)
	cancelBusy()
	extra, err := controls.NewControlStream(cancelled, address)
	require.ErrorIs(t, err, context.Canceled, "a saturated control pool must still observe caller cancellation")
	require.Nil(t, extra)
	execution, err := client.NewStream(ctx, address)
	require.NoError(t, err, "the control root must not consume the only execution backend")
	t.Cleanup(func() { require.NoError(t, execution.Close(true)) })
	exchange := func(stream morpc.Stream, cmd pipeline.Method, payload string) {
		replies, err := stream.Receive()
		require.NoError(t, err)
		msg := AcquireMessage()
		msg.Id = stream.ID()
		msg.Cmd = cmd
		msg.Data = []byte(payload)
		require.NoError(t, stream.Send(ctx, msg))
		select {
		case raw := <-replies:
			require.NotNil(t, raw)
			require.Equal(t, payload, string(raw.(*pipeline.Message).Data))
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		}
	}
	exchange(control, pipeline.Method_StopSending, "control")
	exchange(execution, pipeline.Method_PipelineMessage, "execution")
	var connection context.Context
	select {
	case connection = <-sessions:
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	require.NoError(t, control.Close(true))
	select {
	case <-connection.Done():
	case <-ctx.Done():
		t.Fatal("Close(true) did not close the server connection lifetime")
	}
	// The execution pool remains usable while the control backend is closed.
	exchange(execution, pipeline.Method_PipelineMessage, "after-control-close")
}
