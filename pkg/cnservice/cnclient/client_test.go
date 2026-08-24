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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestNewPipelineClient(t *testing.T) {
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	client, err := NewPipelineClient(sid, "127.0.0.1:6001", &PipelineConfig{})
	require.NoError(t, err)
	require.NotNil(t, client.(*pipelineClient).client)
	require.NoError(t, client.Close())
}

type retryablePipelineBackendFactory struct {
	calls atomic.Int32
}

func (f *retryablePipelineBackendFactory) Create(
	string,
	...morpc.BackendOption,
) (morpc.Backend, error) {
	f.calls.Add(1)
	// resetConn reports this after its connect retry window expires. MORPC
	// deliberately treats it as retryable, so the client-level recovery budget
	// must terminate the retry loop.
	return nil, moerr.NewRPCTimeoutNoCtx()
}

func TestPipelineBackendCreateOptionsBoundRetry(t *testing.T) {
	cfg := &PipelineConfig{TimeOutForEachConnect: 37 * time.Millisecond}
	cfg.fill()
	factory := &retryablePipelineBackendFactory{}
	options := pipelineBackendCreateOptions(cfg)
	options = append(options,
		morpc.WithClientDisableCircuitBreaker(),
		morpc.WithClientLogger(zap.NewNop()),
	)
	client, err := morpc.NewClient(t.Name(), factory, options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	stream, err := client.NewStream(ctx, "dead-pipeline-endpoint", false)
	require.Nil(t, stream)
	require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
	require.Less(t, time.Since(started), 500*time.Millisecond,
		"pipeline recovery budget must expire before the query context")
	require.Positive(t, factory.calls.Load())
}

// TestPipelineClientDeadEndpointUsesConnectBudget is a failing white-box
// counterexample for #27523 on revisions that only bound each individual TCP
// dial. The Pipeline client must also bind that budget to MORPC queue admission
// and retry completion, otherwise a fixed stale CN endpoint inherits the much
// longer statement context.
func TestPipelineClientDeadEndpointUsesConnectBudget(t *testing.T) {
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	client, err := NewPipelineClient(sid, "127.0.0.1:6001", &PipelineConfig{
		TimeOutForEachConnect: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	queryCtx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	started := time.Now()
	stream, err := client.NewStream(queryCtx, "127.0.0.1:1")
	if stream != nil {
		_ = stream.Close(true)
	}
	require.ErrorIs(t, err, morpc.ErrBackendCreateTimeout)
	require.NoError(t, context.Cause(queryCtx),
		"pipeline backend retries consumed the outer statement deadline")
	require.Less(t, time.Since(started), 500*time.Millisecond)
}
