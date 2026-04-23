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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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
