// Copyright 2021 - 2022 Matrix Origin
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

package taestorage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
	"github.com/stretchr/testify/require"
)

type recordingQueryClient struct {
	closed atomic.Bool
	closes atomic.Int32
}

func (c *recordingQueryClient) ServiceID() string {
	return "recording-query-client"
}

func (c *recordingQueryClient) SendMessage(
	context.Context,
	string,
	*querypb.Request,
) (*querypb.Response, error) {
	if c.closed.Load() {
		return nil, errors.New("query client closed")
	}
	return &querypb.Response{}, nil
}

func (c *recordingQueryClient) NewRequest(method querypb.CmdMethod) *querypb.Request {
	return &querypb.Request{CmdMethod: method}
}

func (c *recordingQueryClient) Release(*querypb.Response) {}

func (c *recordingQueryClient) Close() error {
	c.closes.Add(1)
	c.closed.Store(true)
	return nil
}

type noopRPCServer struct{}

func (noopRPCServer) Start() error { return nil }

func (noopRPCServer) Close() error { return nil }

func (noopRPCServer) RegisterRequestHandler(
	func(context.Context, morpc.RPCMessage, uint64, morpc.ClientSession) error,
) {
}

func TestNewTAEStorageReturnsTAEOpenError(t *testing.T) {
	rt := runtime.DefaultRuntime()
	opt := &options.Options{SID: rt.ServiceUUID()}
	var err error
	require.NotPanics(t, func() {
		_, err = NewTAEStorage(
			context.Background(),
			"/dev/null/cannot-create-tae-store",
			opt,
			metadata.TNShard{},
			rt,
			"",
			options.NewDefaultLogtailServerCfg(),
			nil,
			nil,
		)
	})
	require.Error(t, err)
}

func TestNewTAEStorageClosesTAEWhenLogtailServerCreationFails(t *testing.T) {
	ctx := context.Background()
	rt := runtime.DefaultRuntime()
	dataDir := t.TempDir()
	expectedErr := errors.New("injected logtail server failure")
	queryClient := new(recordingQueryClient)

	_, err := newTAEStorage(
		ctx,
		dataDir,
		(&options.Options{SID: rt.ServiceUUID()}).FillDefaults(dataDir),
		metadata.TNShard{},
		rt,
		"",
		options.NewDefaultLogtailServerCfg(),
		nil,
		queryClient,
		func(
			string,
			string,
			*service.LogtailServer,
			...morpc.ServerOption,
		) (morpc.RPCServer, error) {
			return nil, expectedErr
		},
	)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, int32(0), queryClient.closes.Load())

	req := queryClient.NewRequest(querypb.CmdMethod_GetCacheInfo)
	resp, err := queryClient.SendMessage(ctx, "retry-address", req)
	require.NoError(t, err)
	queryClient.Release(resp)

	handle, err := rpc.NewTAEHandle(
		ctx,
		dataDir,
		nil,
		(&options.Options{}).FillDefaults(dataDir),
	)
	require.NoError(t, err)
	require.NoError(t, handle.HandleClose(ctx))

	storage, err := newTAEStorage(
		ctx,
		dataDir,
		(&options.Options{SID: rt.ServiceUUID()}).FillDefaults(dataDir),
		metadata.TNShard{},
		rt,
		"",
		options.NewDefaultLogtailServerCfg(),
		nil,
		queryClient,
		func(
			string,
			string,
			*service.LogtailServer,
			...morpc.ServerOption,
		) (morpc.RPCServer, error) {
			return noopRPCServer{}, nil
		},
	)
	require.NoError(t, err)
	require.NoError(t, storage.Close(ctx))
	require.Equal(t, int32(0), queryClient.closes.Load())
}
