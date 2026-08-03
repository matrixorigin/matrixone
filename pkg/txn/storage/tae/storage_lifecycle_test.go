// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	txnstorage "github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

type lifecycleTestLogtailServer struct {
	closeErr   error
	closeCalls int
}

func (s *lifecycleTestLogtailServer) Start() error { return nil }

func (s *lifecycleTestLogtailServer) Close() error {
	s.closeCalls++
	return s.closeErr
}

type lifecycleTestHandler struct {
	rpchandle.Handler
	db           *db.DB
	closeErr     error
	destroyErr   error
	closeCalls   int
	destroyCalls int
}

func (h *lifecycleTestHandler) GetDB() *db.DB { return h.db }

func (h *lifecycleTestHandler) HandleClose(context.Context) error {
	h.closeCalls++
	return h.closeErr
}

func (h *lifecycleTestHandler) HandleDestroy(context.Context) error {
	h.destroyCalls++
	return h.destroyErr
}

func TestDestroyClosesTAEStorage(t *testing.T) {
	closeErr := errors.New("close failed")
	destroyErr := errors.New("destroy failed")
	server := &lifecycleTestLogtailServer{closeErr: closeErr}
	handler := &lifecycleTestHandler{destroyErr: destroyErr}
	s := &taeStorage{logtailServer: server, taeHandler: handler}

	err := s.Destroy(context.Background())
	require.ErrorIs(t, err, closeErr)
	require.ErrorIs(t, err, destroyErr)
	require.Equal(t, 1, server.closeCalls)
	require.Equal(t, 1, handler.closeCalls)
	require.Equal(t, 1, handler.destroyCalls)
}

func TestNewTAEStorageHandleCreationFailure(t *testing.T) {
	primaryErr := errors.New("handle creation failed")
	handleCalls := 0
	serverCalls := 0
	deps := taeStorageDependencies{
		newTAEHandle: func(
			context.Context,
			string,
			client.QueryClient,
			*options.Options,
		) (taeHandle, error) {
			handleCalls++
			return nil, primaryErr
		},
		newLogtailServer: func(
			context.Context,
			*db.DB,
			string,
			*options.LogtailServerCfg,
			runtime.Runtime,
		) (logtailServer, error) {
			serverCalls++
			return nil, nil
		},
	}

	storage, err := newTAEStorageForTest(t, deps)
	require.Nil(t, storage)
	require.ErrorIs(t, err, primaryErr)
	require.Equal(t, 1, handleCalls)
	require.Equal(t, 0, serverCalls)
}

func TestNewTAEStorageRejectsInvalidLogtailMessageSizeBeforeOpeningHandle(t *testing.T) {
	handleCalls := 0
	deps := taeStorageDependencies{
		newTAEHandle: func(
			context.Context,
			string,
			client.QueryClient,
			*options.Options,
		) (taeHandle, error) {
			handleCalls++
			return nil, nil
		},
	}
	rt := runtime.DefaultRuntime()
	cfg := options.NewDefaultLogtailServerCfg()
	cfg.RpcMaxMessageSize = 1

	storage, err := newTAEStorage(
		context.Background(), t.TempDir(),
		&options.Options{SID: rt.ServiceUUID()}, metadata.TNShard{}, rt,
		"", cfg, nil, nil, deps)
	require.Nil(t, storage)
	require.Error(t, err)
	require.Zero(t, handleCalls)
}

func TestNewTAEStorageLogtailServerFailureClosesHandle(t *testing.T) {
	primaryErr := errors.New("logtail server creation failed")
	cleanupErr := errors.New("handle close failed")
	handler := &lifecycleTestHandler{db: &db.DB{}, closeErr: cleanupErr}
	serverCalls := 0
	deps := taeStorageDependencies{
		newTAEHandle: func(
			context.Context,
			string,
			client.QueryClient,
			*options.Options,
		) (taeHandle, error) {
			return handler, nil
		},
		newLogtailServer: func(
			context.Context,
			*db.DB,
			string,
			*options.LogtailServerCfg,
			runtime.Runtime,
		) (logtailServer, error) {
			serverCalls++
			return nil, primaryErr
		},
	}

	storage, err := newTAEStorageForTest(t, deps)
	require.Nil(t, storage)
	require.ErrorIs(t, err, primaryErr)
	require.ErrorIs(t, err, cleanupErr)
	require.Equal(t, 1, serverCalls)
	require.Equal(t, 1, handler.closeCalls)
}

func newTAEStorageForTest(
	t *testing.T,
	deps taeStorageDependencies,
) (txnstorage.TxnStorage, error) {
	t.Helper()
	rt := runtime.DefaultRuntime()
	return newTAEStorage(
		context.Background(),
		t.TempDir(),
		&options.Options{SID: rt.ServiceUUID()},
		metadata.TNShard{},
		rt,
		"",
		options.NewDefaultLogtailServerCfg(),
		nil,
		nil,
		deps,
	)
}
