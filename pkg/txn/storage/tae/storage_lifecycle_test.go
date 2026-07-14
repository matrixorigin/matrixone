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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/rpchandle"
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
	closeErr     error
	destroyErr   error
	closeCalls   int
	destroyCalls int
}

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
