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

package rpc

import (
	"context"
	"errors"
	"testing"

	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

type closeErrorQueryClient struct {
	client.QueryClient
	err        error
	closeCalls int
	closed     bool
}

func (c *closeErrorQueryClient) Close() error {
	c.closeCalls++
	c.closed = true
	return c.err
}

func TestHandlePrecommitWrite_Deprecated(t *testing.T) {
	h := mockTAEHandle(context.Background(), t, &options.Options{})

	// HandlePreCommitWrite is deprecated and does nothing
	err := h.HandlePreCommitWrite(context.Background(), txn.TxnMeta{}, &apipb.PrecommitWriteCmd{}, &apipb.TNStringResponse{})
	require.NoError(t, err)
}

func TestNewTAEHandleWithErrorReturnsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, err := NewTAEHandleWithError(ctx, t.TempDir(), nil, &options.Options{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, h)
}

func TestHandleCloseClosesDBWhenQueryClientCloseFails(t *testing.T) {
	clientErr := errors.New("query client close failed")
	c := &closeErrorQueryClient{err: clientErr}
	h := NewTAEHandle(context.Background(), t.TempDir(), c, &options.Options{})

	err := h.HandleClose(context.Background())
	require.ErrorIs(t, err, clientErr)
	require.Equal(t, 1, c.closeCalls)
	require.NotNil(t, h.db.Closed.Load())
}

func TestHandleCloseKeepsSharedQueryClientOpen(t *testing.T) {
	sharedClient := &closeErrorQueryClient{}
	existing, err := NewTAEHandleWithError(
		context.Background(), t.TempDir(), sharedClient, &options.Options{})
	require.NoError(t, err)
	canceled, err := NewTAEHandleWithError(
		context.Background(), t.TempDir(), sharedClient, &options.Options{})
	require.NoError(t, err)

	require.NoError(t, canceled.HandleClose(context.Background()))
	require.Equal(t, 0, sharedClient.closeCalls)
	require.False(t, sharedClient.closed)

	require.NoError(t, existing.HandleClose(context.Background()))
}
