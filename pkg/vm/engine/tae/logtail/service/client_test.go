// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

type testMorpcStream struct {
	id   uint64
	recv chan morpc.Message

	mu   sync.Mutex
	sent []*LogtailRequest
}

func newTestMorpcStream(id uint64) *testMorpcStream {
	return &testMorpcStream{
		id:   id,
		recv: make(chan morpc.Message),
	}
}

func (s *testMorpcStream) ID() uint64 {
	return s.id
}

func (s *testMorpcStream) Send(_ context.Context, request morpc.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, request.(*LogtailRequest))
	return nil
}

func (s *testMorpcStream) Receive() (chan morpc.Message, error) {
	return s.recv, nil
}

func (s *testMorpcStream) Close(bool) error {
	close(s.recv)
	return nil
}

func (s *testMorpcStream) latestRequest() *LogtailRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sent) == 0 {
		return nil
	}
	return s.sent[len(s.sent)-1]
}

func TestLogtailClientSubscribe(t *testing.T) {
	stream := newTestMorpcStream(42)
	client, err := NewLogtailClient(context.Background(), stream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	table := api.TableID{DbId: 1, TbId: 2, PartitionId: 3}
	err = client.Subscribe(context.Background(), table)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return stream.latestRequest() != nil
	}, time.Second, time.Millisecond*10)

	req := stream.latestRequest()
	require.Equal(t, stream.id, req.GetRequestId())
	sub := req.GetSubscribeTable()
	require.NotNil(t, sub)
	require.False(t, sub.GetLazyCatalog())
	require.Empty(t, sub.GetInitialActiveAccounts())
	require.Equal(t, table.String(), sub.GetTable().String())
}

func TestLogtailClientSubscribeCatalogTable(t *testing.T) {
	stream := newTestMorpcStream(43)
	client, err := NewLogtailClient(context.Background(), stream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	table := api.TableID{DbId: catalog.MO_CATALOG_ID, TbId: catalog.MO_TABLES_ID}
	initialActiveAccounts := []uint32{0, 10}
	err = client.SubscribeCatalogTable(context.Background(), table, initialActiveAccounts)
	require.NoError(t, err)
	initialActiveAccounts[0] = 99

	require.Eventually(t, func() bool {
		return stream.latestRequest() != nil
	}, time.Second, time.Millisecond*10)

	req := stream.latestRequest()
	require.Equal(t, stream.id, req.GetRequestId())
	sub := req.GetSubscribeTable()
	require.NotNil(t, sub)
	require.True(t, sub.GetLazyCatalog())
	require.Equal(t, []uint32{0, 10}, sub.GetInitialActiveAccounts())
	require.Equal(t, table.String(), sub.GetTable().String())
}

func TestLogtailClientSubscribeCatalogTableRejectsNonCatalogTable(t *testing.T) {
	stream := newTestMorpcStream(44)
	client, err := NewLogtailClient(context.Background(), stream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.SubscribeCatalogTable(context.Background(), api.TableID{DbId: 10, TbId: 100}, []uint32{0})
	require.Error(t, err)
	require.Nil(t, stream.latestRequest())
}

func TestLogtailClientActivateAccountForCatalog(t *testing.T) {
	stream := newTestMorpcStream(99)
	client, err := NewLogtailClient(context.Background(), stream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.Close())
	}()

	err = client.ActivateAccountForCatalog(context.Background(), 7, 88)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return stream.latestRequest() != nil
	}, time.Second, time.Millisecond*10)

	req := stream.latestRequest()
	require.Equal(t, stream.id, req.GetRequestId())
	activate := req.GetActivateAccountForCatalog()
	require.NotNil(t, activate)
	require.Equal(t, uint32(7), activate.GetAccountId())
	require.Equal(t, uint64(88), activate.GetSeq())
}
