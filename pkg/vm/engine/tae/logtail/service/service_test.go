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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tableA := mockTable(1, 1, 1)
	tableB := mockTable(2, 2, 2)
	tableC := mockTable(3, 3, 3)

	address := "127.0.0.1:9999"
	logtailer := mockLocktailer(tableA, tableB, tableC)
	clock := clock.NewUnixNanoHLCClock(ctx, 500*time.Millisecond)

	/* ---- construct logtail server ---- */
	logtailServer, err := NewLogtailServer(
		address, options.NewDefaultLogtailServerCfg(), logtailer, clock,
		WithServerCollectInterval(500*time.Millisecond),
		WithServerSendTimeout(5*time.Second),
		WithServerEnableChecksum(true),
		WithServerMaxMessageSize(16*mpool.KB),
		WithServerPayloadCopyBufferSize(16*mpool.KB),
		WithServerMaxLogtailFetchFailure(5),
	)
	require.NoError(t, err)

	/* ---- start logtail server ---- */
	err = logtailServer.Start()
	require.NoError(t, err)
	defer func() {
		err := logtailServer.Close()
		require.NoError(t, err)
	}()

	/* ---- construct logtail client ---- */
	codec := morpc.NewMessageCodec(func() morpc.Message { return &LogtailResponse{} },
		morpc.WithCodecPayloadCopyBufferSize(16*mpool.KB),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(16*mpool.KB),
	)
	bf := morpc.NewGoettyBasedBackendFactory(codec)
	rpcClient, err := morpc.NewClient(bf, morpc.WithClientMaxBackendPerHost(1))
	require.NoError(t, err)

	rpcStream, err := rpcClient.NewStream(address, false)
	require.NoError(t, err)

	logtailClient, err := NewLogtailClient(rpcStream, WithClientRequestPerSecond(100))
	require.NoError(t, err)
	defer func() {
		err := logtailClient.Close()
		require.NoError(t, err)
	}()

	/* ---- send subscription request via logtail client ---- */
	{
		t.Log("send subscription request via logtail client")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := logtailClient.Subscribe(ctx, tableA)
		require.NoError(t, err)
	}

	/* ---- wait subscription response via logtail client ---- */
	{
		t.Log("wait subscription response via logtail client")
		resp, err := logtailClient.Receive()
		require.NoError(t, err)
		require.NotNil(t, resp.GetSubscribeResponse())
		require.Equal(t, tableA.String(), resp.GetSubscribeResponse().Logtail.Table.String())
	}

	/* ---- wait update response via logtail client ---- */
	{
		t.Log("wait update response via logtail client")
		resp, err := logtailClient.Receive()
		require.NoError(t, err)
		require.NotNil(t, resp.GetUpdateResponse())
		require.Equal(t, 1, len(resp.GetUpdateResponse().LogtailList))
		require.Equal(t, tableA.String(), resp.GetUpdateResponse().LogtailList[0].Table.String())
	}

	/* ---- send unsubscription request via logtail client ---- */
	{
		t.Log("send unsubscription request via logtail client")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := logtailClient.Unsubscribe(ctx, tableA)
		require.NoError(t, err)
	}

	/* ---- wait subscription response via logtail client ---- */
	{
		t.Log("wait unsubscription response via logtail client")
		for {
			resp, err := logtailClient.Receive()
			require.NoError(t, err)
			if resp.GetUnsubscribeResponse() != nil {
				require.Equal(t, tableA.String(), resp.GetUnsubscribeResponse().Table.String())
				break
			}
		}
	}

	/* ---- wait update response via logtail client ---- */
	{
		t.Log("wait update response via logtail client")
		resp, err := logtailClient.Receive()
		require.NoError(t, err)
		require.NotNil(t, resp.GetUpdateResponse())
		require.Equal(t, 0, len(resp.GetUpdateResponse().LogtailList))
	}
}

type logtailer struct {
	tables []api.TableID
}

func mockLocktailer(tables ...api.TableID) taelogtail.Logtailer {
	return &logtailer{
		tables: tables,
	}
}

func (m *logtailer) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, error) {
	tails := make([]logtail.TableLogtail, 0, len(m.tables))
	for _, table := range m.tables {
		tails = append(tails, mockLogtail(table))
	}
	return tails, nil
}

func (m *logtailer) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, error) {
	for _, t := range m.tables {
		if t.String() == table.String() {
			return mockLogtail(table), nil
		}
	}
	return logtail.TableLogtail{Table: &table, Ts: &to}, nil
}
