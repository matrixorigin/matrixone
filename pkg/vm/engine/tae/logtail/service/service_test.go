package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
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
	server, err := NewLogtailServer(address, logtailer, clock)
	require.NoError(t, err)

	/* ---- start logtail server ---- */
	err = server.Start()
	require.NoError(t, err)
	defer func() {
		err := server.Close()
		require.NoError(t, err)
	}()

	/* ---- construct logtail client ---- */
	requestPool := &sync.Pool{
		New: func() any {
			return &LogtailRequest{}
		},
	}

	acquireRequest := func() morpc.Message {
		return requestPool.Get().(*LogtailRequest)
	}

	codec := morpc.NewMessageCodec(acquireRequest,
		morpc.WithCodecPayloadCopyBufferSize(16*KiB),
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(16*KiB),
	)
	bf := morpc.NewGoettyBasedBackendFactory(codec)
	rpcClient, err := morpc.NewClient(bf, morpc.WithClientMaxBackendPerHost(1))
	require.NoError(t, err)

	rpcStream, err := rpcClient.NewStream(address, false)
	require.NoError(t, err)

	logtailClient, err := NewLogtailClient(rpcStream)
	require.NoError(t, err)
	defer func() {
		err := logtailClient.Close()
		require.NoError(t, err)
	}()

	/* ---- send request via logtail client ---- */
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := logtailClient.Subscribe(ctx, tableA)
		require.NoError(t, err)
	}

	/* ---- wait subscription response via logtail client ---- */
	{
		resp, err := logtailClient.Receive()
		require.NoError(t, err)
		require.NotNil(t, resp.GetSubscribeResponse())
		require.Equal(t, tableA.String(), resp.GetSubscribeResponse().Logtail.Table.String())
	}

	/* ---- wait update response via logtail client ---- */
	{
		resp, err := logtailClient.Receive()
		require.NoError(t, err)
		require.NotNil(t, resp.GetUpdateResponse())
		require.Equal(t, 1, len(resp.GetUpdateResponse().LogtailList))
		require.Equal(t, tableA.String(), resp.GetUpdateResponse().LogtailList[0].Table.String())
	}
}

type logtailer struct {
	tables []api.TableID
}

func mockLocktailer(tables ...api.TableID) Logtailer {
	return &logtailer{
		tables: tables,
	}
}

func (m *logtailer) TableTotal(
	ctx context.Context, table api.TableID, end timestamp.Timestamp,
) (*logtail.TableLogtail, error) {
	return mockLogtail(table), nil
}

func (m *logtailer) RangeTotal(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]*logtail.TableLogtail, error) {
	tails := make([]*logtail.TableLogtail, 0, len(m.tables))
	for _, table := range m.tables {
		tails = append(tails, mockLogtail(table))
	}
	return tails, nil
}
