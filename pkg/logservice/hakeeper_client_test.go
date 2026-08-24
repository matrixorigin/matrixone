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

package logservice

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type countingErrorRPCClient struct {
	err    error
	sends  atomic.Int32
	closes atomic.Int32
}

func (c *countingErrorRPCClient) Send(
	_ context.Context,
	_ string,
	request morpc.Message,
) (*morpc.Future, error) {
	c.sends.Add(1)
	request.(*RPCRequest).Release()
	return nil, c.err
}

func (c *countingErrorRPCClient) NewStream(
	context.Context,
	string,
	bool,
) (morpc.Stream, error) {
	return nil, c.err
}

func (c *countingErrorRPCClient) Ping(context.Context, string) error {
	return c.err
}

func (c *countingErrorRPCClient) Close() error {
	c.closes.Add(1)
	return nil
}

func (c *countingErrorRPCClient) CloseBackend() error {
	return nil
}

func TestHAKeeperClientConfigIsValidated(t *testing.T) {
	cfg := HAKeeperClientConfig{}
	cc1, err := NewCNHAKeeperClient(context.TODO(), "", cfg)
	assert.Nil(t, cc1)
	assert.Error(t, err)
	cc2, err := NewTNHAKeeperClient(context.TODO(), "", cfg)
	assert.Nil(t, cc2)
	assert.Error(t, err)
	cc3, err := NewLogHAKeeperClient(context.TODO(), "", cfg)
	assert.Nil(t, cc3)
	assert.Error(t, err)
}

func TestHAKeeperClientConstructorsRejectContextWithoutDeadline(t *testing.T) {
	original := newHAKeeperClientFunc
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		t.Fatal("constructor must not dial without a context deadline")
		return nil, nil
	}
	defer func() {
		newHAKeeperClientFunc = original
	}()

	cfg := HAKeeperClientConfig{}
	tests := []struct {
		name string
		fn   func(context.Context, string, HAKeeperClientConfig) (any, error)
	}{
		{
			name: "cluster",
			fn: func(ctx context.Context, sid string, cfg HAKeeperClientConfig) (any, error) {
				return NewClusterHAKeeperClient(ctx, sid, cfg)
			},
		},
		{
			name: "cn",
			fn: func(ctx context.Context, sid string, cfg HAKeeperClientConfig) (any, error) {
				return NewCNHAKeeperClient(ctx, sid, cfg)
			},
		},
		{
			name: "tn",
			fn: func(ctx context.Context, sid string, cfg HAKeeperClientConfig) (any, error) {
				return NewTNHAKeeperClient(ctx, sid, cfg)
			},
		},
		{
			name: "log",
			fn: func(ctx context.Context, sid string, cfg HAKeeperClientConfig) (any, error) {
				return NewLogHAKeeperClient(ctx, sid, cfg)
			},
		},
		{
			name: "proxy",
			fn: func(ctx context.Context, sid string, cfg HAKeeperClientConfig) (any, error) {
				return NewProxyHAKeeperClient(ctx, sid, cfg)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := tt.fn(context.TODO(), "", cfg)
			require.Nil(t, c)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		})
	}
}

func TestHAKeeperClientsCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		assert.NoError(t, c1.Close())
		c2, err := NewTNHAKeeperClient(ctx, "", cfg)
		assert.NoError(t, err)
		assert.NoError(t, c2.Close())
		c3, err := NewLogHAKeeperClient(ctx, "", cfg)
		assert.NoError(t, err)
		assert.NoError(t, c3.Close())
	}
	runServiceTest(t, true, true, fn)
}

func TestScheduleCommandPollUsesIndependentMORPCConnection(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(
		metadata.ServiceType_LOG,
		"schedule-command-test",
		logutil.GetGlobalLogger(),
	))
	requestPool := &sync.Pool{}
	requestPool.New = func() any {
		return &RPCRequest{pool: requestPool}
	}
	codec := morpc.NewMessageCodec(
		"",
		func() morpc.Message { return requestPool.Get().(*RPCRequest) },
		morpc.WithCodecEnableChecksum(),
		morpc.WithCodecMaxBodySize(defaultMaxMessageSize),
	)
	socketPath := "/tmp/mo-hakeeper-" + uuid.NewString() + ".sock"
	address := "unix://" + socketPath
	server, err := morpc.NewRPCServer("schedule-command-server", address, codec)
	require.NoError(t, err)
	heartbeatEntered := make(chan struct{}, 1)
	heartbeatRelease := make(chan struct{})
	var upgraded atomic.Bool
	var releaseHeartbeat sync.Once
	t.Cleanup(func() {
		releaseHeartbeat.Do(func() { close(heartbeatRelease) })
		require.NoError(t, server.Close())
		removeErr := os.Remove(socketPath)
		require.True(t, removeErr == nil || os.IsNotExist(removeErr), removeErr)
	})
	server.RegisterRequestHandler(func(
		ctx context.Context,
		message morpc.RPCMessage,
		_ uint64,
		session morpc.ClientSession,
	) error {
		request := message.Message.(*RPCRequest)
		defer request.Release()
		response := pb.Response{
			RequestID: request.RequestID,
			Method:    request.Method,
		}
		switch request.Method {
		case pb.CHECK_HAKEEPER:
			response.IsHAKeeper = true
		case pb.CN_HEARTBEAT:
			select {
			case heartbeatEntered <- struct{}{}:
			default:
			}
			select {
			case <-heartbeatRelease:
			case <-ctx.Done():
				return ctx.Err()
			}
		case pb.GET_SCHEDULE_COMMANDS:
			if !upgraded.Load() {
				response.ErrorCode, response.ErrorMessage = toErrorCode(
					moerr.NewNotSupported(ctx, "schedule-command polling"))
				break
			}
			response.CommandBatch = &pb.CommandBatch{BatchID: 7}
		default:
			return moerr.NewInternalErrorf(ctx, "unexpected request method %s", request.Method)
		}
		return session.Write(ctx, &RPCResponse{Response: response})
	})
	require.NoError(t, server.Start())

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := connectToHAKeeper(
		connectCtx,
		"",
		[]string{address},
		HAKeeperClientConfig{},
	)
	cancelConnect()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.close()) })
	managed := &managedHAKeeperClient{sid: "cn-1"}
	managed.mu.client = client

	// Admission happened while the endpoint behaved like an old HAKeeper. The
	// additive method must be one compatible no-op, not a permanent local gate.
	oldCtx, cancelOld := context.WithTimeout(context.Background(), time.Second)
	oldBatch, err := managed.GetScheduleCommands(oldCtx, pb.CNService)
	cancelOld()
	require.NoError(t, err)
	require.Empty(t, oldBatch.Commands)

	// Keep the managed generation and both MORPC clients alive while the same
	// endpoint upgrades. The next heartbeat is deliberately blocked, so only the
	// independent read can discover the new capability and make progress.
	upgraded.Store(true)

	heartbeatDone := make(chan error, 1)
	heartbeatCtx, cancelHeartbeat := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancelHeartbeat)
	go func() {
		_, err := client.sendCNHeartbeat(heartbeatCtx, pb.CNStoreHeartbeat{UUID: "cn-1"})
		heartbeatDone <- err
	}()
	select {
	case <-heartbeatEntered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not enter the synchronous MORPC handler")
	}

	pollCtx, cancelPoll := context.WithTimeout(context.Background(), time.Second)
	batch, err := managed.GetScheduleCommands(pollCtx, pb.CNService)
	cancelPoll()
	require.NoError(t, err)
	require.Equal(t, uint64(7), batch.BatchID)
	select {
	case err := <-heartbeatDone:
		t.Fatalf("heartbeat unexpectedly completed before its handler was released: %v", err)
	default:
	}

	releaseHeartbeat.Do(func() { close(heartbeatRelease) })
	require.NoError(t, <-heartbeatDone)
}

func TestScheduleCommandInitialPollDelayPreservesProgressBound(t *testing.T) {
	for _, serviceID := range []string{"cn-1", "cn-2", "tn-1"} {
		delay := ScheduleCommandInitialPollDelay(serviceID)
		require.GreaterOrEqual(t, delay, 750*time.Millisecond)
		require.LessOrEqual(t, delay, ScheduleCommandPollInterval)
		require.Equal(t, delay, ScheduleCommandInitialPollDelay(serviceID))
	}
	require.NotEqual(t,
		ScheduleCommandInitialPollDelay("cn-1"),
		ScheduleCommandInitialPollDelay("cn-2"),
	)
}

func TestAllocateIDByKeyWithRequestID(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		proceedHAKeeperToRunning(t, s.store)
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, client.Close())
		}()

		managed := client.(*managedHAKeeperClient)
		firstID, err := managed.AllocateIDByKeyWithRequestID(ctx, "bootstrap", 1, "cn-1")
		require.NoError(t, err)
		secondID, err := managed.AllocateIDByKeyWithRequestID(ctx, "bootstrap", 1, "cn-1")
		require.NoError(t, err)
		require.Equal(t, firstID, secondID)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientMethodsRejectContextWithoutDeadline(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cnClient, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, cnClient.Close())
		}()
		tnClient, err := NewTNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, tnClient.Close())
		}()
		logClient, err := NewLogHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, logClient.Close())
		}()
		proxyClient, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, proxyClient.Close())
		}()

		tests := []struct {
			name string
			fn   func(context.Context) error
		}{
			{
				name: "CheckLogServiceHealth",
				fn:   cnClient.CheckLogServiceHealth,
			},
			{
				name: "GetClusterDetails",
				fn: func(ctx context.Context) error {
					_, err := cnClient.GetClusterDetails(ctx)
					return err
				},
			},
			{
				name: "GetClusterState",
				fn: func(ctx context.Context) error {
					_, err := cnClient.GetClusterState(ctx)
					return err
				},
			},
			{
				name: "AllocateID",
				fn: func(ctx context.Context) error {
					_, err := cnClient.AllocateID(ctx)
					return err
				},
			},
			{
				name: "AllocateIDByKey",
				fn: func(ctx context.Context) error {
					_, err := cnClient.AllocateIDByKey(ctx, "test-key")
					return err
				},
			},
			{
				name: "AllocateIDByKeyWithBatch",
				fn: func(ctx context.Context) error {
					_, err := cnClient.AllocateIDByKeyWithBatch(ctx, "test-key-batch", 2)
					return err
				},
			},
			{
				name: "SendCNHeartbeat",
				fn: func(ctx context.Context) error {
					_, err := cnClient.SendCNHeartbeat(ctx, pb.CNStoreHeartbeat{})
					return err
				},
			},
			{
				name: "UpdateNonVotingReplicaNum",
				fn: func(ctx context.Context) error {
					return cnClient.UpdateNonVotingReplicaNum(ctx, 1)
				},
			},
			{
				name: "UpdateNonVotingLocality",
				fn: func(ctx context.Context) error {
					return cnClient.UpdateNonVotingLocality(ctx, pb.Locality{})
				},
			},
			{
				name: "GetBackupData",
				fn: func(ctx context.Context) error {
					_, err := cnClient.GetBackupData(ctx)
					return err
				},
			},
			{
				name: "SendTNHeartbeat",
				fn: func(ctx context.Context) error {
					_, err := tnClient.SendTNHeartbeat(ctx, pb.TNStoreHeartbeat{})
					return err
				},
			},
			{
				name: "SendLogHeartbeat",
				fn: func(ctx context.Context) error {
					_, err := logClient.SendLogHeartbeat(ctx, pb.LogStoreHeartbeat{})
					return err
				},
			},
			{
				name: "GetCNState",
				fn: func(ctx context.Context) error {
					_, err := proxyClient.GetCNState(ctx)
					return err
				},
			},
			{
				name: "UpdateCNLabel",
				fn: func(ctx context.Context) error {
					return proxyClient.UpdateCNLabel(ctx, pb.CNStoreLabel{})
				},
			},
			{
				name: "UpdateCNWorkState",
				fn: func(ctx context.Context) error {
					return proxyClient.UpdateCNWorkState(ctx, pb.CNWorkState{})
				},
			},
			{
				name: "PatchCNStore",
				fn: func(ctx context.Context) error {
					return proxyClient.PatchCNStore(ctx, pb.CNStateLabel{})
				},
			},
			{
				name: "DeleteCNStore",
				fn: func(ctx context.Context) error {
					return proxyClient.DeleteCNStore(ctx, pb.DeleteCNStore{})
				},
			},
			{
				name: "SendProxyHeartbeat",
				fn: func(ctx context.Context) error {
					_, err := proxyClient.SendProxyHeartbeat(ctx, pb.ProxyHeartbeat{})
					return err
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require.NotPanics(t, func() {
					err := tt.fn(context.Background())
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				})
			})
		}
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientsCanBeCreatedWithRetry(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		c := NewLogHAKeeperClientWithRetry(context.Background(), "", cfg)
		assert.NoError(t, c.Close())
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientCanNotConnectToNonHAKeeperNode(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
		_, err = NewTNHAKeeperClient(ctx, "", cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
		_, err = NewLogHAKeeperClient(ctx, "", cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
	}
	runServiceTest(t, false, true, fn)
}

func TestHAKeeperClientConnectByReverseProxy(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		testServiceAddress := s.cfg.LogServiceServiceAddr()
		done := false
		for i := 0; i < 1000; i++ {
			si, ok, err := GetShardInfo("", testServiceAddress, hakeeper.DefaultHAKeeperShardID)
			if err != nil || !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			done = true
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), si.ReplicaID)
			addr, ok := si.Replicas[si.ReplicaID]
			assert.True(t, ok)
			assert.Equal(t, testServiceAddress, addr)
			break
		}
		if !done {
			t.Fatalf("failed to get shard info")
		}
		// now shard info can be queried
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{"localhost:53033"}, // obvious not reachable
			DiscoveryAddress: testServiceAddress,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		c, err := NewLogHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		hb := s.store.getHeartbeatMessage()
		cb, err := c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.TNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendCNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
		}()

		// should be transparently handled
		cc := c1.(*managedHAKeeperClient)
		assert.NoError(t, cc.mu.client.close())
		cc.mu.client = nil

		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
			CommitID:       "c123",
		}
		_, err = c1.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)

		c2, err := NewTNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c2.Close())
		}()

		// should be transparently handled
		cc = c2.(*managedHAKeeperClient)
		assert.NoError(t, cc.mu.client.close())
		cc.mu.client = nil

		hb2 := pb.TNStoreHeartbeat{
			UUID:                 s.ID(),
			ServiceAddress:       "addr2",
			LogtailServerAddress: "addr3",
		}
		cb, err := c2.SendTNHeartbeat(ctx, hb2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		// should be transparently handled
		cc = c1.(*managedHAKeeperClient)
		assert.NoError(t, cc.mu.client.close())
		cc.mu.client = nil

		cd, err := c1.GetClusterDetails(ctx)
		require.NoError(t, err)
		cn := pb.CNStore{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
			WorkState:      metadata.WorkState_Working,
			UpTime:         cd.CNStores[0].UpTime,
			CommitID:       hb.CommitID,
		}
		tn := pb.TNStore{
			UUID:                 s.ID(),
			ServiceAddress:       "addr2",
			LogtailServerAddress: "addr3",
		}
		assert.Equal(t, []pb.CNStore{cn}, cd.CNStores)
		assert.Equal(t, []pb.TNStore{tn}, cd.TNStores)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendTNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewTNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		hb := pb.TNStoreHeartbeat{
			UUID: s.ID(),
		}
		cb, err := c.SendTNHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.TNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendTNHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientPollScheduleCommands(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		baseClient, err := NewTNHAKeeperClient(ctx, s.ID(), cfg)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, baseClient.Close())
		}()
		client := baseClient.(ScheduleCommandHAKeeperClient)
		activateCommandDelivery(t, ctx, s)

		command := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.TNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: s.ID(),
			},
		}
		require.NoError(t,
			s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{command}))

		batch, err := client.GetScheduleCommands(ctx, pb.TNService)
		require.NoError(t, err)
		require.Equal(t, []pb.ScheduleCommand{command}, batch.Commands)
		require.NotZero(t, batch.BatchID)

		retry, err := client.GetScheduleCommands(ctx, pb.TNService)
		require.NoError(t, err)
		require.Equal(t, batch, retry)

		delivered, err := baseClient.SendTNHeartbeat(ctx, pb.TNStoreHeartbeat{
			UUID:                        s.ID(),
			CommandDeliveryAckSupported: true,
		})
		require.NoError(t, err)
		require.Equal(t, batch, delivered)

		acked, err := baseClient.SendTNHeartbeat(ctx, pb.TNStoreHeartbeat{
			UUID:                        s.ID(),
			AckedCommandBatchID:         batch.BatchID,
			CommandDeliveryAckSupported: true,
		})
		require.NoError(t, err)
		require.Empty(t, acked.Commands)
		afterAck, err := client.GetScheduleCommands(ctx, pb.TNService)
		require.NoError(t, err)
		require.Empty(t, afterAck.Commands)
	}
	runServiceTest(t, true, true, fn)
}

func TestManagedHAKeeperClientCommandPollMakesOneAttempt(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	defer func() {
		newHAKeeperClientFunc = originalNew
	}()

	requestPool := &sync.Pool{}
	requestPool.New = func() any {
		return &RPCRequest{pool: requestPool}
	}
	transport := &countingErrorRPCClient{err: io.EOF}
	newInnerClient := func() *hakeeperClient {
		return &hakeeperClient{pool: requestPool, pollClient: transport}
	}
	var reconnectAttempts atomic.Int32
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		reconnectAttempts.Add(1)
		return newInnerClient(), nil
	}

	client := &managedHAKeeperClient{}
	client.mu.client = newInnerClient()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := client.GetScheduleCommands(ctx, pb.TNService)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))
	require.Equal(t, int32(1), transport.sends.Load())
	require.Zero(t, transport.closes.Load(),
		"a poll failure must not close the heartbeat's managed generation")
	require.Zero(t, reconnectAttempts.Load(),
		"the outer command worker owns the next retry cadence")

	// A later cadence gets exactly one new transport attempt. MORPC owns the
	// failed poll backend; polling must neither replace the managed generation
	// nor close the independent client between cadences.
	_, err = client.GetScheduleCommands(ctx, pb.TNService)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))
	require.Equal(t, int32(2), transport.sends.Load())
	require.Zero(t, transport.closes.Load())
	require.Zero(t, reconnectAttempts.Load(),
		"heartbeat exclusively owns managed-generation replacement")

	// With no admitted generation, polling returns immediately instead of
	// entering the heartbeat client's discovery/reconnect policy.
	empty := &managedHAKeeperClient{}
	_, err = empty.GetScheduleCommands(ctx, pb.TNService)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
	require.Zero(t, reconnectAttempts.Load())

	require.NoError(t, client.Close())
	require.Equal(t, int32(1), transport.closes.Load())
}

func TestManagedHAKeeperClientResetIsGenerationScoped(t *testing.T) {
	client := &managedHAKeeperClient{}
	oldClient := &hakeeperClient{}
	newClient := &hakeeperClient{}
	client.mu.client = oldClient

	snapshot, err := client.getPreparedClient(context.Background())
	require.NoError(t, err)
	require.Same(t, oldClient, snapshot)

	client.mu.Lock()
	client.mu.client = newClient
	client.mu.Unlock()
	client.resetClientIfCurrent(snapshot)

	client.mu.RLock()
	current := client.mu.client
	client.mu.RUnlock()
	require.Same(t, newClient, current,
		"a late failure from an old request must not close the replacement client")

	client.resetClientIfCurrent(newClient)
	client.mu.RLock()
	current = client.mu.client
	client.mu.RUnlock()
	require.Nil(t, current,
		"a failure from the current generation must invalidate that generation")
}

func TestManagedHAKeeperClientRejectsNilPreparedClient(t *testing.T) {
	original := newHAKeeperClientFunc
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		return nil, nil
	}
	defer func() {
		newHAKeeperClientFunc = original
	}()

	client := &managedHAKeeperClient{}
	prepared, err := client.getPreparedClient(context.Background())
	require.Nil(t, prepared)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
}

func TestScheduleCommandBatchFingerprintDeterministic(t *testing.T) {
	batch := pb.CommandBatch{
		Term:    7,
		BatchID: 11,
		Commands: []pb.ScheduleCommand{{
			UUID:        "tn-1",
			ServiceType: pb.TNService,
			ConfigChange: &pb.ConfigChange{
				InitialMembers: map[uint64]string{
					3: "c",
					1: "a",
					2: "b",
				},
			},
		}},
	}
	want := ScheduleCommandBatchFingerprint(batch)
	for range 100 {
		require.Equal(t, want, ScheduleCommandBatchFingerprint(batch))
	}

	batch.Term++
	batch.BatchID++
	require.Equal(t, want, ScheduleCommandBatchFingerprint(batch),
		"delivery metadata must not change command identity")
}

func TestFilterUnappliedScheduleCommandsAcrossGenerations(t *testing.T) {
	command := func(replicaID uint64) pb.ScheduleCommand {
		return pb.ScheduleCommand{
			UUID:        "tn-1",
			ServiceType: pb.TNService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.AddReplica,
				Replica: pb.Replica{
					ShardID:   1,
					ReplicaID: replicaID,
				},
				InitialMembers: map[uint64]string{
					2: "b",
					1: "a",
				},
			},
		}
	}
	first := command(1)
	second := command(2)
	firstID := pb.ScheduleCommandID{OriginBatchID: 10}
	secondID := pb.ScheduleCommandID{OriginBatchID: 11}

	filtered, applied, ok := FilterUnappliedScheduleCommands(
		pb.CommandBatch{
			BatchID:    10,
			Commands:   []pb.ScheduleCommand{first},
			CommandIDs: []pb.ScheduleCommandID{firstID},
		},
		nil,
	)
	require.True(t, ok)
	require.Equal(t, []pb.ScheduleCommand{first}, filtered)
	require.Len(t, applied, 1)

	filtered, next, ok := FilterUnappliedScheduleCommands(
		pb.CommandBatch{
			BatchID:    11,
			Commands:   []pb.ScheduleCommand{first, second},
			CommandIDs: []pb.ScheduleCommandID{firstID, secondID},
		},
		applied,
	)
	require.True(t, ok)
	require.Equal(t, []pb.ScheduleCommand{second}, filtered)
	require.Len(t, next, 2)

	filtered, next, ok = FilterUnappliedScheduleCommands(
		pb.CommandBatch{
			BatchID:    11,
			Commands:   []pb.ScheduleCommand{first, first},
			CommandIDs: []pb.ScheduleCommandID{firstID, secondID},
		},
		applied,
	)
	require.True(t, ok)
	require.Equal(t, []pb.ScheduleCommand{first}, filtered,
		"a newly scheduled identical command has a distinct identity")
	require.Len(t, next, 2)

	filtered, _, ok = FilterUnappliedScheduleCommands(
		pb.CommandBatch{BatchID: 12, Commands: []pb.ScheduleCommand{first}},
		applied,
	)
	require.False(t, ok)
	require.Empty(t, filtered, "a batch without stable command IDs must not be acknowledged")

	filtered, _, ok = FilterUnappliedScheduleCommands(
		pb.CommandBatch{
			BatchID:    12,
			Commands:   []pb.ScheduleCommand{first, second},
			CommandIDs: []pb.ScheduleCommandID{firstID, firstID},
		},
		applied,
	)
	require.False(t, ok)
	require.Empty(t, filtered, "duplicate command IDs must fail closed")
}

func TestIsRetryableScheduleCommand(t *testing.T) {
	command := pb.ScheduleCommand{
		Bootstrapping: true,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StartReplica,
		},
	}
	require.True(t, IsRetryableScheduleCommand(command))
	command.Bootstrapping = false
	require.False(t, IsRetryableScheduleCommand(command))
	command.Bootstrapping = true
	command.ConfigChange.ChangeType = pb.AddReplica
	require.False(t, IsRetryableScheduleCommand(command))
	command.ConfigChange = nil
	require.False(t, IsRetryableScheduleCommand(command))
}

func TestHAKeeperClientSendLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewLogHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()

		// should be transparently handled
		cc := c.(*managedHAKeeperClient)
		assert.NoError(t, cc.mu.client.close())
		cc.mu.client = nil

		hb := s.store.getHeartbeatMessage()
		cb, err := c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.TNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}

type notHAKeeperTestCase struct {
	name string
	run  func(*testing.T, *managedHAKeeperClient)
}

func testNotHAKeeperErrorIsHandled(t *testing.T, cases []notHAKeeperTestCase) {
	defer leaktest.AfterTest(t)()
	cfg1 := DefaultConfig()
	cfg1.UUID = uuid.New().String()
	cfg1.FS = vfs.NewStrictMem()
	cfg1.DeploymentID = 1
	cfg1.RTTMillisecond = 5
	cfg1.DataDir = "data-1"
	cfg1.LogServicePort = 9002
	cfg1.RaftPort = 9000
	cfg1.GossipPort = 9001
	cfg1.GossipSeedAddresses = []string{"127.0.0.1:9011"}
	cfg1.DisableWorkers = true
	cfg2 := DefaultConfig()
	cfg2.UUID = uuid.New().String()
	cfg2.FS = vfs.NewStrictMem()
	cfg2.DeploymentID = 1
	cfg2.RTTMillisecond = 5
	cfg2.DataDir = "data-2"
	cfg2.LogServicePort = 9012
	cfg2.RaftPort = 9010
	cfg2.GossipPort = 9011
	cfg2.GossipSeedAddresses = []string{"127.0.0.1:9001"}
	cfg2.DisableWorkers = true

	rt := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime("", rt)
	runtime.SetupServiceBasedRuntime(cfg1.UUID, rt)
	runtime.SetupServiceBasedRuntime(cfg2.UUID, rt)

	service1, err := NewService(cfg1,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service1.Close())
	}()
	service2, err := NewService(cfg2,
		newFS(),
		nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service2.Close())
	}()
	// service2 is HAKeeper
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = service2.ID()
	assert.NoError(t, service2.store.startHAKeeperReplica(1, peers, false))
	require.Eventually(t, func() bool {
		isLeader, _, err := service2.store.isLeaderHAKeeper()
		return err == nil && isLeader
	}, 10*time.Second, 10*time.Millisecond,
		"service2 did not become HAKeeper leader")
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// Each operation starts from a fresh client generation connected to
			// service1, which deliberately is not the HAKeeper. The two services
			// are immutable shared fixture state; client generations remain isolated.
			pool := &sync.Pool{}
			pool.New = func() interface{} {
				return &RPCRequest{pool: pool}
			}
			respPool := &sync.Pool{}
			respPool.New = func() interface{} {
				return &RPCResponse{pool: respPool}
			}
			cfg := HAKeeperClientConfig{
				ServiceAddresses: []string{
					cfg1.LogServiceServiceAddr(),
					cfg2.LogServiceServiceAddr(),
				},
			}
			c := &hakeeperClient{
				cfg:      cfg,
				pool:     pool,
				respPool: respPool,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cc, err := getRPCClient(
				ctx,
				"",
				cfg1.LogServiceServiceAddr(),
				c.respPool,
				defaultMaxMessageSize,
				false,
				0,
			)
			require.NoError(t, err)
			c.addr = cfg1.LogServiceServiceAddr()
			c.client = cc
			client := &managedHAKeeperClient{cfg: cfg}
			client.mu.client = c
			defer func() {
				require.NoError(t, client.Close())
			}()
			testCase.run(t, client)
		})
	}
}

func TestNotHAKeeperErrorIsHandled(t *testing.T) {
	checkGenerationReplaced := func(
		t *testing.T,
		c *managedHAKeeperClient,
		call func(context.Context) error,
	) {
		oldClient := c.getCurrentClient()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, call(ctx))
		require.NotSame(t, oldClient, c.getCurrentClient())
	}

	testNotHAKeeperErrorIsHandled(t, []notHAKeeperTestCase{
		{
			name: "get_cluster_details",
			run: func(t *testing.T, c *managedHAKeeperClient) {
				checkGenerationReplaced(t, c, func(ctx context.Context) error {
					_, err := c.GetClusterDetails(ctx)
					return err
				})
			},
		},
		{
			name: "send_cn_heartbeat",
			run: func(t *testing.T, c *managedHAKeeperClient) {
				checkGenerationReplaced(t, c, func(ctx context.Context) error {
					_, err := c.SendCNHeartbeat(ctx, pb.CNStoreHeartbeat{})
					return err
				})
			},
		},
		{
			name: "send_tn_heartbeat",
			run: func(t *testing.T, c *managedHAKeeperClient) {
				checkGenerationReplaced(t, c, func(ctx context.Context) error {
					_, err := c.SendTNHeartbeat(ctx, pb.TNStoreHeartbeat{})
					return err
				})
			},
		},
		{
			name: "send_log_heartbeat",
			run: func(t *testing.T, c *managedHAKeeperClient) {
				checkGenerationReplaced(t, c, func(ctx context.Context) error {
					_, err := c.SendLogHeartbeat(ctx, pb.LogStoreHeartbeat{})
					return err
				})
			},
		},
	})
}

func TestHAKeeperClientUpdateCNLabel(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		c2, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
			assert.NoError(t, c2.Close())
		}()

		label := pb.CNStoreLabel{
			UUID: s.ID(),
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err = c1.UpdateCNLabel(ctx, label)
		require.Error(t, err)

		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		_, err = c2.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)

		label = pb.CNStoreLabel{
			UUID: s.ID(),
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err = c1.UpdateCNLabel(ctx, label)
		require.NoError(t, err)

		state, err := c1.GetClusterState(ctx)
		info, ok1 := state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})
		require.NoError(t, err)

		label = pb.CNStoreLabel{
			UUID: s.ID(),
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
			},
		}
		err = c1.UpdateCNLabel(ctx, label)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 = state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		_, ok3 = info.Labels["role"]
		assert.False(t, ok3)
	}
	runServiceTest(t, true, true, fn)
}

func TestAllocateIDError(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		// inject bad address to make the client fail
		inner := c.(*managedHAKeeperClient)
		inner.mu.client.addr = "127.0.0.1:12345"

		_, err = c.AllocateID(ctx)
		require.Error(t, err)
	}
	runServiceTest(t, true, true, fn)
}

func TestAllocateBatchIDError(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		// inject bad address to make the client fail
		inner := c.(*managedHAKeeperClient)
		inner.mu.client.addr = "127.0.0.1:12345"

		_, err = c.AllocateIDByKeyWithBatch(ctx, "x", 2)
		require.Error(t, err)
	}
	runServiceTest(t, true, true, fn)
}

func TestNormalizeHAKeeperClientError(t *testing.T) {
	ctx := context.Background()

	err := normalizeHAKeeperClientError(ctx, io.EOF)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))

	err = normalizeHAKeeperClientError(ctx, net.ErrClosed)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))

	err = normalizeHAKeeperClientError(ctx, context.DeadlineExceeded)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPrepareClientLockedNormalizesInitialConnectionError(t *testing.T) {
	original := newHAKeeperClientFunc
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		return nil, net.ErrClosed
	}
	defer func() {
		newHAKeeperClientFunc = original
	}()

	c := &managedHAKeeperClient{}
	_, err := c.getPreparedClient(context.Background())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))
}

func TestNewManagedHAKeeperClientNormalizesInitialConnectionError(t *testing.T) {
	original := newHAKeeperClientFunc
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		return nil, net.ErrClosed
	}
	defer func() {
		newHAKeeperClientFunc = original
	}()

	_, err := newManagedHAKeeperClient(context.Background(), "", HAKeeperClientConfig{})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF))
}

func TestHAKeeperClientRetryableEOFError(t *testing.T) {
	c := &managedHAKeeperClient{}
	ctx := context.Background()

	require.True(t, c.isRetryableError(io.EOF))
	require.True(t, c.isRetryableError(io.ErrUnexpectedEOF))
	require.True(t, c.isRetryableError(moerr.NewUnexpectedEOF(ctx, io.EOF.Error())))
}

func TestAllocateIDRetriesPrepareClientError(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDFunc
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDFunc = originalSend
	}()

	attempts := 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		attempts++
		if attempts == 1 {
			return nil, net.ErrClosed
		}
		return &hakeeperClient{}, nil
	}

	sendCalls := 0
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		sendCalls++
		require.Empty(t, key)
		require.Equal(t, uint64(2), batch)
		return 42, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstID, err := c.AllocateID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(42), firstID)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, sendCalls)
}

func TestAllocateIDRetriesPrepareClientErrorUntilContextDone(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	attempts := 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		attempts++
		return nil, net.ErrClosed
	}
	hakeeperClientRetryInterval = 20 * time.Millisecond

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()

	_, err := c.AllocateID(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, attempts, 2)
	require.Less(t, attempts, 10)
}

func TestAllocateIDRetriesEOFSendError(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDFunc = originalSend
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	hakeeperClientRetryInterval = 0
	attempts := 0
	clients := []*hakeeperClient{{}, {}}
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		client := clients[attempts]
		attempts++
		return client, nil
	}

	sendCalls := 0
	sendCNAllocateIDFunc = func(
		client *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		sendCalls++
		require.Empty(t, key)
		require.Equal(t, uint64(2), batch)
		require.Same(t, clients[sendCalls-1], client)
		if sendCalls == 1 {
			return 0, io.EOF
		}
		return 42, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstID, err := c.AllocateID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(42), firstID)
	require.Equal(t, 2, attempts)
	require.Equal(t, 2, sendCalls)
}

func TestAllocateIDByKeyWithRequestIDRetriesLostResponse(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDWithRequestIDFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDWithRequestIDFunc = originalSend
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	hakeeperClientRetryInterval = 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		return &hakeeperClient{}, nil
	}

	attempts := 0
	sendCNAllocateIDWithRequestIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
		requestID string,
	) (uint64, error) {
		attempts++
		require.Equal(t, "bootstrap", key)
		require.Equal(t, uint64(1), batch)
		require.Equal(t, "cn-1", requestID)
		if attempts == 1 {
			// The allocation was committed, but the reply did not reach the CN.
			return 0, io.ErrUnexpectedEOF
		}
		return 1, nil
	}

	c := &managedHAKeeperClient{cfg: HAKeeperClientConfig{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	id, err := c.AllocateIDByKeyWithRequestID(ctx, "bootstrap", 1, "cn-1")
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
	require.Equal(t, 2, attempts)
}

func TestAllocateIDByKeyWithRequestIDRejectsInvalidInputBeforeRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c := &managedHAKeeperClient{}

	tests := []struct {
		name      string
		ctx       context.Context
		key       string
		batchSize uint64
		requestID string
		errCode   uint16
	}{
		{
			name:      "context without deadline",
			ctx:       context.Background(),
			key:       "bootstrap",
			batchSize: 1,
			requestID: "cn-1",
			errCode:   moerr.ErrInvalidInput,
		},
		{
			name:      "empty key",
			ctx:       ctx,
			batchSize: 1,
			requestID: "cn-1",
			errCode:   moerr.ErrInternal,
		},
		{
			name:      "batch is not one",
			ctx:       ctx,
			key:       "bootstrap",
			batchSize: 2,
			requestID: "cn-1",
			errCode:   moerr.ErrInvalidInput,
		},
		{
			name:      "empty request ID",
			ctx:       ctx,
			key:       "bootstrap",
			batchSize: 1,
			errCode:   moerr.ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.AllocateIDByKeyWithRequestID(tt.ctx, tt.key, tt.batchSize, tt.requestID)
			require.True(t, moerr.IsMoErrCode(err, tt.errCode))
			require.Nil(t, c.mu.client)
		})
	}
}

func TestAllocateIDByKeyWithRequestIDRetriesPrepareClientError(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDWithRequestIDFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDWithRequestIDFunc = originalSend
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	hakeeperClientRetryInterval = 0
	newCalls := 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		newCalls++
		if newCalls == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		return &hakeeperClient{}, nil
	}

	sendCalls := 0
	sendCNAllocateIDWithRequestIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		_ uint64,
		_ string,
	) (uint64, error) {
		sendCalls++
		return 42, nil
	}

	c := &managedHAKeeperClient{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	id, err := c.AllocateIDByKeyWithRequestID(ctx, "bootstrap", 1, "cn-1")
	require.NoError(t, err)
	require.Equal(t, uint64(42), id)
	require.Equal(t, 2, newCalls)
	require.Equal(t, 1, sendCalls)
}

func TestAllocateIDByKeyWithRequestIDReturnsNonRetryableSendError(t *testing.T) {
	originalSend := sendCNAllocateIDWithRequestIDFunc
	defer func() {
		sendCNAllocateIDWithRequestIDFunc = originalSend
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sendErr := moerr.NewInternalError(ctx, "send failed")
	sendCNAllocateIDWithRequestIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		_ uint64,
		_ string,
	) (uint64, error) {
		return 0, sendErr
	}

	c := &managedHAKeeperClient{}
	c.mu.client = &hakeeperClient{}
	_, err := c.AllocateIDByKeyWithRequestID(ctx, "bootstrap", 1, "cn-1")
	require.ErrorIs(t, err, sendErr)
	require.Nil(t, c.mu.client)
}

func TestAllocateBatchIDRetriesPrepareClientError(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDFunc
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDFunc = originalSend
	}()

	attempts := 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		attempts++
		if attempts == 1 {
			return nil, net.ErrClosed
		}
		return &hakeeperClient{}, nil
	}

	sendCalls := 0
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		sendCalls++
		require.Equal(t, "x", key)
		require.Equal(t, uint64(2), batch)
		return 100, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.allocMu.allocIDByKey = make(map[string]*allocID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstID, err := c.AllocateIDByKeyWithBatch(ctx, "x", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(100), firstID)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, sendCalls)
}

func TestAllocateBatchIDRetriesPrepareClientErrorUntilContextDone(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	attempts := 0
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		attempts++
		return nil, net.ErrClosed
	}
	hakeeperClientRetryInterval = 20 * time.Millisecond

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.allocMu.allocIDByKey = make(map[string]*allocID)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()

	_, err := c.AllocateIDByKeyWithBatch(ctx, "x", 2)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, attempts, 2)
	require.Less(t, attempts, 10)
}

func TestAllocateBatchIDRetriesEOFSendError(t *testing.T) {
	originalNew := newHAKeeperClientFunc
	originalSend := sendCNAllocateIDFunc
	originalRetryInterval := hakeeperClientRetryInterval
	defer func() {
		newHAKeeperClientFunc = originalNew
		sendCNAllocateIDFunc = originalSend
		hakeeperClientRetryInterval = originalRetryInterval
	}()

	hakeeperClientRetryInterval = 0
	attempts := 0
	clients := []*hakeeperClient{{}, {}}
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		client := clients[attempts]
		attempts++
		return client, nil
	}

	sendCalls := 0
	sendCNAllocateIDFunc = func(
		client *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		sendCalls++
		require.Equal(t, "x", key)
		require.Equal(t, uint64(2), batch)
		require.Same(t, clients[sendCalls-1], client)
		if sendCalls == 1 {
			return 0, io.EOF
		}
		return 100, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.allocMu.allocIDByKey = make(map[string]*allocID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstID, err := c.AllocateIDByKeyWithBatch(ctx, "x", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(100), firstID)
	require.Equal(t, 2, attempts)
	require.Equal(t, 2, sendCalls)
}

func TestAllocateBatchIDKeepsClientOnContextError(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	tests := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &hakeeperClient{}
			sendCalls := 0
			sendCNAllocateIDFunc = func(
				actualClient *hakeeperClient,
				_ context.Context,
				key string,
				batch uint64,
			) (uint64, error) {
				sendCalls++
				require.Same(t, client, actualClient)
				require.Equal(t, "x", key)
				require.Equal(t, uint64(2), batch)
				if sendCalls == 1 {
					return 0, tt.err
				}
				return 100, nil
			}

			c := &managedHAKeeperClient{
				cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
			}
			c.mu.client = client
			c.allocMu.allocIDByKey = make(map[string]*allocID)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			_, err := c.AllocateIDByKeyWithBatch(ctx, "x", 2)
			require.ErrorIs(t, err, tt.err)
			require.Same(t, client, c.mu.client,
				"a caller-scoped context error must not close the shared client")

			firstID, err := c.AllocateIDByKeyWithBatch(ctx, "x", 2)
			require.NoError(t, err)
			require.Equal(t, uint64(100), firstID)
			require.Equal(t, 2, sendCalls)
		})
	}
}

func TestAllocateIDByKeyRejectsExpiredContextBeforeRPC(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	var sendCalls atomic.Int64
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		_ uint64,
	) (uint64, error) {
		sendCalls.Add(1)
		return 100, nil
	}

	client := &hakeeperClient{}
	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.mu.client = client
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()

	_, err := c.AllocateIDByKeyWithBatch(ctx, "connection", 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, sendCalls.Load())
	require.Same(t, client, c.mu.client)
}

func TestAllocateIDByKeyWaiterHonorsContextDuringRefill(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	type refillRequest struct {
		key   string
		batch uint64
	}
	refillStarted := make(chan refillRequest, 1)
	releaseRefill := make(chan struct{})
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		refillStarted <- refillRequest{key: key, batch: batch}
		<-releaseRefill
		return 100, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.mu.client = &hakeeperClient{}
	c.allocMu.allocIDByKey = make(map[string]*allocID)
	leaderDone := make(chan error, 1)
	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), time.Second)
	defer cancelLeader()
	go func() {
		_, err := c.AllocateIDByKeyWithBatch(leaderCtx, "connection", 2)
		leaderDone <- err
	}()
	request := <-refillStarted
	require.Equal(t, "connection", request.key)
	require.Equal(t, uint64(2), request.batch)

	waiterDone := make(chan error, 1)
	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWaiter()
	go func() {
		_, err := c.AllocateIDByKeyWithBatch(waiterCtx, "connection", 2)
		waiterDone <- err
	}()

	var waiterErr error
	returnedBeforeRefill := false
	select {
	case waiterErr = <-waiterDone:
		returnedBeforeRefill = true
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseRefill)
	require.NoError(t, <-leaderDone)
	if !returnedBeforeRefill {
		waiterErr = <-waiterDone
		t.Fatalf("waiter remained blocked behind the refill after its context expired: %v", waiterErr)
	}
	require.ErrorIs(t, waiterErr, context.DeadlineExceeded)
}

func TestAllocateIDByKeySlowRefillDoesNotBlockCachedOtherKey(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	refillStarted := make(chan struct{})
	releaseRefill := make(chan struct{})
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		_ uint64,
	) (uint64, error) {
		close(refillStarted)
		<-releaseRefill
		return 100, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	c.mu.client = &hakeeperClient{}
	cached := c.getAllocID("cached")
	cached.nextID = 42
	cached.lastID = 42
	leaderDone := make(chan error, 1)
	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), time.Second)
	defer cancelLeader()
	go func() {
		_, err := c.AllocateIDByKeyWithBatch(leaderCtx, "slow", 2)
		leaderDone <- err
	}()
	<-refillStarted

	cachedDone := make(chan struct {
		id  uint64
		err error
	}, 1)
	cachedCtx, cancelCached := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelCached()
	go func() {
		id, err := c.AllocateIDByKeyWithBatch(cachedCtx, "cached", 2)
		cachedDone <- struct {
			id  uint64
			err error
		}{id: id, err: err}
	}()

	var cachedResult struct {
		id  uint64
		err error
	}
	returnedBeforeRefill := false
	select {
	case cachedResult = <-cachedDone:
		returnedBeforeRefill = true
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseRefill)
	require.NoError(t, <-leaderDone)
	if !returnedBeforeRefill {
		cachedResult = <-cachedDone
		t.Fatalf("cached allocation for another key waited behind refill: %v", cachedResult.err)
	}
	require.NoError(t, cachedResult.err)
	require.Equal(t, uint64(42), cachedResult.id)
}

func TestAllocateIDByKeyBurstSharesRefills(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	const (
		connections = 1000
		batchSize   = 100
	)
	refillStarted := make(chan struct{})
	releaseRefill := make(chan struct{})
	var firstRefill sync.Once
	var sendCalls atomic.Int64
	var nextID atomic.Uint64
	nextID.Store(101)
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		batch uint64,
	) (uint64, error) {
		sendCalls.Add(1)
		firstRefill.Do(func() {
			close(refillStarted)
			<-releaseRefill
		})
		return nextID.Add(batch) - batch, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: batchSize},
	}
	c.mu.client = &hakeeperClient{}
	ids := c.getAllocID("connection")
	// The incident entered the burst with 92 IDs left in the current batch.
	ids.nextID = 9
	ids.lastID = 100

	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(connections)
	results := make(chan struct {
		id  uint64
		err error
	}, connections)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range connections {
		go func() {
			ready.Done()
			<-start
			id, err := c.AllocateIDByKeyWithBatch(ctx, "connection", batchSize)
			results <- struct {
				id  uint64
				err error
			}{id: id, err: err}
		}()
	}
	ready.Wait()
	close(start)
	<-refillStarted
	close(releaseRefill)

	seen := make(map[uint64]struct{}, connections)
	for range connections {
		result := <-results
		require.NoError(t, result.err)
		_, exists := seen[result.id]
		require.False(t, exists, "duplicate ID %d", result.id)
		seen[result.id] = struct{}{}
	}
	require.Len(t, seen, connections)
	for id := uint64(9); id <= 1008; id++ {
		_, ok := seen[id]
		require.True(t, ok, "missing ID %d", id)
	}
	require.Equal(t, int64(10), sendCalls.Load())
}

func TestAllocateIDByKeyWaiterRetriesAfterRefillFailure(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	firstRefillStarted := make(chan struct{})
	var sendCalls atomic.Int64
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		ctx context.Context,
		_ string,
		_ uint64,
	) (uint64, error) {
		if sendCalls.Add(1) == 1 {
			close(firstRefillStarted)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 200, nil
	}

	c := &managedHAKeeperClient{
		cfg: HAKeeperClientConfig{AllocateIDBatch: 2},
	}
	client := &hakeeperClient{}
	c.mu.client = client
	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelLeader()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := c.AllocateIDByKeyWithBatch(leaderCtx, "connection", 2)
		leaderDone <- err
	}()
	<-firstRefillStarted

	waiterCtx, cancelWaiter := context.WithTimeout(context.Background(), time.Second)
	defer cancelWaiter()
	waiterID, waiterErr := c.AllocateIDByKeyWithBatch(waiterCtx, "connection", 2)
	require.NoError(t, waiterErr)
	require.Equal(t, uint64(200), waiterID)
	require.ErrorIs(t, <-leaderDone, context.DeadlineExceeded)
	require.Equal(t, int64(2), sendCalls.Load())
	require.Same(t, client, c.mu.client)
}

func TestAllocateIDConsumesEntireBatch(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	tests := []struct {
		name        string
		key         string
		allocateIDs func(context.Context, *managedHAKeeperClient) ([]uint64, error)
	}{
		{
			name: "shared",
			allocateIDs: func(ctx context.Context, c *managedHAKeeperClient) ([]uint64, error) {
				ids := make([]uint64, 0, 3)
				for range 3 {
					id, err := c.AllocateID(ctx)
					if err != nil {
						return nil, err
					}
					ids = append(ids, id)
				}
				return ids, nil
			},
		},
		{
			name: "keyed",
			key:  "key",
			allocateIDs: func(ctx context.Context, c *managedHAKeeperClient) ([]uint64, error) {
				ids := make([]uint64, 0, 3)
				for range 3 {
					id, err := c.AllocateIDByKeyWithBatch(ctx, "key", 3)
					if err != nil {
						return nil, err
					}
					ids = append(ids, id)
				}
				return ids, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sendCalls := 0
			sendCNAllocateIDFunc = func(
				_ *hakeeperClient,
				_ context.Context,
				key string,
				batch uint64,
			) (uint64, error) {
				sendCalls++
				require.Equal(t, tt.key, key)
				require.Equal(t, uint64(3), batch)
				return uint64((sendCalls-1)*3 + 1), nil
			}

			c := &managedHAKeeperClient{
				cfg: HAKeeperClientConfig{AllocateIDBatch: 3},
			}
			c.mu.client = &hakeeperClient{}
			c.allocMu.allocIDByKey = make(map[string]*allocID)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			ids, err := tt.allocateIDs(ctx, c)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2, 3}, ids)
			require.Equal(t, 1, sendCalls)
		})
	}
}

func TestAllocateBatchOneIDsRemainUniqueAcrossClients(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	nextID := uint64(1)
	sendCalls := 0
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		key string,
		batch uint64,
	) (uint64, error) {
		require.Equal(t, "locker", key)
		require.Equal(t, uint64(1), batch)
		sendCalls++
		firstID := nextID
		nextID += batch
		return firstID, nil
	}

	newClient := func() *managedHAKeeperClient {
		c := &managedHAKeeperClient{}
		c.mu.client = &hakeeperClient{}
		c.allocMu.allocIDByKey = make(map[string]*allocID)
		return c
	}
	clientA := newClient()
	clientB := newClient()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	a1, err := clientA.AllocateIDByKeyWithBatch(ctx, "locker", 1)
	require.NoError(t, err)
	a2, err := clientA.AllocateIDByKeyWithBatch(ctx, "locker", 1)
	require.NoError(t, err)
	b1, err := clientB.AllocateIDByKeyWithBatch(ctx, "locker", 1)
	require.NoError(t, err)

	require.Equal(t, []uint64{1, 2, 3}, []uint64{a1, a2, b1})
	require.Equal(t, 3, sendCalls)
}

func TestAllocateIDByKeyRejectsZeroBatchBeforeRPC(t *testing.T) {
	originalSend := sendCNAllocateIDFunc
	defer func() {
		sendCNAllocateIDFunc = originalSend
	}()

	sendCalls := 0
	sendCNAllocateIDFunc = func(
		_ *hakeeperClient,
		_ context.Context,
		_ string,
		_ uint64,
	) (uint64, error) {
		sendCalls++
		return 1, nil
	}

	c := &managedHAKeeperClient{}
	c.mu.client = &hakeeperClient{}
	c.allocMu.allocIDByKey = make(map[string]*allocID)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := c.AllocateIDByKeyWithBatch(ctx, "key", 0)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.Equal(t, 0, sendCalls)
}

func TestHAKeeperClientUpdateCNWorkState(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		c2, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
			assert.NoError(t, c2.Close())
		}()

		workState := pb.CNWorkState{
			UUID:  s.ID(),
			State: metadata.WorkState_Unknown,
		}
		err = c1.UpdateCNWorkState(ctx, workState)
		require.Error(t, err)

		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		_, err = c2.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)

		workState = pb.CNWorkState{
			UUID:  s.ID(),
			State: metadata.WorkState_Working,
		}
		err = c1.UpdateCNWorkState(ctx, workState)
		require.NoError(t, err)

		state, err := c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 := state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Working, info.WorkState)

		workState = pb.CNWorkState{
			UUID:  s.ID(),
			State: metadata.WorkState_Draining,
		}
		err = c1.UpdateCNWorkState(ctx, workState)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 = state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Draining, info.WorkState)

		workState = pb.CNWorkState{
			UUID:  s.ID(),
			State: metadata.WorkState_Working,
		}
		err = c1.UpdateCNWorkState(ctx, workState)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 = state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Working, info.WorkState)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientPatchCNStore(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		c2, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
			assert.NoError(t, c2.Close())
		}()

		stateLabel := pb.CNStateLabel{
			UUID:  s.ID(),
			State: metadata.WorkState_Unknown,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err = c1.PatchCNStore(ctx, stateLabel)
		require.Error(t, err)

		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		_, err = c2.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)

		stateLabel = pb.CNStateLabel{
			UUID:  s.ID(),
			State: metadata.WorkState_Working,
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
				"role":    {Labels: []string{"1", "2"}},
			},
		}
		err = c1.PatchCNStore(ctx, stateLabel)
		require.NoError(t, err)

		state, err := c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 := state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Working, info.WorkState)
		labels1, ok2 := info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 := info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		stateLabel = pb.CNStateLabel{
			UUID:  s.ID(),
			State: metadata.WorkState_Draining,
		}
		err = c1.PatchCNStore(ctx, stateLabel)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 = state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Draining, info.WorkState)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		labels2, ok3 = info.Labels["role"]
		assert.True(t, ok3)
		assert.Equal(t, labels2.Labels, []string{"1", "2"})

		stateLabel = pb.CNStateLabel{
			UUID: s.ID(),
			Labels: map[string]metadata.LabelList{
				"account": {Labels: []string{"a", "b"}},
			},
		}
		err = c1.PatchCNStore(ctx, stateLabel)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		info, ok1 = state.CNState.Stores[s.ID()]
		assert.True(t, ok1)
		require.Equal(t, metadata.WorkState_Working, info.WorkState)
		labels1, ok2 = info.Labels["account"]
		assert.True(t, ok2)
		assert.Equal(t, labels1.Labels, []string{"a", "b"})
		labels2, ok3 = info.Labels["role"]
		assert.False(t, ok3)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientDeleteCNStore(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		c2, err := NewCNHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
			assert.NoError(t, c2.Close())
		}()

		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		_, err = c2.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)
		state, err := c1.GetClusterState(ctx)
		require.NoError(t, err)
		_, ok := state.CNState.Stores[s.ID()]
		assert.True(t, ok)

		cnStore := pb.DeleteCNStore{
			StoreID: s.ID(),
		}
		err = c1.DeleteCNStore(ctx, cnStore)
		require.NoError(t, err)

		state, err = c1.GetClusterState(ctx)
		require.NoError(t, err)
		_, ok = state.CNState.Stores[s.ID()]
		assert.False(t, ok)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendProxyHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewProxyHAKeeperClient(ctx, "", cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
		}()

		hb := pb.ProxyHeartbeat{
			UUID:          s.ID(),
			ListenAddress: "addr1",
		}
		cb, err := c1.SendProxyHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		cd, err := c1.GetClusterDetails(ctx)
		require.NoError(t, err)
		p := pb.ProxyStore{
			UUID:          s.ID(),
			ListenAddress: "addr1",
		}
		assert.Equal(t, []pb.ProxyStore{p}, cd.ProxyStores)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientCheckLogServiceHealth(t *testing.T) {
	t.Run("no tn stores", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			cfg := HAKeeperClientConfig{
				ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c, err := NewClusterHAKeeperClient(ctx, "", cfg)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()
			err = c.CheckLogServiceHealth(ctx)
			require.NoError(t, err)
		}
		runServiceTest(t, true, true, fn)
	})

	t.Run("ok", func(t *testing.T) {
		fn := func(t *testing.T, s *Service) {
			peers := map[uint64]dragonboat.Target{100: s.ID()}
			require.NoError(t, s.store.startReplica(1, 100, peers, false))
			require.Eventually(t, func() bool {
				_, _, ok, err := s.store.nh.GetLeaderID(1)
				return err == nil && ok
			}, 5*time.Second, 10*time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := pb.Request{
				Method: pb.TN_HEARTBEAT,
				TNHeartbeat: &pb.TNStoreHeartbeat{
					UUID: "uuid1",
					Shards: []pb.TNShardInfo{
						{
							ShardID:   2,
							ReplicaID: 100,
						},
					},
				},
			}
			resp := s.handleTNHeartbeat(ctx, req)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

			req = pb.Request{
				Method: pb.LOG_HEARTBEAT,
				LogHeartbeat: &pb.LogStoreHeartbeat{
					UUID: s.ID(),
					Replicas: []pb.LogReplicaInfo{
						{
							LogShardInfo: pb.LogShardInfo{
								ShardID: 1,
								Replicas: map[uint64]string{
									100: "uuid1",
								},
							},
						},
					},
				},
			}
			resp = s.handleLogHeartbeat(ctx, req)
			assert.Equal(t, uint32(moerr.Ok), resp.ErrorCode)

			cfg := HAKeeperClientConfig{
				ServiceAddresses: []string{s.cfg.LogServiceServiceAddr()},
			}
			c, err := NewClusterHAKeeperClient(ctx, "", cfg)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()
			err = c.CheckLogServiceHealth(ctx)
			require.NoError(t, err)
		}
		runServiceTest(t, true, true, fn)
	})
}

func Test_NewLogHAKeeperClientWithRetry(t *testing.T) {
	original := newHAKeeperClientFunc
	attempted := make(chan struct{}, 2)
	newHAKeeperClientFunc = func(
		context.Context,
		string,
		HAKeeperClientConfig,
	) (*hakeeperClient, error) {
		attempted <- struct{}{}
		return nil, moerr.NewInternalErrorNoCtx("injected creation failure")
	}
	defer func() {
		newHAKeeperClientFunc = original
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := HAKeeperClientConfig{
		DiscoveryAddress: "unused",
	}
	done := make(chan ClusterHAKeeperClient, 1)
	go func() {
		done <- NewLogHAKeeperClientWithRetry(ctx, "", cfg)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-attempted:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "HAKeeper client retry was not attempted")
		}
	}
	cancel()

	select {
	case client := <-done:
		require.Nil(t, client)
	case <-time.After(time.Second):
		require.FailNow(t, "retry backoff did not observe context cancellation")
	}
}
