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
	"sync"
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
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestHAKeeperClientConfigIsValidated(t *testing.T) {
	cfg := HAKeeperClientConfig{}
	cc1, err := NewCNHAKeeperClient(context.TODO(), cfg)
	assert.Nil(t, cc1)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
	cc2, err := NewDNHAKeeperClient(context.TODO(), cfg)
	assert.Nil(t, cc2)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
	cc3, err := NewLogHAKeeperClient(context.TODO(), cfg)
	assert.Nil(t, cc3)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
}

func TestHAKeeperClientsCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		assert.NoError(t, c1.Close())
		c2, err := NewDNHAKeeperClient(ctx, cfg)
		assert.NoError(t, err)
		assert.NoError(t, c2.Close())
		c3, err := NewLogHAKeeperClient(ctx, cfg)
		assert.NoError(t, err)
		assert.NoError(t, c3.Close())
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientCanNotConnectToNonHAKeeperNode(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := NewCNHAKeeperClient(ctx, cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
		_, err = NewDNHAKeeperClient(ctx, cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
		_, err = NewLogHAKeeperClient(ctx, cfg)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper))
	}
	runServiceTest(t, false, true, fn)
}

func TestHAKeeperClientConnectByReverseProxy(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		done := false
		for i := 0; i < 1000; i++ {
			si, ok, err := GetShardInfo(testServiceAddress, hakeeper.DefaultHAKeeperShardID)
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
		c, err := NewLogHAKeeperClient(ctx, cfg)
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
			ServiceType: pb.DNService,
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
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, cfg)
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
		}
		_, err = c1.SendCNHeartbeat(ctx, hb)
		require.NoError(t, err)

		c2, err := NewDNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c2.Close())
		}()

		// should be transparently handled
		cc = c2.(*managedHAKeeperClient)
		assert.NoError(t, cc.mu.client.close())
		cc.mu.client = nil

		hb2 := pb.DNStoreHeartbeat{
			UUID:                 s.ID(),
			ServiceAddress:       "addr2",
			LogtailServerAddress: "addr3",
		}
		cb, err := c2.SendDNHeartbeat(ctx, hb2)
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
		}
		dn := pb.DNStore{
			UUID:                 s.ID(),
			ServiceAddress:       "addr2",
			LogtailServerAddress: "addr3",
		}
		assert.Equal(t, []pb.CNStore{cn}, cd.CNStores)
		assert.Equal(t, []pb.DNStore{dn}, cd.DNStores)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendDNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewDNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		hb := pb.DNStoreHeartbeat{
			UUID: s.ID(),
		}
		cb, err := c.SendDNHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.DNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendDNHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewLogHAKeeperClient(ctx, cfg)
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
			ServiceType: pb.DNService,
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

func testNotHAKeeperErrorIsHandled(t *testing.T, fn func(*testing.T, *managedHAKeeperClient)) {
	defer leaktest.AfterTest(t)()
	cfg1 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9002",
		RaftAddress:         "127.0.0.1:9000",
		GossipAddress:       "127.0.0.1:9001",
		GossipSeedAddresses: []string{"127.0.0.1:9011"},
		DisableWorkers:      true,
	}
	cfg2 := Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-2",
		ServiceAddress:      "127.0.0.1:9012",
		RaftAddress:         "127.0.0.1:9010",
		GossipAddress:       "127.0.0.1:9011",
		GossipSeedAddresses: []string{"127.0.0.1:9001"},
		DisableWorkers:      true,
	}
	cfg1.Fill()
	service1, err := NewService(cfg1,
		testutil.NewFS(),
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
			return true
		}),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, service1.Close())
	}()
	cfg2.Fill()
	service2, err := NewService(cfg2,
		testutil.NewFS(),
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
	// manually construct a HAKeeper client that is connected to service1
	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	cfg := HAKeeperClientConfig{
		ServiceAddresses: []string{cfg1.ServiceAddress, cfg2.ServiceAddress},
	}
	c := &hakeeperClient{
		cfg:      cfg,
		pool:     pool,
		respPool: respPool,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cc, err := getRPCClient(ctx, cfg1.ServiceAddress, c.respPool, defaultMaxMessageSize, false)
	require.NoError(t, err)
	c.addr = cfg1.ServiceAddress
	c.client = cc
	client := &managedHAKeeperClient{cfg: cfg}
	client.mu.client = c
	defer func() {
		require.NoError(t, client.Close())
	}()
	fn(t, client)
}

func TestGetClusterDetailsWhenNotConnectedToHAKeeper(t *testing.T) {
	fn := func(t *testing.T, c *managedHAKeeperClient) {
		oldc := c.mu.client
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.GetClusterDetails(ctx)
		require.NoError(t, err)
		require.True(t, oldc != c.mu.client)
	}
	testNotHAKeeperErrorIsHandled(t, fn)
}

func TestSendCNHeartbeatWhenNotConnectedToHAKeeper(t *testing.T) {
	fn := func(t *testing.T, c *managedHAKeeperClient) {
		oldc := c.mu.client
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.SendCNHeartbeat(ctx, pb.CNStoreHeartbeat{})
		require.NoError(t, err)
		require.True(t, oldc != c.mu.client)
	}
	testNotHAKeeperErrorIsHandled(t, fn)
}

func TestSendDNHeartbeatWhenNotConnectedToHAKeeper(t *testing.T) {
	fn := func(t *testing.T, c *managedHAKeeperClient) {
		oldc := c.mu.client
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.SendDNHeartbeat(ctx, pb.DNStoreHeartbeat{})
		require.NoError(t, err)
		require.True(t, oldc != c.mu.client)
	}
	testNotHAKeeperErrorIsHandled(t, fn)
}

func TestSendLogHeartbeatWhenNotConnectedToHAKeeper(t *testing.T) {
	fn := func(t *testing.T, c *managedHAKeeperClient) {
		oldc := c.mu.client
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.SendLogHeartbeat(ctx, pb.LogStoreHeartbeat{})
		require.NoError(t, err)
		require.True(t, oldc != c.mu.client)
	}
	testNotHAKeeperErrorIsHandled(t, fn)
}
