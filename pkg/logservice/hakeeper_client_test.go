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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHAKeeperClientsCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
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

func TestHAKeeperClientsCanBeCreatedWithRetry(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		c := NewLogHAKeeperClientWithRetry(context.Background(), "", cfg)
		assert.NoError(t, c.Close())
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
			ServiceAddresses: []string{testServiceAddress},
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
			ServiceAddresses: []string{testServiceAddress},
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

func TestHAKeeperClientSendLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
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

func testNotHAKeeperErrorIsHandled(t *testing.T, fn func(*testing.T, *managedHAKeeperClient)) {
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
		ServiceAddresses: []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr()},
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

func TestSendTNHeartbeatWhenNotConnectedToHAKeeper(t *testing.T) {
	fn := func(t *testing.T, c *managedHAKeeperClient) {
		oldc := c.mu.client
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := c.SendTNHeartbeat(ctx, pb.TNStoreHeartbeat{})
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

func TestHAKeeperClientUpdateCNLabel(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
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

func TestHAKeeperClientUpdateCNWorkState(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
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
			ServiceAddresses: []string{testServiceAddress},
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
			ServiceAddresses: []string{testServiceAddress},
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
			ServiceAddresses: []string{testServiceAddress},
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
