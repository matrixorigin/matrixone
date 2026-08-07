// Copyright 2024 Matrix Origin
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

package tnservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
)

type blockingHeartbeatCommandClient struct {
	*testHAClient
	heartbeatEntered chan struct{}
	pollEntered      chan struct{}
	command          pb.ScheduleCommand
}

type canceledTNResponseClient struct {
	*testHAClient
	heartbeatEntered chan struct{}
	pollEntered      chan struct{}
	command          pb.ScheduleCommand
}

func (c *canceledTNResponseClient) SendTNHeartbeat(
	ctx context.Context,
	_ pb.TNStoreHeartbeat,
) (pb.CommandBatch, error) {
	select {
	case <-c.heartbeatEntered:
	default:
		close(c.heartbeatEntered)
	}
	<-ctx.Done()
	return pb.CommandBatch{BatchID: 7, Commands: []pb.ScheduleCommand{c.command}}, nil
}

func (c *canceledTNResponseClient) GetScheduleCommands(
	ctx context.Context,
	_ pb.ServiceType,
) (pb.CommandBatch, error) {
	select {
	case <-c.pollEntered:
	default:
		close(c.pollEntered)
	}
	<-ctx.Done()
	return pb.CommandBatch{BatchID: 7, Commands: []pb.ScheduleCommand{c.command}}, nil
}

func (c *blockingHeartbeatCommandClient) SendTNHeartbeat(
	ctx context.Context,
	hb pb.TNStoreHeartbeat,
) (pb.CommandBatch, error) {
	select {
	case <-c.heartbeatEntered:
	default:
		close(c.heartbeatEntered)
	}
	<-ctx.Done()
	return pb.CommandBatch{}, ctx.Err()
}

func (c *blockingHeartbeatCommandClient) GetScheduleCommands(
	context.Context,
	pb.ServiceType,
) (pb.CommandBatch, error) {
	select {
	case <-c.pollEntered:
	default:
		close(c.pollEntered)
	}
	return pb.CommandBatch{BatchID: 1, Commands: []pb.ScheduleCommand{c.command}}, nil
}

var _ logservice.TNHAKeeperClient = new(testHAClient)

type testHAClient struct {
	lastHeartbeat pb.TNStoreHeartbeat
}

func (client *testHAClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateID(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (client *testHAClient) SendTNHeartbeat(ctx context.Context, hb pb.TNStoreHeartbeat) (pb.CommandBatch, error) {
	client.lastHeartbeat = hb
	return pb.CommandBatch{}, context.DeadlineExceeded
}

func Test_heartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, moerr.NewInternalErrorNoCtx("ut tester"))
	defer cancel()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)

	cfg := &Config{}

	client := &testHAClient{}
	lstore := &store{
		cfg:            cfg,
		replicas:       &sync.Map{},
		config:         &util.ConfigData{},
		hakeeperClient: client,
		rt:             rt,
	}
	lstore.heartbeat(ctx)
	if !client.lastHeartbeat.AutoIncrEpochFenceSupported {
		t.Fatal("TN heartbeat must advertise AUTO_INCREMENT epoch enforcement")
	}
}

func TestCommandPollProgressesWhileHeartbeatIsBlocked(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	client := &blockingHeartbeatCommandClient{
		testHAClient:     &testHAClient{},
		heartbeatEntered: make(chan struct{}),
		pollEntered:      make(chan struct{}),
		command: pb.ScheduleCommand{
			UUID:        "tn-1",
			ServiceType: pb.TNService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "tn-1",
			},
		},
	}
	store := &store{
		cfg: &Config{
			UUID: "tn-1",
		},
		replicas:       &sync.Map{},
		config:         &util.ConfigData{},
		hakeeperClient: client,
		rt:             rt,
		shutdownC:      make(chan struct{}, 1),
	}
	store.cfg.HAKeeper.HeatbeatInterval.Duration = 10 * time.Millisecond
	store.cfg.HAKeeper.HeatbeatTimeout.Duration = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		store.controlTask(ctx)
	}()
	select {
	case <-client.heartbeatEntered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not enter the injected blocked RPC")
	}

	select {
	case <-store.shutdownC:
	case <-time.After(2 * time.Second):
		t.Fatal("schedule command did not progress independently of heartbeat")
	}

	cancel()
	select {
	case <-controlDone:
	case <-time.After(time.Second):
		t.Fatal("control-plane workers did not terminate after cancellation")
	}
}

func TestCommandTaskSkipsPollWithoutInFlightHeartbeat(t *testing.T) {
	client := &blockingHeartbeatCommandClient{
		testHAClient:     &testHAClient{},
		heartbeatEntered: make(chan struct{}),
		pollEntered:      make(chan struct{}),
	}
	store := &store{hakeeperClient: client}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store.commandTask(ctx)
	select {
	case <-client.pollEntered:
		t.Fatal("healthy idle path issued an unnecessary command poll")
	default:
	}
}

func TestCanceledControlResponsesAreNotApplied(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	command := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: "tn-1",
		},
	}
	store := &store{
		cfg: &Config{
			UUID: "tn-1",
		},
		replicas: &sync.Map{},
		config:   &util.ConfigData{},
		hakeeperClient: &canceledTNResponseClient{
			testHAClient:     &testHAClient{},
			heartbeatEntered: make(chan struct{}),
			pollEntered:      make(chan struct{}),
			command:          command,
		},
		rt:        rt,
		shutdownC: make(chan struct{}, 1),
	}
	store.cfg.HAKeeper.HeatbeatInterval.Duration = 10 * time.Millisecond
	store.cfg.HAKeeper.HeatbeatTimeout.Duration = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.controlTask(ctx)
	}()
	client := store.hakeeperClient.(*canceledTNResponseClient)
	for name, entered := range map[string]<-chan struct{}{
		"heartbeat": client.heartbeatEntered,
		"poll":      client.pollEntered,
	} {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s request did not enter", name)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("control-plane workers did not terminate after cancellation")
	}
	select {
	case <-store.shutdownC:
		t.Fatal("response returned after cancellation was applied")
	default:
	}
	require.Zero(t, store.ackedCommandBatchID.Load(),
		"a response returned after cancellation must not be acknowledged")
}

func TestHeartbeatDropsResponseAfterRequestDeadline(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	command := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: "tn-1",
		},
	}
	store := &store{
		cfg: &Config{
			UUID: "tn-1",
		},
		replicas: &sync.Map{},
		config:   &util.ConfigData{},
		hakeeperClient: &canceledTNResponseClient{
			testHAClient:     &testHAClient{},
			heartbeatEntered: make(chan struct{}),
			pollEntered:      make(chan struct{}),
			command:          command,
		},
		rt:        rt,
		shutdownC: make(chan struct{}, 1),
	}
	store.cfg.HAKeeper.HeatbeatTimeout.Duration = time.Millisecond

	store.heartbeat(context.Background())
	select {
	case <-store.shutdownC:
		t.Fatal("response returned after its request deadline was applied")
	default:
	}
}

func TestCommandBatchDeduplicatesPollHeartbeatRace(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	command := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: "tn-1",
		},
	}
	store := &store{
		rt:        rt,
		shutdownC: make(chan struct{}, 4),
	}

	store.handleCommandBatch(pb.CommandBatch{BatchID: 10, Commands: []pb.ScheduleCommand{command}})
	require.Equal(t, uint64(10), store.ackedCommandBatchID.Load())
	store.handleCommandBatch(pb.CommandBatch{BatchID: 10, Commands: []pb.ScheduleCommand{command}})
	store.handleCommandBatch(pb.CommandBatch{BatchID: 9, Commands: []pb.ScheduleCommand{command}})
	require.Equal(t, 1, len(store.shutdownC), "duplicate and stale batches must not be applied")
	store.handleCommandBatch(pb.CommandBatch{Commands: []pb.ScheduleCommand{command}})
	require.Equal(t, 1, len(store.shutdownC), "one legacy replay after leader downgrade must be suppressed")
	store.handleCommandBatch(pb.CommandBatch{Commands: []pb.ScheduleCommand{command}})
	require.Equal(t, 2, len(store.shutdownC), "a later legacy checker retry must remain applicable")

	store.handleCommandBatch(pb.CommandBatch{BatchID: 11, Commands: []pb.ScheduleCommand{command}})
	require.Equal(t, 3, len(store.shutdownC), "a new checker generation must remain retryable")
	require.Equal(t, uint64(11), store.ackedCommandBatchID.Load())
}
