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

package cnservice

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type blockingCNHeartbeatCommandClient struct {
	*testHAKClient
	heartbeatEntered   chan struct{}
	heartbeatRelease   chan struct{}
	heartbeatReentered chan struct{}
	pollEntered        chan struct{}
	heartbeatCalls     atomic.Int32
	commandBatch       pb.CommandBatch
}

type canceledCNResponseClient struct {
	*testHAKClient
	heartbeatEntered chan struct{}
	pollEntered      chan struct{}
}

type observingTaskHolder struct {
	createErr   error
	createCount atomic.Int32
	created     chan struct{}
}

func (h *observingTaskHolder) Close() error {
	return nil
}

func (h *observingTaskHolder) Get() (taskservice.TaskService, bool) {
	return nil, false
}

func (h *observingTaskHolder) Create(pb.CreateTaskService) error {
	h.createCount.Add(1)
	select {
	case h.created <- struct{}{}:
	default:
	}
	return h.createErr
}

func (c *canceledCNResponseClient) SendCNHeartbeat(
	ctx context.Context,
	_ pb.CNStoreHeartbeat,
) (pb.CommandBatch, error) {
	select {
	case <-c.heartbeatEntered:
	default:
		close(c.heartbeatEntered)
	}
	<-ctx.Done()
	return pb.CommandBatch{BatchID: 7, Commands: []pb.ScheduleCommand{{ServiceType: pb.TNService}}}, nil
}

func (c *canceledCNResponseClient) GetScheduleCommands(
	ctx context.Context,
	_ pb.ServiceType,
) (pb.CommandBatch, error) {
	select {
	case <-c.pollEntered:
	default:
		close(c.pollEntered)
	}
	<-ctx.Done()
	return pb.CommandBatch{BatchID: 7, Commands: []pb.ScheduleCommand{{ServiceType: pb.TNService}}}, nil
}

func (c *blockingCNHeartbeatCommandClient) SendCNHeartbeat(
	ctx context.Context,
	hb pb.CNStoreHeartbeat,
) (pb.CommandBatch, error) {
	if c.heartbeatCalls.Add(1) == 1 {
		close(c.heartbeatEntered)
		select {
		case <-ctx.Done():
			return pb.CommandBatch{}, ctx.Err()
		case <-c.heartbeatRelease:
			return c.commandBatch, nil
		}
	}
	select {
	case <-c.heartbeatReentered:
	default:
		close(c.heartbeatReentered)
	}
	<-ctx.Done()
	return pb.CommandBatch{}, ctx.Err()
}

func (c *blockingCNHeartbeatCommandClient) GetScheduleCommands(
	context.Context,
	pb.ServiceType,
) (pb.CommandBatch, error) {
	select {
	case <-c.pollEntered:
	default:
		close(c.pollEntered)
	}
	return c.commandBatch, nil
}

func Test_heartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf := &Config{}
	client := &testHAKClient{
		cfg: conf,
	}

	sv := &service{
		cfg:             conf,
		_hakeeperClient: client,
		config:          &util.ConfigData{},
		logger:          logutil.GetPanicLogger(),
	}
	sv.heartbeat(ctx)
}

func TestCNCommandPollProgressesWhileHeartbeatIsBlocked(t *testing.T) {
	conf := &Config{}
	conf.UUID = "cn-1"
	conf.HAKeeper.HeatbeatInterval.Duration = 10 * time.Millisecond
	conf.HAKeeper.HeatbeatTimeout.Duration = 5 * time.Second
	commandBatch := pb.CommandBatch{
		BatchID: 1,
		Commands: []pb.ScheduleCommand{{
			UUID:        conf.UUID,
			ServiceType: pb.CNService,
			CreateTaskService: &pb.CreateTaskService{
				User: pb.TaskTableUser{
					Username: "cn-command-poll-test",
					Password: "test-password",
				},
			},
		}},
	}
	client := &blockingCNHeartbeatCommandClient{
		testHAKClient:      &testHAKClient{cfg: conf},
		heartbeatEntered:   make(chan struct{}),
		heartbeatRelease:   make(chan struct{}),
		heartbeatReentered: make(chan struct{}),
		pollEntered:        make(chan struct{}),
		commandBatch:       commandBatch,
	}
	holder := &observingTaskHolder{
		createErr: errors.New("stop after observing command application"),
		created:   make(chan struct{}, 1),
	}
	service := &service{
		cfg:               conf,
		_hakeeperClient:   client,
		config:            &util.ConfigData{},
		logger:            logutil.GetPanicLogger(),
		hakeeperConnected: make(chan struct{}),
	}
	service.task.holder = holder

	ctx, cancel := context.WithCancel(context.Background())
	controlDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-controlDone:
		case <-time.After(time.Second):
			t.Error("control-plane workers did not terminate during cleanup")
		}
	})
	go func() {
		defer close(controlDone)
		service.controlTask(ctx)
	}()
	select {
	case <-client.heartbeatEntered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not enter the injected blocked RPC")
	}

	select {
	case <-holder.created:
	case <-time.After(2 * time.Second):
		t.Fatal("polled command was not applied while heartbeat was blocked")
	}
	require.Equal(t, int32(1), holder.createCount.Load())

	// Let the heartbeat return the same batch. Entering the next heartbeat
	// proves the first response was fully handled, so the exact count below
	// verifies poll/heartbeat deduplication without a scheduling sleep.
	close(client.heartbeatRelease)
	select {
	case <-client.heartbeatReentered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat response was not handled")
	}
	require.Equal(t, uint64(1), service.ackedCommandBatchID.Load())
	require.Equal(t, int32(1), holder.createCount.Load(),
		"the same command batch must be applied exactly once")

	cancel()
	select {
	case <-controlDone:
	case <-time.After(time.Second):
		t.Fatal("control-plane workers did not terminate after cancellation")
	}
}

func TestCNCommandPollProgressesAfterHeartbeatFailure(t *testing.T) {
	conf := &Config{UUID: "cn-1"}
	conf.HAKeeper.HeatbeatInterval.Duration = 10 * time.Millisecond
	conf.HAKeeper.HeatbeatTimeout.Duration = 10 * time.Millisecond
	commandBatch := pb.CommandBatch{
		BatchID: 2,
		Commands: []pb.ScheduleCommand{{
			UUID:        conf.UUID,
			ServiceType: pb.CNService,
			CreateTaskService: &pb.CreateTaskService{
				User: pb.TaskTableUser{Username: "cn-command-poll-failure"},
			},
		}},
	}
	client := &blockingCNHeartbeatCommandClient{
		testHAKClient:      &testHAKClient{cfg: conf},
		heartbeatEntered:   make(chan struct{}),
		heartbeatRelease:   make(chan struct{}),
		heartbeatReentered: make(chan struct{}),
		pollEntered:        make(chan struct{}),
		commandBatch:       commandBatch,
	}
	holder := &observingTaskHolder{createErr: errors.New("stop after observing command application"), created: make(chan struct{}, 1)}
	service := &service{
		cfg:               conf,
		_hakeeperClient:   client,
		config:            &util.ConfigData{},
		logger:            logutil.GetPanicLogger(),
		hakeeperConnected: make(chan struct{}),
	}
	service.task.holder = holder

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("control-plane workers did not terminate during cleanup")
		}
	})
	go func() {
		defer close(done)
		service.controlTask(ctx)
	}()

	select {
	case <-holder.created:
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not take over after heartbeat failure")
	}
	require.Equal(t, int32(1), holder.createCount.Load())
}

func TestCNCommandTaskSkipsPollWithoutInFlightHeartbeat(t *testing.T) {
	conf := &Config{UUID: "cn-1"}
	client := &blockingCNHeartbeatCommandClient{
		testHAKClient:    &testHAKClient{cfg: conf},
		heartbeatEntered: make(chan struct{}),
		pollEntered:      make(chan struct{}),
	}
	service := &service{
		cfg:             conf,
		_hakeeperClient: client,
		config:          &util.ConfigData{},
		logger:          logutil.GetPanicLogger(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service.commandTask(ctx)
	select {
	case <-client.pollEntered:
		t.Fatal("healthy idle path issued an unnecessary command poll")
	default:
	}
}

func TestCNCanceledControlResponsesAreNotApplied(t *testing.T) {
	conf := &Config{}
	conf.UUID = "cn-1"
	conf.HAKeeper.HeatbeatInterval.Duration = 10 * time.Millisecond
	conf.HAKeeper.HeatbeatTimeout.Duration = 5 * time.Second
	service := &service{
		cfg: conf,
		_hakeeperClient: &canceledCNResponseClient{
			testHAKClient:    &testHAKClient{cfg: conf},
			heartbeatEntered: make(chan struct{}),
			pollEntered:      make(chan struct{}),
		},
		config: util.NewConfigData(nil),
		logger: logutil.GetPanicLogger(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.controlTask(ctx)
	}()
	client := service._hakeeperClient.(*canceledCNResponseClient)
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
	require.Zero(t, service.ackedCommandBatchID.Load(),
		"a response returned after cancellation must not be acknowledged")
}

func TestCNHeartbeatDropsResponseAfterRequestDeadline(t *testing.T) {
	conf := &Config{UUID: "cn-1"}
	conf.HAKeeper.HeatbeatTimeout.Duration = time.Millisecond
	service := &service{
		cfg: conf,
		_hakeeperClient: &canceledCNResponseClient{
			testHAKClient:    &testHAKClient{cfg: conf},
			heartbeatEntered: make(chan struct{}),
			pollEntered:      make(chan struct{}),
		},
		config: util.NewConfigData(nil),
		logger: logutil.GetPanicLogger(),
	}

	// A nil hakeeperConnected channel would panic if the successful-looking
	// late response escaped the per-request deadline guard.
	service.heartbeat(context.Background())
}

func TestCNCommandGenerationRolloverDoesNotReplayInheritedCommands(t *testing.T) {
	conf := &Config{UUID: "cn-1"}
	holder := &observingTaskHolder{
		createErr: errors.New("observe command application"),
		created:   make(chan struct{}, 3),
	}
	service := &service{
		cfg:    conf,
		logger: logutil.GetPanicLogger(),
	}
	service.task.holder = holder
	command := func(user string) pb.ScheduleCommand {
		return pb.ScheduleCommand{
			UUID:        conf.UUID,
			ServiceType: pb.CNService,
			CreateTaskService: &pb.CreateTaskService{
				User: pb.TaskTableUser{Username: user},
			},
		}
	}
	first := command("first")
	second := command("second")

	service.handleCommandBatch(pb.CommandBatch{
		BatchID: 10,
		Commands: []pb.ScheduleCommand{
			first,
		},
	})
	service.handleCommandBatch(pb.CommandBatch{
		BatchID: 11,
		Commands: []pb.ScheduleCommand{
			first,
			second,
		},
	})
	require.Equal(t, int32(2), holder.createCount.Load(),
		"a newer generation must apply only newly appended commands")

	service.handleHeartbeatResponse(10, pb.CommandBatch{})
	service.handleCommandBatch(pb.CommandBatch{
		BatchID: 12,
		Commands: []pb.ScheduleCommand{
			first,
			second,
			command("third"),
		},
	})
	require.Equal(t, int32(3), holder.createCount.Load(),
		"a stale acknowledgement must not erase the newer generation's lineage")

	service.handleHeartbeatResponse(12, pb.CommandBatch{})
	service.handleCommandBatch(pb.CommandBatch{
		BatchID: 13,
		Commands: []pb.ScheduleCommand{
			first,
		},
	})
	require.Equal(t, int32(4), holder.createCount.Load(),
		"the same command may be intentional work after the prior batch is acknowledged")
}
