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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type blockingCNHeartbeatCommandClient struct {
	*testHAKClient
	heartbeatEntered chan struct{}
	pollEntered      chan struct{}
}

type canceledCNResponseClient struct {
	*testHAKClient
	heartbeatEntered chan struct{}
	pollEntered      chan struct{}
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
	return pb.CommandBatch{Commands: []pb.ScheduleCommand{{ServiceType: pb.TNService}}}, nil
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
	return pb.CommandBatch{Commands: []pb.ScheduleCommand{{ServiceType: pb.TNService}}}, nil
}

func (c *blockingCNHeartbeatCommandClient) SendCNHeartbeat(
	ctx context.Context,
	hb pb.CNStoreHeartbeat,
) (pb.CommandBatch, error) {
	select {
	case <-c.heartbeatEntered:
	default:
		close(c.heartbeatEntered)
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
	return pb.CommandBatch{}, nil
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
	controlDone := make(chan struct{})
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
	case <-client.pollEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("command poll did not progress independently of heartbeat")
	}

	cancel()
	select {
	case <-controlDone:
	case <-time.After(time.Second):
		t.Fatal("control-plane workers did not terminate after cancellation")
	}
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
