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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var _ logservice.ProxyHAKeeperClient = new(testHAClient)

type testHAClient struct {
	succeed bool
	sent    chan pb.ProxyHeartbeat
}

func (tclient *testHAClient) Close() error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) AllocateID(ctx context.Context) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) AllocateIDByKeyWithBatch(ctx context.Context, key string, batch uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) CheckLogServiceHealth(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) GetCNState(ctx context.Context) (pb.CNState, error) {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) UpdateCNLabel(ctx context.Context, label pb.CNStoreLabel) error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) UpdateCNWorkState(ctx context.Context, state pb.CNWorkState) error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) PatchCNStore(ctx context.Context, stateLabel pb.CNStateLabel) error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) DeleteCNStore(ctx context.Context, cnStore pb.DeleteCNStore) error {
	//TODO implement me
	panic("implement me")
}

func (tclient *testHAClient) SendProxyHeartbeat(ctx context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error) {
	if tclient.sent != nil {
		tclient.sent <- hb
	}
	if tclient.succeed {
		return pb.CommandBatch{}, nil
	}
	return pb.CommandBatch{}, moerr.NewInternalErrorNoCtx("return err")
}

func TestServer_doHeartbeat(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()
	ser := &Server{
		haKeeperClient: &testHAClient{},
		configData:     util.NewConfigData(nil),
		runtime:        runtime.ServiceRuntime(""),
	}
	ser.doHeartbeat(ctx)
}

func TestServerHeartbeatSendsImmediately(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(t.Name(), rt)
	sent := make(chan pb.ProxyHeartbeat, 1)
	ctx, cancel := context.WithCancel(context.Background())
	client := &testHAClient{succeed: true, sent: sent}
	ser := &Server{
		haKeeperClient: client,
		configData:     util.NewConfigData(nil),
		runtime:        runtime.ServiceRuntime(t.Name()),
	}
	ser.config.HAKeeper.HeartbeatInterval.Duration = time.Hour
	ser.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	done := make(chan struct{})
	go func() {
		defer close(done)
		ser.heartbeat(ctx)
	}()

	select {
	case <-sent:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("Proxy heartbeat waited for the first ticker")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Proxy heartbeat did not stop after cancellation")
	}
}

func TestServer_NewServer(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()
	_, err := NewServer(ctx, Config{}, WithRuntime(rt))
	assert.Error(t, err)
}
