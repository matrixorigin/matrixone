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
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var _ logservice.ProxyHAKeeperClient = new(testHAClient)

type testHAClient struct {
	succeed     bool
	sent        chan pb.ProxyHeartbeat
	heartbeatFn func(context.Context, pb.ProxyHeartbeat) (pb.CommandBatch, error)
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
	if tclient.heartbeatFn != nil {
		return tclient.heartbeatFn(ctx, hb)
	}
	if tclient.sent != nil {
		tclient.sent <- hb
	}
	if tclient.succeed {
		return pb.CommandBatch{}, nil
	}
	return pb.CommandBatch{}, moerr.NewInternalErrorNoCtx("return err")
}

type admissionHeartbeatCluster struct {
	clusterservice.MOCluster
	admission    pb.ViewMetadataAdmission
	refreshDelay time.Duration
}

func (c *admissionHeartbeatCluster) Refresh(ctx context.Context) error {
	if c.refreshDelay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.refreshDelay):
		return nil
	}
}

func (c *admissionHeartbeatCluster) GetViewMetadataAdmission() pb.ViewMetadataAdmission {
	return c.admission
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

func TestServerHeartbeatUsesIndependentRefreshDeadline(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(t.Name(), rt)
	client := &testHAClient{heartbeatFn: func(ctx context.Context, _ pb.ProxyHeartbeat) (pb.CommandBatch, error) {
		select {
		case <-ctx.Done():
			return pb.CommandBatch{}, ctx.Err()
		case <-time.After(35 * time.Millisecond):
			return pb.CommandBatch{ViewMetadataAdmission: &pb.ViewMetadataAdmission{
				Enabled: true, Epoch: 5, Generation: 9,
			}}, nil
		}
	}}
	cluster := &admissionHeartbeatCluster{
		admission:    pb.ViewMetadataAdmission{Enabled: true, Epoch: 5},
		refreshDelay: 25 * time.Millisecond,
	}
	ser := &Server{
		haKeeperClient:                  client,
		configData:                      util.NewConfigData(nil),
		runtime:                         runtime.ServiceRuntime(t.Name()),
		handler:                         &handler{moCluster: cluster},
		viewMetadataAdmissionGeneration: 9,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		viewMetadataHeartbeatWakeup:     make(chan struct{}, 1),
	}
	ser.config.HAKeeper.HeartbeatTimeout.Duration = 50 * time.Millisecond
	ser.doHeartbeat(context.Background())
	require.Equal(t, uint64(5), ser.viewMetadataObservedEpoch.Load())
}

func TestServerHeartbeatImmediatelyReportsObservedAdmissionEpoch(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime(t.Name(), rt)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	calls := make(chan pb.ProxyHeartbeat, 2)
	callIndex := 0
	client := &testHAClient{heartbeatFn: func(ctx context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error) {
		index := callIndex
		callIndex++
		calls <- hb
		select {
		case <-ctx.Done():
			return pb.CommandBatch{}, ctx.Err()
		case <-releases[index]:
		}
		return pb.CommandBatch{ViewMetadataAdmission: &pb.ViewMetadataAdmission{
			Enabled:    true,
			Epoch:      5,
			Generation: 9,
			Ready:      index == 1,
		}}, nil
	}}
	cluster := &admissionHeartbeatCluster{admission: pb.ViewMetadataAdmission{
		Enabled: true,
		Epoch:   5,
	}}
	ser := &Server{
		haKeeperClient:                  client,
		configData:                      util.NewConfigData(nil),
		runtime:                         runtime.ServiceRuntime(t.Name()),
		handler:                         &handler{moCluster: cluster},
		viewMetadataAdmissionGeneration: 9,
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		viewMetadataHeartbeatWakeup:     make(chan struct{}, 1),
	}
	ser.config.HAKeeper.HeartbeatInterval.Duration = time.Hour
	ser.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ser.heartbeat(ctx)
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- ser.waitForViewMetadataAdmission(waitCtx) }()

	select {
	case first := <-calls:
		require.Zero(t, first.ViewMetadataObservedEpoch)
	case <-time.After(time.Second):
		t.Fatal("Proxy did not send the initial heartbeat")
	}
	close(releases[0])
	select {
	case second := <-calls:
		require.Equal(t, uint64(5), second.ViewMetadataObservedEpoch)
	case <-time.After(time.Second):
		t.Fatal("Proxy waited for the one-hour ticker before reporting the observed epoch")
	}
	close(releases[1])
	require.NoError(t, <-waitDone)
	cancel()
	select {
	case <-heartbeatDone:
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
