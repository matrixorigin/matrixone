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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util"
)

var _ logservice.ProxyHAKeeperClient = new(testHAClient)

type testHAClient struct {
}

type watermarkHAClient struct {
	*testHAClient
	sync.Mutex
	details   pb.ClusterDetails
	heartbeat pb.ProxyHeartbeat
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
	return pb.CommandBatch{}, moerr.NewInternalErrorNoCtx("return err")
}

func (client *watermarkHAClient) GetClusterDetails(context.Context) (pb.ClusterDetails, error) {
	client.Lock()
	defer client.Unlock()
	return client.details, nil
}

func (client *watermarkHAClient) SendProxyHeartbeat(
	_ context.Context,
	hb pb.ProxyHeartbeat,
) (pb.CommandBatch, error) {
	client.Lock()
	defer client.Unlock()
	client.heartbeat = hb
	return pb.CommandBatch{}, nil
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

func TestServerInitialRouteBarrierAcknowledgesPublishedWatermark(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	client := &watermarkHAClient{
		testHAClient: &testHAClient{},
		details:      pb.ClusterDetails{GlobalSysVarCommitTS: commitTS},
	}
	cluster := clusterservice.NewMOCluster("", client, time.Hour)
	defer cluster.Close()
	server := &Server{
		haKeeperClient:         client,
		configData:             util.NewConfigData(nil),
		runtime:                runtime.ServiceRuntime(""),
		globalSysVarGeneration: "proxy-generation",
		handler:                &handler{moCluster: cluster},
	}
	server.config.UUID = "proxy-1"
	server.config.HAKeeper.HeartbeatInterval.Duration = time.Second
	server.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	require.NoError(t, server.initializeGlobalSysVarRouteBarrier(context.Background()))
	require.True(t, server.canAcceptNewConnections())

	client.Lock()
	hb := client.heartbeat
	client.Unlock()
	require.Equal(t, commitTS, hb.GlobalSysVarCommitTS)
	require.Equal(t, "proxy-generation", hb.GlobalSysVarGeneration)
	require.Equal(t, defines.MORPCLatestVersion, hb.ProtocolVersion)
}

func TestProxyServingLeaseExpiresAndHeartbeatFailureRevokesIt(t *testing.T) {
	server := &Server{
		haKeeperClient: &testHAClient{},
		configData:     util.NewConfigData(nil),
		runtime:        runtime.ServiceRuntime(""),
	}
	server.config.HAKeeper.HeartbeatInterval.Duration = time.Second
	server.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	deadline := time.Now().Add(time.Minute)
	server.servingLeaseDeadline.Store(&deadline)
	require.True(t, server.canAcceptNewConnections())
	server.doHeartbeat(context.Background())
	require.False(t, server.canAcceptNewConnections(),
		"a failed HAKeeper heartbeat must immediately fail-close Proxy admission")

	deadline = time.Now().Add(-time.Nanosecond)
	server.servingLeaseDeadline.Store(&deadline)
	require.False(t, server.canAcceptNewConnections())
}

func TestProxyHeartbeatDoesNotRenewLeaseAfterCallerCancellation(t *testing.T) {
	client := &watermarkHAClient{testHAClient: &testHAClient{}}
	server := &Server{
		haKeeperClient: client,
		configData:     util.NewConfigData(nil),
	}
	server.config.HAKeeper.HeartbeatInterval.Duration = time.Second
	server.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, server.sendHeartbeat(ctx), context.Canceled)
	require.False(t, server.canAcceptNewConnections())
}

func TestServer_NewServer(t *testing.T) {
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()
	_, err := NewServer(ctx, Config{}, WithRuntime(rt))
	assert.Error(t, err)
}
