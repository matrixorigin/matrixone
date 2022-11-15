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
	"fmt"
	"math/rand"
	"sync"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type basicHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// AllocateID allocate a globally unique ID
	AllocateID(ctx context.Context) (uint64, error)
	// GetClusterDetails queries the HAKeeper and return CN and DN nodes that are
	// known to the HAKeeper.
	GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error)
	// GetClusterState queries the cluster state
	GetClusterState(ctx context.Context) (pb.CheckerState, error)
}

// CNHAKeeperClient is the HAKeeper client used by a CN store.
type CNHAKeeperClient interface {
	basicHAKeeperClient
	// SendCNHeartbeat sends the specified heartbeat message to the HAKeeper.
	SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error)
}

// DNHAKeeperClient is the HAKeeper client used by a DN store.
type DNHAKeeperClient interface {
	basicHAKeeperClient
	// SendDNHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// DN store.
	SendDNHeartbeat(ctx context.Context, hb pb.DNStoreHeartbeat) (pb.CommandBatch, error)
}

// LogHAKeeperClient is the HAKeeper client used by a Log store.
type LogHAKeeperClient interface {
	basicHAKeeperClient
	// SendLogHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// Log store.
	SendLogHeartbeat(ctx context.Context, hb pb.LogStoreHeartbeat) (pb.CommandBatch, error)
}

// TODO: HAKeeper discovery to be implemented

var _ CNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ DNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ LogHAKeeperClient = (*managedHAKeeperClient)(nil)

// NewCNHAKeeperClient creates a HAKeeper client to be used by a CN node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewCNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (CNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, cfg)
}

// NewDNHAKeeperClient creates a HAKeeper client to be used by a DN node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewDNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (DNHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, cfg)
}

// NewLogHAKeeperClient creates a HAKeeper client to be used by a Log Service node.
//
// NB: caller could specify options for morpc.Client via ctx.
func NewLogHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (LogHAKeeperClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newManagedHAKeeperClient(ctx, cfg)
}

func newManagedHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (*managedHAKeeperClient, error) {
	c, err := newHAKeeperClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mc := &managedHAKeeperClient{
		cfg:            cfg,
		backendOptions: GetBackendOptions(ctx),
		clientOptions:  GetClientOptions(ctx),
	}
	mc.mu.client = c
	return mc, nil
}

type managedHAKeeperClient struct {
	cfg HAKeeperClientConfig

	// Method `prepareClient` may update moprc.Client.
	// So we need to keep options for morpc.Client.
	backendOptions []morpc.BackendOption
	clientOptions  []morpc.ClientOption

	mu struct {
		sync.RWMutex
		nextID uint64
		lastID uint64
		client *hakeeperClient
	}
}

func (c *managedHAKeeperClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.client == nil {
		return nil
	}
	return c.mu.client.close()
}

func (c *managedHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.ClusterDetails{}, err
		}
		cd, err := c.getClient().getClusterDetails(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cd, err
	}
}

func (c *managedHAKeeperClient) GetClusterState(ctx context.Context) (pb.CheckerState, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CheckerState{}, err
		}
		s, err := c.getClient().getClusterState(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return s, err
	}
}

func (c *managedHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	if c.mu.nextID != c.mu.lastID {
		v := c.mu.nextID
		c.mu.nextID++
		c.mu.Unlock()
		return v, nil
	}

	for {
		if err := c.prepareClientLocked(ctx); err != nil {
			return 0, err
		}
		firstID, err := c.mu.client.sendCNAllocateID(ctx, c.cfg.AllocateIDBatch)
		if err != nil {
			c.resetClientLocked()
		}
		if c.isRetryableError(err) {
			continue
		}

		c.mu.nextID = firstID + 1
		c.mu.lastID = firstID + c.cfg.AllocateIDBatch - 1
		c.mu.Unlock()
		return firstID, err
	}
}

func (c *managedHAKeeperClient) SendCNHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		result, err := c.getClient().sendCNHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return result, err
	}
}

func (c *managedHAKeeperClient) SendDNHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.getClient().sendDNHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cb, err
	}
}

func (c *managedHAKeeperClient) SendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.getClient().sendLogHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cb, err
	}
}

func (c *managedHAKeeperClient) isRetryableError(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrNoHAKeeper)
}

func (c *managedHAKeeperClient) resetClient() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetClientLocked()
}

func (c *managedHAKeeperClient) prepareClient(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.prepareClientLocked(ctx)
}

func (c *managedHAKeeperClient) resetClientLocked() {
	if c.mu.client != nil {
		cc := c.mu.client
		c.mu.client = nil
		if err := cc.close(); err != nil {
			logutil.Error("failed to close client", zap.Error(err))
		}
	}
}

func (c *managedHAKeeperClient) prepareClientLocked(ctx context.Context) error {
	if c.mu.client != nil {
		return nil
	}

	// we must use the recoreded options for morpc.Client
	ctx = SetBackendOptions(ctx, c.backendOptions...)
	ctx = SetClientOptions(ctx, c.clientOptions...)

	cc, err := newHAKeeperClient(ctx, c.cfg)
	if err != nil {
		return err
	}
	c.mu.client = cc
	return nil
}

type hakeeperClient struct {
	cfg      HAKeeperClientConfig
	client   morpc.RPCClient
	addr     string
	pool     *sync.Pool
	respPool *sync.Pool
}

func newHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (*hakeeperClient, error) {
	client, err := connectToHAKeeper(ctx, cfg.ServiceAddresses, cfg)
	if client != nil && err == nil {
		return client, nil
	}
	if len(cfg.DiscoveryAddress) > 0 {
		return connectByReverseProxy(ctx, cfg.DiscoveryAddress, cfg)
	}
	if err != nil {
		return nil, err
	}
	return nil, moerr.NewNoHAKeeper()
}

func connectByReverseProxy(ctx context.Context,
	discoveryAddress string, cfg HAKeeperClientConfig) (*hakeeperClient, error) {
	si, ok, err := GetShardInfo(discoveryAddress, hakeeper.DefaultHAKeeperShardID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	addresses := make([]string, 0)
	leaderAddress, ok := si.Replicas[si.ReplicaID]
	if ok {
		addresses = append(addresses, leaderAddress)
	}
	for replicaID, address := range si.Replicas {
		if replicaID != si.ReplicaID {
			addresses = append(addresses, address)
		}
	}
	return connectToHAKeeper(ctx, addresses, cfg)
}

func connectToHAKeeper(ctx context.Context,
	targets []string, cfg HAKeeperClientConfig) (*hakeeperClient, error) {
	if len(targets) == 0 {
		return nil, nil
	}

	pool := &sync.Pool{}
	pool.New = func() interface{} {
		return &RPCRequest{pool: pool}
	}
	respPool := &sync.Pool{}
	respPool.New = func() interface{} {
		return &RPCResponse{pool: respPool}
	}
	c := &hakeeperClient{
		cfg:      cfg,
		pool:     pool,
		respPool: respPool,
	}
	var e error
	addresses := append([]string{}, targets...)
	rand.Shuffle(len(addresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	for _, addr := range addresses {
		cc, err := getRPCClient(ctx, addr, c.respPool, defaultMaxMessageSize, "connectToHAKeeper")
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		isHAKeeper, err := c.checkIsHAKeeper(ctx)
		logutil.Info(fmt.Sprintf("isHAKeeper: %t, err: %v", isHAKeeper, err))
		if err == nil && isHAKeeper {
			return c, nil
		} else if err != nil {
			e = err
		}
		if err := cc.Close(); err != nil {
			logutil.Error("failed to close the client", zap.Error(err))
		}
	}
	if e == nil {
		// didn't encounter any error
		return nil, moerr.NewNoHAKeeper()
	}
	return nil, e
}

func (c *hakeeperClient) close() error {
	if c == nil {
		panic("!!!")
	}

	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

func (c *hakeeperClient) getClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_DETAILS,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.ClusterDetails{}, err
	}
	return *resp.ClusterDetails, nil
}

func (c *hakeeperClient) getClusterState(ctx context.Context) (pb.CheckerState, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_STATE,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CheckerState{}, err
	}
	return *resp.CheckerState, nil
}

func (c *hakeeperClient) sendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.CN_HEARTBEAT,
		CNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendCNAllocateID(ctx context.Context, batch uint64) (uint64, error) {
	req := pb.Request{
		Method:       pb.CN_ALLOCATE_ID,
		CNAllocateID: &pb.CNAllocateID{Batch: batch},
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return 0, err
	}
	return resp.AllocateID.FirstID, nil
}

func (c *hakeeperClient) sendDNHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.DN_HEARTBEAT,
		DNHeartbeat: &hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:       pb.LOG_HEARTBEAT,
		LogHeartbeat: &hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	for _, cmd := range cb.Commands {
		logutil.Info("hakeeper client received cmd", zap.String("cmd", cmd.LogString()))
	}
	return cb, nil
}

func (c *hakeeperClient) sendHeartbeat(ctx context.Context,
	req pb.Request) (pb.CommandBatch, error) {
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	if resp.CommandBatch == nil {
		return pb.CommandBatch{}, nil
	}
	return *resp.CommandBatch, nil
}

func (c *hakeeperClient) checkIsHAKeeper(ctx context.Context) (bool, error) {
	req := pb.Request{
		Method: pb.CHECK_HAKEEPER,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return false, err
	}
	return resp.IsHAKeeper, nil
}

func (c *hakeeperClient) request(ctx context.Context, req pb.Request) (pb.Response, error) {
	if c == nil {
		return pb.Response{}, moerr.NewNoHAKeeper()
	}
	r := c.pool.Get().(*RPCRequest)
	r.Request = req
	future, err := c.client.Send(ctx, c.addr, r)
	if err != nil {
		return pb.Response{}, err
	}
	defer future.Close()
	msg, err := future.Get()
	if err != nil {
		return pb.Response{}, err
	}
	response, ok := msg.(*RPCResponse)
	if !ok {
		panic("unexpected response type")
	}
	resp := response.Response
	defer response.Release()
	err = toError(response.Response)
	if err != nil {
		return pb.Response{}, err
	}
	return resp, nil
}

func (c *managedHAKeeperClient) getClient() *hakeeperClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.client
}
