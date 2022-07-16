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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	ErrNoHAKeeper = moerr.NewError(moerr.INVALID_STATE, "failed to locate HAKeeper")
)

// CNHAKeeperClient is the HAKeeper client used by a CN store.
type CNHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// GetClusterDetails queries the HAKeeper and return CN and DN nodes that are
	// known to the HAKeeper.
	GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error)
	// SendCNHeartbeat sends the specified heartbeat message to the HAKeeper.
	SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) error
}

// DNHAKeeperClient is the HAKeeper client used by a DN store.
type DNHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// SendDNHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// DN store.
	SendDNHeartbeat(ctx context.Context, hb pb.DNStoreHeartbeat) (pb.CommandBatch, error)
}

// LogHAKeeperClient is the HAKeeper client used by a Log store.
type LogHAKeeperClient interface {
	// Close closes the hakeeper client.
	Close() error
	// SendLogHeartbeat sends the specified heartbeat message to the HAKeeper. The
	// returned CommandBatch contains Schedule Commands to be executed by the local
	// Log store.
	SendLogHeartbeat(ctx context.Context, hb pb.LogStoreHeartbeat) (pb.CommandBatch, error)
}

// HAKeeperClientConfig is the config for HAKeeper clients.
type HAKeeperClientConfig struct {
	// DiscoveryAddress is the Log Service discovery address provided by k8s.
	DiscoveryAddress string
	// ServiceAddresses is a list of well known Log Services' service addresses.
	ServiceAddresses []string
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
	for _, addr := range cfg.ServiceAddresses {
		cc, err := getRPCClient(ctx, addr, c.respPool)
		if err != nil {
			e = err
			continue
		}
		c.addr = addr
		c.client = cc
		isHAKeeper, err := c.checkRemote(ctx)
		plog.Infof("isHAKeeper: %t, err: %v", isHAKeeper, err)
		if err == nil && isHAKeeper {
			return c, nil
		} else if err != nil {
			e = err
		}
		if err := cc.Close(); err != nil {
			plog.Errorf("failed to close the client %v", err)
		}
	}
	if e == nil {
		// didn't encounter any error
		return nil, ErrNoHAKeeper
	}
	return nil, e
}

var _ CNHAKeeperClient = (*hakeeperClient)(nil)
var _ DNHAKeeperClient = (*hakeeperClient)(nil)
var _ LogHAKeeperClient = (*hakeeperClient)(nil)

func NewCNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (CNHAKeeperClient, error) {
	return newHAKeeperClient(ctx, cfg)
}

func NewDNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (DNHAKeeperClient, error) {
	return newHAKeeperClient(ctx, cfg)
}

func NewLogHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (LogHAKeeperClient, error) {
	return newHAKeeperClient(ctx, cfg)
}

func (c *hakeeperClient) Close() error {
	return c.client.Close()
}

func (c *hakeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	req := pb.Request{
		Method: pb.GET_CLUSTER_DETAILS,
	}
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.ClusterDetails{}, err
	}
	return resp.ClusterDetails, nil
}

func (c *hakeeperClient) SendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) error {
	req := pb.Request{
		Method:      pb.CN_HEARTBEAT,
		CNHeartbeat: hb,
	}
	_, err := c.sendHeartbeat(ctx, req)
	return err
}

func (c *hakeeperClient) SendDNHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.DN_HEARTBEAT,
		DNHeartbeat: hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) SendLogHeartbeat(ctx context.Context,
	hb pb.LogStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:       pb.LOG_HEARTBEAT,
		LogHeartbeat: hb,
	}
	cb, err := c.sendHeartbeat(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	for _, cmd := range cb.Commands {
		plog.Infof("hakeeper client received cmd: %s", cmd.LogString())
	}
	return cb, nil
}

func (c *hakeeperClient) sendHeartbeat(ctx context.Context,
	req pb.Request) (pb.CommandBatch, error) {
	resp, err := c.request(ctx, req)
	if err != nil {
		return pb.CommandBatch{}, err
	}
	return resp.CommandBatch, nil
}

func (c *hakeeperClient) checkRemote(ctx context.Context) (bool, error) {
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
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return pb.Response{}, err
	}
	req.Timeout = int64(timeout)
	r := c.pool.Get().(*RPCRequest)
	r.Request = req
	future, err := c.client.Send(ctx,
		c.addr, r, morpc.SendOptions{Timeout: time.Duration(timeout)})
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
