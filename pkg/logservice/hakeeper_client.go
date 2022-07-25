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

	"github.com/cockroachdb/errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	// ErrNotHAKeeper is returned to indicate that HAKeeper can not be located.
	ErrNotHAKeeper = moerr.NewError(moerr.INVALID_STATE, "failed to locate HAKeeper")
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

// TODO: HAKeeper discovery to be implemented

var _ CNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ DNHAKeeperClient = (*managedHAKeeperClient)(nil)
var _ LogHAKeeperClient = (*managedHAKeeperClient)(nil)

// NewCNHAKeeperClient creates a HAKeeper client to be used by a CN node.
func NewCNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (CNHAKeeperClient, error) {
	return newManagedHAKeeperClient(ctx, cfg)
}

// NewDNHAKeeperClient creates a HAKeeper client to be used by a DN node.
func NewDNHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (DNHAKeeperClient, error) {
	return newManagedHAKeeperClient(ctx, cfg)
}

// NewLogHAKeeperClient creates a HAKeeper client to be used by a Log Service node.
func NewLogHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (LogHAKeeperClient, error) {
	return newManagedHAKeeperClient(ctx, cfg)
}

func newManagedHAKeeperClient(ctx context.Context,
	cfg HAKeeperClientConfig) (*managedHAKeeperClient, error) {
	c, err := newHAKeeperClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &managedHAKeeperClient{client: c, cfg: cfg}, nil
}

type managedHAKeeperClient struct {
	cfg    HAKeeperClientConfig
	client *hakeeperClient
}

func (c *managedHAKeeperClient) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.close()
}

func (c *managedHAKeeperClient) GetClusterDetails(ctx context.Context) (pb.ClusterDetails, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.ClusterDetails{}, err
		}
		cd, err := c.client.getClusterDetails(ctx)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return cd, err
	}
}

func (c *managedHAKeeperClient) SendCNHeartbeat(ctx context.Context,
	hb pb.CNStoreHeartbeat) error {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return err
		}
		err := c.client.sendCNHeartbeat(ctx, hb)
		if err != nil {
			c.resetClient()
		}
		if c.isRetryableError(err) {
			continue
		}
		return err
	}
}

func (c *managedHAKeeperClient) SendDNHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	for {
		if err := c.prepareClient(ctx); err != nil {
			return pb.CommandBatch{}, err
		}
		cb, err := c.client.sendDNHeartbeat(ctx, hb)
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
		cb, err := c.client.sendLogHeartbeat(ctx, hb)
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
	return errors.Is(err, ErrNotHAKeeper)
}

func (c *managedHAKeeperClient) resetClient() {
	if c.client != nil {
		cc := c.client
		c.client = nil
		if err := cc.close(); err != nil {
			plog.Errorf("failed to close client %v", err)
		}
	}
}

func (c *managedHAKeeperClient) prepareClient(ctx context.Context) error {
	if c.client != nil {
		return nil
	}
	cc, err := newHAKeeperClient(ctx, c.cfg)
	if err != nil {
		return err
	}
	c.client = cc
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
		isHAKeeper, err := c.checkIsHAKeeper(ctx)
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
		return nil, ErrNotHAKeeper
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
	return resp.ClusterDetails, nil
}

func (c *hakeeperClient) sendCNHeartbeat(ctx context.Context, hb pb.CNStoreHeartbeat) error {
	req := pb.Request{
		Method:      pb.CN_HEARTBEAT,
		CNHeartbeat: hb,
	}
	_, err := c.sendHeartbeat(ctx, req)
	return err
}

func (c *hakeeperClient) sendDNHeartbeat(ctx context.Context,
	hb pb.DNStoreHeartbeat) (pb.CommandBatch, error) {
	req := pb.Request{
		Method:      pb.DN_HEARTBEAT,
		DNHeartbeat: hb,
	}
	return c.sendHeartbeat(ctx, req)
}

func (c *hakeeperClient) sendLogHeartbeat(ctx context.Context,
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
