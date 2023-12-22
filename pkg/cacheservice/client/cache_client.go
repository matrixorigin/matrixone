// Copyright 2021 - 2023 Matrix Origin
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

package client

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/cache"
)

type CacheClient interface {
	// SendMessage send message to a cache server.
	SendMessage(ctx context.Context, address string, req *cache.Request) (*cache.CacheResponse, error)
	// NewRequest creates a new request by cmd method.
	NewRequest(cache.CmdMethod) *cache.Request
	// Release releases the response.
	Release(*cache.CacheResponse)
	// Close closes the cache client.
	Close() error
}

// ClientConfig a config to init a CNClient
type ClientConfig struct {
	// RPC rpc config
	RPC morpc.Config
}

type cacheClient struct {
	config ClientConfig
	client morpc.RPCClient
	pool   morpc.MessagePool[*cache.Request, *cache.CacheResponse]
}

func NewCacheClient(cfg ClientConfig) (CacheClient, error) {
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	pool := morpc.NewMessagePool(
		func() *cache.Request { return &cache.Request{} },
		func() *cache.CacheResponse { return &cache.CacheResponse{} },
	)
	c, err := cfg.RPC.NewClient("cache-client", rt.Logger().Named("cache-client").RawLogger(),
		func() morpc.Message {
			return pool.AcquireResponse()
		},
	)
	if err != nil {
		return nil, err
	}
	cc := &cacheClient{
		config: cfg,
		client: c,
		pool:   pool,
	}
	return cc, nil
}

// NewRequest implements the CacheClient interface.
func (c *cacheClient) NewRequest(method cache.CmdMethod) *cache.Request {
	req := c.pool.AcquireRequest()
	req.CmdMethod = method
	return req
}

// SendMessage implements the CacheClient interface.
func (c *cacheClient) SendMessage(
	ctx context.Context, address string, req *cache.Request,
) (*cache.CacheResponse, error) {
	if address == "" {
		return nil, moerr.NewInternalError(ctx, "invalid CN query address %s", address)
	}
	f, err := c.client.Send(ctx, address, req)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	v, err := f.Get()
	if err != nil {
		return nil, err
	}
	resp := v.(*cache.CacheResponse)
	return c.unwrapResponseError(resp)
}

// Release implements the CacheService interface.
func (c *cacheClient) Release(resp *cache.CacheResponse) {
	c.pool.ReleaseResponse(resp)
}

// Close implements the CacheService interface.
func (c *cacheClient) Close() error {
	return c.client.Close()
}

func (c *cacheClient) unwrapResponseError(resp *cache.CacheResponse) (*cache.CacheResponse, error) {
	if err := resp.UnwrapError(); err != nil {
		c.pool.ReleaseResponse(resp)
		return nil, err
	}
	return resp, nil
}
