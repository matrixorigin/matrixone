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

package cnclient

import (
	"context"
	"sync"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

var Client *CNClient

type CNClient struct {
	config *ClientConfig
	client morpc.RPCClient

	// pool for send message
	requestPool *sync.Pool
}

func (c *CNClient) Send(ctx context.Context, backend string, request morpc.Message) (*morpc.Future, error) {
	return c.client.Send(ctx, backend, request)
}

func (c *CNClient) NewStream(backend string) (morpc.Stream, error) {
	return c.client.NewStream(backend)
}

func (c *CNClient) Close() error {
	return c.client.Close()
}

const (
	dfMaxSenderNumber       = 10
	dfClientReadBufferSize  = 1 << 10
	dfClientWriteBufferSize = 1 << 10
)

// ClientConfig a config to init a CNClient
type ClientConfig struct {
	// MaxSenderNumber is the max number of backends per host for compute node service.
	MaxSenderNumber int
	ReadBufferSize  int
	WriteBufferSize int
}

func NewCNClient(cfg *ClientConfig) error {
	var err error
	cfg.Fill()
	Client = &CNClient{config: cfg}
	Client.requestPool = &sync.Pool{New: func() any { return &pipeline.Message{} }}

	// FIXME: checksum needed? hlc integration needed?
	codec := morpc.NewMessageCodec(Client.acquireMessage)
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendConnectWhenCreate(),
		morpc.WithBackendGoettyOptions(goetty.WithSessionRWBUfferSize(
			cfg.ReadBufferSize, cfg.WriteBufferSize)),
	)

	Client.client, err = morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(cfg.MaxSenderNumber),
	)
	return err
}

func (c *CNClient) acquireMessage() morpc.Message {
	// TODO: pipeline.Message has many []byte fields, maybe can use PayloadMessage to avoid mem copy.
	return c.requestPool.Get().(*pipeline.Message)
}

// Fill set some default value for client config.
func (cfg *ClientConfig) Fill() {
	if cfg.MaxSenderNumber <= 0 {
		cfg.MaxSenderNumber = dfMaxSenderNumber
	}
	if cfg.ReadBufferSize < 0 {
		cfg.ReadBufferSize = dfClientReadBufferSize
	}
	if cfg.WriteBufferSize < 0 {
		cfg.WriteBufferSize = dfClientWriteBufferSize
	}
}
