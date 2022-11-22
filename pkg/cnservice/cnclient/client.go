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

	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

// client each node will hold only one client.
// It is responsible for sending messages to other nodes. and messages were received
// and handled by cn-server.
var client *CNClient

func CloseCNClient() error {
	return client.Close()
}

func GetStreamSender(backend string) (morpc.Stream, error) {
	return client.NewStream(backend)
}

func AcquireMessage() *pipeline.Message {
	return client.acquireMessage().(*pipeline.Message)
}

func IsCNClientReady() bool {
	return client != nil && client.ready
}

type CNClient struct {
	ready  bool
	config *ClientConfig
	client morpc.RPCClient

	// pool for send message
	requestPool *sync.Pool
}

func (c *CNClient) Send(ctx context.Context, backend string, request morpc.Message) (*morpc.Future, error) {
	return c.client.Send(ctx, backend, request)
}

func (c *CNClient) NewStream(backend string) (morpc.Stream, error) {
	return c.client.NewStream(backend, true)
}

func (c *CNClient) Close() error {
	c.ready = false
	return c.client.Close()
}

const (
	dfMaxSenderNumber       = 100000
	dfConnectTimeout        = 5 * time.Second
	dfClientReadBufferSize  = 1 << 10
	dfClientWriteBufferSize = 1 << 10
)

// ClientConfig a config to init a CNClient
type ClientConfig struct {
	// MaxSenderNumber is the max number of backends per host for compute node service.
	MaxSenderNumber int
	// TimeOutForEachConnect is the out time for each tcp connect.
	TimeOutForEachConnect time.Duration
	// related buffer size.
	ReadBufferSize  int
	WriteBufferSize int
}

func NewCNClient(cfg *ClientConfig) error {
	var err error
	cfg.Fill()
	client = &CNClient{config: cfg}
	client.requestPool = &sync.Pool{New: func() any { return &pipeline.Message{} }}

	codec := morpc.NewMessageCodec(client.acquireMessage)
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(cfg.ReadBufferSize, cfg.WriteBufferSize),
			goetty.WithSessionReleaseMsgFunc(func(v any) {
				m := v.(morpc.RPCMessage)
				client.releaseMessage(m.Message.(*pipeline.Message))
			}),
		),
		morpc.WithBackendConnectTimeout(cfg.TimeOutForEachConnect),
		morpc.WithBackendLogger(logutil.GetGlobalLogger().Named("cn-backend")),
	)

	client.client, err = morpc.NewClient(factory,
		morpc.WithClientMaxBackendPerHost(cfg.MaxSenderNumber),
		morpc.WithClientTag("cn-client"),
	)
	client.ready = true
	return err
}

func (c *CNClient) acquireMessage() morpc.Message {
	// TODO: pipeline.Message has many []byte fields, maybe can use PayloadMessage to avoid mem copy.
	return c.requestPool.Get().(*pipeline.Message)
}

func (c *CNClient) releaseMessage(m *pipeline.Message) {
	if c.requestPool != nil {
		m.Sid = 0
		m.Err = nil
		m.Data = nil
		m.ProcInfoData = nil
		m.Analyse = nil
		c.requestPool.Put(m)
	}
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
	if cfg.TimeOutForEachConnect <= 0 {
		cfg.TimeOutForEachConnect = dfConnectTimeout
	}
}
