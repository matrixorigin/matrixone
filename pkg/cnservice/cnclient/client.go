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
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
)

const (
	//@todo need to find out why rpc timeout, or move heartbeat check to another way.
	defaultRPCTimeout = 120 * time.Second
)

// client each node will hold only one client.
// It is responsible for sending messages to other nodes. and messages were received
// and handled by cn-server.
var client = &CNClient{
	ready:       false,
	requestPool: &sync.Pool{New: func() any { return &pipeline.Message{} }},
}

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
	client.Lock()
	defer client.Unlock()

	return client.ready
}

type CNClient struct {
	sync.Mutex

	localServiceAddress string
	ready               bool
	config              *ClientConfig
	client              morpc.RPCClient

	// pool for send message
	requestPool *sync.Pool
}

func (c *CNClient) NewStream(backend string) (morpc.Stream, error) {
	c.Lock()
	defer c.Unlock()
	if !c.ready {
		return nil, moerr.NewInternalErrorNoCtx("cn client is not ready")
	}

	if backend == c.localServiceAddress {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("remote run pipeline in local: %s", backend))
	}
	return c.client.NewStream(backend, true)
}

func (c *CNClient) Close() error {
	c.Lock()
	defer c.Unlock()

	c.ready = false
	if c.client != nil {
		return c.client.Close()
	}
	return nil
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
	// RPC rpc config
	RPC rpc.Config
}

// TODO: Here it needs to be refactored together with Runtime
func NewCNClient(
	localServiceAddress string,
	cfg *ClientConfig) error {
	logger := logutil.GetGlobalLogger().Named("cn-backend")

	var err error
	cfg.Fill()

	client.Lock()
	defer client.Unlock()
	if client.ready {
		return nil
	}

	cli := client
	cli.config = cfg
	cli.localServiceAddress = localServiceAddress
	cli.requestPool = &sync.Pool{New: func() any { return &pipeline.Message{} }}

	codec := morpc.NewMessageCodec(cli.acquireMessage,
		morpc.WithCodecMaxBodySize(int(cfg.RPC.MaxMessageSize)))
	factory := morpc.NewGoettyBasedBackendFactory(codec,
		morpc.WithBackendGoettyOptions(
			goetty.WithSessionRWBUfferSize(cfg.ReadBufferSize, cfg.WriteBufferSize),
			goetty.WithSessionReleaseMsgFunc(func(v any) {
				m := v.(morpc.RPCMessage)
				if !m.InternalMessage() {
					cli.releaseMessage(m.Message.(*pipeline.Message))
				}
			}),
		),
		morpc.WithBackendReadTimeout(defaultRPCTimeout),
		morpc.WithBackendConnectTimeout(cfg.TimeOutForEachConnect),
		morpc.WithBackendLogger(logger),
	)

	cli.client, err = morpc.NewClient(
		"pipeline-client",
		factory,
		morpc.WithClientMaxBackendPerHost(cfg.MaxSenderNumber),
		morpc.WithClientLogger(logger),
	)
	cli.ready = err == nil
	return nil
}

func (c *CNClient) acquireMessage() morpc.Message {
	// TODO: pipeline.Message has many []byte fields, maybe can use PayloadMessage to avoid mem copy.
	return c.requestPool.Get().(*pipeline.Message)
}

func (c *CNClient) releaseMessage(m *pipeline.Message) {
	if c.requestPool != nil {
		m.Reset()
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

func GetRPCClient() morpc.RPCClient {
	client.Lock()
	defer client.Unlock()

	if client.ready {
		return client.client
	}
	return nil
}
