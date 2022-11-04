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

package rpc

import (
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"go.uber.org/zap"
)

var (
	defaultMaxConnections  = 400
	defaultMaxIdleDuration = time.Minute
	defaultSendQueueSize   = 10240
	defaultBufferSize      = 1024
	defaultMaxMessageSize  = 1024 * 1024 * 10
)

// Config txn sender config
type Config struct {
	// MaxConnections maximum number of connections to communicate with each DNStore.
	// Default is 400.
	MaxConnections int `toml:"max-connections"`
	// MaxIdleDuration maximum connection idle time, connection will be closed automatically
	// if this value is exceeded. Default is 1 min.
	MaxIdleDuration toml.Duration `toml:"max-idle-duration"`
	// SendQueueSize maximum capacity of the send request queue per connection, when the
	// queue is full, the send request will be blocked. Default is 10240.
	SendQueueSize int `toml:"send-queue-size"`
	// BusyQueueSize when the length of the send queue reaches the currently set value, the
	// current connection is busy with high load. When any connection with Busy status exists,
	// a new connection will be created until the value set by MaxConnections is reached.
	// Default is 3/4 of SendQueueSize.
	BusyQueueSize int `toml:"busy-queue-size"`
	// WriteBufferSize buffer size for write messages per connection. Default is 1kb
	WriteBufferSize toml.ByteSize `toml:"write-buffer-size"`
	// ReadBufferSize buffer size for read messages per connection. Default is 1kb
	ReadBufferSize toml.ByteSize `toml:"read-buffer-size"`
	// MaxMessageSize max size for read messages from dn. Default is 10M
	MaxMessageSize toml.ByteSize `toml:"max-message-size"`
}

func (c *Config) adjust() {
	if c.MaxConnections == 0 {
		c.MaxConnections = defaultMaxConnections
	}
	if c.SendQueueSize == 0 {
		c.SendQueueSize = defaultSendQueueSize
	}
	if c.BusyQueueSize == 0 {
		c.BusyQueueSize = c.SendQueueSize * 3 / 4
	}
	if c.WriteBufferSize == 0 {
		c.WriteBufferSize = toml.ByteSize(defaultBufferSize)
	}
	if c.ReadBufferSize == 0 {
		c.ReadBufferSize = toml.ByteSize(defaultBufferSize)
	}
	if c.MaxIdleDuration.Duration == 0 {
		c.MaxIdleDuration.Duration = defaultMaxIdleDuration
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = toml.ByteSize(defaultMaxMessageSize)
	}
}

func (c Config) getBackendOptions(logger *zap.Logger) []morpc.BackendOption {
	return []morpc.BackendOption{
		morpc.WithBackendLogger(logger),
		morpc.WithBackendBusyBufferSize(c.BusyQueueSize),
		morpc.WithBackendBufferSize(c.SendQueueSize),
		morpc.WithBackendGoettyOptions(goetty.WithSessionRWBUfferSize(int(c.ReadBufferSize),
			int(c.WriteBufferSize))),
	}
}

func (c Config) getClientOptions(logger *zap.Logger) []morpc.ClientOption {
	return []morpc.ClientOption{
		morpc.WithClientLogger(logger),
		morpc.WithClientMaxBackendPerHost(c.MaxConnections),
		morpc.WithClientMaxBackendMaxIdleDuration(c.MaxIdleDuration.Duration),
	}
}
