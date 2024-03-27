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

package morpc

import (
	"math"
	"runtime"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"go.uber.org/zap"
)

var (
	defaultMaxConnections        = 1
	defaultMaxIdleDuration       = time.Minute
	defaultSendQueueSize         = 10240
	defaultBufferSize            = 1024
	defaultPayloadCopyBufferSize = 16 * 1024
	defaultMaxMessageSize        = 1024 * 1024 * 1024 * 10 // 10GB
)

// Config rpc client config
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
	// MaxMessageSize max message size for rpc. Default is 100M
	MaxMessageSize toml.ByteSize `toml:"max-message-size"`
	// PayloadCopyBufferSize buffer size for copy payload to socket. Default is 16kb
	PayloadCopyBufferSize toml.ByteSize `toml:"payload-copy-buffer-size"`
	// EnableCompress enable compress message
	EnableCompress bool `toml:"enable-compress"`

	// ServerWorkers number of server workers for handle requests
	ServerWorkers int `toml:"server-workers"`
	// ServerBufferQueueSize queue size for server buffer requetsts
	ServerBufferQueueSize int `toml:"server-buffer-queue-size"`

	// BackendOptions extra backend options
	BackendOptions []BackendOption `toml:"-"`
	// ClientOptions extra client options
	ClientOptions []ClientOption `toml:"-"`
	// CodecOptions extra codec options
	CodecOptions []CodecOption `toml:"-"`
}

// Adjust adjust config, fill default value
func (c *Config) Adjust() {
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
	if c.PayloadCopyBufferSize == 0 {
		c.PayloadCopyBufferSize = toml.ByteSize(defaultPayloadCopyBufferSize)
	}
	if c.SendQueueSize == 0 {
		c.SendQueueSize = 100000
	}
	if c.ServerWorkers == 0 {
		c.ServerWorkers = int(math.Max(100, float64(8*runtime.NumCPU())))
	}
	if c.ServerBufferQueueSize == 0 {
		c.ServerBufferQueueSize = 100000
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = toml.ByteSize(defaultMaxMessageSize)
	}
}

// NewClient create client from config
func (c Config) NewClient(
	name string,
	logger *zap.Logger,
	responseFactory func() Message) (RPCClient, error) {
	var codecOpts []CodecOption
	codecOpts = append(codecOpts,
		WithCodecEnableChecksum(),
		WithCodecPayloadCopyBufferSize(int(c.PayloadCopyBufferSize)),
		WithCodecMaxBodySize(int(c.MaxMessageSize)))
	codecOpts = append(codecOpts, c.CodecOptions...)
	if c.EnableCompress {
		mp, err := mpool.NewMPool(name, 0, mpool.NoFixed)
		if err != nil {
			return nil, err
		}
		codecOpts = append(codecOpts, WithCodecEnableCompress(mp))
	}

	codec := NewMessageCodec(
		responseFactory,
		codecOpts...)
	bf := NewGoettyBasedBackendFactory(codec, c.getBackendOptions(logger.Named(name))...)
	return NewClient(name, bf, c.getClientOptions(logger.Named(name))...)
}

// NewServer new rpc server
func (c Config) NewServer(
	name string,
	address string,
	logger *zap.Logger,
	requestFactory func() Message,
	responseReleaseFunc func(Message),
	opts ...ServerOption) (RPCServer, error) {
	var codecOpts []CodecOption
	codecOpts = append(codecOpts,
		WithCodecEnableChecksum(),
		WithCodecPayloadCopyBufferSize(int(c.PayloadCopyBufferSize)),
		WithCodecMaxBodySize(int(c.MaxMessageSize)))
	codecOpts = append(codecOpts, c.CodecOptions...)
	if c.EnableCompress {
		mp, err := mpool.NewMPool(name, 0, mpool.NoFixed)
		if err != nil {
			return nil, err
		}
		codecOpts = append(codecOpts, WithCodecEnableCompress(mp))
	}
	opts = append(opts,
		WithServerLogger(logger.Named(name)),
		WithServerGoettyOptions(goetty.WithSessionReleaseMsgFunc(func(v interface{}) {
			m := v.(RPCMessage)
			if !m.InternalMessage() {
				responseReleaseFunc(m.Message)
			}
		})))
	return NewRPCServer(
		name,
		address,
		NewMessageCodec(requestFactory, codecOpts...),
		opts...)
}

func (c Config) getBackendOptions(logger *zap.Logger) []BackendOption {
	var opts []BackendOption
	opts = append(opts,
		WithBackendLogger(logger),
		WithBackendBusyBufferSize(c.BusyQueueSize),
		WithBackendBufferSize(c.SendQueueSize),
		WithBackendGoettyOptions(goetty.WithSessionRWBUfferSize(
			int(c.ReadBufferSize),
			int(c.WriteBufferSize))))
	opts = append(opts, c.BackendOptions...)
	return opts
}

func (c Config) getClientOptions(logger *zap.Logger) []ClientOption {
	var opts []ClientOption
	opts = append(opts,
		WithClientLogger(logger),
		WithClientMaxBackendPerHost(c.MaxConnections),
		WithClientMaxBackendMaxIdleDuration(c.MaxIdleDuration.Duration))
	opts = append(opts, c.ClientOptions...)
	return opts
}
