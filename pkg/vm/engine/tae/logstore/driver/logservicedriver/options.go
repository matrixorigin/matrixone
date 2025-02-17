// Copyright 2021 Matrix Origin
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

package logservicedriver

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
)

const (
	DefaultMaxClient     = 100
	DefaultClientBufSize = mpool.MB
	DefaultMaxTimeout    = time.Second * 30
	DefaultMaxRetryCount = 10
)

type Config struct {
	ClientMaxCount int
	ClientBufSize  int

	MaxTimeout    time.Duration
	MaxRetryCount int

	ClientFactory LogServiceClientFactory
	IsMockBackend bool
}

type LogServiceClientFactory logservice.ClientFactory

type ConfigOption func(*Config)

func NewMockServiceAndClientFactory() (MockBackend, LogServiceClientFactory) {
	backend := NewMockBackend()
	return backend, func() (logservice.Client, error) {
		return newMockBackendClient(backend), nil
	}
}

func WithConfigOptClientFactory(f LogServiceClientFactory) ConfigOption {
	return func(cfg *Config) {
		cfg.IsMockBackend = false
		cfg.ClientFactory = f
	}
}

func WithConfigOptClientConfig(sid string, clientCfg *logservice.ClientConfig) ConfigOption {
	return func(cfg *Config) {
		cfg.IsMockBackend = false
		cfg.ClientFactory = func() (client logservice.Client, err error) {
			ctx, cancel := context.WithTimeoutCause(
				context.Background(), cfg.MaxTimeout, moerr.CauseNewLogServiceClient,
			)
			defer cancel()
			if client, err = logservice.NewClient(ctx, sid, *clientCfg); err != nil {
				err = moerr.AttachCause(ctx, err)
			}
			return
		}
	}
}

func WithConfigOptMaxClient(maxCount int) ConfigOption {
	return func(cfg *Config) {
		cfg.ClientMaxCount = maxCount
	}
}

func WithConfigOptClientBufSize(bufSize int) ConfigOption {
	return func(cfg *Config) {
		cfg.ClientBufSize = bufSize
	}
}

func WithConfigOptMaxTimeout(timeout time.Duration) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxTimeout = timeout
	}
}

func WithConfigOptMaxRetryCount(retryCount int) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxRetryCount = retryCount
	}
}

func WithConfigMockClient(backend MockBackend) ConfigOption {
	return func(cfg *Config) {
		cfg.IsMockBackend = true
		cfg.ClientFactory = func() (logservice.Client, error) {
			return newMockBackendClient(backend), nil
		}
	}
}

func NewConfig(
	sid string,
	opts ...ConfigOption,
) (cfg Config) {
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.fillDefaults()
	cfg.validate()
	return cfg
}

func (cfg Config) String() string {
	var w bytes.Buffer
	w.WriteString("LogDriver-Config{")
	w.WriteString("ClientMaxCount:")
	w.WriteString(strconv.Itoa(cfg.ClientMaxCount))
	w.WriteString(",ClientBufSize:")
	w.WriteString(strconv.Itoa(cfg.ClientBufSize))
	w.WriteString(",MaxTimeout:")
	w.WriteString(cfg.MaxTimeout.String())
	w.WriteString(",MaxRetryCount:")
	w.WriteString(strconv.Itoa(cfg.MaxRetryCount))
	w.WriteString(",IsMockBackend:")
	w.WriteString(strconv.FormatBool(cfg.IsMockBackend))
	w.WriteString("}")
	return w.String()
}

func (cfg *Config) fillDefaults() {
	if cfg.MaxRetryCount <= 0 {
		cfg.MaxRetryCount = DefaultMaxRetryCount
	}
	if cfg.ClientMaxCount <= 0 {
		cfg.ClientMaxCount = DefaultMaxClient
	}
	if cfg.ClientBufSize <= 0 {
		cfg.ClientBufSize = DefaultClientBufSize
	}
	if cfg.MaxTimeout <= 0 {
		cfg.MaxTimeout = DefaultMaxTimeout
	}
}

func (cfg *Config) validate() {
	if cfg.ClientFactory == nil {
		panic("ClientFactory is nil")
	}
}

func (cfg Config) RetryInterval() time.Duration {
	if cfg.MaxRetryCount == 0 {
		return 0
	}
	return cfg.MaxTimeout / time.Duration(cfg.MaxRetryCount) / 100
}
