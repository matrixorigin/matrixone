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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
)

var DefaultReadMaxSize = uint64(10)

type Config struct {
	ClientMaxCount        int
	GetClientRetryTimeOut time.Duration

	RecordSize           int
	ReadCacheSize        int
	NewClientDuration    time.Duration
	ClientAppendDuration time.Duration
	TruncateDuration     time.Duration
	// AppendFrequency      time.Duration
	RetryTimeout        time.Duration
	GetTruncateDuration time.Duration
	ReadDuration        time.Duration

	ClientFactory LogServiceClientFactory
}

type LogServiceClientFactory logservice.ClientFactory

func NewDefaultConfig(clientFactory LogServiceClientFactory) *Config {
	return &Config{
		ClientMaxCount:        100,
		GetClientRetryTimeOut: time.Second * 3,

		RecordSize:        int(mpool.MB * 1),
		ReadCacheSize:     100,
		NewClientDuration: time.Second * 3,
		// AppendFrequency:      time.Millisecond * 5,
		RetryTimeout:         time.Minute * 3,
		ClientAppendDuration: time.Second * 10,
		TruncateDuration:     time.Second * 10,
		GetTruncateDuration:  time.Second * 5,
		ReadDuration:         time.Second * 5,
		ClientFactory:        clientFactory,
	}
}

func NewTestConfig(ccfg *logservice.ClientConfig) *Config {
	cfg := &Config{
		ClientMaxCount:        10,
		GetClientRetryTimeOut: time.Second,

		RecordSize:    int(mpool.MB * 10),
		ReadCacheSize: 10,
		// AppendFrequency:      time.Millisecond /1000,
		RetryTimeout:         time.Minute,
		NewClientDuration:    time.Second,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
	}
	cfg.ClientFactory = func() (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.NewClientDuration)
		logserviceClient, err := logservice.NewClient(ctx, *ccfg)
		cancel()
		return logserviceClient, err
	}
	return cfg
}
