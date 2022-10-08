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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var DefaultReadMaxSize = uint64(10)

type Config struct {
	ClientPoolMaxSize     int
	ClientPoolInitSize    int
	GetClientRetryTimeOut time.Duration

	RecordSize           int
	ReadCacheSize        int
	AppenderMaxCount     int
	ReadMaxSize          uint64
	NewRecordSize        int
	NewClientDuration    time.Duration
	ClientAppendDuration time.Duration
	TruncateDuration     time.Duration
	// AppendFrequency      time.Duration
	RetryTimeout        time.Duration
	GetTruncateDuration time.Duration
	ReadDuration        time.Duration

	ClientConfig *logservice.ClientConfig
}

func NewDefaultConfig(cfg *logservice.ClientConfig) *Config {
	return &Config{
		ClientPoolMaxSize:     100,
		ClientPoolInitSize:    100,
		GetClientRetryTimeOut: time.Second,

		RecordSize:        int(common.K * 16),
		ReadCacheSize:     100,
		ReadMaxSize:       common.K * 20,
		AppenderMaxCount:  100,
		NewRecordSize:     int(common.K * 20),
		NewClientDuration: time.Second,
		// AppendFrequency:      time.Millisecond * 5,
		RetryTimeout:         time.Minute,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}

func NewTestConfig(cfg *logservice.ClientConfig) *Config {
	return &Config{
		ClientPoolMaxSize:     10,
		ClientPoolInitSize:    5,
		GetClientRetryTimeOut: time.Second,

		RecordSize:       int(common.M * 10),
		ReadCacheSize:    10,
		ReadMaxSize:      common.K * 20,
		AppenderMaxCount: 10,
		NewRecordSize:    int(common.K * 20),
		// AppendFrequency:      time.Millisecond /1000,
		RetryTimeout:         time.Minute,
		NewClientDuration:    time.Second,
		ClientAppendDuration: time.Second,
		TruncateDuration:     time.Second,
		GetTruncateDuration:  time.Second,
		ReadDuration:         time.Second,
		ClientConfig:         cfg,
	}
}
