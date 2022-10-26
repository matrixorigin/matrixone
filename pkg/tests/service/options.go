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

package service

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// default cluster initial information
	defaultDNServiceNum  = 1
	defaultDNShardNum    = 1
	defaultLogServiceNum = 3
	defaultLogShardNum   = 1
	defaultLogReplicaNum = 3
	defaultCNServiceNum  = 1
	defaultCNShardNum    = 1

	// default configuration for services
	defaultHostAddr    = "127.0.0.1"
	defaultRootDataDir = "/tmp/tests/service"

	// default configuration for dn service
	defaultDnStorage = "MEM"

	// default configuration for log service
	defaultGossipSeedNum = 3
	defaultHAKeeperNum   = 3

	// default hakeeper configuration
	defaultTickPerSecond   = 10
	defaultLogStoreTimeout = 4 * time.Second
	defaultDNStoreTimeout  = 3 * time.Second
	defaultCNStoreTimeout  = 3 * time.Second
	defaultCheckInterval   = 1 * time.Second

	// default heartbeat configuration
	defaultLogHeartbeatInterval = 1 * time.Second
	defaultDNHeartbeatInterval  = 1 * time.Second
)

// Options are params for creating test cluster.
type Options struct {
	hostAddr    string
	rootDataDir string
	logger      *zap.Logger

	initial struct {
		dnServiceNum  int
		logServiceNum int
		cnServiceNum  int
		dnShardNum    uint64
		logShardNum   uint64
		cnShardNum    uint64
		logReplicaNum uint64
	}

	dn struct {
		txnStorageBackend string
		heartbeatInterval time.Duration
	}

	log struct {
		heartbeatInterval time.Duration
	}

	hakeeper struct {
		tickPerSecond   int
		checkInterval   time.Duration
		logStoreTimeout time.Duration
		dnStoreTimeout  time.Duration
		cnStoreTimeout  time.Duration
	}
}

// DefaultOptions sets a list of recommended options.
func DefaultOptions() Options {
	opt := Options{}
	opt.validate()
	return opt
}

// validate check and fill empty configurations.
func (opt *Options) validate() {
	if opt.hostAddr == "" {
		opt.hostAddr = defaultHostAddr
	}
	if opt.rootDataDir == "" {
		opt.rootDataDir = defaultRootDataDir
	}
	if opt.initial.dnServiceNum <= 0 {
		opt.initial.dnServiceNum = defaultDNServiceNum
	}
	if opt.initial.logServiceNum <= 0 {
		opt.initial.logServiceNum = defaultLogServiceNum
	}
	if opt.initial.cnServiceNum <= 0 {
		opt.initial.cnServiceNum = defaultCNServiceNum
	}
	if opt.initial.dnShardNum <= 0 {
		opt.initial.dnShardNum = defaultDNShardNum
	}
	if opt.initial.logShardNum <= 0 {
		opt.initial.logShardNum = defaultLogShardNum
	}
	if opt.initial.cnShardNum <= 0 {
		opt.initial.cnShardNum = defaultCNShardNum
	}
	if opt.initial.logReplicaNum <= 0 {
		opt.initial.logReplicaNum = defaultLogReplicaNum
	}
	if opt.dn.txnStorageBackend == "" {
		opt.dn.txnStorageBackend = defaultDnStorage
	}

	// hakeeper configuration
	if opt.hakeeper.tickPerSecond == 0 {
		opt.hakeeper.tickPerSecond = defaultTickPerSecond
	}
	if opt.hakeeper.logStoreTimeout == 0 {
		opt.hakeeper.logStoreTimeout = defaultLogStoreTimeout
	}
	if opt.hakeeper.dnStoreTimeout == 0 {
		opt.hakeeper.dnStoreTimeout = defaultDNStoreTimeout
	}
	if opt.hakeeper.cnStoreTimeout == 0 {
		opt.hakeeper.cnStoreTimeout = defaultCNStoreTimeout
	}
	if opt.hakeeper.checkInterval == 0 {
		opt.hakeeper.checkInterval = defaultCheckInterval
	}

	// heartbeat configuration
	if opt.log.heartbeatInterval == 0 {
		opt.log.heartbeatInterval = defaultLogHeartbeatInterval
	}
	if opt.dn.heartbeatInterval == 0 {
		opt.dn.heartbeatInterval = defaultDNHeartbeatInterval
	}
}

// BuildHAKeeperConfig returns hakeeper.Config
//
// We could check timeout for dn/log store via hakeeper.Config.
func (opt Options) BuildHAKeeperConfig() hakeeper.Config {
	return hakeeper.Config{
		TickPerSecond:   opt.hakeeper.tickPerSecond,
		LogStoreTimeout: opt.hakeeper.logStoreTimeout,
		DNStoreTimeout:  opt.hakeeper.dnStoreTimeout,
		CNStoreTimeout:  opt.hakeeper.cnStoreTimeout,
	}
}

// WithDNServiceNum sets dn service number in the cluster.
func (opt Options) WithDNServiceNum(num int) Options {
	opt.initial.dnServiceNum = num
	return opt
}

// WithLogServiceNum sets log service number in the cluster.
func (opt Options) WithLogServiceNum(num int) Options {
	opt.initial.logServiceNum = num
	return opt
}

func (opt Options) WithCNServiceNum(num int) Options {
	opt.initial.cnServiceNum = num
	return opt
}

// WithLogShardNum sets log shard number in the cluster.
func (opt Options) WithLogShardNum(num uint64) Options {
	opt.initial.logShardNum = num
	return opt
}

// WithDNShardNum sets dn shard number in the cluster.
func (opt Options) WithDNShardNum(num uint64) Options {
	opt.initial.dnShardNum = num
	return opt
}

func (opt Options) WithCNShardNum(num uint64) Options {
	opt.initial.cnShardNum = num
	return opt
}

// WithLogReplicaNum sets log replica number for the cluster.
func (opt Options) WithLogReplicaNum(num uint64) Options {
	opt.initial.logReplicaNum = num
	return opt
}

// WithRootDataDir sets root for service data directory.
func (opt Options) WithRootDataDir(root string) Options {
	opt.rootDataDir = root
	return opt
}

// WithDNStorage sets dn transaction storage.
func (opt Options) WithDNTxnStorage(s string) Options {
	opt.dn.txnStorageBackend = s
	return opt
}

// WithHostAddress sets host address for all services.
func (opt Options) WithHostAddress(host string) Options {
	opt.hostAddr = host
	return opt
}

// WithLogLevel sets log level.
func (opt Options) WithLogLevel(lvl zapcore.Level) Options {
	opt.logger = logutil.GetPanicLoggerWithLevel(lvl)
	return opt
}

// WithLogger sets logger.
func (opt Options) WithLogger(logger *zap.Logger) Options {
	opt.logger = logger
	return opt
}

// WithHKTickPerSecond sets tick per second for hakeeper.
func (opt Options) WithHKTickPerSecond(tick int) Options {
	opt.hakeeper.tickPerSecond = tick
	return opt
}

// WithHKLogStoreTimeout sets log store timeout for hakeeper.
func (opt Options) WithHKLogStoreTimeout(timeout time.Duration) Options {
	opt.hakeeper.logStoreTimeout = timeout
	return opt
}

// WithHKDNStoreTimeout sets dn store timeout for hakeeper.
func (opt Options) WithHKDNStoreTimeout(timeout time.Duration) Options {
	opt.hakeeper.dnStoreTimeout = timeout
	return opt
}

func (opt Options) WithHKCNStoreTimeout(timeout time.Duration) Options {
	opt.hakeeper.cnStoreTimeout = timeout
	return opt
}

// WithHKCheckInterval sets check interval for hakeeper.
func (opt Options) WithHKCheckInterval(interval time.Duration) Options {
	opt.hakeeper.checkInterval = interval
	return opt
}

// WithDNHeartbeatInterval sets heartbeat interval fo dn service.
func (opt Options) WithDNHeartbeatInterval(interval time.Duration) Options {
	opt.dn.heartbeatInterval = interval
	return opt
}

// WithLogHeartbeatInterval sets heartbeat interval fo log service.
func (opt Options) WithLogHeartbeatInterval(interval time.Duration) Options {
	opt.log.heartbeatInterval = interval
	return opt
}

// GetTxnStorageBackend returns the txn storage backend
func (opt Options) GetTxnStorageBackend() string {
	return opt.dn.txnStorageBackend
}

// gossipSeedNum calculates the count of gossip seed.
//
// Select gossip addresses of the first 3 log services.
// If the number of log services was less than 3,
// then select all of them.
func gossipSeedNum(logServiceNum int) int {
	if logServiceNum < defaultGossipSeedNum {
		return logServiceNum
	}
	return defaultGossipSeedNum
}

// haKeeperNum calculates the count of hakeeper replica.
//
// Select the first 3 log services to start hakeeper replica.
// If the number of log services was less than 3,
// then select the first of them.
func haKeeperNum(logServiceNum int) int {
	if logServiceNum < defaultHAKeeperNum {
		return 1
	}
	return defaultHAKeeperNum
}
