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

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// default cluster initial information
	defaultDNServiceNum  = 1
	defaultDNShardNum    = 1
	defaultLogServiceNum = 1
	defaultLogShardNum   = 1
	defaultLogReplicaNum = 1
	defaultCNServiceNum  = 1
	defaultCNShardNum    = 1

	// default configuration for services
	defaultHostAddr    = "127.0.0.1"
	defaultRootDataDir = "/tmp/mo/tests"

	// default configuration for dn service
	defaultDNStorage = dnservice.StorageTAE
	defaultCNEngine  = cnservice.EngineDistributedTAE

	// default configuration for log service
	defaultGossipSeedNum = 1
	defaultHAKeeperNum   = 1
	// we only use 3 seeds at most.
	maxGossipSeedNum = 3
	maxHAKeeperNum   = 3

	// default hakeeper configuration
	defaultTickPerSecond   = 10
	defaultLogStoreTimeout = 4 * time.Second
	defaultDNStoreTimeout  = 3 * time.Second
	defaultCNStoreTimeout  = 3 * time.Second
	defaultCheckInterval   = 1 * time.Second

	// default heartbeat configuration
	defaultLogHeartbeatInterval = 1 * time.Second
	defaultDNHeartbeatInterval  = 1 * time.Second
	defaultCNHeartbeatInterval  = 1 * time.Second
)

// Options are params for creating test cluster.
type Options struct {
	hostAddr    string
	rootDataDir string
	keepData    bool
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

	heartbeat struct {
		dn  time.Duration
		cn  time.Duration
		log time.Duration
	}

	storage struct {
		dnStorage dnservice.StorageType
		cnEngine  cnservice.EngineType
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
	if opt.storage.dnStorage == "" {
		opt.storage.dnStorage = defaultDNStorage
	}
	if opt.storage.cnEngine == "" {
		opt.storage.cnEngine = defaultCNEngine
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
	if opt.heartbeat.log == 0 {
		opt.heartbeat.log = defaultLogHeartbeatInterval
	}
	if opt.heartbeat.dn == 0 {
		opt.heartbeat.dn = defaultDNHeartbeatInterval
	}
	if opt.heartbeat.cn == 0 {
		opt.heartbeat.cn = defaultCNHeartbeatInterval
	}
	if opt.logger == nil {
		opt.logger = logutil.GetGlobalLogger()
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

// WithDNUseTAEStorage sets dn transaction use tae storage.
func (opt Options) WithDNUseTAEStorage() Options {
	opt.storage.dnStorage = dnservice.StorageTAE
	return opt
}

// WithDNUseMEMStorage sets dn transaction use mem storage.
func (opt Options) WithDNUseMEMStorage() Options {
	opt.storage.dnStorage = dnservice.StorageMEM
	return opt
}

// WithCNUseTAEEngine use tae engine as cn engine
func (opt Options) WithCNUseTAEEngine() Options {
	opt.storage.cnEngine = cnservice.EngineTAE
	return opt
}

// WithCNUseDistributedTAEEngine use distributed tae engine as cn engine
func (opt Options) WithCNUseDistributedTAEEngine() Options {
	opt.storage.cnEngine = cnservice.EngineDistributedTAE
	return opt
}

// WithCNUseMemoryEngine use memory engine as cn engine
func (opt Options) WithCNUseMemoryEngine() Options {
	opt.storage.cnEngine = cnservice.EngineDistributedTAE
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
	opt.heartbeat.dn = interval
	return opt
}

// WithLogHeartbeatInterval sets heartbeat interval fo log service.
func (opt Options) WithLogHeartbeatInterval(interval time.Duration) Options {
	opt.heartbeat.log = interval
	return opt
}

// WithCNHeartbeatInterval sets heartbeat interval fo cn service.
func (opt Options) WithCNHeartbeatInterval(interval time.Duration) Options {
	opt.heartbeat.cn = interval
	return opt
}

// GetDNStorageType returns the storage type that the dnservice used
func (opt Options) GetDNStorageType() dnservice.StorageType {
	return opt.storage.dnStorage
}

// GetCNEngineType returns the engine type that the cnservice used
func (opt Options) GetCNEngineType() dnservice.StorageType {
	return opt.storage.dnStorage
}

// WithKeepData sets keep data after cluster closed.
func (opt Options) WithKeepData() Options {
	opt.keepData = true
	return opt
}

// gossipSeedNum calculates the count of gossip seed.
//
// Select gossip addresses of the first 3 log services.
// If the number of log services was less than 3,
// then just return the number.
func gossipSeedNum(logServiceNum int) int {
	if logServiceNum < defaultGossipSeedNum {
		return defaultGossipSeedNum
	}
	if logServiceNum > maxGossipSeedNum {
		return maxGossipSeedNum
	}
	return logServiceNum
}

// haKeeperNum calculates the count of hakeeper replica.
//
// Select the first 3 log services to start hakeeper replica.
// If the number of log services was less than 3,
// then just return the number.
func haKeeperNum(haKeeperNum int) int {
	if haKeeperNum < defaultHAKeeperNum {
		return defaultHAKeeperNum
	}
	if haKeeperNum > maxHAKeeperNum {
		return maxHAKeeperNum
	}
	return haKeeperNum
}
