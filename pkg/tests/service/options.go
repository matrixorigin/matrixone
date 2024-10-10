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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// default cluster initial information
	defaultTNServiceNum  = 1
	defaultTNShardNum    = 1
	defaultLogServiceNum = 1
	defaultLogShardNum   = 1
	defaultLogReplicaNum = 1
	defaultCNServiceNum  = 1
	defaultCNShardNum    = 1

	// default configuration for services
	defaultHostAddr    = "127.0.0.1"
	defaultRootDataDir = "/tmp/mo/tests"

	// default configuration for tn service
	defaultTNStorage = tnservice.StorageTAE
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
	defaultTNStoreTimeout  = 3 * time.Second
	defaultCNStoreTimeout  = 3 * time.Second
	defaultCheckInterval   = 1 * time.Second

	// default heartbeat configuration
	defaultLogHeartbeatInterval = 1 * time.Second
	defaultTNHeartbeatInterval  = 1 * time.Second
	defaultCNHeartbeatInterval  = 1 * time.Second

	// default logtail push server configuration
	defaultRpcMaxMessageSize          = 16 * mpool.KB
	defaultRPCStreamPoisonTime        = 5 * time.Second
	defaultLogtailCollectInterval     = 50 * time.Millisecond
	defaultLogtailResponseSendTimeout = 10 * time.Second
	defaultPullWorkerPoolSize         = 50
)

// Options are params for creating test cluster.
type Options struct {
	hostAddr    string
	rootDataDir string
	keepData    bool
	logger      *zap.Logger

	initial struct {
		tnServiceNum  int
		logServiceNum int
		cnServiceNum  int
		tnShardNum    uint64
		logShardNum   uint64
		cnShardNum    uint64
		logReplicaNum uint64
	}

	heartbeat struct {
		tn  time.Duration
		cn  time.Duration
		log time.Duration
	}

	storage struct {
		tnStorage tnservice.StorageType
		cnEngine  cnservice.EngineType
	}

	hakeeper struct {
		tickPerSecond   int
		checkInterval   time.Duration
		logStoreTimeout time.Duration
		tnStoreTimeout  time.Duration
		cnStoreTimeout  time.Duration
	}

	logtailPushServer struct {
		rpcMaxMessageSize          int64
		logtailRPCStreamPoisonTIme time.Duration
		logtailCollectInterval     time.Duration
		logtailResponseSendTimeout time.Duration
		pullWorkerPoolSize         int64
	}

	cn struct {
		optionFunc func(i int) []cnservice.Option
	}
}

// DefaultOptions sets a list of recommended options.
func DefaultOptions() Options {
	opt := Options{}
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
	if opt.initial.tnServiceNum <= 0 {
		opt.initial.tnServiceNum = defaultTNServiceNum
	}
	if opt.initial.logServiceNum <= 0 {
		opt.initial.logServiceNum = defaultLogServiceNum
	}
	if opt.initial.cnServiceNum <= 0 {
		opt.initial.cnServiceNum = defaultCNServiceNum
	}
	if opt.initial.tnShardNum <= 0 {
		opt.initial.tnShardNum = defaultTNShardNum
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
	if opt.storage.tnStorage == "" {
		opt.storage.tnStorage = defaultTNStorage
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
	if opt.hakeeper.tnStoreTimeout == 0 {
		opt.hakeeper.tnStoreTimeout = defaultTNStoreTimeout
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
	if opt.heartbeat.tn == 0 {
		opt.heartbeat.tn = defaultTNHeartbeatInterval
	}
	if opt.heartbeat.cn == 0 {
		opt.heartbeat.cn = defaultCNHeartbeatInterval
	}
	if opt.logger == nil {
		opt.logger = logutil.GetGlobalLogger()
	}

	// logtail push server configuration
	if opt.logtailPushServer.rpcMaxMessageSize <= 0 {
		opt.logtailPushServer.rpcMaxMessageSize = defaultRpcMaxMessageSize
	}
	if opt.logtailPushServer.logtailRPCStreamPoisonTIme <= 0 {
		opt.logtailPushServer.logtailRPCStreamPoisonTIme = defaultRPCStreamPoisonTime
	}
	if opt.logtailPushServer.logtailCollectInterval <= 0 {
		opt.logtailPushServer.logtailCollectInterval = defaultLogtailCollectInterval
	}
	if opt.logtailPushServer.logtailResponseSendTimeout <= 0 {
		opt.logtailPushServer.logtailResponseSendTimeout = defaultLogtailResponseSendTimeout
	}
	if opt.logtailPushServer.pullWorkerPoolSize <= 0 {
		opt.logtailPushServer.pullWorkerPoolSize = defaultPullWorkerPoolSize
	}
}

// BuildHAKeeperConfig returns hakeeper.Config
//
// We could check timeout for dn/log store via hakeeper.Config.
func (opt Options) BuildHAKeeperConfig() hakeeper.Config {
	return hakeeper.Config{
		TickPerSecond:   opt.hakeeper.tickPerSecond,
		LogStoreTimeout: opt.hakeeper.logStoreTimeout,
		TNStoreTimeout:  opt.hakeeper.tnStoreTimeout,
		CNStoreTimeout:  opt.hakeeper.cnStoreTimeout,
	}
}

// WithTNServiceNum sets tn service number in the cluster.
func (opt Options) WithTNServiceNum(num int) Options {
	opt.initial.tnServiceNum = num
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

// WithTNShardNum sets tn shard number in the cluster.
func (opt Options) WithTNShardNum(num uint64) Options {
	opt.initial.tnShardNum = num
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

// WithTNUseTAEStorage sets tn transaction use tae storage.
func (opt Options) WithTNUseTAEStorage() Options {
	opt.storage.tnStorage = tnservice.StorageTAE
	return opt
}

// WithTNUseMEMStorage sets tn transaction use mem storage.
func (opt Options) WithTNUseMEMStorage() Options {
	opt.storage.tnStorage = tnservice.StorageMEM
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

// WithHKTNStoreTimeout sets tn store timeout for hakeeper.
func (opt Options) WithHKTNStoreTimeout(timeout time.Duration) Options {
	opt.hakeeper.tnStoreTimeout = timeout
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

// WithTNHeartbeatInterval sets heartbeat interval fo tn service.
func (opt Options) WithTNHeartbeatInterval(interval time.Duration) Options {
	opt.heartbeat.tn = interval
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

// GetTNStorageType returns the storage type that the tnservice used
func (opt Options) GetTNStorageType() tnservice.StorageType {
	return opt.storage.tnStorage
}

// GetCNEngineType returns the engine type that the cnservice used
func (opt Options) GetCNEngineType() tnservice.StorageType {
	return opt.storage.tnStorage
}

// WithKeepData sets keep data after cluster closed.
func (opt Options) WithKeepData() Options {
	opt.keepData = true
	return opt
}

// WithLogtailRpcMaxMessageSize sets max rpc message size for logtail push server.
func (opt Options) WithLogtailRpcMaxMessageSize(size int64) Options {
	opt.logtailPushServer.rpcMaxMessageSize = size
	return opt
}

// WithLogtailCollectInterval sets collection interval for logtail push server.
func (opt Options) WithLogtailCollectInterval(interval time.Duration) Options {
	opt.logtailPushServer.logtailCollectInterval = interval
	return opt
}

// WithLogtailResponseSendTimeout sets response send timeout for logtail push server.
func (opt Options) WithLogtailResponseSendTimeout(timeout time.Duration) Options {
	opt.logtailPushServer.logtailResponseSendTimeout = timeout
	return opt
}

// WithLogtailResponseSendTimeout sets response send timeout for logtail push server.
func (opt Options) WithPullWorkerPoolSize(s int64) Options {
	opt.logtailPushServer.pullWorkerPoolSize = s
	return opt
}

// WithCNOptionFunc set build cn options func
func (opt Options) WithCNOptionFunc(fn func(i int) []cnservice.Option) Options {
	opt.cn.optionFunc = fn
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
