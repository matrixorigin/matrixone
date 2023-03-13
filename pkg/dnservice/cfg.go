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

package dnservice

import (
	"context"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

var (
	defaultListenAddress         = "0.0.0.0:22000"
	defaultServiceAddress        = "127.0.0.1:22000"
	defaultLogtailListenAddress  = "0.0.0.0:22001"
	defaultLogtailServiceAddress = "127.0.0.1:22001"
	defaultLockListenAddress     = "0.0.0.0:22002"
	defaultLockServiceAddress    = "127.0.0.1:22002"
	defaultZombieTimeout         = time.Hour
	defaultDiscoveryTimeout      = time.Second * 30
	defaultHeatbeatInterval      = time.Second
	defaultConnectTimeout        = time.Second * 30
	defaultHeatbeatTimeout       = time.Second * 3

	defaultFlushInterval       = time.Second * 60
	defaultScanInterval        = time.Second * 5
	defaultIncrementalInterval = time.Minute
	defaultGlobalMinCount      = int64(60)
	defaultMinCount            = int64(100)
	defaultLogBackend          = string(options.LogstoreLogservice)

	defaultRpcMaxMsgSize              = 1024 * mpool.KB
	defaultRpcPayloadCopyBufferSize   = 1024 * mpool.KB
	defaultLogtailCollectInterval     = 50 * time.Millisecond
	defaultLogtailResponseSendTimeout = 10 * time.Second
	defaultMaxLogtailFetchFailure     = 5

	storageDir     = "storage"
	defaultDataDir = "./mo-data"
)

// Config dn store configuration
type Config struct {
	// DataDir data dir
	DataDir string `toml:"-"`
	// UUID dn store uuid
	UUID string `toml:"uuid"`
	// ListenAddress listening address for receiving external requests.
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`
	// LockListenAddress listening address for receiving external lock table allocator requests.
	LockListenAddress string `toml:"lock-listen-address"`
	// LockServiceAddress service address for communication, if this address is not set, use
	// LockListenAddress as the communication address.
	LockServiceAddress string `toml:"lock-service-address"`

	// HAKeeper configuration
	HAKeeper struct {
		// HeatbeatInterval heartbeat interval to send message to hakeeper. Default is 1s
		HeatbeatInterval toml.Duration `toml:"hakeeper-heartbeat-interval"`
		// HeatbeatTimeout heartbeat request timeout. Default is 3s
		HeatbeatTimeout toml.Duration `toml:"hakeeper-heartbeat-timeout"`
		// DiscoveryTimeout discovery HAKeeper service timeout. Default is 30s
		DiscoveryTimeout toml.Duration `toml:"hakeeper-discovery-timeout"`
		// ClientConfig hakeeper client configuration
		ClientConfig logservice.HAKeeperClientConfig
	}

	// LogService log service configuration
	LogService struct {
		// ConnectTimeout timeout for connect to logservice. Default is 30s.
		ConnectTimeout toml.Duration `toml:"connect-timeout"`
	}

	// RPC configuration
	RPC rpc.Config `toml:"rpc"`

	Ckp struct {
		FlushInterval       toml.Duration `toml:"flush-interval"`
		ScanInterval        toml.Duration `toml:"scan-interval"`
		MinCount            int64         `toml:"min-count"`
		IncrementalInterval toml.Duration `toml:"incremental-interval"`
		GlobalMinCount      int64         `toml:"global-min-count"`
	}

	LogtailServer struct {
		ListenAddress              string        `toml:"listen-address"`
		ServiceAddress             string        `toml:"service-address"`
		RpcMaxMessageSize          toml.ByteSize `toml:"rpc-max-message-size"`
		RpcPayloadCopyBufferSize   toml.ByteSize `toml:"rpc-payload-copy-buffer-size"`
		RpcEnableChecksum          bool          `toml:"rpc-enable-checksum"`
		LogtailCollectInterval     toml.Duration `toml:"logtail-collect-interval"`
		LogtailResponseSendTimeout toml.Duration `toml:"logtail-response-send-timeout"`
		MaxLogtailFetchFailure     int           `toml:"max-logtail-fetch-failure"`
	}

	// Txn transactions configuration
	Txn struct {
		// ZombieTimeout A transaction timeout, if an active transaction has not operated for more
		// than the specified time, it will be considered a zombie transaction and the backend will
		// roll back the transaction.
		ZombieTimeout toml.Duration `toml:"zombie-timeout"`

		// Storage txn storage config
		Storage struct {
			// dataDir data dir used to store the data
			dataDir string `toml:"-"`
			// Backend txn storage backend implementation. [TAE|Mem], default TAE.
			Backend StorageType `toml:"backend"`
			// LogBackend the backend used to store logs
			LogBackend string `toml:"log-backend"`
		}
	}

	// Cluster configuration
	Cluster struct {
		// RefreshInterval refresh cluster info from hakeeper interval
		RefreshInterval toml.Duration `toml:"refresh-interval"`
	}
}

func (c *Config) Validate() error {
	if c.UUID == "" {
		return moerr.NewInternalError(context.Background(), "Config.UUID not set")
	}
	if c.DataDir == "" {
		c.DataDir = defaultDataDir
	}
	c.Txn.Storage.dataDir = filepath.Join(c.DataDir, storageDir)
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = defaultServiceAddress
	}
	if c.LockListenAddress == "" {
		c.LockListenAddress = defaultLockListenAddress
	}
	if c.LockServiceAddress == "" {
		c.LockServiceAddress = defaultLockServiceAddress
	}
	if c.Txn.Storage.Backend == "" {
		c.Txn.Storage.Backend = StorageTAE
	}
	if c.Txn.Storage.LogBackend == "" {
		c.Txn.Storage.LogBackend = defaultLogBackend
	}
	if _, ok := supportTxnStorageBackends[c.Txn.Storage.Backend]; !ok {
		return moerr.NewInternalError(context.Background(), "%s txn storage backend not support", c.Txn.Storage)
	}
	if c.Txn.ZombieTimeout.Duration == 0 {
		c.Txn.ZombieTimeout.Duration = defaultZombieTimeout
	}
	if c.HAKeeper.DiscoveryTimeout.Duration == 0 {
		c.HAKeeper.DiscoveryTimeout.Duration = defaultDiscoveryTimeout
	}
	if c.HAKeeper.HeatbeatInterval.Duration == 0 {
		c.HAKeeper.HeatbeatInterval.Duration = defaultHeatbeatInterval
	}
	if c.HAKeeper.HeatbeatTimeout.Duration == 0 {
		c.HAKeeper.HeatbeatTimeout.Duration = defaultHeatbeatTimeout
	}
	if c.LogService.ConnectTimeout.Duration == 0 {
		c.LogService.ConnectTimeout.Duration = defaultConnectTimeout
	}
	if c.Ckp.ScanInterval.Duration == 0 {
		c.Ckp.ScanInterval.Duration = defaultScanInterval
	}
	if c.Ckp.FlushInterval.Duration == 0 {
		c.Ckp.FlushInterval.Duration = defaultFlushInterval
	}
	if c.Ckp.MinCount == 0 {
		c.Ckp.MinCount = defaultMinCount
	}
	if c.Ckp.IncrementalInterval.Duration == 0 {
		c.Ckp.IncrementalInterval.Duration = defaultIncrementalInterval
	}
	if c.Ckp.GlobalMinCount == 0 {
		c.Ckp.GlobalMinCount = defaultGlobalMinCount
	}
	if c.LogtailServer.ListenAddress == "" {
		c.LogtailServer.ListenAddress = defaultLogtailListenAddress
	}
	if c.LogtailServer.ServiceAddress == "" {
		c.LogtailServer.ServiceAddress = defaultLogtailServiceAddress
	}
	if c.LogtailServer.RpcMaxMessageSize <= 0 {
		c.LogtailServer.RpcMaxMessageSize = toml.ByteSize(defaultRpcMaxMsgSize)
	}
	if c.LogtailServer.RpcPayloadCopyBufferSize <= 0 {
		c.LogtailServer.RpcPayloadCopyBufferSize = toml.ByteSize(defaultRpcPayloadCopyBufferSize)
	}
	if c.LogtailServer.LogtailCollectInterval.Duration <= 0 {
		c.LogtailServer.LogtailCollectInterval.Duration = defaultLogtailCollectInterval
	}
	if c.LogtailServer.LogtailResponseSendTimeout.Duration <= 0 {
		c.LogtailServer.LogtailResponseSendTimeout.Duration = defaultLogtailResponseSendTimeout
	}
	if c.LogtailServer.MaxLogtailFetchFailure <= 0 {
		c.LogtailServer.MaxLogtailFetchFailure = defaultMaxLogtailFetchFailure
	}
	if c.Cluster.RefreshInterval.Duration == 0 {
		c.Cluster.RefreshInterval.Duration = time.Second * 10
	}
	return nil
}
