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
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	defaultListenAddress    = "0.0.0.0:22000"
	defaultServiceAddress   = "127.0.0.1:22000"
	defaultZombieTimeout    = time.Hour
	defaultDiscoveryTimeout = time.Second * 30
	defaultHeatbeatDuration = time.Second
	defaultConnectTimeout   = time.Second * 30
	defaultHeatbeatTimeout  = time.Millisecond * 500

	defaultFlushInterval       = time.Second * 60
	defaultScanInterval        = time.Second * 5
	defaultIncrementalInterval = time.Minute
	defaultGlobalInterval      = time.Minute * 60
	defaultMinCount            = int64(100)
	defaultLogBackend          = "batchstore"

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

	// HAKeeper configuration
	HAKeeper struct {
		// HeatbeatDuration heartbeat duration to send message to hakeeper. Default is 1s
		HeatbeatDuration toml.Duration `toml:"hakeeper-heartbeat-duration"`
		// HeatbeatTimeout heartbeat request timeout. Default is 500ms
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
		GlobalInterval      toml.Duration `toml:"global-interval"`
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
}

func (c *Config) Validate() error {
	if c.UUID == "" {
		return moerr.NewInternalError("Config.UUID not set")
	}
	if c.DataDir == "" {
		c.DataDir = defaultDataDir
	}
	c.Txn.Storage.dataDir = filepath.Join(c.DataDir, storageDir)
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
		c.ServiceAddress = defaultServiceAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	}
	if c.Txn.Storage.Backend == "" {
		c.Txn.Storage.Backend = StorageTAE
	}
	if c.Txn.Storage.LogBackend == "" {
		c.Txn.Storage.LogBackend = defaultLogBackend
	}
	if _, ok := supportTxnStorageBackends[c.Txn.Storage.Backend]; !ok {
		return moerr.NewInternalError("%s txn storage backend not support", c.Txn.Storage)
	}
	if c.Txn.ZombieTimeout.Duration == 0 {
		c.Txn.ZombieTimeout.Duration = defaultZombieTimeout
	}
	if c.HAKeeper.DiscoveryTimeout.Duration == 0 {
		c.HAKeeper.DiscoveryTimeout.Duration = defaultDiscoveryTimeout
	}
	if c.HAKeeper.HeatbeatDuration.Duration == 0 {
		c.HAKeeper.HeatbeatDuration.Duration = defaultHeatbeatDuration
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
	if c.Ckp.GlobalInterval.Duration == 0 {
		c.Ckp.GlobalInterval.Duration = defaultGlobalInterval
	}
	return nil
}
