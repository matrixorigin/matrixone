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

package dn

import (
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	defaultListenAddress    = "unix:///tmp/dn.sock"
	defaultMaxConnections   = 400
	defaultSendQueueSize    = 10240
	defaultMaxClockOffset   = time.Millisecond * 500
	defaultTxnStorage       = "TAE"
	defaultClock            = "HLC"
	defaultZombieTimeout    = time.Hour
	defaultDiscoveryTimeout = time.Second * 30
	defaultHeatbeatDuration = time.Second
	defaultConnectTimeout   = time.Second * 30
)

// Config dn store configuration
type Config struct {
	// StoreID dn store uuid
	StoreID string `toml:"node-id"`
	// DataDir storage directory for local data. Include DNShard metadata and TAE data.
	DataDir string `toml:"data-dir"`
	// ListenAddress listening address for receiving external requests.
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`

	// HAKeeper configuration
	HAKeeper struct {
		// HeatbeatDuration heartbeat duration to send message to hakeeper. Default is 1s
		HeatbeatDuration toml.Duration `toml:"hakeeper-heartbeat-duration"`
		// DiscoveryTimeout discovery HAKeeper service timeout. Default is 30s
		DiscoveryTimeout toml.Duration `toml:"hakeeper-discovery-timeout"`
		// ClientConfig hakeeper client configuration
		ClientConfig logservice.HAKeeperClientConfig `toml:"hakeeper-client"`
	}

	// LogService log service configuration
	LogService struct {
		// ConnectTimeout timeout for connect to logservice. Default is 30s.
		ConnectTimeout toml.Duration `toml:"connect-timeout"`
		// ClientConfig logservice client configuration
		ClientConfig logservice.ClientConfig
	}

	// FileService file service configuration
	FileService struct {
		// Backend file service backend. [Mem|DISK|S3], default S3
		Backend string `toml:"backend"`
		// S3 s3 configuration
		S3 fileservice.S3Config `toml:"s3"`
	}

	// RPC configuration
	RPC struct {
		// MaxConnections maximum number of connections to communicate with each DNStore.
		// Default is 400.
		MaxConnections int `toml:"max-connections"`
		// SendQueueSize maximum capacity of the send request queue per connection, when the
		// queue is full, the send request will be blocked. Default is 10240.
		SendQueueSize int `toml:"send-queue-size"`
		// BusyQueueSize when the length of the send queue reaches the currently set value, the
		// current connection is busy with high load. When any connection with Busy status exists,
		// a new connection will be created until the value set by MaxConnections is reached.
		// Default is 3/4 of SendQueueSize.
		BusyQueueSize int `toml:"busy-queue-size"`
		// WriteBufferSize buffer size for write messages per connection. Default is 1kb
		WriteBufferSize toml.ByteSize `toml:"send-buffer-size"`
		// ReadBufferSize buffer size for read messages per connection. Default is 1kb
		ReadBufferSize toml.ByteSize `toml:"send-buffer-size"`
	}

	// Txn transactions configuration
	Txn struct {
		// ZombieTimeout A transaction timeout, if an active transaction has not operated for more
		// than the specified time, it will be considered a zombie transaction and the backend will
		// roll back the transaction.
		ZombieTimeout toml.Duration `toml:"zombie-timeout"`

		// Storage txn storage config
		Storage struct {
			// Engine storage engine. [TAE|Sqlite|Mem], default TAE.
			Engine string `toml:"engine"`

			// TAEEngine tae storage configuration
			TAEEngine struct {
			}

			// SqliteEngine sqlite storage configuration
			SqliteEngine struct {
			}

			// MemEngine mem storage configuration
			MemEngine struct {
			}
		}

		// Clock txn clock type. [LOCAL|HLC], deafult is LOCAL.
		Clock struct {
			// Source clock source type. [LOCAL|HLC], default LOCAL.
			Source string `toml:"source"`
			// MaxClockOffset max clock offset between two nodes. Default is 500ms
			MaxClockOffset toml.Duration `toml:"max-clock-offset"`
		}
	}
}

func (c *Config) validate() error {
	if c.StoreID == "" {
		return fmt.Errorf("Config.StoreID not set")
	}
	if c.DataDir == "" {
		return fmt.Errorf("Config.DataDir not set")
	}
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	}
	if c.RPC.MaxConnections == 0 {
		c.RPC.MaxConnections = defaultMaxConnections
	}
	if c.RPC.SendQueueSize == 0 {
		c.RPC.SendQueueSize = defaultSendQueueSize
	}
	if c.RPC.BusyQueueSize == 0 {
		c.RPC.BusyQueueSize = c.RPC.SendQueueSize * 3 / 4
	}
	if c.RPC.WriteBufferSize == 0 {
		c.RPC.WriteBufferSize = 1024
	}
	if c.RPC.ReadBufferSize == 0 {
		c.RPC.ReadBufferSize = 1024
	}
	if c.Txn.Clock.MaxClockOffset.Duration == 0 {
		c.Txn.Clock.MaxClockOffset.Duration = defaultMaxClockOffset
	}
	if c.Txn.Clock.Source == "" {
		c.Txn.Clock.Source = defaultClock
	}
	if _, ok := supportTxnClockSources[strings.ToUpper(c.Txn.Clock.Source)]; !ok {
		return fmt.Errorf("%s clock not support", c.Txn.Storage)
	}
	if c.Txn.Storage.Engine == "" {
		c.Txn.Storage.Engine = defaultTxnStorage
	}
	if _, ok := supportTxnStorageEngines[strings.ToUpper(c.Txn.Storage.Engine)]; !ok {
		return fmt.Errorf("%s txn storage not support", c.Txn.Storage)
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
	if c.LogService.ConnectTimeout.Duration == 0 {
		c.LogService.ConnectTimeout.Duration = defaultConnectTimeout
	}
	if _, ok := supportFileServiceBackends[strings.ToUpper(c.FileService.Backend)]; !ok {
		return fmt.Errorf("%s file service backend not support", c.Txn.Storage)
	}
	return nil
}
