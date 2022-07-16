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
		// HAKeeperClient
		HAKeeperClient logservice.HAKeeperClientConfig `toml:"hakeeper-client"`
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
		// SendBufferSize buffer size for send messages per connection. Default is 1kb
		SendBufferSize toml.ByteSize `toml:"send-buffer-size"`
		// ReceiveBufferSize buffer size for receive messages per connection. Default is 1kb
		ReceiveBufferSize toml.ByteSize `toml:"send-buffer-size"`
	}

	// Txn transactions configuration
	Txn struct {
		// Storage storage engine type. [TAE|Sqlite|Mem], default TAE.
		Storage string `toml:"storage"`
		// Clock clock type. [LOCAL|HLC], default LOCAL.
		Clock string `toml:"clock"`
		// MaxClockOffset max clock offset between two nodes. Default is 500ms
		MaxClockOffset toml.Duration `toml:"max-clock-offset"`
		// ZombieTimeout A transaction timeout, if an active transaction has not operated for more
		// than the specified time, it will be considered a zombie transaction and the backend will
		// roll back the transaction.
		ZombieTimeout toml.Duration `toml:"zombie-timeout"`
	}

	// TAE tae storage configuration
	TAE struct {
	}

	// Sqlite sqlite storage configuration
	Sqlite struct {
	}

	// Mem mem storage configuration
	Mem struct {
	}
}

func (c *Config) adjust() error {
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
	if c.RPC.SendBufferSize == 0 {
		c.RPC.SendBufferSize = 1024
	}
	if c.RPC.ReceiveBufferSize == 0 {
		c.RPC.ReceiveBufferSize = 1024
	}
	if c.Txn.MaxClockOffset.Duration == 0 {
		c.Txn.MaxClockOffset.Duration = defaultMaxClockOffset
	}
	if c.Txn.Clock == "" {
		c.Txn.Clock = defaultClock
	}
	if _, ok := clockFactories[strings.ToUpper(c.Txn.Clock)]; !ok {
		return fmt.Errorf("%s clock not support", c.Txn.Storage)
	}
	if c.Txn.Storage == "" {
		c.Txn.Storage = defaultTxnStorage
	}
	if _, ok := storageFactories[strings.ToUpper(c.Txn.Storage)]; !ok {
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
	return nil
}
