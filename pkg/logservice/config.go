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

package logservice

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

const (
	defaultDataDir        = "mo-logservice-data"
	defaultServiceAddress = "0.0.0.0:32000"
	defaultRaftAddress    = "0.0.0.0:32001"
	defaultGossipAddress  = "0.0.0.0:32002"
)

var (
	ErrInvalidConfig = moerr.NewError(moerr.BAD_CONFIGURATION, "invalid log service configuration")
)

const (
	defaultHeartbeatInterval = time.Second
)

// HAKeeperClientConfig is the config for HAKeeper clients.
type HAKeeperClientConfig struct {
	// DiscoveryAddress is the Log Service discovery address provided by k8s.
	DiscoveryAddress string
	// ServiceAddresses is a list of well known Log Services' service addresses.
	ServiceAddresses []string
}

// Config defines the Configurations supported by the Log Service.
type Config struct {
	FS                   vfs.FS
	DeploymentID         uint64   `toml:"deployment-id"`
	UUID                 string   `toml:"uuid"`
	RTTMillisecond       uint64   `toml:"rttmillisecond"`
	DataDir              string   `toml:"data-dir"`
	ServiceAddress       string   `toml:"service-address"`
	ServiceListenAddress string   `toml:"serfice-listen-address"`
	RaftAddress          string   `toml:"raft-address"`
	RaftListenAddress    string   `toml:"raft-listen-address"`
	GossipAddress        string   `toml:"gossip-address"`
	GossipListenAddress  string   `toml:"gossip-listen-address"`
	GossipSeedAddresses  []string `toml:"uuid"`

	HeartbeatInterval     time.Duration `toml:"heartbeat-interval"`
	HAKeeperTickInterval  time.Duration `toml:"tick-interval"`
	HAKeeperCheckInterval time.Duration `toml:"check-interval"`

	HAKeeperConfig struct {
		// TickPerSecond indicates how many ticks every second.
		// In HAKeeper, we do not use actual time to measure time elapse.
		// Instead, we use ticks.
		TickPerSecond int `toml:"tick-per-second"`
		// LogStoreTimeout is the actual time limit between a log store's heartbeat.
		// If HAKeeper does not receive two heartbeat within LogStoreTimeout,
		// it regards the log store as down.
		LogStoreTimeout time.Duration `toml:"log-store-timeout"`
		// DnStoreTimeout is the actual time limit between a dn store's heartbeat.
		// If HAKeeper does not receive two heartbeat within DnStoreTimeout,
		// it regards the dn store as down.
		DnStoreTimeout time.Duration `toml:"dn-store-timeout"`
	}

	HAKeeperClientConfig struct {
		// DiscoveryAddress is the Log Service discovery address provided by k8s.
		DiscoveryAddress string `toml:"hakeeper-discovery-address"`
		// ServiceAddresses is a list of well known Log Services' service addresses.
		ServiceAddresses []string `toml:"hakeeper-service-addresses"`
	}

	// DisableWorkers disables the HAKeeper ticker and HAKeeper client in tests.
	// Never set this field to true in production
	DisableWorkers bool
}

func (c *Config) GetHAKeeperConfig() hakeeper.Config {
	return hakeeper.Config{
		TickPerSecond:   c.HAKeeperConfig.TickPerSecond,
		LogStoreTimeout: c.HAKeeperConfig.LogStoreTimeout,
		DnStoreTimeout:  c.HAKeeperConfig.DnStoreTimeout,
	}
}

func (c *Config) GetHAKeeperClientConfig() HAKeeperClientConfig {
	addrs := make([]string, 0)
	addrs = append(addrs, c.HAKeeperClientConfig.ServiceAddresses...)
	return HAKeeperClientConfig{
		DiscoveryAddress: c.HAKeeperClientConfig.DiscoveryAddress,
		ServiceAddresses: addrs,
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.DeploymentID == 0 {
		return errors.Wrapf(ErrInvalidConfig, "DeploymentID not set")
	}
	// when *ListenAddress is not empty and *Address is empty, consider it as an
	// error
	if len(c.ServiceAddress) == 0 && len(c.ServiceListenAddress) != 0 {
		return errors.Wrapf(ErrInvalidConfig, "ServiceAddress not set")
	}
	if len(c.RaftAddress) == 0 && len(c.RaftListenAddress) != 0 {
		return errors.Wrapf(ErrInvalidConfig, "RaftAddress not set")
	}
	if len(c.GossipAddress) == 0 && len(c.GossipListenAddress) != 0 {
		return errors.Wrapf(ErrInvalidConfig, "GossipAddress not set")
	}
	if len(c.GossipSeedAddresses) == 0 {
		return errors.Wrapf(ErrInvalidConfig, "GossipSeedAddresses not set")
	}
	if c.HAKeeperConfig.TickPerSecond == 0 {
		return errors.Wrapf(ErrInvalidConfig, "TickPerSecond not set")
	}
	if c.HAKeeperConfig.LogStoreTimeout == 0 {
		return errors.Wrapf(ErrInvalidConfig, "LogStoreTimeout not set")
	}
	if c.HAKeeperConfig.DnStoreTimeout == 0 {
		return errors.Wrapf(ErrInvalidConfig, "DnStoreTimeout not set")
	}
	return nil
}

func (c *Config) Fill() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.HAKeeperTickInterval == 0 {
		c.HAKeeperTickInterval = hakeeper.TickDuration
	}
	if c.HAKeeperCheckInterval == 0 {
		c.HAKeeperCheckInterval = hakeeper.CheckDuration
	}
	if c.RTTMillisecond == 0 {
		c.RTTMillisecond = 200
	}
	if len(c.DataDir) == 0 {
		c.DataDir = defaultDataDir
	}
	if len(c.ServiceAddress) == 0 {
		c.ServiceAddress = defaultServiceAddress
		c.ServiceListenAddress = defaultServiceAddress
	} else if len(c.ServiceAddress) != 0 && len(c.ServiceListenAddress) == 0 {
		c.ServiceListenAddress = c.ServiceAddress
	}
	if len(c.RaftAddress) == 0 {
		c.RaftAddress = defaultRaftAddress
		c.RaftListenAddress = defaultRaftAddress
	} else if len(c.RaftAddress) != 0 && len(c.RaftListenAddress) == 0 {
		c.RaftListenAddress = c.RaftAddress
	}
	if len(c.GossipAddress) == 0 {
		c.GossipAddress = defaultGossipAddress
		c.GossipListenAddress = defaultGossipAddress
	} else if len(c.GossipAddress) != 0 && len(c.GossipListenAddress) == 0 {
		c.GossipListenAddress = c.GossipAddress
	}
	if c.HAKeeperConfig.TickPerSecond == 0 {
		c.HAKeeperConfig.TickPerSecond = hakeeper.DefaultTickPerSecond
	}
	if c.HAKeeperConfig.LogStoreTimeout == 0 {
		c.HAKeeperConfig.LogStoreTimeout = hakeeper.DefaultLogStoreTimeout
	}
	if c.HAKeeperConfig.DnStoreTimeout == 0 {
		c.HAKeeperConfig.DnStoreTimeout = hakeeper.DefaultDnStoreTimeout
	}
}
