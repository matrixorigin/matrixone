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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	defaultDataDir        = "mo-data/logservice"
	defaultServiceAddress = "0.0.0.0:32000"
	defaultRaftAddress    = "0.0.0.0:32001"
	defaultGossipAddress  = "0.0.0.0:32002"
)

var (
	ErrInvalidConfig = moerr.NewError(moerr.BAD_CONFIGURATION, "invalid log configuration")
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
	// FS is the underlying virtual FS used by the log service. Leave it as empty
	// in production.
	FS vfs.FS
	// DeploymentID is basically the Cluster ID, nodes with different DeploymentID
	// will not be able to communicate via raft.
	DeploymentID uint64 `toml:"deployment-id"`
	// UUID is the UUID of the log service node. UUID value must be set.
	UUID string `toml:"uuid"`
	// RTTMillisecond is the average round trip time between log service nodes in
	// milliseconds.
	RTTMillisecond uint64 `toml:"rttmillisecond"`
	// DataDir is the name of the directory for storing all log service data. It
	// should a locally mount partition with good write and fsync performance.
	DataDir string `toml:"data-dir"`
	// ServiceAddress is log service's service address that can be reached by
	// other nodes such as DN nodes.
	ServiceAddress string `toml:"logservice-address"`
	// ServiceListenAddress is the local listen address of the ServiceAddress.
	ServiceListenAddress string `toml:"logservice-listen-address"`
	// RaftAddress is the address that can be reached by other log service nodes
	// via their raft layer.
	RaftAddress string `toml:"raft-address"`
	// RaftListenAddress is the local listen address of the RaftAddress.
	RaftListenAddress string `toml:"raft-listen-address"`
	// GossipAddress is the address used for accepting gossip communication.
	GossipAddress string `toml:"gossip-address"`
	// GossipListenAddress is the local listen address of the GossipAddress
	GossipListenAddress string `toml:"gossip-listen-address"`
	// GossipSeedAddresses is list of semicolon separated seed addresses that
	// are used for introducing the local node into the gossip network.
	GossipSeedAddresses string `toml:"gossip-seed-addresses"`
	// HeartbeatInterval is the interval of how often log service node should be
	// sending heartbeat message to the HAKeeper.
	HeartbeatInterval toml.Duration `toml:"logservice-heartbeat-interval"`
	// HAKeeperTickInterval is the interval of how often log service node should
	// tick the HAKeeper.
	HAKeeperTickInterval toml.Duration `toml:"hakeeper-tick-interval"`
	// HAKeeperCheckInterval is the internval of how often HAKeeper should run
	// cluster health checks.
	HAKeeperCheckInterval toml.Duration `toml:"hakeeper-check-interval"`

	HAKeeperConfig struct {
		// TickPerSecond indicates how many ticks every second.
		// In HAKeeper, we do not use actual time to measure time elapse.
		// Instead, we use ticks.
		TickPerSecond int `toml:"tick-per-second"`
		// LogStoreTimeout is the actual time limit between a log store's heartbeat.
		// If HAKeeper does not receive two heartbeat within LogStoreTimeout,
		// it regards the log store as down.
		LogStoreTimeout toml.Duration `toml:"log-store-timeout"`
		// DnStoreTimeout is the actual time limit between a dn store's heartbeat.
		// If HAKeeper does not receive two heartbeat within DnStoreTimeout,
		// it regards the dn store as down.
		DnStoreTimeout toml.Duration `toml:"dn-store-timeout"`
	}

	HAKeeperClientConfig struct {
		// DiscoveryAddress is the Log Service discovery address provided by k8s.
		DiscoveryAddress string `toml:"hakeeper-discovery-address"`
		// ServiceAddresses is a list of semicolon separated well known Log Services'
		// service addresses.
		ServiceAddresses string `toml:"hakeeper-service-addresses"`
	}

	// DisableWorkers disables the HAKeeper ticker and HAKeeper client in tests.
	// Never set this field to true in production
	DisableWorkers bool
}

// GetGossipSeedAddresses returns a slice of gossip seed addresses split from
// the GossipSeedAddresses field.
func (c *Config) GetGossipSeedAddresses() []string {
	return splitAddresses(c.GossipSeedAddresses)
}

func (c *Config) GetHAKeeperServiceAddresses() []string {
	return splitAddresses(c.HAKeeperClientConfig.ServiceAddresses)
}

func (c *Config) GetHAKeeperConfig() hakeeper.Config {
	return hakeeper.Config{
		TickPerSecond:   c.HAKeeperConfig.TickPerSecond,
		LogStoreTimeout: c.HAKeeperConfig.LogStoreTimeout.Duration,
		DnStoreTimeout:  c.HAKeeperConfig.DnStoreTimeout.Duration,
	}
}

func (c *Config) GetHAKeeperClientConfig() HAKeeperClientConfig {
	return HAKeeperClientConfig{
		DiscoveryAddress: c.HAKeeperClientConfig.DiscoveryAddress,
		ServiceAddresses: c.GetHAKeeperServiceAddresses(),
	}
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.UUID) == 0 {
		return errors.Wrapf(ErrInvalidConfig, "UUID not set")
	}
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
	if c.HAKeeperConfig.LogStoreTimeout.Duration == 0 {
		return errors.Wrapf(ErrInvalidConfig, "LogStoreTimeout not set")
	}
	if c.HAKeeperConfig.DnStoreTimeout.Duration == 0 {
		return errors.Wrapf(ErrInvalidConfig, "DnStoreTimeout not set")
	}
	return nil
}

func (c *Config) Fill() {
	if c.FS == nil {
		c.FS = vfs.Default
	}
	if c.HeartbeatInterval.Duration == 0 {
		c.HeartbeatInterval.Duration = defaultHeartbeatInterval
	}
	if c.HAKeeperTickInterval.Duration == 0 {
		c.HAKeeperTickInterval.Duration = hakeeper.TickDuration
	}
	if c.HAKeeperCheckInterval.Duration == 0 {
		c.HAKeeperCheckInterval.Duration = hakeeper.CheckDuration
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
	if c.HAKeeperConfig.LogStoreTimeout.Duration == 0 {
		c.HAKeeperConfig.LogStoreTimeout.Duration = hakeeper.DefaultLogStoreTimeout
	}
	if c.HAKeeperConfig.DnStoreTimeout.Duration == 0 {
		c.HAKeeperConfig.DnStoreTimeout.Duration = hakeeper.DefaultDnStoreTimeout
	}
}

func splitAddresses(v string) []string {
	results := make([]string, 0)
	parts := strings.Split(v, ";")
	for _, v := range parts {
		t := strings.TrimSpace(v)
		if len(t) > 0 {
			results = append(results, t)
		}
	}
	return results
}
