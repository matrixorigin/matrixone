// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	// The default value of proxy service.
	defaultListenAddress = "127.0.0.1:6009"
	// The default value of refresh interval to HAKeeper.
	defaultRefreshInterval = 5 * time.Second
	// The default value of rebalance interval.
	defaultRebalanceInterval = 30 * time.Second
	// The default value of rebalnce tolerance.
	defaultRebalanceTolerance = 0.3
)

// Config is the configuration of proxy server.
type Config struct {
	UUID          string `toml:"uuid"`
	ListenAddress string `toml:"listen-address"`
	// RebalanceInterval is the interval between two rebalance operations.
	RebalanceInterval toml.Duration `toml:"rebalance-interval"`
	// RebalanceDisabled indicates that the rebalancer is disabled.
	RebalanceDisabled bool `toml:"rebalance-disabled"`
	// RebalanceToerance indicates the rebalancer's tolerance.
	// Connections above the avg*(1+tolerance) will be migrated to
	// other CN servers. This value should be less than 1.
	RebalanceToerance float64 `toml:"rebalance-tolerance"`

	// HAKeeper is the configuration of HAKeeper.
	HAKeeper struct {
		// ClientConfig is HAKeeper client configuration.
		ClientConfig logservice.HAKeeperClientConfig
	}
	// Cluster is the configuration of MO Cluster.
	Cluster struct {
		// RefreshInterval refresh cluster info from hakeeper interval
		RefreshInterval toml.Duration `toml:"refresh-interval"`
	}
}

// Option is used to set up configuration.
type Option func(*Server)

// WithRuntime sets the runtime of proxy server.
func WithRuntime(runtime runtime.Runtime) Option {
	return func(s *Server) {
		s.runtime = runtime
	}
}

// FillDefault fill the default config values of proxy server.
func (c *Config) FillDefault() {
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.RebalanceInterval.Duration == 0 {
		c.RebalanceInterval.Duration = defaultRebalanceInterval
	}
	if c.Cluster.RefreshInterval.Duration == 0 {
		c.Cluster.RefreshInterval.Duration = defaultRefreshInterval
	}
	if c.RebalanceToerance == 0 {
		c.RebalanceToerance = defaultRebalanceTolerance
	}
}
