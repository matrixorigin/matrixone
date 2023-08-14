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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
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

	// Default is false. With true. Server will support tls.
	// This value should be ths same with all CN servers, and the name
	// of this parameter is enableTls.
	TLSEnabled bool `toml:"tls-enabled"`
	// TSLCAFile is the file path of file that contains list of trusted
	// SSL CAs for client.
	TLSCAFile string `toml:"tls-ca-file"`
	// TLSCertFile is the file path of file that contains X509 certificate
	// in PEM format for client.
	TLSCertFile string `toml:"tls-cert-file"`
	// TLSKeyFile is the file path of file that contains X509 key in PEM
	// format for client.
	TLSKeyFile string `toml:"tls-key-file"`

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
	// Plugin specifies an optional proxy plugin backend
	//
	// NB: the connection between proxy and plugin is assumed to be stable, external orchestrators
	// are responsible for ensuring the stability of rpc tunnels, for example, by deploying proxy and
	// plugin in a same machine and communicate through local loopback address
	Plugin *PluginConfig `toml:"plugin"`
}

type PluginConfig struct {
	// Backend is the plugin backend URL
	Backend string `toml:"backend"`
	// Timeout is the rpc timeout when communicate with the plugin backend
	Timeout time.Duration `toml:"timeout"`
}

// Option is used to set up configuration.
type Option func(*Server)

// WithRuntime sets the runtime of proxy server.
func WithRuntime(runtime runtime.Runtime) Option {
	return func(s *Server) {
		s.runtime = runtime
	}
}

// WithTLSEnabled enable the TLS.
func WithTLSEnabled() Option {
	return func(s *Server) {
		s.config.TLSEnabled = true
	}
}

// WithTLSCAFile sets the CA file.
func WithTLSCAFile(f string) Option {
	return func(s *Server) {
		s.config.TLSCAFile = f
	}
}

// WithTLSCertFile sets the cert file.
func WithTLSCertFile(f string) Option {
	return func(s *Server) {
		s.config.TLSCertFile = f
	}
}

// WithTLSKeyFile sets the key file.
func WithTLSKeyFile(f string) Option {
	return func(s *Server) {
		s.config.TLSKeyFile = f
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
	if c.Plugin != nil {
		if c.Plugin.Timeout == 0 {
			c.Plugin.Timeout = time.Second
		}
	}
}

// Validate validates the configuration of proxy server.
func (c *Config) Validate() error {
	noReport := errutil.ContextWithNoReport(context.Background(), true)
	if c.Plugin != nil {
		if c.Plugin.Backend == "" {
			return moerr.NewInternalError(noReport, "proxy plugin backend must be set")
		}
		if c.Plugin.Timeout == 0 {
			return moerr.NewInternalError(noReport, "proxy plugin backend timeout must be set")
		}
	}
	return nil
}
