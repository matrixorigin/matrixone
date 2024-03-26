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
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	// The default value of proxy service.
	defaultListenAddress = "127.0.0.1:6009"
	// The default value of refresh interval to HAKeeper.
	defaultRefreshInterval = 5 * time.Second
	// The default value of rebalance interval.
	defaultRebalanceInterval = 10 * time.Second
	// The default value of rebalnce tolerance.
	defaultRebalanceTolerance = 0.3
	// The default value of rebalance policy.
	defaultRebalancePolicy = "passive"
	// The default value of heartbeat interval.
	defaultHeartbeatInterval = time.Second * 3
	// The default value of heartbeat timeout.
	defaultHeartbeatTimeout = time.Second * 3
	// The default value of connect timeout.
	defaultConnectTimeout = time.Second * 3
	// The default value of handshake auth timeout.
	defaultAuthTimeout = time.Second * 10
	// The default value of TSL connect timeout.
	defaultTLSConnectTimeout = time.Second * 10
)

type RebalancePolicy int

const (
	RebalancePolicyActive RebalancePolicy = iota
	RebalancePolicyPassive
)

var RebalancePolicyMapping = map[string]RebalancePolicy{
	"active":  RebalancePolicyActive,
	"passive": RebalancePolicyPassive,
}

// Config is the configuration of proxy server.
type Config struct {
	UUID          string `toml:"uuid"`
	ListenAddress string `toml:"listen-address" user_setting:"basic"`
	// RebalanceInterval is the interval between two rebalance operations.
	RebalanceInterval toml.Duration `toml:"rebalance-interval" user_setting:"advanced"`
	// RebalanceDisabled indicates that the rebalancer is disabled.
	RebalanceDisabled bool `toml:"rebalance-disabled" user_setting:"advanced"`
	// RebalanceTolerance indicates the rebalancer's tolerance.
	// Connections above the avg*(1+tolerance) will be migrated to
	// other CN servers. This value should be less than 1.
	RebalanceTolerance float64 `toml:"rebalance-tolerance" user_setting:"advanced"`
	// RebalancePolicy indicates that the rebalance policy, which could be active or
	// passive currently. Active means that the connection transfer will be more proactive
	// to make sure the sessions are balanced in all CN servers. Default value is "active".
	RebalancePolicy string `toml:"rebalance-proactive" user_setting:"advanced"`
	// ConnectTimeout is the timeout duration when proxy connects to backend
	// CN servers. If proxy connects to cn timeout, it will return a retryable
	// error and try to connect to other cn servers.
	ConnectTimeout toml.Duration `toml:"connect-timeout" user_setting:"advanced"`
	// AuthTimeout is the timeout duration when proxy handshakes with backend
	// CN servers. If proxy handshakes with cn timeout, it will return a retryable
	// error and try to connect to other cn servers.
	AuthTimeout toml.Duration `toml:"auth-timeout" user_setting:"advanced"`
	// TLSConnectTimeout is the timeout duration when TLS connect to server.
	TLSConnectTimeout toml.Duration `toml:"tls-connect-timeout" user_setting:"advanced"`

	// Default is false. With true. Server will support tls.
	// This value should be ths same with all CN servers, and the name
	// of this parameter is enableTls.
	TLSEnabled bool `toml:"tls-enabled" user_setting:"advanced"`
	// TSLCAFile is the file path of file that contains list of trusted
	// SSL CAs for client.
	TLSCAFile string `toml:"tls-ca-file" user_setting:"advanced"`
	// TLSCertFile is the file path of file that contains X509 certificate
	// in PEM format for client.
	TLSCertFile string `toml:"tls-cert-file" user_setting:"advanced"`
	// TLSKeyFile is the file path of file that contains X509 key in PEM
	// format for client.
	TLSKeyFile string `toml:"tls-key-file" user_setting:"advanced"`
	// InternalCIDRs is the config which indicates that the CIDR list of
	// internal network. The addresses outside the range are external
	// addresses.
	InternalCIDRs []string `toml:"internal-cidrs"`

	// HAKeeper is the configuration of HAKeeper.
	HAKeeper struct {
		// ClientConfig is HAKeeper client configuration.
		ClientConfig logservice.HAKeeperClientConfig
		// HeartbeatInterval heartbeat interval to send message to HAKeeper. Default is 1s.
		HeartbeatInterval toml.Duration `toml:"hakeeper-heartbeat-interval"`
		// HeartbeatTimeout heartbeat request timeout. Default is 3s.
		HeartbeatTimeout toml.Duration `toml:"hakeeper-heartbeat-timeout"`
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

// WithConfigData saves the data from the config file
func WithConfigData(data map[string]*logservicepb.ConfigItem) Option {
	return func(s *Server) {
		s.configData = util.NewConfigData(data)
	}
}

// FillDefault fill the default config values of proxy server.
func (c *Config) FillDefault() {
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.ConnectTimeout.Duration == 0 {
		c.ConnectTimeout.Duration = defaultConnectTimeout
	}
	if c.AuthTimeout.Duration == 0 {
		c.AuthTimeout.Duration = defaultAuthTimeout
	}
	if c.TLSConnectTimeout.Duration == 0 {
		c.TLSConnectTimeout.Duration = defaultTLSConnectTimeout
	}
	if c.RebalanceInterval.Duration == 0 {
		c.RebalanceInterval.Duration = defaultRebalanceInterval
	}
	if c.Cluster.RefreshInterval.Duration == 0 {
		c.Cluster.RefreshInterval.Duration = defaultRefreshInterval
	}
	if c.RebalanceTolerance == 0 {
		c.RebalanceTolerance = defaultRebalanceTolerance
	}
	if c.RebalancePolicy == "" {
		c.RebalancePolicy = defaultRebalancePolicy
	}
	if c.Plugin != nil {
		if c.Plugin.Timeout == 0 {
			c.Plugin.Timeout = time.Second
		}
	}
	if c.HAKeeper.HeartbeatInterval.Duration == 0 {
		c.HAKeeper.HeartbeatInterval.Duration = defaultHeartbeatInterval
	}
	if c.HAKeeper.HeartbeatTimeout.Duration == 0 {
		c.HAKeeper.HeartbeatTimeout.Duration = defaultHeartbeatTimeout
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
	if _, ok := RebalancePolicyMapping[c.RebalancePolicy]; !ok {
		c.RebalancePolicy = defaultRebalancePolicy
	}
	return nil
}

func dumpProxyConfig(cfg Config) (map[string]*logservicepb.ConfigItem, error) {
	defCfg := Config{}
	return util.DumpConfig(cfg, defCfg)
}
