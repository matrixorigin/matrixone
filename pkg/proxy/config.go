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
	defaultRefreshInterval = 2 * time.Second
	// The default value of rebalance interval.
	defaultRebalanceInterval = 10 * time.Second
	// The default value of rebalnce tolerance.
	defaultRebalanceTolerance = 0.3
	// The default value of rebalance policy.
	defaultRebalancePolicy = "active"
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
	// The default deadline for receiving a client login packet. It applies only
	// before authentication; established tunnel traffic has no such deadline.
	defaultClientHandshakeTimeout = time.Second * 10
	// The default base cooldown of the CN health circuit breaker.
	defaultCNHealthCheckBaseCooldown = time.Second * 5
	// The default max cooldown of the CN health circuit breaker.
	defaultCNHealthCheckMaxCooldown = time.Second * 30
	// Default connection budgets bound Proxy memory independently of client
	// cleanup behavior. Per-tenant headroom prevents one tenant from consuming
	// all capacity while still allowing large connection pools.
	defaultMaxConnections          = 10000
	defaultMaxConnectionsPerTenant = 8000
	// Protocol memory covers the shared allocator used by client and backend
	// MySQL protocol sessions. Login packets are retained for migration, so
	// their independent per-connection bound is intentionally small.
	defaultProtocolMemoryLimit        = toml.ByteSize(1 << 30)
	defaultClientHandshakePacketLimit = toml.ByteSize(64 << 10)
	// Proxy only consumes the address block; leave bounded room for common TLVs
	// without coupling this unauthenticated framing limit to the MySQL login.
	defaultProxyProtocolBodyLimit = toml.ByteSize(4 << 10)
	// A protocol 4.1 SSLRequest contains only the 32-byte fixed response
	// prefix. A usable final login must additionally carry at least a one-byte
	// username, its NUL terminator, and one auth length/terminator byte. Keep the
	// configured minimum tied to a complete login rather than the shorter
	// intermediate TLS request.
	protocol41SSLRequestPayloadSize   = toml.ByteSize(32)
	minimumClientHandshakePacketLimit = protocol41SSLRequestPayloadSize + 3
	// The packet header itself can encode at most (1<<24)-1 payload bytes.
	maximumClientHandshakePacketLimit = toml.ByteSize((1 << 24) - 1)
	minimumProxyProtocolBodyLimit     = toml.ByteSize(2*ipv6AddrLength + 4)
	maximumProxyProtocolBodyLimit     = toml.ByteSize((1 << 16) - 1)
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
	// ClientHandshakeTimeout bounds how long an unauthenticated client may hold
	// a Proxy connection slot while sending its login packet.
	ClientHandshakeTimeout toml.Duration `toml:"client-handshake-timeout" user_setting:"advanced"`

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
	// ConnCacheEnabled indicates if the connection cache feature is enabled.
	ConnCacheEnabled bool `toml:"conn-cache-enabled"`
	// MaxConnections bounds live client connections owned by one Proxy.
	MaxConnections int `toml:"max-connections" user_setting:"advanced"`
	// MaxConnectionsPerTenant bounds one tenant's live client connections.
	// It must not exceed MaxConnections.
	MaxConnectionsPerTenant int `toml:"max-connections-per-tenant" user_setting:"advanced"`
	// ProtocolMemoryLimit bounds retained and phase-overlap MySQL protocol
	// memory, including shared session buffers, pre-auth framing and login
	// packets retained for connection migration.
	ProtocolMemoryLimit toml.ByteSize `toml:"protocol-memory-limit" user_setting:"advanced"`
	// ClientHandshakePacketLimit bounds the login packet retained for routing
	// and migration for the lifetime of a client connection.
	ClientHandshakePacketLimit toml.ByteSize `toml:"client-handshake-packet-limit" user_setting:"advanced"`
	// ProxyProtocolBodyLimit bounds the unauthenticated PROXY v2 address and
	// TLV body independently of the MySQL login packet.
	ProxyProtocolBodyLimit toml.ByteSize `toml:"proxy-protocol-body-limit" user_setting:"advanced"`

	// CNHealthCheckDisabled disables the CN health circuit breaker. By default
	// the breaker is enabled: it temporarily skips CN servers that fail to
	// accept connections (e.g. overloaded CNs whose handshake times out), so
	// new connections are not repeatedly routed to a known-bad CN.
	CNHealthCheckDisabled bool `toml:"cn-health-check-disabled" user_setting:"advanced"`
	// CNHealthCheckBaseCooldown is the initial cooldown applied to a CN when
	// its health breaker first trips. Subsequent consecutive failures back off
	// exponentially up to CNHealthCheckMaxCooldown.
	CNHealthCheckBaseCooldown toml.Duration `toml:"cn-health-check-base-cooldown" user_setting:"advanced"`
	// CNHealthCheckMaxCooldown caps the exponential backoff of the CN health
	// breaker, ensuring a failing CN is probed (and may recover) at least once
	// per this window.
	CNHealthCheckMaxCooldown toml.Duration `toml:"cn-health-check-max-cooldown" user_setting:"advanced"`
	// CNHealthCheckFailThreshold is the number of consecutive connect failures
	// required before a CN's health breaker trips. A value >1 tolerates a
	// single transient blip; this matters most for single-CN deployments,
	// where tripping the only CN makes new connections fast-fail during the
	// cooldown. Default is 2. Values < 1 fall back to the default.
	CNHealthCheckFailThreshold int `toml:"cn-health-check-fail-threshold" user_setting:"advanced"`

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

// WithTest set the test field to true.
func WithTest() Option {
	return func(s *Server) {
		s.test = true
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
	if c.ClientHandshakeTimeout.Duration == 0 {
		c.ClientHandshakeTimeout.Duration = defaultClientHandshakeTimeout
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
	if c.CNHealthCheckBaseCooldown.Duration == 0 {
		c.CNHealthCheckBaseCooldown.Duration = defaultCNHealthCheckBaseCooldown
	}
	if c.CNHealthCheckMaxCooldown.Duration == 0 {
		c.CNHealthCheckMaxCooldown.Duration = defaultCNHealthCheckMaxCooldown
	}
	if c.CNHealthCheckFailThreshold < 1 {
		c.CNHealthCheckFailThreshold = defaultCNHealthFailThreshold
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = defaultMaxConnections
	}
	if c.MaxConnectionsPerTenant == 0 {
		c.MaxConnectionsPerTenant = min(defaultMaxConnectionsPerTenant, c.MaxConnections)
	}
	if c.ProtocolMemoryLimit == 0 {
		c.ProtocolMemoryLimit = defaultProtocolMemoryLimit
	}
	if c.ClientHandshakePacketLimit == 0 {
		c.ClientHandshakePacketLimit = defaultClientHandshakePacketLimit
	}
	if c.ProxyProtocolBodyLimit == 0 {
		c.ProxyProtocolBodyLimit = defaultProxyProtocolBodyLimit
	}
}

// Validate validates the configuration of proxy server.
func (c *Config) Validate() error {
	noReport := errutil.ContextWithNoReport(context.Background(), true)
	if c.MaxConnections < 0 {
		return moerr.NewInternalError(noReport, "proxy max-connections must be positive")
	}
	if c.ClientHandshakeTimeout.Duration < 0 {
		return moerr.NewInternalError(noReport, "proxy client-handshake-timeout must be positive")
	}
	if c.MaxConnectionsPerTenant < 0 {
		return moerr.NewInternalError(noReport, "proxy max-connections-per-tenant must be positive")
	}
	if c.MaxConnections > 0 && c.MaxConnectionsPerTenant > c.MaxConnections {
		return moerr.NewInternalError(noReport,
			"proxy max-connections-per-tenant must not exceed max-connections")
	}
	if c.ProtocolMemoryLimit > toml.ByteSize(^uint64(0)>>1) {
		return moerr.NewInternalError(noReport, "proxy protocol-memory-limit is too large")
	}
	if c.ClientHandshakePacketLimit > 0 &&
		c.ClientHandshakePacketLimit < minimumClientHandshakePacketLimit {
		return moerr.NewInternalError(noReport,
			"proxy client-handshake-packet-limit is smaller than a complete protocol 4.1 login")
	}
	if c.ClientHandshakePacketLimit > maximumClientHandshakePacketLimit {
		return moerr.NewInternalError(noReport,
			"proxy client-handshake-packet-limit exceeds the MySQL packet payload limit")
	}
	if c.ProxyProtocolBodyLimit > 0 && c.ProxyProtocolBodyLimit < minimumProxyProtocolBodyLimit {
		return moerr.NewInternalError(noReport,
			"proxy proxy-protocol-body-limit is smaller than a TCP/IPv6 address block")
	}
	if c.ProxyProtocolBodyLimit > maximumProxyProtocolBodyLimit {
		return moerr.NewInternalError(noReport,
			"proxy proxy-protocol-body-limit exceeds the PROXY v2 body length")
	}
	if _, err := calculateProtocolMemoryBudget(c); err != nil {
		return err
	}
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
