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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

const (
	defaultDeploymentID      = 1
	defaultDataDir           = "mo-data/logservice"
	defaultSnapshotExportDir = "exported-snapshot"
	defaultRaftAddress       = "0.0.0.0:32000"
	defaultServiceAddress    = "0.0.0.0:32001"
	defaultGossipAddress     = "0.0.0.0:32002"
	defaultGossipSeedAddress = "127.0.0.1:32002"
	defaultRaftPort          = 32000
	defaultGossipPort        = 32002

	defaultGossipProbeInterval = 5 * time.Second
	defaultHeartbeatInterval   = time.Second
	defaultLogDBBufferSize     = 768 * 1024
	defaultTruncateInterval    = 10 * time.Second
	defaultMaxExportedSnapshot = 20
	defaultMaxMessageSize      = 1024 * 1024 * 100
	// The default value for HAKeeper truncate interval.
	defaultHAKeeperTruncateInterval = 24 * time.Hour

	DefaultListenHost     = "0.0.0.0"
	DefaultServiceHost    = "127.0.0.1"
	DefaultLogServicePort = 32001

	defaultRestoreFilePath = "hk_data"
)

var (
	DefaultLogServiceServiceAddress = fmt.Sprintf("%s:%d", DefaultServiceHost, DefaultLogServicePort)
	DefaultGossipServiceAddress     = fmt.Sprintf("%s:%d", DefaultServiceHost, defaultGossipPort)
)

// Config defines the Configurations supported by the Log Service.
type Config struct {
	// FS is the underlying virtual FS used by the log service. Leave it as empty
	// in production.
	FS vfs.FS
	// DeploymentID is basically the Cluster ID, nodes with different DeploymentID
	// will not be able to communicate via raft.
	DeploymentID uint64 `toml:"deployment-id"`
	// UUID is the UUID of the log service node. UUID value must be set.
	UUID string `toml:"uuid" user_setting:"basic"`
	// RTTMillisecond is the average round trip time between log service nodes in
	// milliseconds.
	RTTMillisecond uint64 `toml:"rttmillisecond"`
	// DataDir is the name of the directory for storing all log service data. It
	// should a locally mounted partition with good write and fsync performance.
	DataDir string `toml:"data-dir" user_setting:"basic"`
	// SnapshotExportDir is the directory where the dragonboat snapshots are
	// exported.
	SnapshotExportDir string `toml:"snapshot-export-dir"`
	// MaxExportedSnapshot is the max count of exported snapshots. If there are
	// already MaxExportedSnapshot exported snapshots, no exported snapshot will
	// be generated.
	MaxExportedSnapshot int `toml:"max-exported-snapshot"`
	// ServiceHost is the host name/IP for the service address of RPC request. There is
	// no port value in it.
	ServiceHost string `toml:"service-host"`
	// ServiceAddress is log service's service address that can be reached by
	// other nodes such as TN nodes. It is deprecated and will be removed.
	ServiceAddress string `toml:"logservice-address" user_setting:"advanced"`
	// ServiceListenAddress is the local listen address of the ServiceAddress.
	// It is deprecated and will be removed.
	ServiceListenAddress string `toml:"logservice-listen-address"`
	// ServicePort is log service's service address port that can be reached by
	// other nodes such as TN nodes.
	LogServicePort int `toml:"logservice-port"`
	// RaftAddress is the address that can be reached by other log service nodes
	// via their raft layer. It is deprecated and will be removed.
	RaftAddress string `toml:"raft-address" user_setting:"advanced"`
	// RaftListenAddress is the local listen address of the RaftAddress.
	// It is deprecated and will be removed.
	RaftListenAddress string `toml:"raft-listen-address"`
	// RaftPort is the address port that can be reached by other log service nodes
	// via their raft layer.
	RaftPort int `toml:"raft-port"`
	// UseTeeLogDB enables the log service to use tee based LogDB which is backed
	// by both a pebble and a tan based LogDB. This field should only be set to
	// true during testing.
	UseTeeLogDB bool `toml:"use-tee-logdb"`
	// LogDBBufferSize is the size of the logdb buffer in bytes.
	LogDBBufferSize uint64 `toml:"logdb-buffer-size"`
	// GossipAddress is the address used for accepting gossip communication.
	// It is deprecated and will be removed.
	GossipAddress string `toml:"gossip-address" user_setting:"advanced"`
	// GossipAddressV2 is the address used for accepting gossip communication.
	// This is for domain name support. It is deprecated and will be removed.
	GossipAddressV2 string `toml:"gossip-address-v2"`
	// GossipListenAddress is the local listen address of the GossipAddress
	// It is deprecated and will be removed.
	GossipListenAddress string `toml:"gossip-listen-address"`
	// GossipPort is the port address port used for accepting gossip communication.
	GossipPort int `toml:"gossip-port"`
	// GossipSeedAddresses is list of seed addresses that are used for
	// introducing the local node into the gossip network.
	GossipSeedAddresses []string `toml:"gossip-seed-addresses" user_setting:"advanced"`
	// GossipProbeInterval how often gossip nodes probe each other.
	GossipProbeInterval toml.Duration `toml:"gossip-probe-interval"`
	// GossipAllowSelfAsSeed allow use self as gossip seed
	GossipAllowSelfAsSeed bool `toml:"gossip-allow-self-as-seed"`
	// HeartbeatInterval is the interval of how often log service node should be
	// sending heartbeat message to the HAKeeper.
	HeartbeatInterval toml.Duration `toml:"logservice-heartbeat-interval"`
	// HAKeeperTickInterval is the interval of how often log service node should
	// tick the HAKeeper.
	HAKeeperTickInterval toml.Duration `toml:"hakeeper-tick-interval"`
	// HAKeeperCheckInterval is the interval of how often HAKeeper should run
	// cluster health checks.
	HAKeeperCheckInterval toml.Duration `toml:"hakeeper-check-interval"`
	// TruncateInterval is the interval of how often log service should
	// process truncate for regular shards.
	TruncateInterval toml.Duration `toml:"truncate-interval"`
	// HAKeeperTruncateInterval is the interval of how often log service should
	// process truncate for HAKeeper shard.
	HAKeeperTruncateInterval toml.Duration `toml:"hakeeper-truncate-interval"`
	// ExplicitHostname is the hostname used in draogboat.
	ExplicitHostname string `toml:"explicit-hostname"`
	// Locality sets the locality information of the logservice instance.
	// The format is "key1:value1;key2:value2".
	// It is mainly used to allocate non-voting replicas.
	Locality string `toml:"locality"`
	// IsNonVoting indicates if this node is a non-voting node.
	// If it is a non-voting node, it can only receive replicated states from leader,
	// but it is neither allowed to vote for leader, nor considered as a part of the
	// quorum when replicating state.
	IsNonVoting bool `toml:"is-non-voting"`
	// MembershipImmovable indicates if keep the membership as the one in the
	// snapshot. If it is true, all non-voting nodes and witness nodes will
	// change to normal nodes when startup, otherwise, keep their roles as
	// in snapshot. Default value is true.
	MembershipImmovable bool `toml:"membership-immovable"`

	RPC struct {
		// MaxMessageSize is the max size for RPC message. The default value is 10MiB.
		MaxMessageSize toml.ByteSize `toml:"max-message-size"`
		// EnableCompress enable compress
		EnableCompress bool `toml:"enable-compress"`
	}

	// BootstrapConfig is the configuration specified for the bootstrapping
	// procedure. It only needs to be specified for Log Stores selected to host
	// initial HAKeeper replicas during bootstrapping.
	BootstrapConfig struct {
		// BootstrapCluster indicates whether the cluster should be bootstrapped.
		// Note the bootstrapping procedure will only be executed if BootstrapCluster
		// is true and Config.UUID is found in Config.BootstrapConfig.InitHAKeeperMembers.
		BootstrapCluster bool `toml:"bootstrap-cluster"`
		// NumOfLogShards defines the number of Log shards in the initial deployment.
		NumOfLogShards uint64 `toml:"num-of-log-shards"`
		// NumOfTNShards defines the number of TN shards in the initial deployment.
		// The count must be the same as NumOfLogShards in the current implementation.
		NumOfTNShards uint64 `toml:"num-of-tn-shards"`
		// NumOfLogShardReplicas is the number of replicas for each shard managed by
		// Log Stores, including Log Service shards and the HAKeeper.
		NumOfLogShardReplicas uint64 `toml:"num-of-log-shard-replicas"`
		// InitHAKeeperMembers defines the initial members of the HAKeeper as a list
		// of HAKeeper replicaID and UUID pairs. For example,
		// when the initial HAKeeper members are
		// replica with replica ID 101 running on Log Store uuid1
		// replica with replica ID 102 running on Log Store uuid2
		// replica with replica ID 103 running on Log Store uuid3
		// the InitHAKeeperMembers string value should be
		// []string{"101:uuid1", "102:uuid2", "103:uuid3"}
		// Note that these initial HAKeeper replica IDs must be assigned by k8s
		// from the range [K8SIDRangeStart, K8SIDRangeEnd) as defined in pkg/hakeeper.
		// All uuid values are assigned by k8s, they are used to uniquely identify
		// CN/DN/Log stores.
		// Config.UUID and Config.BootstrapConfig values are considered together to
		// figure out what is the replica ID of the initial HAKeeper replica. That
		// is when Config.UUID is found in InitHAKeeperMembers, then the corresponding
		// replica ID value will be used to launch a HAKeeper replica on the Log
		// Service instance.
		InitHAKeeperMembers []string `toml:"init-hakeeper-members" user_setting:"advanced"`
		// Restore structure is used when the cluster needs to restore data.
		Restore struct {
			// FilePath is the path of the file, which contains the backup data.
			// If is not set, nothing will be done for restore.
			FilePath string `toml:"file-path"`
			// Force means that we force to do restore even if RESTORED tag file
			// already exists.
			Force bool `toml:"force"`
		} `toml:"restore"`
		// NonVotingLocality is the locality for non-voting replicas.
		NonVotingLocality string `toml:"non-voting-locality" user_setting:"advanced"`
		// StandbyEnabled is true means that the standby cluster is enabled and there will
		// be another new shard for the s3 data.
		StandbyEnabled bool `toml:"standby-enabled" user_setting:"advanced"`
	}

	HAKeeperConfig struct {
		// TickPerSecond indicates how many ticks every second.
		// In HAKeeper, we do not use actual time to measure time elapse.
		// Instead, we use ticks.
		TickPerSecond int `toml:"tick-per-second"`
		// LogStoreTimeout is the actual time limit between a log store's heartbeat.
		// If HAKeeper does not receive two heartbeat within LogStoreTimeout,
		// it regards the log store as down.
		LogStoreTimeout toml.Duration `toml:"log-store-timeout"`
		// TNStoreTimeout is the actual time limit between a tn store's heartbeat.
		// If HAKeeper does not receive two heartbeat within TNStoreTimeout,
		// it regards the tn store as down.
		TNStoreTimeout toml.Duration `toml:"tn-store-timeout"`
		// CNStoreTimeout is the actual time limit between a cn store's heartbeat.
		// If HAKeeper does not receive two heartbeat within CNStoreTimeout,
		// it regards the tn store as down.
		CNStoreTimeout toml.Duration `toml:"cn-store-timeout"`
	}

	// HAKeeperClientConfig is the config for HAKeeperClient
	HAKeeperClientConfig HAKeeperClientConfig

	// DisableWorkers disables the HAKeeper ticker and HAKeeper client in tests.
	// Never set this field to true in production
	DisableWorkers bool

	Ctl struct {
		// ListenAddress ctl service listen address for receiving ctl requests
		ListenAddress string `toml:"listen-address"`
		// ServiceAddress service address for communication, if this address is not set, use
		// ListenAddress as the communication address.
		ServiceAddress string `toml:"service-address"`
	} `toml:"ctl"`
}

func (c *Config) GetHAKeeperConfig() hakeeper.Config {
	return hakeeper.Config{
		TickPerSecond:   c.HAKeeperConfig.TickPerSecond,
		LogStoreTimeout: c.HAKeeperConfig.LogStoreTimeout.Duration,
		TNStoreTimeout:  c.HAKeeperConfig.TNStoreTimeout.Duration,
		CNStoreTimeout:  c.HAKeeperConfig.CNStoreTimeout.Duration,
	}
}

func (c *Config) GetHAKeeperClientConfig() HAKeeperClientConfig {
	saddr := make([]string, 0)
	saddr = append(saddr, c.HAKeeperClientConfig.ServiceAddresses...)
	return HAKeeperClientConfig{
		DiscoveryAddress: c.HAKeeperClientConfig.DiscoveryAddress,
		ServiceAddresses: saddr,
	}
}

// returns replica ID of the HAKeeper replica and a boolean indicating whether
// we should run the bootstrap procedure.
func (c *Config) Bootstrapping() (uint64, bool) {
	if !c.BootstrapConfig.BootstrapCluster {
		return 0, false
	}
	members, err := c.GetInitHAKeeperMembers()
	if err != nil {
		return 0, false
	}
	for replicaID, uuid := range members {
		if uuid == c.UUID {
			return replicaID, true
		}
	}
	return 0, false
}

func (c *Config) GetInitHAKeeperMembers() (map[uint64]dragonboat.Target, error) {
	result := make(map[uint64]dragonboat.Target)
	for _, pair := range c.BootstrapConfig.InitHAKeeperMembers {
		pair = strings.TrimSpace(pair)
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			id := strings.TrimSpace(parts[0])
			target := strings.TrimSpace(parts[1])
			if _, err := uuid.Parse(target); err != nil {
				return nil, moerr.NewBadConfigNoCtxf("uuid %s", target)
			}
			idn, err := strconv.ParseUint(id, 10, 64)
			if err != nil {
				return nil, moerr.NewBadConfigNoCtxf("replicateID '%v'", id)
			}
			result[idn] = target
		} else {
			return nil, moerr.NewBadConfigNoCtxf("replicaID:target %s", pair)
		}
	}
	return result, nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if len(c.UUID) == 0 {
		return moerr.NewBadConfigNoCtx("uuid not set")
	}
	if c.DeploymentID == 0 {
		return moerr.NewBadConfigNoCtx("deploymentID not set")
	}
	// when *ListenAddress is not empty and *Address is empty, consider it as an
	// error
	if len(c.ServiceAddress) == 0 && len(c.ServiceListenAddress) != 0 && c.LogServicePort == 0 {
		return moerr.NewBadConfigNoCtx("ServiceAddress and LogServicePort not set")
	}
	if len(c.RaftAddress) == 0 && len(c.RaftListenAddress) != 0 && c.RaftPort == 0 {
		return moerr.NewBadConfigNoCtx("RaftAddress and RaftPort not set")
	}
	if c.LogDBBufferSize == 0 {
		return moerr.NewBadConfigNoCtx("LogDBBufferSize not set")
	}
	if len(c.GossipAddress) == 0 && len(c.GossipListenAddress) != 0 && c.GossipPort == 0 {
		return moerr.NewBadConfigNoCtx("GossipAddress and GossipPort not set")
	}
	if len(c.GossipSeedAddresses) == 0 {
		return moerr.NewBadConfigNoCtx("GossipSeedAddress not set")
	}
	if c.HAKeeperConfig.TickPerSecond == 0 {
		return moerr.NewBadConfigNoCtx("TickPerSecond not set")
	}
	if c.HAKeeperConfig.LogStoreTimeout.Duration == 0 {
		return moerr.NewBadConfigNoCtx("LogStoreTimeout not set")
	}
	if c.HAKeeperConfig.TNStoreTimeout.Duration == 0 {
		return moerr.NewBadConfigNoCtx("DNStoreTimeout not set")
	}
	if c.GossipProbeInterval.Duration == 0 {
		return moerr.NewBadConfigNoCtx("GossipProbeInterval not set")
	}
	if c.TruncateInterval.Duration == 0 {
		return moerr.NewBadConfigNoCtx("TruncateInterval not set")
	}
	if c.HAKeeperTruncateInterval.Duration == 0 {
		return moerr.NewBadConfigNoCtx("HAKeeperTruncateInterval not set")
	}
	if c.RPC.MaxMessageSize == 0 {
		return moerr.NewBadConfigNoCtx("MaxMessageSize not set")
	}
	// validate BootstrapConfig
	if c.BootstrapConfig.BootstrapCluster {
		if c.BootstrapConfig.NumOfLogShards == 0 {
			return moerr.NewBadConfigNoCtx("NumOfLogShards not set")
		}
		if c.BootstrapConfig.NumOfTNShards == 0 {
			return moerr.NewBadConfigNoCtx("NumOfDNShards not set")
		}
		if c.BootstrapConfig.NumOfLogShardReplicas == 0 {
			return moerr.NewBadConfigNoCtx("NumOfLogShardReplica not set")
		}
		if c.BootstrapConfig.NumOfTNShards != c.BootstrapConfig.NumOfLogShards {
			return moerr.NewBadConfigNoCtx("NumOfDNShards does not match NumOfLogShards")
		}
		members, err := c.GetInitHAKeeperMembers()
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return moerr.NewBadConfigNoCtx("InitHAKeeperMembers not set")
		}
		if uint64(len(members)) != c.BootstrapConfig.NumOfLogShardReplicas {
			return moerr.NewBadConfigNoCtx("InitHAKeeperMembers does not match NumOfLogShardReplicas")
		}
	}

	return nil
}

func (c *Config) UpdateAddresses(
	host string,
	servicePort int,
	raftPort int,
	gossipPort int,
) {
	c.ServiceHost = host
	c.ServiceAddress = fmt.Sprintf("0.0.0.0:%d", servicePort)
	c.RaftAddress = fmt.Sprintf("0.0.0.0:%d", raftPort)
	c.GossipAddress = fmt.Sprintf("0.0.0.0:%d", gossipPort)
	c.GossipSeedAddresses = []string{fmt.Sprintf("%s:%d", host, gossipPort)}
}

func DefaultConfig() Config {
	uid := "7c4dccb4-4d3c-41f8-b482-5251dc7a41bf"
	return Config{
		FS:                       vfs.Default,
		DeploymentID:             defaultDeploymentID,
		UUID:                     uid,
		RTTMillisecond:           200,
		DataDir:                  defaultDataDir,
		SnapshotExportDir:        defaultSnapshotExportDir,
		MaxExportedSnapshot:      defaultMaxExportedSnapshot,
		ServiceAddress:           defaultServiceAddress,
		RaftAddress:              defaultRaftAddress,
		ServiceHost:              DefaultServiceHost,
		UseTeeLogDB:              false,
		LogDBBufferSize:          defaultLogDBBufferSize,
		GossipAddress:            defaultGossipAddress,
		GossipSeedAddresses:      []string{DefaultGossipServiceAddress},
		GossipProbeInterval:      toml.Duration{Duration: defaultGossipProbeInterval},
		GossipAllowSelfAsSeed:    true,
		HeartbeatInterval:        toml.Duration{Duration: defaultHeartbeatInterval},
		HAKeeperTickInterval:     toml.Duration{Duration: time.Second / hakeeper.DefaultTickPerSecond},
		HAKeeperCheckInterval:    toml.Duration{Duration: hakeeper.CheckDuration},
		TruncateInterval:         toml.Duration{Duration: defaultTruncateInterval},
		HAKeeperTruncateInterval: toml.Duration{Duration: defaultHAKeeperTruncateInterval},
		IsNonVoting:              false,
		MembershipImmovable:      true,
		RPC: struct {
			MaxMessageSize toml.ByteSize `toml:"max-message-size"`
			EnableCompress bool          `toml:"enable-compress"`
		}(struct {
			MaxMessageSize toml.ByteSize
			EnableCompress bool
		}{
			MaxMessageSize: toml.ByteSize(defaultMaxMessageSize),
			EnableCompress: false,
		}),
		BootstrapConfig: struct {
			BootstrapCluster      bool     `toml:"bootstrap-cluster"`
			NumOfLogShards        uint64   `toml:"num-of-log-shards"`
			NumOfTNShards         uint64   `toml:"num-of-tn-shards"`
			NumOfLogShardReplicas uint64   `toml:"num-of-log-shard-replicas"`
			InitHAKeeperMembers   []string `toml:"init-hakeeper-members" user_setting:"advanced"`
			Restore               struct {
				FilePath string `toml:"file-path"`
				Force    bool   `toml:"force"`
			} `toml:"restore"`
			NonVotingLocality string `toml:"non-voting-locality" user_setting:"advanced"`
			StandbyEnabled    bool   `toml:"standby-enabled" user_setting:"advanced"`
		}(struct {
			BootstrapCluster      bool
			NumOfLogShards        uint64
			NumOfTNShards         uint64
			NumOfLogShardReplicas uint64
			InitHAKeeperMembers   []string
			Restore               struct {
				FilePath string
				Force    bool
			}
			NonVotingLocality string
			StandbyEnabled    bool
		}{
			BootstrapCluster:      true,
			NumOfLogShards:        1,
			NumOfTNShards:         1,
			NumOfLogShardReplicas: 1,
			InitHAKeeperMembers:   []string{"131072:" + uid},
			Restore: struct {
				FilePath string
				Force    bool
			}{
				FilePath: defaultRestoreFilePath,
				Force:    false,
			},
			NonVotingLocality: "",
			StandbyEnabled:    false,
		}),
		HAKeeperConfig: struct {
			TickPerSecond   int           `toml:"tick-per-second"`
			LogStoreTimeout toml.Duration `toml:"log-store-timeout"`
			TNStoreTimeout  toml.Duration `toml:"tn-store-timeout"`
			CNStoreTimeout  toml.Duration `toml:"cn-store-timeout"`
		}(struct {
			TickPerSecond   int
			LogStoreTimeout toml.Duration
			TNStoreTimeout  toml.Duration
			CNStoreTimeout  toml.Duration
		}{
			TickPerSecond:   hakeeper.DefaultTickPerSecond,
			LogStoreTimeout: toml.Duration{Duration: hakeeper.DefaultLogStoreTimeout},
			TNStoreTimeout:  toml.Duration{Duration: hakeeper.DefaultTNStoreTimeout},
			CNStoreTimeout:  toml.Duration{Duration: hakeeper.DefaultCNStoreTimeout},
		}),
		HAKeeperClientConfig: HAKeeperClientConfig{
			DiscoveryAddress: "",
			ServiceAddresses: []string{DefaultLogServiceServiceAddress},
			AllocateIDBatch:  100,
			EnableCompress:   false,
		},
		DisableWorkers: false,
		// Not used for now.
		Ctl: struct {
			ListenAddress  string `toml:"listen-address"`
			ServiceAddress string `toml:"service-address"`
		}(struct {
			ListenAddress  string
			ServiceAddress string
		}{ListenAddress: "", ServiceAddress: ""}),
	}
}

// Fill just fills the listen addresses. This function is deprecated and will be removed
// as the configurations are all deprecated.
func (c *Config) Fill() {
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
	// If GossipAddressV2 is set, we use it as gossip address, and GossipAddress
	// will be overridden by it.
	if len(c.GossipAddressV2) != 0 {
		c.GossipAddress = c.GossipAddressV2
	}
	if len(c.GossipAddress) == 0 {
		c.GossipAddress = defaultGossipAddress
		c.GossipListenAddress = defaultGossipAddress
	} else if len(c.GossipAddress) != 0 && len(c.GossipListenAddress) == 0 {
		c.GossipListenAddress = c.GossipAddress
	}
	if len(c.GossipSeedAddresses) == 0 {
		c.GossipSeedAddresses = []string{defaultGossipSeedAddress}
	}
}

// HAKeeperClientConfig is the config for HAKeeper clients.
type HAKeeperClientConfig struct {
	// DiscoveryAddress is the Log Service discovery address provided by k8s.
	DiscoveryAddress string `toml:"discovery-address"`
	// ServiceAddresses is a list of well known Log Services' service addresses.
	ServiceAddresses []string `toml:"service-addresses"`
	// AllocateIDBatch how many IDs are assigned from hakeeper each time. Default is
	// 100.
	AllocateIDBatch uint64 `toml:"allocate-id-batch"`
	// EnableCompress enable compress
	EnableCompress bool `toml:"enable-compress"`
}

// Validate validates the HAKeeperClientConfig.
func (c *HAKeeperClientConfig) Validate() error {
	if len(c.DiscoveryAddress) == 0 && len(c.ServiceAddresses) == 0 {
		c.ServiceAddresses = []string{DefaultLogServiceServiceAddress}
	}
	if c.AllocateIDBatch == 0 {
		c.AllocateIDBatch = 100
	}
	return nil
}

// ClientConfig is the configuration for log service clients.
type ClientConfig struct {
	// Tag client tag
	Tag string
	// ReadOnly indicates whether this is a read-only client.
	ReadOnly bool
	// LogShardID is the shard ID of the log service shard to be used.
	LogShardID uint64
	// TNReplicaID is the replica ID of the TN that owns the created client.
	TNReplicaID uint64
	// DiscoveryAddress is the Log Service discovery address provided by k8s.
	DiscoveryAddress string
	// LogService nodes service addresses. This field is provided for testing
	// purposes only.
	ServiceAddresses []string
	// MaxMessageSize is the max message size for RPC.
	MaxMessageSize int
	// EnableCompress enable compress
	EnableCompress bool
}

// ValidateClient validates the ClientConfig.
func (c *ClientConfig) ValidateClient() error {
	if c.LogShardID == 0 {
		return moerr.NewBadConfigNoCtx("LogShardID value cannot be 0")
	}
	if c.TNReplicaID == 0 {
		return moerr.NewBadConfigNoCtx("DNReplicaID value cannot be 0")
	}
	if len(c.DiscoveryAddress) == 0 && len(c.ServiceAddresses) == 0 {
		c.ServiceAddresses = []string{DefaultLogServiceServiceAddress}
	}
	return nil
}

// ValidateStandbyClient validates the ClientConfig.
func (c *ClientConfig) ValidateStandbyClient() error {
	if c.LogShardID == 0 {
		return moerr.NewBadConfigNoCtx("LogShardID value cannot be 0")
	}
	if len(c.DiscoveryAddress) == 0 && len(c.ServiceAddresses) == 0 {
		c.ServiceAddresses = []string{DefaultLogServiceServiceAddress}
	}
	return nil
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

func (c *Config) LogServiceListenAddr() string {
	if c.LogServicePort != 0 {
		return fmt.Sprintf("%s:%d", DefaultListenHost, c.LogServicePort)
	}
	return c.ServiceListenAddress
}

func (c *Config) LogServiceServiceAddr() string {
	if c.LogServicePort != 0 {
		return fmt.Sprintf("%s:%d", c.ServiceHost, c.LogServicePort)
	}
	return c.ServiceAddress
}

func (c *Config) RaftListenAddr() string {
	if c.RaftPort != 0 {
		return fmt.Sprintf("%s:%d", DefaultListenHost, c.RaftPort)
	}
	return c.RaftListenAddress
}

func (c *Config) RaftServiceAddr() string {
	if c.RaftPort != 0 {
		return fmt.Sprintf("%s:%d", c.ServiceHost, c.RaftPort)
	}
	return c.RaftAddress
}

func (c *Config) GossipListenAddr() string {
	if c.GossipPort != 0 {
		return fmt.Sprintf("%s:%d", DefaultListenHost, c.GossipPort)
	}
	return c.GossipListenAddress
}

func (c *Config) GossipServiceAddr() string {
	if c.GossipPort != 0 {
		return fmt.Sprintf("%s:%d", c.ServiceHost, c.GossipPort)
	}
	return c.GossipAddress
}

// getLocality returns a pointer to logservicepb.Locality instance.
// It creates a instance by parsing the content in Config.Locality,
// which is just a string value. The format of the string value
// should be like: "key1:value1;key2:value2[;]"
func (c *Config) getLocality() logservicepb.Locality {
	if c.Locality == "" {
		return logservicepb.Locality{}
	}
	locality := logservicepb.Locality{
		Value: make(map[string]string),
	}
	sep1, sep2 := ";", ":"
	kvs := strings.Split(c.Locality, sep1)
	for _, kv := range kvs {
		pair := strings.Split(kv, sep2)
		if len(pair) != 2 || pair[0] == "" || pair[1] == "" {
			continue
		}
		locality.Value[pair[0]] = pair[1]
	}
	return locality
}

func dumpLogConfig(cfg Config) (map[string]*logservicepb.ConfigItem, error) {
	defCfg := Config{}
	return util.DumpConfig(cfg, defCfg)
}
