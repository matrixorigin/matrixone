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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
)

func getTestConfig() Config {
	c := Config{
		UUID:                 "uuid",
		DeploymentID:         100,
		DataDir:              "/mydata/dir",
		ServiceAddress:       "localhost:9000",
		ServiceListenAddress: "localhost:9000",
		RaftAddress:          "localhost:9001",
		RaftListenAddress:    "localhost:9001",
		GossipAddress:        "localhost:9002",
		GossipListenAddress:  "localhost:9002",
		GossipSeedAddresses:  []string{"localhost:9002"},
	}
	c.Fill()
	return c
}

func TestSplitAddress(t *testing.T) {
	tests := []struct {
		input  string
		output []string
	}{
		{"", []string{}},
		{" ; ", []string{}},
		{" ;; ", []string{}},
		{";", []string{}},
		{"localhost:1000;localhost:1001", []string{"localhost:1000", "localhost:1001"}},
		{" localhost:1000 ; localhost:1001\t\n", []string{"localhost:1000", "localhost:1001"}},
		{"localhost:1000 \n", []string{"localhost:1000"}},
		{"localhost:1000;", []string{"localhost:1000"}},
		{";localhost:1000", []string{"localhost:1000"}},
	}

	for _, tt := range tests {
		v := splitAddresses(tt.input)
		assert.Equal(t, tt.output, v)
	}
}

func TestGetInitHAKeeperMembers(t *testing.T) {
	cfg1 := Config{}
	cfg1.BootstrapConfig.InitHAKeeperMembers = []string{" 131072 :9c4dccb4-4d3c-41f8-b482-5251dc7a41bf "}
	result, err := cfg1.GetInitHAKeeperMembers()
	require.NoError(t, err)
	assert.Equal(t, 1, len(result))
	v, ok := result[131072]
	assert.True(t, ok)
	assert.Equal(t, "9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", v)

	cfg2 := Config{}
	cfg2.BootstrapConfig.InitHAKeeperMembers = []string{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", "131073:9c4dccb4-4d3c-41f8-b482-5251dc7a41be"}
	result, err = cfg2.GetInitHAKeeperMembers()
	require.NoError(t, err)
	assert.Equal(t, 2, len(result))
	v1, ok1 := result[131072]
	v2, ok2 := result[131073]
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.Equal(t, "9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", v1)
	assert.Equal(t, "9c4dccb4-4d3c-41f8-b482-5251dc7a41be", v2)

	tests := [][]string{
		{"131071:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"},
		{"262144:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"},
		{"262145:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"},
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41b"},
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", ""},
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", "1:1"},
	}

	for _, v := range tests {
		cfg := Config{}
		cfg.BootstrapConfig.InitHAKeeperMembers = v
		_, err := cfg.GetInitHAKeeperMembers()
		assert.Equal(t, ErrInvalidBootstrapConfig, err)
	}
}

func TestConfigCanBeValidated(t *testing.T) {
	c := getTestConfig()
	assert.NoError(t, c.Validate())

	c1 := c
	c1.DeploymentID = 0
	assert.True(t, errors.Is(c1.Validate(), ErrInvalidConfig))

	c2 := c
	c2.ServiceAddress = ""
	assert.True(t, errors.Is(c2.Validate(), ErrInvalidConfig))

	c3 := c
	c3.RaftAddress = ""
	assert.True(t, errors.Is(c3.Validate(), ErrInvalidConfig))

	c4 := c
	c4.GossipAddress = ""
	assert.True(t, errors.Is(c4.Validate(), ErrInvalidConfig))

	c5 := c
	c5.GossipSeedAddresses = []string{}
	assert.True(t, errors.Is(c5.Validate(), ErrInvalidConfig))
}

func TestBootstrapConfigCanBeValidated(t *testing.T) {
	c := getTestConfig()
	assert.NoError(t, c.Validate())

	c.BootstrapConfig.BootstrapCluster = true
	c.BootstrapConfig.NumOfLogShards = 3
	c.BootstrapConfig.NumOfDNShards = 3
	c.BootstrapConfig.NumOfLogShardReplicas = 1
	c.BootstrapConfig.InitHAKeeperMembers = []string{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"}
	assert.NoError(t, c.Validate())

	c1 := c
	c1.BootstrapConfig.NumOfLogShards = 0
	assert.True(t, errors.Is(c1.Validate(), ErrInvalidConfig))

	c2 := c
	c2.BootstrapConfig.NumOfDNShards = 0
	assert.True(t, errors.Is(c2.Validate(), ErrInvalidConfig))

	c3 := c
	c3.BootstrapConfig.NumOfDNShards = 2
	assert.True(t, errors.Is(c3.Validate(), ErrInvalidConfig))

	c4 := c
	c4.BootstrapConfig.NumOfLogShardReplicas = 2
	assert.True(t, errors.Is(c4.Validate(), ErrInvalidConfig))
}

func TestFillConfig(t *testing.T) {
	c := Config{}
	c.Fill()

	assert.Equal(t, uint64(0), c.DeploymentID)
	assert.Equal(t, defaultDataDir, c.DataDir)
	assert.Equal(t, defaultServiceAddress, c.ServiceAddress)
	assert.Equal(t, defaultServiceAddress, c.ServiceListenAddress)
	assert.Equal(t, defaultRaftAddress, c.RaftAddress)
	assert.Equal(t, defaultRaftAddress, c.RaftListenAddress)
	assert.Equal(t, defaultGossipAddress, c.GossipAddress)
	assert.Equal(t, defaultGossipAddress, c.GossipListenAddress)
	assert.Equal(t, 0, len(c.GossipSeedAddresses))
	assert.Equal(t, hakeeper.DefaultTickPerSecond, c.HAKeeperConfig.TickPerSecond)
	assert.Equal(t, hakeeper.DefaultLogStoreTimeout, c.HAKeeperConfig.LogStoreTimeout.Duration)
	assert.Equal(t, hakeeper.DefaultDnStoreTimeout, c.HAKeeperConfig.DnStoreTimeout.Duration)
}

func TestListenAddressCanBeFilled(t *testing.T) {
	cfg := Config{
		DeploymentID:        1,
		RTTMillisecond:      5,
		DataDir:             "data-1",
		ServiceAddress:      "127.0.0.1:9002",
		RaftAddress:         "127.0.0.1:9000",
		GossipAddress:       "127.0.0.1:9001",
		GossipSeedAddresses: []string{"127.0.0.1:9011"},
	}
	cfg.Fill()
	assert.Equal(t, cfg.ServiceAddress, cfg.ServiceListenAddress)
	assert.Equal(t, cfg.RaftAddress, cfg.RaftListenAddress)
	assert.Equal(t, cfg.GossipAddress, cfg.GossipListenAddress)
}

func TestClientConfigValidate(t *testing.T) {
	tests := []struct {
		cfg ClientConfig
		ok  bool
	}{
		{
			ClientConfig{}, false,
		},
		{
			ClientConfig{LogShardID: 1, DiscoveryAddress: "localhost:9090"}, false,
		},
		{
			ClientConfig{LogShardID: 1, DiscoveryAddress: "localhost:9090"}, false,
		},
		{
			ClientConfig{LogShardID: 1, DNReplicaID: 100, DiscoveryAddress: "localhost:9090"}, true,
		},
		{
			ClientConfig{
				LogShardID:       1,
				DNReplicaID:      100,
				DiscoveryAddress: "localhost:9090",
				ServiceAddresses: []string{"localhost:9091"},
			},
			true,
		},
	}

	for _, tt := range tests {
		err := tt.cfg.Validate()
		if tt.ok {
			assert.NoError(t, err)
		} else {
			assert.True(t, errors.Is(err, ErrInvalidConfig))
		}
	}
}

func TestHAKeeperClientConfigValidate(t *testing.T) {
	tests := []struct {
		cfg HAKeeperClientConfig
		ok  bool
	}{
		{
			HAKeeperClientConfig{}, false,
		},
		{
			HAKeeperClientConfig{DiscoveryAddress: "localhost:9090"}, true,
		},
		{
			HAKeeperClientConfig{ServiceAddresses: []string{"localhost:9090"}}, true,
		},
		{
			HAKeeperClientConfig{
				DiscoveryAddress: "localhost:9091",
				ServiceAddresses: []string{"localhost:9090"},
			}, true,
		},
	}

	for _, tt := range tests {
		err := tt.cfg.Validate()
		if tt.ok {
			assert.NoError(t, err)
		} else {
			assert.True(t, errors.Is(err, ErrInvalidConfig))
		}
	}
}
