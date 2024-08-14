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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestConfig() Config {
	c := DefaultConfig()
	c.UUID = "uuid"
	c.DeploymentID = 100
	c.DataDir = "/mydata/dir"
	c.LogServicePort = 9000
	c.RaftPort = 9001
	c.GossipPort = 9002
	c.GossipSeedAddresses = []string{"localhost:9002"}
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
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41b"},
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", ""},
		{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf", "1:1"},
	}

	for _, v := range tests {
		cfg := Config{}
		cfg.BootstrapConfig.InitHAKeeperMembers = v
		_, err := cfg.GetInitHAKeeperMembers()
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
	}
}

func TestConfigCanBeValidated(t *testing.T) {
	c := getTestConfig()
	assert.NoError(t, c.Validate())

	c1 := c
	c1.DeploymentID = 0
	err := c1.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c2 := c
	c2.ServiceAddress = ""
	c2.LogServicePort = 0
	err = c2.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c3 := c
	c3.RaftAddress = ""
	c3.RaftPort = 0
	err = c2.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c4 := c
	c4.GossipAddress = ""
	c4.GossipPort = 0
	err = c4.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c5 := c
	c5.GossipSeedAddresses = []string{}
	err = c5.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c6 := c
	c6.GossipProbeInterval.Duration = 0
	err = c6.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
}

func TestBootstrapConfigCanBeValidated(t *testing.T) {
	c := getTestConfig()
	assert.NoError(t, c.Validate())

	c.BootstrapConfig.BootstrapCluster = true
	c.BootstrapConfig.NumOfLogShards = 3
	c.BootstrapConfig.NumOfTNShards = 3
	c.BootstrapConfig.NumOfLogShardReplicas = 1
	c.BootstrapConfig.InitHAKeeperMembers = []string{"131072:9c4dccb4-4d3c-41f8-b482-5251dc7a41bf"}
	assert.NoError(t, c.Validate())

	c1 := c
	c1.BootstrapConfig.NumOfLogShards = 0
	err := c1.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c2 := c
	c2.BootstrapConfig.NumOfTNShards = 0
	err = c2.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c3 := c
	c3.BootstrapConfig.NumOfTNShards = 2
	err = c3.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))

	c4 := c
	c4.BootstrapConfig.NumOfLogShardReplicas = 2
	err = c4.Validate()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
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
			ClientConfig{LogShardID: 1, TNReplicaID: 100, DiscoveryAddress: "localhost:9090"}, true,
		},
		{
			ClientConfig{
				LogShardID:       1,
				TNReplicaID:      100,
				DiscoveryAddress: "localhost:9090",
				ServiceAddresses: []string{"localhost:9091"},
			},
			true,
		},
	}

	for _, tt := range tests {
		err := tt.cfg.ValidateClient()
		if tt.ok {
			assert.NoError(t, err)
		} else {
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
		}
	}
}

func TestHAKeeperClientConfigValidate(t *testing.T) {
	tests := []struct {
		cfg HAKeeperClientConfig
		ok  bool
	}{
		{
			HAKeeperClientConfig{}, true,
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
			assert.True(t, moerr.IsMoErrCode(err, moerr.ErrBadConfig))
		}
	}
}
