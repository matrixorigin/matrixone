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
)

func getTestConfig() Config {
	return Config{
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
}

func TestConfigCanBeValidated(t *testing.T) {
	c := getTestConfig()
	assert.Nil(t, c.Validate())

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
}
