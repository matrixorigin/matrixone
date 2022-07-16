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
	"github.com/cockroachdb/errors"
	"github.com/lni/vfs"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// TODO: add toml or json support

// Config defines the Configurations supported by the Log Service.
type Config struct {
	FS                   vfs.FS
	DeploymentID         uint64
	NodeHostID           string
	RTTMillisecond       uint64
	DataDir              string
	ServiceAddress       string
	ServiceListenAddress string
	RaftAddress          string
	RaftListenAddress    string
	GossipAddress        string
	GossipListenAddress  string
	GossipSeedAddresses  []string
	// DisableHAKeeperTicker disables the HAKeeper ticker in test. Never set this
	// field to true in production
	DisableHAKeeperTicker bool
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
	return nil
}

func (c *Config) Fill() {
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
}
