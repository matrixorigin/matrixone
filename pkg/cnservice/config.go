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

package cnservice

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type EngineType string

const (
	EngineTAE            EngineType = "tae"
	EngineDistributedTAE EngineType = "distributed-tae"
	EngineMemory         EngineType = "memory"
)

// Config cn service
type Config struct {
	// ListenAddress listening address for receiving external requests
	ListenAddress string `toml:"listen-address"`
	// FileService file service configuration

	Engine struct {
		Type EngineType `toml:"type"`
	}

	FileService struct {
		// Backend file service backend implementation. [Mem|DISK|S3|MINIO]. Default is DISK.
		Backend string `toml:"backend"`
		// S3 s3 configuration
		S3 fileservice.S3Config `toml:"s3"`
	}

	// Pipeline configuration
	Pipeline struct {
		// HostSize is the memory limit
		HostSize int64 `toml:"host-size"`
		// GuestSize is the memory limit for one query
		GuestSize int64 `toml:"guest-size"`
		// OperatorSize is the memory limit for one operator
		OperatorSize int64 `toml:"operator-size"`
		// BatchRows is the batch rows limit for one batch
		BatchRows int64 `toml:"batch-rows"`
		// BatchSize is the memory limit for one batch
		BatchSize int64 `toml:"batch-size"`
	}

	//parameters for the frontend
	Frontend config.FrontendParameters `toml:"frontend"`

	// HAKeeper configuration
	HAKeeper struct {
		// HeatbeatDuration heartbeat duration to send message to hakeeper. Default is 1s
		HeatbeatDuration toml.Duration `toml:"hakeeper-heartbeat-duration"`
		// HeatbeatTimeout heartbeat request timeout. Default is 500ms
		HeatbeatTimeout toml.Duration `toml:"hakeeper-heartbeat-timeout"`
		// DiscoveryTimeout discovery HAKeeper service timeout. Default is 30s
		DiscoveryTimeout toml.Duration `toml:"hakeeper-discovery-timeout"`
		// ClientConfig hakeeper client configuration
		ClientConfig logservice.HAKeeperClientConfig
	}

	// RPC rpc config used to build txn sender
	RPC rpc.Config `toml:"rpc"`
}

func (c *Config) Validate() error {
	if c.HAKeeper.DiscoveryTimeout.Duration == 0 {
		c.HAKeeper.DiscoveryTimeout.Duration = time.Second * 30
	}
	if c.HAKeeper.HeatbeatDuration.Duration == 0 {
		c.HAKeeper.HeatbeatDuration.Duration = time.Second
	}
	if c.HAKeeper.HeatbeatTimeout.Duration == 0 {
		c.HAKeeper.HeatbeatTimeout.Duration = time.Millisecond * 500
	}
	return nil
}
