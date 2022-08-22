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
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"go.uber.org/zap"
)

type Service interface {
	Start() error
	Close() error
}

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
}

type service struct {
	cfg                    *Config
	pool                   *sync.Pool
	logger                 *zap.Logger
	server                 morpc.RPCServer
	cancelMoServerFunc     context.CancelFunc
	mo                     *frontend.MOServer
	initHakeeperClientOnce sync.Once
	_hakeeperClient        logservice.CNHAKeeperClient
	initTxnSenderOnce      sync.Once
	_txnSender             rpc.TxnSender
	initTxnClientOnce      sync.Once
	_txnClient             client.TxnClient
}
