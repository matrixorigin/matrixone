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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"go.uber.org/zap"
)

type Service interface {
	Start() error
	Close() error
}

// Config cn service
type Config struct {
	// ListenAddress listening address for receiving external requests
	ListenAddress string `toml:"listen-address"`
	// FileService file service configuration
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
}

type service struct {
	cfg    *Config
	pool   *sync.Pool
	logger *zap.Logger
	server morpc.RPCServer
}
