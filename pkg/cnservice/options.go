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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// Option option to create cn service
type Option func(*service)

// WithLogger setup cn service's logger
func WithLogger(logger *zap.Logger) Option {
	return func(s *service) {
		s.logger = logger
	}
}

// WithTaskStorageFactory setup the special task strorage factory
func WithTaskStorageFactory(factory taskservice.TaskStorageFactory) Option {
	return func(s *service) {
		s.task.storageFactory = factory
	}
}

// WithMessageHandle setup message handle
func WithMessageHandle(f func(ctx context.Context,
	cnAddr string,
	message morpc.Message,
	cs morpc.ClientSession,
	engine engine.Engine,
	fs fileservice.FileService,
	lockService lockservice.LockService,
	queryService queryservice.QueryService,
	hakeeper logservice.CNHAKeeperClient,
	udfService udf.Service,
	cli client.TxnClient,
	aicm *defines.AutoIncrCacheManager,
	mAcquirer func() morpc.Message) error) Option {
	return func(s *service) {
		s.requestHandler = f
	}
}

// WithConfigData saves the data from the config file
func WithConfigData(data map[string]*logservicepb.ConfigItem) Option {
	return func(s *service) {
		if s.config == nil {
			s.config = util.NewConfigData(data)
		} else {
			util.MergeConfig(s.config, data)
		}
	}
}
