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

package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

// TNService describes expected behavior for tn service.
type TNService interface {
	// Start sends heartbeat and start to handle command.
	Start() error
	// Close stops store
	Close() error
	// Status returns the status of service.
	Status() ServiceStatus

	// ID returns uuid of store
	ID() string

	// StartTNReplica start the TNShard replica
	StartTNReplica(shard metadata.TNShard) error
	// CloseTNReplica close the TNShard replica.
	CloseTNReplica(shard metadata.TNShard) error

	// GetTaskService returns the taskservice
	GetTaskService() (taskservice.TaskService, bool)
}

// tnService wraps tnservice.Service.
//
// The main purpose of this structure is to maintain status.
type tnService struct {
	sync.Mutex
	status ServiceStatus
	uuid   string
	svc    tnservice.Service
}

func (ds *tnService) Start() error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status == ServiceInitialized {
		err := ds.svc.Start()
		if err != nil {
			return err
		}
		ds.status = ServiceStarted
	}

	return nil
}

func (ds *tnService) Close() error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status == ServiceStarted {
		err := ds.svc.Close()
		if err != nil {
			return err
		}
		ds.status = ServiceClosed
	}

	return nil
}

func (ds *tnService) Status() ServiceStatus {
	ds.Lock()
	defer ds.Unlock()
	return ds.status
}

func (ds *tnService) ID() string {
	ds.Lock()
	defer ds.Unlock()
	return ds.uuid
}

func (ds *tnService) StartTNReplica(shard metadata.TNShard) error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status != ServiceStarted {
		return moerr.NewNoServiceNoCtx(ds.uuid)
	}

	return ds.svc.StartTNReplica(shard)
}

func (ds *tnService) CloseTNReplica(shard metadata.TNShard) error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status != ServiceStarted {
		return moerr.NewNoServiceNoCtx(ds.uuid)
	}

	return ds.svc.CloseTNReplica(shard)
}

func (ds *tnService) GetTaskService() (taskservice.TaskService, bool) {
	return ds.svc.GetTaskService()
}

// tnOptions is options for a tn service.
type tnOptions []tnservice.Option

// newTNService initializes an instance of `TNService`.
func newTNService(
	cfg *tnservice.Config,
	rt runtime.Runtime,
	fs fileservice.FileService,
	opts tnOptions,
) (TNService, error) {
	svc, err := tnservice.NewService(cfg, rt, fs, nil, opts...)
	if err != nil {
		return nil, err
	}
	return &tnService{
		status: ServiceInitialized,
		uuid:   cfg.UUID,
		svc:    svc,
	}, nil
}

// buildTNConfig builds configuration for a tn service.
func buildTNConfig(
	index int, opt Options, address serviceAddresses,
) *tnservice.Config {
	uid, _ := uuid.NewV7()
	cfg := &tnservice.Config{
		UUID:          uid.String(),
		ListenAddress: address.getTnListenAddress(index),
	}
	cfg.ServiceAddress = cfg.ListenAddress
	cfg.LogtailServer.ListenAddress = address.getTnLogtailAddress(index)
	cfg.DataDir = filepath.Join(opt.rootDataDir, cfg.UUID)
	cfg.HAKeeper.ClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	cfg.HAKeeper.HeatbeatInterval.Duration = opt.heartbeat.tn
	cfg.Txn.Storage.Backend = opt.storage.tnStorage

	// FIXME: disable tae flush
	cfg.Ckp.MinCount = 2000000
	cfg.Ckp.FlushInterval.Duration = time.Second * 100000
	cfg.Ckp.ScanInterval.Duration = time.Second * 100000
	cfg.Ckp.IncrementalInterval.Duration = time.Second * 100000
	cfg.Ckp.GlobalMinCount = 10000

	// logtail push service config for tae storage
	cfg.LogtailServer.RpcMaxMessageSize = toml.ByteSize(opt.logtailPushServer.rpcMaxMessageSize)
	cfg.LogtailServer.LogtailCollectInterval.Duration = opt.logtailPushServer.logtailCollectInterval
	cfg.LogtailServer.LogtailResponseSendTimeout.Duration = opt.logtailPushServer.logtailResponseSendTimeout

	// We need the filled version of configuration.
	// It's necessary when building tnservice.Option.
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("fatal when building tnservice.Config: %s", err))
	}

	return cfg
}

// buildTNOptions builds options for a tn service.
//
// NB: We need the filled version of tnservice.Config.
func buildTNOptions(cfg *tnservice.Config, filter FilterFunc) tnOptions {
	// factory to construct client for hakeeper
	hakeeperClientFactory := func() (logservice.TNHAKeeperClient, error) {
		ctx, cancel := context.WithTimeout(
			context.Background(), cfg.HAKeeper.DiscoveryTimeout.Duration,
		)
		defer cancel()

		// transfer morpc.BackendOption via context
		ctx = logservice.SetBackendOptions(ctx, morpc.WithBackendFilter(filter))

		client, err := logservice.NewTNHAKeeperClient(
			ctx, cfg.HAKeeper.ClientConfig,
		)
		if err != nil {
			return nil, err
		}
		return client, nil
	}

	// factory to construct client for log service
	logServiceClientFactory := func(shard metadata.TNShard) (logservice.Client, error) {
		ctx, cancel := context.WithTimeout(
			context.Background(), cfg.LogService.ConnectTimeout.Duration,
		)
		defer cancel()

		// transfer morpc.BackendOption via context
		ctx = logservice.SetBackendOptions(ctx, morpc.WithBackendFilter(filter))

		return logservice.NewClient(ctx, logservice.ClientConfig{
			Tag:              "Test-TN",
			ReadOnly:         false,
			LogShardID:       shard.LogShardID,
			TNReplicaID:      shard.ReplicaID,
			ServiceAddresses: cfg.HAKeeper.ClientConfig.ServiceAddresses,
		})
	}

	return []tnservice.Option{
		tnservice.WithHAKeeperClientFactory(hakeeperClientFactory),
		tnservice.WithLogServiceClientFactory(logServiceClientFactory),
		tnservice.WithBackendFilter(filter),
	}
}
