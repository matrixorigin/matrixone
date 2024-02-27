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
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	defaultDeploymentID   uint64 = 1
	defaultRTTMillisecond uint64 = 5
)

// LogService describes expected behavior for log service.
type LogService interface {
	// Start sends heartbeat and start to handle command.
	Start() error
	// Close stops store
	Close() error
	// Status returns the status of service
	Status() ServiceStatus

	// ID returns uuid of store
	ID() string

	// IsLeaderHakeeper checks hakeeper information.
	IsLeaderHakeeper() (bool, error)

	// GetClusterState returns cluster information from hakeeper leader.
	GetClusterState() (*logpb.CheckerState, error)

	// SetInitialClusterInfo sets cluster initialize state.
	SetInitialClusterInfo(numOfLogShards, numOfTNShards, numOfLogReplicas uint64) error

	// StartHAKeeperReplica starts hakeeper replicas.
	StartHAKeeperReplica(replicaID uint64, initialReplicas map[uint64]dragonboat.Target, join bool) error

	// GetTaskService returns the taskService
	GetTaskService() (taskservice.TaskService, bool)
}

// logService wraps logservice.WrappedService.
//
// The main purpose of this structure is to maintain status
type logService struct {
	sync.Mutex
	status ServiceStatus
	svc    *logservice.WrappedService
}

func (ls *logService) Start() error {
	ls.Lock()
	defer ls.Unlock()

	if ls.status == ServiceInitialized {
		err := ls.svc.Start()
		if err != nil {
			return err
		}
		ls.status = ServiceStarted
	}

	return nil
}

func (ls *logService) Close() error {
	ls.Lock()
	defer ls.Unlock()

	if ls.status == ServiceStarted {
		err := ls.svc.Close()
		if err != nil {
			return err
		}
		ls.status = ServiceClosed
	}

	return nil
}

func (ls *logService) Status() ServiceStatus {
	ls.Lock()
	defer ls.Unlock()
	return ls.status
}

func (ls *logService) ID() string {
	return ls.svc.ID()
}

func (ls *logService) IsLeaderHakeeper() (bool, error) {
	return ls.svc.IsLeaderHakeeper()
}

func (ls *logService) GetClusterState() (*logpb.CheckerState, error) {
	return ls.svc.GetClusterState()
}

func (ls *logService) SetInitialClusterInfo(
	numOfLogShards, numOfTNShards, numOfLogReplicas uint64,
) error {
	return ls.svc.SetInitialClusterInfo(
		numOfLogShards, numOfTNShards, numOfLogReplicas,
	)
}

func (ls *logService) StartHAKeeperReplica(
	replicaID uint64, initialReplicas map[uint64]dragonboat.Target, join bool,
) error {
	return ls.svc.StartHAKeeperReplica(replicaID, initialReplicas, join)
}

func (ls *logService) GetTaskService() (taskservice.TaskService, bool) {
	return ls.svc.GetTaskService()
}

// logOptions is options for a log service.
type logOptions []logservice.Option

// newLogService constructs an instance of `LogService`.
func newLogService(
	cfg logservice.Config,
	fs fileservice.FileService,
	opts logOptions,
) (LogService, error) {
	svc, err := logservice.NewWrappedService(cfg, fs, nil, opts...)
	if err != nil {
		return nil, err
	}
	return &logService{status: ServiceInitialized, svc: svc}, nil
}

// buildLogConfig builds configuration for a log service.
func buildLogConfig(
	index int, opt Options, address *serviceAddresses,
) logservice.Config {
	cfg := logservice.DefaultConfig()
	uid, _ := uuid.NewV7()
	cfg.UUID = uid.String()
	cfg.FS = vfs.NewStrictMem()
	cfg.DeploymentID = defaultDeploymentID
	cfg.RTTMillisecond = defaultRTTMillisecond
	cfg.LogServicePort = getPort(address.getLogListenAddress(index)) // hakeeper client use this address
	cfg.RaftPort = getPort(address.getLogRaftAddress(index))
	cfg.GossipPort = getPort(address.getLogGossipAddress(index))
	cfg.GossipSeedAddresses = address.getLogGossipSeedAddresses()
	cfg.GossipAllowSelfAsSeed = opt.initial.logReplicaNum == 1
	cfg.DataDir = filepath.Join(opt.rootDataDir, cfg.UUID)
	cfg.HeartbeatInterval.Duration = opt.heartbeat.log
	cfg.HAKeeperCheckInterval.Duration = opt.hakeeper.checkInterval
	cfg.HAKeeperClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	// setting hakeeper configuration
	cfg.HAKeeperConfig.TickPerSecond = opt.hakeeper.tickPerSecond
	cfg.HAKeeperConfig.LogStoreTimeout.Duration = opt.hakeeper.logStoreTimeout
	cfg.HAKeeperConfig.TNStoreTimeout.Duration = opt.hakeeper.tnStoreTimeout
	cfg.HAKeeperConfig.CNStoreTimeout.Duration = opt.hakeeper.cnStoreTimeout

	return cfg
}

// buildLogOptions builds options for a log service.
//
// NB: We need the filled version of logservice.Config.
func buildLogOptions(cfg logservice.Config, filter FilterFunc) logOptions {
	return []logservice.Option{
		logservice.WithBackendFilter(filter),
		logservice.WithRuntime(runtime.ProcessLevelRuntime()),
	}
}

// startHAKeeperReplica selects the first `n` log services to start hakeeper replica.
func (c *testCluster) startHAKeeperReplica() error {
	selected := c.selectHAkeeperServices()
	assert.NotZero(c.t, len(selected))

	c.logger.Info("start hakeeper replicas", zap.Int("batch", len(selected)))

	indexToReplicaID := func(index int) uint64 {
		return uint64(index + 1)
	}

	// construct peers
	peers := make(map[uint64]dragonboat.Target)
	for i, logsvc := range selected {
		replicaID := indexToReplicaID(i)
		peers[replicaID] = logsvc.ID()
	}

	// start all hakeeper replicas
	for i, logsvc := range selected {
		replicaID := indexToReplicaID(i)
		err := logsvc.StartHAKeeperReplica(replicaID, peers, false)
		if err != nil {
			c.logger.Error("fail to start hakeeper replica", zap.Error(err), zap.Int("index", i))
			return err
		}
		c.logger.Info("hakeeper replica started", zap.Int("index", i))
	}

	return nil
}

// setInitialClusterInfo initializes cluster information.
func (c *testCluster) setInitialClusterInfo() error {
	errChan := make(chan error, 1)

	initialize := func() {
		var err error
		defer func() {
			errChan <- err
		}()

		selected := c.selectHAkeeperServices()
		assert.NotZero(c.t, len(selected))

		c.logger.Info("initialize cluster information")

		err = selected[0].SetInitialClusterInfo(
			c.opt.initial.logShardNum,
			c.opt.initial.tnShardNum,
			c.opt.initial.logReplicaNum,
		)
		if err != nil {
			c.logger.Error("fail to initialize cluster", zap.Error(err))
			return
		}

		c.logger.Info("cluster information initialized")
	}

	// initialize cluster only once
	c.log.once.Do(initialize)
	return <-errChan
}

// listHAKeeperService lists all log services that start hakeeper.
func (c *testCluster) selectHAkeeperServices() []LogService {
	n := haKeeperNum(c.opt.initial.logServiceNum)
	svcs := make([]LogService, n)
	for i := 0; i < n; i++ {
		svcs[i] = c.log.svcs[i]
	}
	return svcs
}
