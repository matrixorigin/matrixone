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
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	defaultDeploymentID         uint64 = 1
	defaultRTTMillisecond       uint64 = 5
	defaultHeartbeatInterval           = 5 * time.Millisecond
	defaultHAKeeperTickInterval        = 5 * time.Millisecond
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
	SetInitialClusterInfo(numOfLogShards, numOfDNShards, numOfLogReplicas uint64) error

	// StartHAKeeperReplica starts hakeeper replicas.
	StartHAKeeperReplica(replicaID uint64, initialReplicas map[uint64]dragonboat.Target, join bool) error
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

	if ls.status == ServiceInitialized || ls.status == ServiceClosed {
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
	numOfLogShards, numOfDNShards, numOfLogReplicas uint64,
) error {
	return ls.svc.SetInitialClusterInfo(
		numOfLogShards, numOfDNShards, numOfLogReplicas,
	)
}

func (ls *logService) StartHAKeeperReplica(
	replicaID uint64, initialReplicas map[uint64]dragonboat.Target, join bool,
) error {
	return ls.svc.StartHAKeeperReplica(replicaID, initialReplicas, join)
}

// newLogService constructs an instance of `LogService`.
func newLogService(cfg logservice.Config) (LogService, error) {
	cfg.Fill()

	svc, err := logservice.NewWrappedService(cfg)
	if err != nil {
		return nil, err
	}
	return &logService{status: ServiceInitialized, svc: svc}, nil
}

// buildLogConfig builds configuration for a log service.
func buildLogConfig(index int, opt Options, address serviceAddress) logservice.Config {
	cfg := logservice.Config{
		UUID:                uuid.New().String(),
		FS:                  vfs.NewStrictMem(),
		DeploymentID:        defaultDeploymentID,
		RTTMillisecond:      defaultRTTMillisecond,
		DataDir:             buildLogDataDir(opt.rootDataDir, index),
		ServiceAddress:      address.getLogListenAddress(index), // hakeeper client use this address
		RaftAddress:         address.getLogRaftAddress(index),
		GossipAddress:       address.getLogGossipAddress(index),
		GossipSeedAddresses: address.getLogGossipSeedAddresses(),
	}
	cfg.HeartbeatInterval.Duration = defaultHeartbeatInterval
	cfg.HAKeeperTickInterval.Duration = defaultHAKeeperTickInterval
	cfg.HAKeeperClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	return cfg
}

// buildLogDataDir generates data directory for a log service.
func buildLogDataDir(root string, index int) string {
	return path.Join(root, "log", strconv.Itoa(index))
}

// startHAKeeperReplica selects the first `n` log services to start hakeeper replica.
func (c *testCluster) startHAKeeperReplica() error {
	selected := c.selectHAkeeperServices()
	assert.NotZero(c.t, len(selected))

	c.logger.Info("start hakeeper replicas", zap.Int("batch", len(selected)))

	indexToReplicaID := func(index int) uint64 {
		return uint64(index + 1)
	}

	// construct ppers
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
			c.opt.initial.dnShardNum,
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
