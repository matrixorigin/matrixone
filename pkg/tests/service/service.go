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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Cluster describes behavior of test framwork.
type Cluster interface {
	// Start starts svcs sequentially
	Start() error
	// Close stops svcs sequentially
	Close() error

	// ClusterOperation
	ClusterAwareness
	ClusterAssertState
	ClusterWaitState
}

// ClusterOperation supports kinds of cluster operations.
type ClusterOperation interface {
	CloseDNService(id string) error
	StartDNService(id string) error

	CloseLogService(id string) error
	StartLogService(id string) error

	// StartNetworkPartition(partitions [][]int) error
	// CloseNetworkPartition() error
}

// ClusterAwareness provides cluster awareness information.
type ClusterAwareness interface {
	// ListDNServices lists all dn svcs
	ListDNServices() []string
	// ListLogServices lists all log svcs
	ListLogServices() []string

	// GetDNService fetches dn service instance
	GetDNService(id string) (DNService, error)
	// GetLogService fetches log service instance
	GetLogService(id string) (LogService, error)
	// GetClusterState fetches current cluster state
	GetClusterState() (*logpb.CheckerState, error)
}

// TODO: add more convenient method
// ClusterAssertState asserts current cluster state.
type ClusterAssertState interface {
	AssertHAKeeperState(svc LogService, expected logpb.HAKeeperState)
	// AssertClusterHealth()
	// AssertClusterUnhealth()

	// AssertShardNum(typ string, exptect int)
	// AssertReplicaNum(shardId uint64, expected int)

	// AssertLeaderHakeeperState(expeted logpb.HAKeeperState)
}

// ClusterWaitState waits cluster state until timeout.
type ClusterWaitState interface {
	WaitHAKeeperState(timeout time.Duration, expected logpb.HAKeeperState)
	// WaitClusterHealth(timeout time.Duration)
	// WaitShardByNum(typ string, batch int, timeout time.Duration)
	// WaitReplicaByNum(shardID uint64, batch int, timeout time.Duration)
}

// ----------------------------------------------------
// The following are implements for interface `Cluster`.
// ----------------------------------------------------

// testCluster simulates a cluster with dn and log service.
type testCluster struct {
	t      *testing.T
	opt    Options
	logger *zap.Logger

	dn struct {
		sync.Mutex
		cfgs []*dnservice.Config
		opts []dnOptions
		svcs []DNService
	}

	log struct {
		once sync.Once

		sync.Mutex
		cfgs []logservice.Config
		svcs []LogService
	}

	address serviceAddress

	fileservices *fileServices

	mu struct {
		sync.Mutex
		running bool
	}
}

// NewCluster construct a cluster for integration test.
func NewCluster(t *testing.T, opt Options) (Cluster, error) {
	opt.validate()

	c := &testCluster{
		t:   t,
		opt: opt,
	}
	c.logger = logutil.Adjust(c.logger).With(
		zap.String("tests", "service"),
	)

	// build addresses for all services
	c.address = c.buildServiceAddress()

	// build FileService instances
	c.fileservices = c.buildFileServices()

	// build log service configurations
	c.log.cfgs = c.buildLogConfigs(c.address)

	// build dn service configurations
	c.dn.cfgs, c.dn.opts = c.buildDnConfigs(c.address)

	return c, nil
}

func (c *testCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.running {
		return nil
	}

	// start log services first
	if err := c.startLogServices(); err != nil {
		return err
	}

	// start dn services
	if err := c.startDNServices(); err != nil {
		return err
	}

	c.mu.running = true
	return nil
}

func (c *testCluster) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mu.running {
		return nil
	}

	// close all dn services first
	if err := c.closeDNServices(); err != nil {
		return err
	}

	// close all log services
	if err := c.closeLogServices(); err != nil {
		return err
	}

	c.mu.running = false
	return nil
}

func (c *testCluster) GetDNService(id string) (DNService, error) {
	c.dn.Lock()
	defer c.dn.Unlock()

	for i, cfg := range c.dn.cfgs {
		if cfg.UUID == id {
			return c.dn.svcs[i], nil
		}
	}

	return nil, wrappedError(ErrServiceNotExist, id)
}

func (c *testCluster) GetLogService(id string) (LogService, error) {
	c.log.Lock()
	defer c.log.Unlock()

	for _, svc := range c.log.svcs {
		if svc.ID() == id {
			return svc, nil
		}
	}

	return nil, wrappedError(ErrServiceNotExist, id)
}

func (c *testCluster) GetClusterState() (*logpb.CheckerState, error) {
	leader := c.WaitHAKeeperLeader(time.Second)
	return leader.GetClusterState()
}

func (c *testCluster) AssertHAKeeperState(svc LogService, expected logpb.HAKeeperState) {
	state, err := svc.GetClusterState()
	require.NoError(c.t, err)
	assert.Equal(c.t, expected, state.State)
}

func (c *testCluster) WaitHAKeeperLeader(timeout time.Duration) LogService {
	timeoutCh := time.After(timeout)
	for {
		select {
		case <-timeoutCh:
			assert.FailNow(c.t, "timeout when waiting for hakeeper leader")
		default:
			time.Sleep(time.Millisecond * 100)

			leader := c.getHAKeeperLeader()
			if leader != nil {
				return leader
			}
		}
	}
}

func (c *testCluster) WaitHAKeeperState(timeout time.Duration, expected logpb.HAKeeperState) {
	timeoutCh := time.After(timeout)
	for {
		select {
		case <-timeoutCh:
			msg := "timeout when waiting for hakeeper running"
			c.logger.Warn(msg)
			assert.FailNow(c.t, msg)
		default:
			time.Sleep(100 * time.Millisecond)

			leader := c.getHAKeeperLeader()
			if leader == nil {
				continue
			}

			state, err := leader.GetClusterState()
			require.NoError(c.t, err)
			if state.State == expected {
				return
			}
		}
	}
}

func (c *testCluster) ListDNServices() []string {
	ids := make([]string, 0, len(c.dn.svcs))
	for _, cfg := range c.dn.cfgs {
		ids = append(ids, cfg.UUID)
	}
	return ids
}

func (c *testCluster) ListLogServices() []string {
	ids := make([]string, 0, len(c.log.svcs))
	for _, svc := range c.log.svcs {
		ids = append(ids, svc.ID())
	}
	return ids
}

// ------------------------------------------------------
// The following are private utilities for `testCluster`.
// ------------------------------------------------------

// buildServiceAddress builds addresses for all services.
func (c *testCluster) buildServiceAddress() serviceAddress {
	return newServiceAddress(
		c.t,
		c.opt.initial.logServiceNum,
		c.opt.initial.dnServiceNum,
		c.opt.hostAddr,
	)
}

// buildFileServices builds all file services.
func (c *testCluster) buildFileServices() *fileServices {
	return newFileServices(c.t, c.opt.initial.dnServiceNum)
}

// buildDnConfigs builds configurations for all dn services.
func (c *testCluster) buildDnConfigs(address serviceAddress) ([]*dnservice.Config, []dnOptions) {
	batch := c.opt.initial.dnServiceNum

	cfgs := make([]*dnservice.Config, 0, batch)
	opts := make([]dnOptions, 0, batch)
	for i := 0; i < batch; i++ {
		cfg, opt := buildDnConfig(i, c.opt, address)
		cfgs = append(cfgs, cfg)
		opts = append(opts, opt)
	}
	return cfgs, opts
}

// buildLogConfigs builds configurations for all log services.
func (c *testCluster) buildLogConfigs(address serviceAddress) []logservice.Config {
	batch := c.opt.initial.logServiceNum

	cfgs := make([]logservice.Config, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := buildLogConfig(i, c.opt, address)
		cfgs = append(cfgs, cfg)
	}
	return cfgs
}

// initDNServices builds all dn services.
//
// Before initializing dn service, log service must be started already.
func (c *testCluster) initDNServices(fileservices *fileServices) []DNService {
	batch := c.opt.initial.dnServiceNum

	c.logger.Info("initialize dn services", zap.Int("batch", batch))

	svcs := make([]DNService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.dn.cfgs[i]
		opt := c.dn.opts[i]
		fsFactory := func(name string) (fileservice.FileService, error) {
			index := i
			switch strings.ToUpper(name) {
			case "LOCAL":
				return fileservices.getLocalFileService(index), nil
			case "S3":
				return fileservices.getS3FileService(), nil
			default:
				return nil, ErrInvalidFSName
			}
		}

		ds, err := newDNService(cfg, fsFactory, opt)
		require.NoError(c.t, err)

		c.logger.Info("dn service initialized", zap.Int("index", i), zap.Any("config", cfg))

		svcs = append(svcs, ds)
	}

	return svcs
}

// initLogServices builds all log services.
func (c *testCluster) initLogServices() []LogService {
	batch := c.opt.initial.logServiceNum

	c.logger.Info("initialize log services", zap.Int("batch", batch))

	svcs := make([]LogService, 0, batch)
	for i := 0; i < batch; i++ {
		cfg := c.log.cfgs[i]
		ls, err := newLogService(cfg)
		require.NoError(c.t, err)

		c.logger.Info("log service initialized", zap.Int("index", i), zap.Any("config", cfg))

		svcs = append(svcs, ls)
	}
	return svcs
}

// startDNServices initializes and starts all dn services.
func (c *testCluster) startDNServices() error {
	// initialize all dn services
	c.dn.svcs = c.initDNServices(c.fileservices)

	// start dn services
	for _, ds := range c.dn.svcs {
		if err := ds.Start(); err != nil {
			return err
		}
	}

	return nil
}

// startLogServices initializes and starts all log services.
func (c *testCluster) startLogServices() error {
	// initialize all log service
	c.log.svcs = c.initLogServices()

	// start log services
	for _, ls := range c.log.svcs {
		if err := ls.Start(); err != nil {
			return err
		}
	}

	// start hakeeper replicas
	if err := c.startHAKeeperReplica(); err != nil {
		return err
	}

	// initialize cluster information
	if err := c.setInitialClusterInfo(); err != nil {
		return err
	}

	return nil
}

// closeDNServices closes all dn services.
func (c *testCluster) closeDNServices() error {
	c.logger.Info("start to close dn services")

	for i, ds := range c.dn.svcs {
		c.logger.Info("close dn service", zap.Int("index", i))
		if err := ds.Close(); err != nil {
			return err
		}
		c.logger.Info("dn service closed", zap.Int("index", i))
	}

	return nil
}

// closeLogServices closes all log services.
func (c *testCluster) closeLogServices() error {
	c.logger.Info("start to close log services")

	for i, ls := range c.log.svcs {
		c.logger.Info("close log service", zap.Int("index", i))
		if err := ls.Close(); err != nil {
			return err
		}
		c.logger.Info("log service closed", zap.Int("index", i))
	}

	return nil
}

// getHAKeeperLeader gets log service which is hakeeper leader.
func (c *testCluster) getHAKeeperLeader() LogService {
	var leader LogService
	for _, svc := range c.selectHAkeeperServices() {
		if svc.IsLeaderHakeeper() {
			return leader
		}
	}
	return nil
}
