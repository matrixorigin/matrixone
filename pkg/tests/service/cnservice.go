// Copyright 2022 Matrix Origin
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
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/tests"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// CNService describes expected behavior for tn service.
type CNService interface {
	// Start sends heartbeat and start to handle command.
	Start() error
	// Close stops store
	Close() error
	// Status returns the status of service.
	Status() ServiceStatus

	// ID returns uuid of store
	ID() string
	// SQLAddress returns the sql listen address
	SQLAddress() string
	//GetTaskRunner returns the taskRunner.
	GetTaskRunner() taskservice.TaskRunner
	// GetTaskService returns the taskservice
	GetTaskService() (taskservice.TaskService, bool)
	// GetSQLExecutor returns sql executor
	GetSQLExecutor() executor.SQLExecutor
	// GetBootstrapService returns bootstrap service
	GetBootstrapService() bootstrap.Service
	//SetCancel sets CancelFunc to stop GetClusterDetailsFromHAKeeper
	SetCancel(context.CancelFunc)
}

// cnService wraps cnservice.Service.
//
// The main purpose of this structure is to maintain status.
type cnService struct {
	sync.Mutex
	status ServiceStatus
	svc    cnservice.Service
	cfg    *cnservice.Config

	cancel context.CancelFunc
}

func (c *cnService) Start() error {
	c.Lock()
	defer c.Unlock()

	if c.status == ServiceInitialized {
		err := c.svc.Start()
		if err != nil {
			return err
		}
		c.status = ServiceStarted
	}

	return nil
}

func (c *cnService) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.status == ServiceStarted {
		err := c.svc.Close()
		c.cancel()
		if err != nil {
			return err
		}
		c.status = ServiceClosed
	}

	return nil
}

func (c *cnService) Status() ServiceStatus {
	c.Lock()
	defer c.Unlock()

	return c.status
}

func (c *cnService) ID() string {
	c.Lock()
	defer c.Unlock()

	return c.cfg.UUID
}

func (c *cnService) SQLAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", c.cfg.Frontend.Port)
}

func (c *cnService) GetTaskRunner() taskservice.TaskRunner {
	return c.svc.GetTaskRunner()
}

func (c *cnService) GetTaskService() (taskservice.TaskService, bool) {
	return c.svc.GetTaskService()
}

func (c *cnService) GetSQLExecutor() executor.SQLExecutor {
	return c.svc.GetSQLExecutor()
}

func (c *cnService) GetBootstrapService() bootstrap.Service {
	return c.svc.GetBootstrapService()
}

func (c *cnService) SetCancel(cancel context.CancelFunc) {
	c.cancel = cancel
}

// cnOptions is options for a cn service.
type cnOptions []cnservice.Option

// newCNService initializes an instance of `CNService`.
func newCNService(
	cfg *cnservice.Config,
	ctx context.Context,
	fileService fileservice.FileService,
	options cnOptions,
) (CNService, error) {
	srv, err := cnservice.NewService(cfg, ctx, fileService, nil, options...)
	if err != nil {
		return nil, err
	}

	return &cnService{
		status: ServiceInitialized,
		svc:    srv,
		cfg:    cfg,
	}, nil
}

func buildCNConfig(index int, opt Options, address *serviceAddresses) *cnservice.Config {
	port, err := tests.GetAvailablePort("127.0.0.1")
	if err != nil {
		panic(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	uid, _ := uuid.NewV7()
	cfg := &cnservice.Config{
		UUID:           uid.String(),
		ListenAddress:  address.getCNListenAddress(index),
		ServiceAddress: address.getCNListenAddress(index),
		SQLAddress:     fmt.Sprintf("127.0.0.1:%d", p),
		Frontend: config.FrontendParameters{
			Port: int64(p),
		},
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	cfg.HAKeeper.HeatbeatInterval.Duration = opt.heartbeat.cn
	cfg.Engine.Type = opt.storage.cnEngine
	cfg.TaskRunner.Parallelism = 4
	cfg.LockService.ListenAddress = address.getCNLockListenAddress(index)
	cfg.LockService.ServiceAddress = cfg.LockService.ListenAddress
	cfg.LockService.KeepBindTimeout.Duration = time.Second * 30
	cfg.QueryServiceConfig.Address.ListenAddress = address.getCNQueryListenAddress(index)
	cfg.QueryServiceConfig.Address.ServiceAddress = cfg.QueryServiceConfig.Address.ListenAddress

	// We need the filled version of configuration.
	// It's necessary when building cnservice.Option.
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("fatal when building cnservice.Config: %s", err))
	}

	return cfg
}
