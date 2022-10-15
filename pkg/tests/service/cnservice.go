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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

// CNService describes expected behavior for dn service.
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
}

// cnService wraps cnservice.Service.
//
// The main purpose of this structure is to maintain status.
type cnService struct {
	sync.Mutex
	status ServiceStatus
	svc    cnservice.Service
	cfg    *cnservice.Config
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

// cnOptions is options for a cn service.
type cnOptions []cnservice.Options

// newCNService initializes an instance of `CNService`.
func newCNService(
	cfg *cnservice.Config,
	ctx context.Context,
	fileService fileservice.FileService,
	taskStorage taskservice.TaskStorage,
	options cnOptions,
) (CNService, error) {
	srv, err := cnservice.NewService(cfg, ctx, fileService, taskservice.NewTaskService(taskStorage, nil), options...)
	if err != nil {
		return nil, err
	}

	return &cnService{
		status: ServiceInitialized,
		svc:    srv,
		cfg:    cfg,
	}, nil
}

func buildCnConfig(index int, opt Options, address serviceAddresses) *cnservice.Config {
	port, err := getAvailablePort("127.0.0.1")
	if err != nil {
		panic(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(err)
	}
	cfg := &cnservice.Config{
		UUID:          uuid.New().String(),
		ListenAddress: address.getCnListenAddress(index),
		Frontend: config.FrontendParameters{
			Port: int64(p),
		},
	}

	cfg.HAKeeper.ClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	cfg.HAKeeper.HeatbeatDuration.Duration = opt.dn.heartbeatInterval

	cfg.TaskRunner.FetchInterval.Duration = opt.task.FetchInterval

	cfg.Engine.Type = cnservice.EngineMemory

	// We need the filled version of configuration.
	// It's necessary when building cnservice.Option.
	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("fatal when building cnservice.Config: %s", err))
	}

	return cfg
}

func buildCnOptions() cnOptions {
	return nil
}
