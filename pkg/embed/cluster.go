// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

type state int

const (
	stopped = state(0)
	started = state(1)
)

type cluster struct {
	sync.RWMutex

	state    state
	files    []string
	services []*operator

	options struct {
		dataPath  string
		cn        int
		withProxy bool
		preStart  func(ServiceOperator)
	}

	gen struct {
		basePort         int
		baseFrontendPort int
	}
}

func NewCluster(
	opts ...Option,
) (Cluster, error) {
	c := &cluster{
		state: stopped,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.adjust()

	if err := c.createServiceOperators(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *cluster) Start() error {
	c.Lock()
	defer c.Unlock()

	if c.state == started {
		return moerr.NewInvalidStateNoCtx("embed mo cluster already started")
	}

	var wg sync.WaitGroup
	errC := make(chan error, 1)
	defer close(errC)
	for _, s := range c.services {
		if s.serviceType != metadata.ServiceType_CN {
			if err := s.Start(); err != nil {
				return err
			}
			continue
		}

		wg.Add(1)
		go func(s *operator) {
			defer wg.Done()
			if err := s.Start(); err != nil {
				select {
				case errC <- err:
					return
				default:
				}
				return
			}
		}(s)
	}
	wg.Wait()

	select {
	case err := <-errC:
		return err
	default:
	}

	c.state = started
	return nil
}

func (c *cluster) Close() error {
	c.Lock()
	defer c.Unlock()

	for i := len(c.services) - 1; i >= 0; i-- {
		s := c.services[i]
		if err := s.Close(); err != nil {
			return err
		}
	}

	c.state = stopped
	return nil
}

func (c *cluster) GetService(
	sid string,
) (ServiceOperator, error) {
	var v ServiceOperator
	c.ForeachServices(
		func(s ServiceOperator) bool {
			if s.ServiceID() == sid {
				v = s
				return false
			}
			return true
		},
	)

	if v == nil {
		return nil, moerr.NewInvalidStateNoCtx("service not found")
	}

	return v, nil
}

func (c *cluster) ForeachServices(
	fn func(ServiceOperator) bool,
) {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.services {
		if !fn(s) {
			return
		}
	}
}

func (c *cluster) GetCNService(
	index int,
) (ServiceOperator, error) {
	var v ServiceOperator
	var i int
	c.ForeachServices(
		func(s ServiceOperator) bool {
			if s.ServiceType() == metadata.ServiceType_CN {
				if i == index {
					v = s
					return false
				}
				i++
				return true
			}
			return true
		},
	)

	if v == nil {
		return nil, moerr.NewInvalidStateNoCtx("service not found")
	}

	return v, nil
}

func (c *cluster) adjust() {
	c.gen.baseFrontendPort = 6001
	c.gen.basePort = 18000

	if c.options.cn == 0 {
		c.options.cn = 1
	}
	if c.options.dataPath == "" {
		c.options.dataPath = filepath.Join(
			os.TempDir(),
			fmt.Sprintf("%d", time.Now().Nanosecond()),
		)
		if err := os.MkdirAll(c.options.dataPath, 0755); err != nil {
			panic(err)
		}
	}

	if c.options.withProxy ||
		c.options.cn > 1 {
		c.gen.baseFrontendPort = 16001
	}
}

func (c *cluster) createServiceOperators() error {
	if err := c.initConfigs(); err != nil {
		return err
	}

	for i, f := range c.files {
		s, err := newService(f, i)
		if err != nil {
			return err
		}
		if c.options.preStart != nil {
			c.options.preStart(s)
		}
		c.services = append(c.services, s)
	}
	return nil
}

func (c *cluster) initConfigs() error {
	if len(c.files) > 0 {
		return nil
	}

	if err := c.initLogServiceConfig(); err != nil {
		return err
	}

	if err := c.initTNServiceConfig(); err != nil {
		return err
	}

	if c.options.withProxy {
		if err := c.initProxyServiceConfig(); err != nil {
			return err
		}

	}

	if err := c.initCNServiceConfig(); err != nil {
		return err
	}

	return nil
}

func (c *cluster) initLogServiceConfig() error {
	file := filepath.Join(c.options.dataPath, "log.toml")
	c.files = append(c.files, file)
	return genConfig(
		file,
		fmt.Sprintf(
			logConfig,
			c.options.dataPath,
		))
}

func (c *cluster) initTNServiceConfig() error {
	file := filepath.Join(c.options.dataPath, "tn.toml")
	c.files = append(c.files, file)
	return genConfig(
		file,
		fmt.Sprintf(
			tnConfig,
			c.options.dataPath,
			c.options.dataPath,
			c.options.dataPath,
			c.getNextBasePort(),
		))
}

func (c *cluster) initCNServiceConfig() error {
	for i := 0; i < c.options.cn; i++ {
		file := filepath.Join(c.options.dataPath, fmt.Sprintf("cn-%d.toml", i))
		c.files = append(c.files, file)
		err := genConfig(
			file,
			fmt.Sprintf(
				cnConfig,
				c.options.dataPath,
				c.options.dataPath,
				c.options.dataPath,
				i,
				c.getNextBasePort(),
				i,
				c.getNextFrontPort(),
				c.options.dataPath,
				i,
			))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) initProxyServiceConfig() error {
	return genConfig(
		filepath.Join(c.options.dataPath, "proxy.toml"),
		fmt.Sprintf(
			proxyConfig,
			c.options.dataPath,
			c.options.dataPath,
			c.options.dataPath,
		))
}

func (c *cluster) getNextBasePort() int {
	v := c.gen.basePort
	c.gen.basePort += 100
	return v
}

func (c *cluster) getNextFrontPort() int {
	v := c.gen.baseFrontendPort
	c.gen.baseFrontendPort++
	return v
}

func genConfig(
	file string,
	cfg string,
) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(cfg); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}
