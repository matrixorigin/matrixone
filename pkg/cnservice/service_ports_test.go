// Copyright 2021 - 2023 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/address"
	"github.com/stretchr/testify/assert"
)

func TestService_NewPortStrategy(t *testing.T) {
	s := &service{cfg: &Config{}}
	assert.False(t, s.newPortStrategy())
	s.cfg.PortBase = 1000
	assert.True(t, s.newPortStrategy())
}

func TestService_RegisterServices(t *testing.T) {
	listenHost := "0.0.0.0"
	serviceHost := "127.0.0.1"
	port1 := 1000
	port2 := 2000

	s := &service{
		cfg: &Config{
			ListenAddress:  fmt.Sprintf("%s:%d", listenHost, port1),
			ServiceAddress: fmt.Sprintf("%s:%d", serviceHost, port1),
			LockService: lockservice.Config{
				ListenAddress:  fmt.Sprintf("%s:%d", listenHost, port1+1),
				ServiceAddress: fmt.Sprintf("%s:%d", serviceHost, port1+1),
			},
			QueryServiceConfig: queryservice.Config{
				Address: address.Address{
					ListenAddress:  fmt.Sprintf("%s:%d", listenHost, port1+3),
					ServiceAddress: fmt.Sprintf("%s:%d", serviceHost, port1+3),
				},
			},
		},
		addressMgr: address.NewAddressManager(serviceHost, port2),
	}

	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port1), s.pipelineServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port1), s.pipelineServiceListenAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port1+1), s.lockServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port1+1), s.lockServiceListenAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port1+3), s.queryServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port1+3), s.queryServiceListenAddr())

	s.cfg.PortBase = port2
	s.registerServices()
	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port2), s.pipelineServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port2), s.pipelineServiceListenAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port2+1), s.lockServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port2+1), s.lockServiceListenAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", serviceHost, port2+2), s.queryServiceServiceAddr())
	assert.Equal(t, fmt.Sprintf("%s:%d", listenHost, port2+2), s.queryServiceListenAddr())
}

func TestDefaultCnConfig(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaultValue()
}
