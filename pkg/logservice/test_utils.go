// Copyright 2021 - 2024 Matrix Origin
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

package logservice

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
)

func getRandomPort() int {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return rand.Intn(65535-1024) + 1024
}

var (
	testServicePort        = getRandomPort()
	testGossipPort         = getRandomPort()
	testServiceAddress     = fmt.Sprintf("127.0.0.1:%d", testServicePort)
	testGossipAddress      = fmt.Sprintf("127.0.0.1:%d", testGossipPort)
	dummyGossipSeedAddress = fmt.Sprintf("127.0.0.1:%d", getRandomPort())
	testServerMaxMsgSize   = 1000
	testRaftAddress        = fmt.Sprintf("0.0.0.0:%d", getRandomPort())
)

var getClientConfig = func(readOnly bool) ClientConfig {
	return ClientConfig{
		ReadOnly:         readOnly,
		LogShardID:       1,
		TNReplicaID:      2,
		ServiceAddresses: []string{testServiceAddress},
		MaxMessageSize:   defaultMaxMessageSize,
	}
}

func getServiceTestConfig() Config {
	c := DefaultConfig()
	c.UUID = uuid.New().String()
	c.RTTMillisecond = 10
	c.RaftAddress = testRaftAddress
	c.GossipPort = testGossipPort
	c.GossipSeedAddresses = []string{testGossipAddress, dummyGossipSeedAddress}
	c.DeploymentID = 1
	c.FS = vfs.NewStrictMem()
	c.LogServicePort = testServicePort
	c.DisableWorkers = true
	c.UseTeeLogDB = true
	c.RPC.MaxMessageSize = toml.ByteSize(testServerMaxMsgSize)

	rt := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime(c.UUID, rt)
	runtime.SetupServiceBasedRuntime("", rt)
	return c
}

func RunClientTest(
	t *testing.T,
	readOnly bool,
	cCfgFn func(bool) ClientConfig,
	fn func(*testing.T, *Service, ClientConfig, Client)) {

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			cfg := getServiceTestConfig()
			defer vfs.ReportLeakedFD(cfg.FS, t)
			service, err := NewService(cfg,
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, service.Close())
			}()

			init := make(map[uint64]string)
			init[2] = service.ID()
			assert.NoError(t, service.store.startReplica(1, 2, init, false))

			if cCfgFn == nil {
				cCfgFn = getClientConfig
			}
			scfg := cCfgFn(readOnly)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			c, err := NewClient(ctx, sid, scfg)
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()

			fn(t, service, scfg, c)
		},
	)
}
