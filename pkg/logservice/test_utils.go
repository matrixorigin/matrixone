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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type allocatedPorts struct {
	sync.Mutex
	ports map[int]struct{}
}

const (
	testPortMin   = 21024
	testPortLimit = 65535
	testPortCount = testPortLimit - testPortMin
)

var errNoAvailableTestPort = moerr.NewInternalErrorNoCtx("no available test port")

var randomPorts = allocatedPorts{
	ports: map[int]struct{}{},
}

func (a *allocatedPorts) allocate(
	count int,
	occupied map[uint16]struct{},
	start int,
) ([]int, error) {
	if count <= 0 || count > testPortCount {
		return nil, errutil.Wrapf(errNoAvailableTestPort, "invalid requested count %d", count)
	}
	if start < 0 || start >= testPortCount {
		return nil, errutil.Wrapf(errNoAvailableTestPort, "invalid start offset %d", start)
	}

	a.Lock()
	defer a.Unlock()
	if a.ports == nil {
		a.ports = make(map[int]struct{})
	}

	available := make([]int, 0, count)
	for offset := 0; offset < testPortCount && len(available) < count; offset++ {
		port := testPortMin + (start+offset)%testPortCount
		if _, ok := a.ports[port]; ok {
			continue
		}
		if _, ok := occupied[uint16(port)]; ok {
			continue
		}
		available = append(available, port)
	}
	if len(available) != count {
		return nil, errutil.Wrapf(
			errNoAvailableTestPort,
			"requested %d in range [%d, %d)",
			count,
			testPortMin,
			testPortLimit,
		)
	}
	for _, port := range available {
		a.ports[port] = struct{}{}
	}
	return available, nil
}

func getAvailablePorts(count int) ([]int, error) {
	return randomPorts.allocate(count, listAllPorts(), rand.Intn(testPortCount))
}

func getAvailablePort() int {
	ports, err := getAvailablePorts(1)
	if err != nil {
		panic(err)
	}
	return ports[0]
}

var getClientConfig = func(readOnly bool, svcAddress ...string) ClientConfig {
	var addr string
	if len(svcAddress) > 0 {
		addr = svcAddress[0]
	}
	return ClientConfig{
		ReadOnly:         readOnly,
		LogShardID:       1,
		TNReplicaID:      2,
		ServiceAddresses: []string{addr},
		MaxMessageSize:   defaultMaxMessageSize,
	}
}

func getServiceTestConfig() Config {
	c := DefaultConfig()
	c.UUID = uuid.New().String()
	c.RTTMillisecond = 10
	c.RaftAddress = getTestRaftAddress()
	c.GossipPort = getTestGossipPort()
	c.GossipSeedAddresses = []string{
		getTestGossipAddress(c.GossipPort),
		getDummyGossipSeedAddress(),
	}
	c.DeploymentID = 1
	c.FS = vfs.NewStrictMem()
	c.LogServicePort = getTestServicePort()
	c.DisableWorkers = true
	c.UseTeeLogDB = false
	c.RPC.MaxMessageSize = toml.ByteSize(getTestServerMaxMsgSize())

	rt := runtime.ServiceRuntime("")
	runtime.SetupServiceBasedRuntime(c.UUID, rt)
	runtime.SetupServiceBasedRuntime("", rt)
	return c
}

func RunClientTest(
	t *testing.T,
	readOnly bool,
	cCfgFn func(bool, ...string) ClientConfig,
	fn func(*testing.T, *Service, ClientConfig, Client)) {

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			var cfg Config
			genCfg := func() Config {
				cfg = getServiceTestConfig()
				return cfg
			}
			defer func() {
				vfs.ReportLeakedFD(cfg.FS, t)
			}()
			service, err := NewServiceWithRetry(genCfg,
				newFS(),
				nil,
				WithBackendFilter(func(msg morpc.Message, backendAddr string) bool {
					return true
				}),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, service.Close())
			}()

			init := make(map[uint64]string)
			init[2] = service.ID()
			require.NoError(t, service.store.startReplica(1, 2, init, false))

			if cCfgFn == nil {
				cCfgFn = getClientConfig
			}
			scfg := cCfgFn(readOnly, cfg.LogServiceServiceAddr())

			ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*3, moerr.CauseRunClientTest)
			defer cancel()
			c, err := NewClient(ctx, sid, scfg)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()

			fn(t, service, scfg, c)
		},
	)
}

func getTestServicePort() int {
	return getAvailablePort()
}

func getTestGossipPort() int {
	return getAvailablePort()
}

func getTestServiceAddress(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func getTestGossipAddress(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func getDummyGossipSeedAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", getAvailablePort())
}

func getTestRaftAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", getAvailablePort())
}

func getTestServerMaxMsgSize() int {
	return 1000
}
