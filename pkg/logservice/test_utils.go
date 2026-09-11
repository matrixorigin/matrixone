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
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type allocatedPorts struct {
	sync.Mutex
	ports map[int]struct{}
}

// testPortAllocationError keeps the probe cause available to callers while
// complying with the repository's error-construction policy.
type testPortAllocationError struct {
	message string
	cause   error
}

func (e *testPortAllocationError) Error() string {
	return e.message
}

func (e *testPortAllocationError) Unwrap() error {
	return e.cause
}

var randomPorts = allocatedPorts{
	ports: map[int]struct{}{},
}

func getAvailablePort() int {
	port, err := randomPorts.allocate(probeTestPort)
	if err != nil {
		panic(err) // Preserve the existing fixture API, but fail promptly with the cause.
	}
	return port
}

const maxPortAllocationAttempts = 128

func (a *allocatedPorts) allocate(probe func(int) error) (int, error) {
	a.Lock()
	defer a.Unlock()
	for range maxPortAllocationAttempts {
		port := rand.Intn(65535-21024) + 21024
		if _, exists := a.ports[port]; exists {
			continue
		}
		if err := probe(port); err != nil {
			if errors.Is(err, syscall.EADDRINUSE) {
				continue
			}
			return 0, &testPortAllocationError{
				message: fmt.Sprintf("probe test port %d: %v", port, err),
				cause:   err,
			}
		}
		a.ports[port] = struct{}{}
		return port, nil
	}
	return 0, &testPortAllocationError{
		message: fmt.Sprintf("no available test port after %d attempts", maxPortAllocationAttempts),
	}
}

func probeTestPort(port int) error {
	// Match wildcard listeners and reject existing loopback listeners too:
	// macOS can allow a wildcard TCP bind beside an existing specific bind.
	// Each probe closes before the next; no socket is reserved until use.
	for _, host := range []string{DefaultListenHost, DefaultServiceHost} {
		if err := probeTestPortAddress(fmt.Sprintf("%s:%d", host, port)); err != nil {
			return err
		}
	}
	return nil
}

func probeTestPortAddress(addr string) error {
	return probeTestPortAddressWithListeners(addr, net.Listen, net.ListenPacket)
}

func probeTestPortAddressWithListeners(
	addr string,
	listen func(string, string) (net.Listener, error),
	listenPacket func(string, string) (net.PacketConn, error),
) error {
	tcp, err := listen("tcp4", addr)
	if err != nil {
		return err
	}
	defer tcp.Close()
	udp, err := listenPacket("udp4", addr)
	if err != nil {
		return err
	}
	return udp.Close()
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
			defer vfs.ReportLeakedFD(cfg.FS, t)
			service, err := NewServiceWithRetry(genCfg,
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
			scfg := cCfgFn(readOnly, cfg.LogServiceServiceAddr())

			ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*3, moerr.CauseRunClientTest)
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
