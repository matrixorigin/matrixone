// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestGossipFixtureStartupNativeConstructorPanic(t *testing.T) {
	testGossipFixtureNativeRollback(t, true)
}

func TestGossipFixtureStartupRealRPCBindCollision(t *testing.T) {
	testGossipFixtureNativeRollback(t, false)
}

func testGossipFixtureNativeRollback(t *testing.T, panicConstructor bool) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		defer leaktest.AfterTest(t)()
		var current, allConfigs, injected []Config
		started := make([]*Service, 0, 2*maxGossipFixtureStartupAttempts+1)
		var captured *Service
		fileSystems := make([]fileservice.FileService, 0, 2*maxGossipFixtureStartupAttempts+1)
		var occupied net.Listener
		defer func() {
			if occupied != nil {
				require.NoError(t, occupied.Close())
			}
			if captured != nil {
				require.NoError(t, captured.Close())
			}
			require.NoError(t, closeGossipFixture(rt, allConfigs, started))
			for _, fs := range fileSystems {
				fs.Close(t.Context())
			}
		}()
		token := make(chan struct{})
		taskDone := make(chan struct{})
		var taskErr, collisionErr error
		rollbacks, closed, buildsAtInjection, builds := 0, 0, 0, 0
		var configs []Config
		var services []*Service
		invoke := func() {
			var err error
			configs, services, err = startGossipFixture(t, t.Context(),
				func() ([]Config, error) {
					if len(injected) != 0 {
						require.Equal(t, 1, rollbacks, "rollback must precede allocation")
						for _, cfg := range injected {
							require.Nil(t, runtime.ServiceRuntime(cfg.UUID))
						}
					}
					builds++
					configs, err := makeGossipFixtureConfigs(t, rt, 2)
					current = configs
					allConfigs = append(allConfigs, configs...)
					return configs, err
				},
				func(cfg Config) (*Service, error) {
					inject := len(injected) == 0 && cfg.UUID == current[1].UUID
					if inject && !panicConstructor {
						listener, err := net.Listen("tcp4", cfg.LogServiceListenAddr())
						if err != nil {
							return nil, err
						}
						occupied = listener
						injected = append([]Config(nil), current...)
						buildsAtInjection = builds
					}
					fs := newFS()
					fileSystems = append(fileSystems, fs)
					options := []Option{WithRuntime(rt)}
					if inject && panicConstructor {
						options = append(options, func(s *Service) {
							captured = s
							injected = append([]Config(nil), current...)
							buildsAtInjection = builds
							taskErr = s.stopper.RunTask(func(ctx context.Context) { <-ctx.Done(); close(taskDone) })
							panic(token)
						})
					}
					service, err := NewService(cfg, fs, nil, options...)
					if inject && !panicConstructor {
						collisionErr = err
					}
					if service != nil {
						started = append(started, service)
					}
					return service, err
				},
				func(configs []Config, services []*Service) error {
					if len(injected) != 0 && configs[0].UUID == injected[0].UUID {
						rollbacks++
						closed += len(services)
					}
					return closeGossipFixture(rt, configs, services)
				})
			require.NoError(t, err)
		}
		if panicConstructor {
			require.PanicsWithValue(t, token, invoke)
			require.Nil(t, configs)
			require.Nil(t, services)
			require.NotNil(t, captured)
			require.NoError(t, taskErr)
			select {
			case <-taskDone:
			case <-time.After(5 * time.Second):
				t.Fatal("constructor did not join its stopper task")
			}
			require.Equal(t, buildsAtInjection, builds, "panic must not retry")
		} else {
			invoke()
			require.Len(t, services, 2)
			require.ErrorIs(t, collisionErr, syscall.EADDRINUSE)
			require.Greater(t, builds, buildsAtInjection)
			for i, cfg := range injected {
				require.NotEqual(t, cfg.UUID, configs[i].UUID)
				require.NotEqual(t, cfg.LogServiceListenAddr(), configs[i].LogServiceListenAddr())
				require.NotEqual(t, cfg.GossipListenAddr(), configs[i].GossipListenAddr())
				require.NotEqual(t, cfg.RaftListenAddr(), configs[i].RaftListenAddr())
			}
		}
		require.Len(t, injected, 2)
		require.Equal(t, 1, rollbacks)
		require.Equal(t, 1, closed)
		for _, cfg := range injected {
			require.Nil(t, runtime.ServiceRuntime(cfg.UUID))
		}
		// Reopening the exact configuration verifies listener and StrictMem lock cleanup.
		runtime.SetupServiceBasedRuntime(injected[0].UUID, rt)
		defer runtime.CompareAndDeleteServiceBasedRuntime(injected[0].UUID, rt)
		fs := newFS()
		fileSystems = append(fileSystems, fs)
		reopened, err := NewService(injected[0], fs, nil, WithRuntime(rt))
		require.NoError(t, err)
		started = append(started, reopened)
		require.NoError(t, reopened.Close())
		if occupied != nil {
			listener, err := net.Listen("tcp4", injected[1].LogServiceListenAddr())
			if listener != nil {
				require.NoError(t, listener.Close())
			}
			require.ErrorIs(t, err, syscall.EADDRINUSE, "fixture closed an independent listener")
		}
	})
}
