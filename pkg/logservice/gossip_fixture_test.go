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

package logservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	stdRuntime "runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

const maxGossipFixtureStartupAttempts = 3

// Probes do not reserve sockets. Only a completely bound cohort can be
// published to the convergence test. Retry endpoint establishment, never the
// test's functional assertions, and finish rollback before choosing new seeds.
func startGossipFixture(
	t *testing.T,
	ctx context.Context,
	build func() ([]Config, error),
	start func(Config) (*Service, error),
	cleanup func([]Config, []*Service) error,
) ([]Config, []*Service, error) {
	t.Helper()
	var lastErr error
	for attempt := 1; attempt <= maxGossipFixtureStartupAttempts; attempt++ {
		if err := context.Cause(ctx); err != nil {
			return nil, nil, err
		}
		configs, buildErr := build()
		if buildErr != nil {
			cleanupErr := cleanup(configs, nil)
			if cleanupErr != nil {
				cleanupErr = fmt.Errorf("gossip fixture rollback: %w", cleanupErr)
			}
			return nil, nil, errors.Join(
				fmt.Errorf("gossip fixture attempt %d configuration: %w", attempt, buildErr),
				cleanupErr, context.Cause(ctx))
		}
		services := make([]*Service, 0, len(configs))
		ownsCohort := true
		defer func() {
			if ownsCohort {
				if err := cleanup(configs, services); err != nil {
					t.Logf("gossip fixture deferred rollback attempt %d failed: %v", attempt, err)
				}
			}
		}()
		var startupErr error
		if len(configs) == 0 {
			startupErr = errors.New("gossip fixture requires at least one node")
		}
		for node, cfg := range configs {
			if err := context.Cause(ctx); err != nil {
				startupErr = err
				break
			}
			service, err := start(cfg)
			if service != nil {
				services = append(services, service)
			}
			if err == nil && service == nil {
				err = errors.New("gossip fixture constructor returned nil service without error")
			}
			if err != nil {
				startupErr = fmt.Errorf("gossip fixture attempt %d node %d (gossip=%s raft=%s service=%s): %w",
					attempt, node, cfg.GossipListenAddr(), cfg.RaftListenAddr(), cfg.LogServiceListenAddr(), err)
				break
			}
		}
		if cause := context.Cause(ctx); cause != nil {
			startupErr = errors.Join(startupErr, cause)
		}
		if startupErr == nil {
			ownsCohort = false
			return configs, services, nil // Transfer ownership of the entire cohort.
		}
		ownsCohort = false
		if err := cleanup(configs, services); err != nil {
			return nil, nil, errors.Join(startupErr, context.Cause(ctx), fmt.Errorf("gossip fixture rollback: %w", err))
		}
		if cause := context.Cause(ctx); cause != nil {
			return nil, nil, errors.Join(startupErr, cause)
		}
		if !isGossipFixtureBindCollision(startupErr) {
			return nil, nil, startupErr
		}
		lastErr = startupErr
		t.Logf("gossip fixture bind collision, rolled back attempt %d/%d: %v",
			attempt, maxGossipFixtureStartupAttempts, startupErr)
	}
	return nil, nil, fmt.Errorf("gossip fixture exhausted %d startup attempts: %w", maxGossipFixtureStartupAttempts, lastErr)
}

func isGossipFixtureBindCollision(err error) bool {
	// Memberlist can format a net.OpError without preserving its errno chain.
	// Keep that fallback specific to the socket bind error, not vague messages
	// saying that something unrelated is already in use.
	return errors.Is(err, syscall.EADDRINUSE) || strings.Contains(err.Error(), "bind: address already in use")
}

func makeGossipFixtureConfigs(t *testing.T, rt runtime.Runtime, nodeCount int) ([]Config, error) {
	t.Helper()
	first, limit, err := testPortRange()
	if err != nil {
		return nil, err
	}
	return makeGossipFixtureConfigsWithAllocator(t, rt, nodeCount, func() (int, error) {
		return randomPorts.allocate(first, limit, probeTestPort)
	})
}

func makeGossipFixtureConfigsWithAllocator(t *testing.T, rt runtime.Runtime, nodeCount int, allocate func() (int, error)) ([]Config, error) {
	t.Helper()
	gossipPorts := make([]int, nodeCount)
	for i := range gossipPorts {
		port, err := allocate()
		if err != nil {
			return nil, fmt.Errorf("gossip fixture node %d gossip port: %w", i, err)
		}
		gossipPorts[i] = port
	}
	seedAddresses := make([]string, min(nodeCount, 10))
	for i := range seedAddresses {
		seedAddresses[i] = getTestGossipAddress(gossipPorts[i])
	}
	configs := make([]Config, 0, nodeCount)
	for i := range nodeCount {
		servicePort, err := allocate()
		if err != nil {
			return configs, fmt.Errorf("gossip fixture node %d service port: %w", i, err)
		}
		raftPort, err := allocate()
		if err != nil {
			return configs, fmt.Errorf("gossip fixture node %d raft port: %w", i, err)
		}
		cfg := DefaultConfig()
		cfg.FS = vfs.NewStrictMem()
		fs := cfg.FS
		t.Cleanup(func() { vfs.ReportLeakedFD(fs, t) })
		cfg.UUID = uuid.New().String()
		cfg.DeploymentID = 1
		cfg.RTTMillisecond = 200
		cfg.DataDir = fmt.Sprintf("data-%d", i)
		cfg.LogServicePort = servicePort
		cfg.RaftPort = raftPort
		cfg.GossipPort = gossipPorts[i]
		cfg.GossipSeedAddresses = append([]string(nil), seedAddresses...)
		cfg.DisableWorkers = true
		cfg.LogDBBufferSize = 1024 * 16
		cfg.GossipProbeInterval.Duration = 350 * time.Millisecond
		configs = append(configs, cfg)
		runtime.SetupServiceBasedRuntime(cfg.UUID, rt)
	}
	return configs, nil
}

func newGossipFixtureService(cfg Config) (*Service, error) {
	return NewService(cfg, newFS(), nil,
		WithBackendFilter(func(msg morpc.Message, backendAddr string) bool { return true }))
}

func closeGossipFixture(rt runtime.Runtime, configs []Config, services []*Service) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var closeErr error
	for _, service := range services {
		if service == nil {
			continue
		}
		wg.Add(1)
		go func(s *Service) {
			defer wg.Done()
			if err := s.Close(); err != nil {
				mu.Lock()
				closeErr = errors.Join(closeErr, err)
				mu.Unlock()
			}
		}(service)
	}
	wg.Wait()
	for _, cfg := range configs {
		runtime.CompareAndDeleteServiceBasedRuntime(cfg.UUID, rt)
	}
	return closeErr
}

func TestGossipFixtureStartupConstructorPanic(t *testing.T) {
	for _, tc := range []struct {
		name           string
		priorNodes     int
		cleanupFailure bool
		replaceRuntime bool
	}{
		{name: "first node"},
		{name: "later node", priorNodes: 1},
		{name: "multiple prior nodes", priorNodes: 2},
		{name: "cleanup error preserves panic", priorNodes: 1, cleanupFailure: true},
		{name: "independent runtime survives", priorNodes: 1, replaceRuntime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := runtime.DefaultRuntime()
			replacement := runtime.DefaultRuntime()
			configs := make([]Config, tc.priorNodes+1)
			for i := range configs {
				configs[i].UUID = uuid.NewString()
				runtime.SetupServiceBasedRuntime(configs[i].UUID, rt)
				t.Cleanup(func() {
					runtime.CompareAndDeleteServiceBasedRuntime(configs[i].UUID, rt)
					runtime.CompareAndDeleteServiceBasedRuntime(configs[i].UUID, replacement)
				})
			}
			panicValue := make(chan struct{}) // Equality proves panic identity, not just its text.
			cleanupErr := errors.New("injected panic rollback failure")
			builds, starts, rollbacks := 0, 0, 0
			var started []*Service
			closed := make(map[*Service]int)
			transferred := false
			require.PanicsWithValue(t, panicValue, func() {
				_, _, _ = startGossipFixture(t, t.Context(),
					func() ([]Config, error) { builds++; return configs, nil },
					func(Config) (*Service, error) {
						starts++
						if starts == tc.priorNodes+1 {
							if tc.replaceRuntime {
								runtime.SetupServiceBasedRuntime(configs[0].UUID, replacement)
							}
							panic(panicValue)
						}
						service := &Service{}
						started = append(started, service)
						return service, nil
					},
					func(owned []Config, services []*Service) error {
						rollbacks++
						if len(owned) != len(configs) || len(services) != tc.priorNodes {
							t.Errorf("wrong panic ownership: configs=%d services=%d", len(owned), len(services))
						}
						for _, service := range services {
							closed[service]++
						}
						for _, cfg := range owned {
							runtime.CompareAndDeleteServiceBasedRuntime(cfg.UUID, rt)
						}
						if tc.cleanupFailure {
							return cleanupErr
						}
						return nil
					})
				transferred = true
			})
			require.False(t, transferred)
			require.Equal(t, 1, builds)
			require.Equal(t, tc.priorNodes+1, starts)
			require.Equal(t, 1, rollbacks)
			for _, service := range started {
				require.Equal(t, 1, closed[service])
			}
			for i, cfg := range configs {
				if tc.replaceRuntime && i == 0 {
					require.Same(t, replacement, runtime.ServiceRuntime(cfg.UUID))
				} else {
					require.Nil(t, runtime.ServiceRuntime(cfg.UUID))
				}
			}
		})
	}
}

func TestGossipFixtureStartupGoexit(t *testing.T) {
	builds, starts, rollbacks, closed := 0, 0, 0, 0
	returned := false
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = startGossipFixture(t, t.Context(),
			func() ([]Config, error) { builds++; return []Config{{}, {}}, nil },
			func(Config) (*Service, error) {
				starts++
				if starts == 2 {
					stdRuntime.Goexit()
				}
				return &Service{}, nil
			},
			func(configs []Config, services []*Service) error {
				rollbacks++
				closed += len(services)
				if len(configs) != 2 {
					t.Errorf("lost configured cohort during Goexit")
				}
				return nil
			})
		returned = true
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Goexit did not finish deferred rollback")
	}
	require.Equal(t, 1, builds)
	require.Equal(t, 2, starts)
	require.Equal(t, 1, rollbacks)
	require.Equal(t, 1, closed)
	require.False(t, returned)
}

func TestGossipFixtureStartupRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		failedNodes []int
	}{
		{name: "late node collision", failedNodes: []int{1}},
		{name: "early collision followed by late collision", failedNodes: []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts, cleanupCalls := 0, 0
			var allStarted []*Service
			closed := make(map[*Service]int)
			build := func() ([]Config, error) {
				attempts++
				for _, service := range allStarted {
					if closed[service] != 1 {
						t.Fatal("next cohort started before partial cohort was closed")
					}
				}
				ports := []int{22000 + attempts*10, 22001 + attempts*10}
				seeds := []string{getTestGossipAddress(ports[0]), getTestGossipAddress(ports[1])}
				return []Config{
					{UUID: fmt.Sprintf("a-%d", attempts), GossipPort: ports[0], GossipSeedAddresses: append([]string(nil), seeds...)},
					{UUID: fmt.Sprintf("b-%d", attempts), GossipPort: ports[1], GossipSeedAddresses: append([]string(nil), seeds...)},
				}, nil
			}
			start := func(cfg Config) (*Service, error) {
				node := 0
				if strings.HasPrefix(cfg.UUID, "b-") {
					node = 1
				}
				if attempts <= len(tc.failedNodes) && node == tc.failedNodes[attempts-1] {
					return nil, &net.OpError{Op: "listen", Net: "tcp", Err: syscall.EADDRINUSE}
				}
				service := &Service{}
				allStarted = append(allStarted, service)
				return service, nil
			}
			cleanup := func(_ []Config, services []*Service) error {
				cleanupCalls++
				for _, service := range services {
					closed[service]++
				}
				return nil
			}
			configs, services, err := startGossipFixture(t, t.Context(), build, start, cleanup)
			if err != nil {
				t.Fatal(err)
			}
			if attempts != len(tc.failedNodes)+1 || cleanupCalls != len(tc.failedNodes) || len(services) != 2 || len(configs) != 2 {
				t.Fatalf("wrong recovered cohort: attempts=%d cleanup=%d nodes=%d", attempts, cleanupCalls, len(services))
			}
			for _, cfg := range configs {
				if !strings.HasSuffix(cfg.UUID, fmt.Sprintf("-%d", attempts)) || len(cfg.GossipSeedAddresses) != 2 || cfg.GossipSeedAddresses[0] != getTestGossipAddress(configs[0].GossipPort) || cfg.GossipSeedAddresses[1] != getTestGossipAddress(configs[1].GossipPort) {
					t.Fatalf("stale cohort configuration: %+v", cfg)
				}
			}
			for _, service := range services {
				if closed[service] != 0 {
					t.Fatal("successful cohort was closed before ownership transfer")
				}
			}
			if err := cleanup(configs, services); err != nil {
				t.Fatal(err)
			}
			for _, service := range allStarted {
				if closed[service] != 1 {
					t.Fatal("service did not have exactly one cleanup owner")
				}
			}
		})
	}
}

func TestGossipFixtureStartupErrors(t *testing.T) {
	bindErr := &net.OpError{Op: "listen", Net: "tcp", Err: syscall.EADDRINUSE}
	textualBind := errors.New("Could not set up network transport: listen tcp 0.0.0.0:22000: bind: address already in use")
	nonBind := errors.New("injected metadata failure")
	vague := errors.New("metadata address already in use")
	cleanupErr := errors.New("injected close failure")
	for _, tc := range []struct {
		name                       string
		constructorErr, cleanupErr error
		attempts                   int
	}{
		{"wrapped bind exhausted", fmt.Errorf("wrapped: %w", bindErr), nil, maxGossipFixtureStartupAttempts},
		{"formatted bind exhausted", textualBind, nil, maxGossipFixtureStartupAttempts},
		{"non bind", nonBind, nil, 1},
		{"vague occupancy", vague, nil, 1},
		{"rollback failure", bindErr, cleanupErr, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attempts, starts, closes := 0, 0, 0
			configs, services, err := startGossipFixture(t, t.Context(),
				func() ([]Config, error) { attempts++; return []Config{{UUID: "failed", GossipPort: 22000}}, nil },
				func(Config) (*Service, error) { starts++; return &Service{}, tc.constructorErr },
				func(_ []Config, services []*Service) error {
					if len(services) != 1 {
						t.Fatal("non-nil error result was not owned by rollback")
					}
					closes += len(services)
					return tc.cleanupErr
				})
			if configs != nil || services != nil || !errors.Is(err, tc.constructorErr) || attempts != tc.attempts || starts != attempts || closes != starts {
				t.Fatalf("unexpected failure ownership/bound: configs=%v services=%v attempts=%d starts=%d closes=%d err=%v", configs, services, attempts, starts, closes, err)
			}
			if tc.cleanupErr != nil && !errors.Is(err, tc.cleanupErr) {
				t.Fatal("lost cleanup error")
			}
			if attempts == maxGossipFixtureStartupAttempts && !strings.Contains(err.Error(), "exhausted 3 startup attempts") {
				t.Fatal("missing exhaustion diagnosis")
			}
		})
	}
	t.Run("nil successful constructor", func(t *testing.T) {
		rollbacks := 0
		_, _, err := startGossipFixture(t, t.Context(), func() ([]Config, error) { return []Config{{}}, nil },
			func(Config) (*Service, error) { return nil, nil },
			func(_ []Config, services []*Service) error {
				rollbacks++
				if len(services) != 0 {
					t.Fatal("unexpected owned service")
				}
				return nil
			})
		if err == nil || rollbacks != 1 {
			t.Fatal("invalid constructor result was accepted")
		}
	})
	t.Run("empty cohort", func(t *testing.T) {
		rollbacks := 0
		_, _, err := startGossipFixture(t, t.Context(), func() ([]Config, error) { return nil, nil },
			func(Config) (*Service, error) { t.Fatal("empty cohort started a node"); return nil, nil },
			func([]Config, []*Service) error { rollbacks++; return nil })
		if err == nil || rollbacks != 1 {
			t.Fatal("empty cohort was accepted")
		}
	})
}

func TestGossipFixtureStartupCancellation(t *testing.T) {
	cause := errors.New("injected cancellation")
	cleanupErr := errors.New("injected rollback failure")
	for _, name := range []string{"before", "after build", "partial cohort", "before transfer", "bind and cancellation", "during rollback", "rollback failure and cancellation"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			builds, starts, closes, rollbacks := 0, 0, 0, 0
			if name == "before" {
				cancel(cause)
			}
			_, _, err := startGossipFixture(t, ctx,
				func() ([]Config, error) {
					builds++
					if name == "after build" {
						cancel(cause)
					}
					configs := []Config{{UUID: "first"}, {UUID: "second"}}
					if name == "before transfer" || name == "bind and cancellation" {
						configs = configs[:1]
					}
					return configs, nil
				},
				func(Config) (*Service, error) {
					starts++
					if name == "during rollback" || name == "rollback failure and cancellation" {
						return &Service{}, syscall.EADDRINUSE
					}
					cancel(cause)
					if name == "bind and cancellation" {
						return &Service{}, syscall.EADDRINUSE
					}
					return &Service{}, nil
				},
				func(_ []Config, services []*Service) error {
					rollbacks++
					closes += len(services)
					if name == "during rollback" || name == "rollback failure and cancellation" {
						cancel(cause)
					}
					if name == "rollback failure and cancellation" {
						return cleanupErr
					}
					return nil
				})
			if !errors.Is(err, cause) || builds > 1 || starts > 1 || closes != starts {
				t.Fatalf("cancellation leaked or retried: builds=%d starts=%d closes=%d err=%v", builds, starts, closes, err)
			}
			if name == "rollback failure and cancellation" && !errors.Is(err, cleanupErr) {
				t.Fatal("lost rollback error during cancellation")
			}
			if name == "before" && (builds != 0 || rollbacks != 0) {
				t.Fatal("cancelled fixture allocated a cohort")
			}
			if name != "before" && rollbacks != 1 {
				t.Fatal("cancelled cohort was not rolled back")
			}
		})
	}
}

// Retry/error branches are covered by the injected startup tests above. One
// real cohort proves memberlist error classification, rollback, and socket reuse.
func TestGossipFixtureStartupRealBindCollision(t *testing.T) {
	runtime.RunTest("", func(rt runtime.Runtime) {
		defer leaktest.AfterTest(t)()
		attempts := 0
		injectedAttempt := 0
		var currentConfigs, injectedConfigs []Config
		var failedConfigs [][]Config
		var occupied net.Listener
		var collisionErr error
		reopenedNode := false
		t.Cleanup(func() {
			if occupied != nil {
				require.NoError(t, occupied.Close())
			}
		})
		configs, services, err := startGossipFixture(t, t.Context(),
			func() ([]Config, error) {
				attempts++
				configs, err := makeGossipFixtureConfigs(t, rt, 2)
				currentConfigs = configs
				return configs, err
			},
			func(cfg Config) (*Service, error) {
				// Incidental collisions may prevent an earlier cohort from reaching
				// this node. Inject once in the first generation that gets here.
				if injectedAttempt == 0 && cfg.UUID == currentConfigs[1].UUID {
					var err error
					occupied, err = net.Listen("tcp4", cfg.GossipListenAddr())
					if err != nil {
						return nil, fmt.Errorf("collision injection failed: %w", err)
					}
					injectedAttempt = attempts
					injectedConfigs = currentConfigs
					service, err := newGossipFixtureService(cfg)
					collisionErr = err
					return service, err
				}
				return newGossipFixtureService(cfg)
			},
			func(configs []Config, services []*Service) error {
				failedConfigs = append(failedConfigs, configs)
				// The collision is already observed. Release the injection before
				// cleanup/reopen: a bare listener would stall seed joins until the
				// memberlist TCP timeout without exercising another contract.
				var listenerErr error
				if occupied != nil {
					listenerErr = occupied.Close()
					if listenerErr == nil {
						occupied = nil
					}
				}
				if err := errors.Join(listenerErr, closeGossipFixture(rt, configs, services)); err != nil {
					return err
				}
				// Reopen the successfully started node from the injected generation
				// to prove rollback released locks, listeners and background workers.
				if attempts == injectedAttempt && len(services) == 1 {
					cfg := configs[0]
					runtime.SetupServiceBasedRuntime(cfg.UUID, rt)
					reopened, err := newGossipFixtureService(cfg)
					if err != nil {
						runtime.CompareAndDeleteServiceBasedRuntime(cfg.UUID, rt)
						return fmt.Errorf("reopen rolled-back node: %w", err)
					}
					reopenedNode = true
					return closeGossipFixture(rt, []Config{cfg}, []*Service{reopened})
				}
				return nil
			})
		if services != nil {
			defer func() { require.NoError(t, closeGossipFixture(rt, configs, services)) }()
		}
		require.NoError(t, err)
		require.Greater(t, injectedAttempt, 0)
		require.Greater(t, attempts, injectedAttempt)
		require.LessOrEqual(t, attempts, maxGossipFixtureStartupAttempts)
		require.Error(t, collisionErr)
		require.True(t, isGossipFixtureBindCollision(collisionErr), "%v", collisionErr)
		require.True(t, reopenedNode)
		t.Logf("real memberlist collision on attempt %d, recovered on attempt %d: %v", injectedAttempt, attempts, collisionErr)
		require.Len(t, services, 2)
		for _, cfg := range configs {
			require.NotEqual(t, injectedConfigs[0].UUID, cfg.UUID)
			require.NotEqual(t, injectedConfigs[1].UUID, cfg.UUID)
			require.Equal(t, []string{getTestGossipAddress(configs[0].GossipPort), getTestGossipAddress(configs[1].GossipPort)}, cfg.GossipSeedAddresses)
			for _, failed := range failedConfigs {
				for _, old := range failed {
					require.NotEqual(t, old.UUID, cfg.UUID)
					require.NotContains(t, cfg.GossipSeedAddresses, getTestGossipAddress(old.GossipPort))
					require.Nil(t, runtime.ServiceRuntime(old.UUID))
				}
			}
		}
	})
}

func TestGossipFixtureStartupConfigurationFailure(t *testing.T) {
	allocationErr := syscall.EMFILE
	cleanupErr := errors.New("injected configuration cleanup failure")
	cancelErr := errors.New("injected configuration cancellation")
	for _, tc := range []struct {
		name                           string
		buildErr, cleanupErr           error
		cancelOnBuild, cancelOnCleanup bool
	}{
		{name: "allocation error", buildErr: allocationErr},
		{name: "factory bind error is terminal", buildErr: syscall.EADDRINUSE},
		{name: "cleanup failure", buildErr: allocationErr, cleanupErr: cleanupErr},
		{name: "cancellation after build", buildErr: allocationErr, cancelOnBuild: true},
		{name: "cleanup failure and cancellation", buildErr: allocationErr, cleanupErr: cleanupErr, cancelOnCleanup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			builds, rollbacks := 0, 0
			configs, services, err := startGossipFixture(t, ctx,
				func() ([]Config, error) {
					builds++
					if tc.cancelOnBuild {
						cancel(cancelErr)
					}
					return []Config{{UUID: "partial"}}, tc.buildErr
				},
				func(Config) (*Service, error) {
					t.Fatal("failed configuration started a service")
					return nil, nil
				},
				func(configs []Config, services []*Service) error {
					rollbacks++
					require.Len(t, configs, 1)
					require.Equal(t, "partial", configs[0].UUID)
					require.Empty(t, services)
					if tc.cancelOnCleanup {
						cancel(cancelErr)
					}
					return tc.cleanupErr
				})
			require.Nil(t, configs)
			require.Nil(t, services)
			require.ErrorIs(t, err, tc.buildErr)
			require.Equal(t, 1, builds)
			require.Equal(t, 1, rollbacks)
			if tc.cleanupErr != nil {
				require.ErrorIs(t, err, tc.cleanupErr)
			}
			if tc.cancelOnBuild || tc.cancelOnCleanup {
				require.ErrorIs(t, err, cancelErr)
			}
		})
	}
}

func TestGossipFixtureConfigPortRange(t *testing.T) {
	first, limit, err := testPortRange()
	require.NoError(t, err)
	rt := runtime.DefaultRuntime()
	configs, err := makeGossipFixtureConfigs(t, rt, 2)
	defer func() { require.NoError(t, closeGossipFixture(rt, configs, nil)) }()
	require.NoError(t, err)
	require.Len(t, configs, 2)
	seen := make(map[int]struct{})
	for _, cfg := range configs {
		for _, port := range []int{cfg.GossipPort, cfg.RaftPort, cfg.LogServicePort} {
			require.GreaterOrEqual(t, port, first)
			require.Less(t, port, limit)
			require.NotContains(t, seen, port)
			seen[port] = struct{}{}
		}
	}
}

func TestGossipFixtureConfigAllocationFailure(t *testing.T) {
	for _, tc := range []struct {
		name                string
		failAt, wantConfigs int
		replaceRuntime      bool
	}{
		{name: "first gossip port", failAt: 1},
		{name: "later service port", failAt: 5, wantConfigs: 1},
		{name: "later raft port", failAt: 6, wantConfigs: 1},
		{name: "independent replacement survives", failAt: 6, wantConfigs: 1, replaceRuntime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := runtime.DefaultRuntime()
			var owned []Config
			t.Cleanup(func() { require.NoError(t, closeGossipFixture(rt, owned, nil)) })
			var replacement runtime.Runtime
			allocations, builds, rollbacks := 0, 0, 0
			configs, services, err := startGossipFixture(t, t.Context(),
				func() ([]Config, error) {
					builds++
					var err error
					owned, err = makeGossipFixtureConfigsWithAllocator(t, rt, 2, func() (int, error) {
						allocations++
						if allocations == tc.failAt {
							return 0, syscall.EMFILE
						}
						return 23000 + allocations, nil
					})
					if tc.replaceRuntime && len(owned) > 0 {
						replacement = runtime.DefaultRuntime()
						id := owned[0].UUID
						runtime.SetupServiceBasedRuntime(id, replacement)
						t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(id, replacement) })
					}
					return owned, err
				},
				func(Config) (*Service, error) {
					t.Fatal("partial configuration started a service")
					return nil, nil
				},
				func(configs []Config, services []*Service) error {
					rollbacks++
					require.Empty(t, services)
					return closeGossipFixture(rt, configs, services)
				})
			require.ErrorIs(t, err, syscall.EMFILE)
			require.Nil(t, configs)
			require.Nil(t, services)
			require.Equal(t, 1, builds)
			require.Equal(t, 1, rollbacks)
			require.Equal(t, tc.failAt, allocations)
			require.Len(t, owned, tc.wantConfigs)
			for _, cfg := range owned {
				if tc.replaceRuntime {
					require.Same(t, replacement, runtime.ServiceRuntime(cfg.UUID))
				} else {
					require.Nil(t, runtime.ServiceRuntime(cfg.UUID))
				}
			}
		})
	}
}
