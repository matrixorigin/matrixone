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
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	mruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type delayedTaskServiceGetter struct {
	mu      sync.RWMutex
	service taskservice.TaskService
	called  chan struct{}
	once    sync.Once
}

func newDelayedTaskServiceGetter() *delayedTaskServiceGetter {
	return &delayedTaskServiceGetter{called: make(chan struct{})}
}

func (g *delayedTaskServiceGetter) GetTaskService() (taskservice.TaskService, bool) {
	g.once.Do(func() { close(g.called) })
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.service, g.service != nil
}

func (g *delayedTaskServiceGetter) set(service taskservice.TaskService) {
	g.mu.Lock()
	g.service = service
	g.mu.Unlock()
}

func (g *delayedTaskServiceGetter) Start() error { return nil }
func (g *delayedTaskServiceGetter) Close() error { return nil }

func TestWaitTaskServiceReadyObservesOwnedService(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	service := taskservice.NewTaskService(
		mruntime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer func() { require.NoError(t, service.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- waitTaskServiceReady(ctx, getter, time.Millisecond)
	}()
	<-getter.called
	getter.set(service)
	require.NoError(t, <-done)
}

func TestWaitTaskServiceReadyHonorsCancellation(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- waitTaskServiceReady(ctx, getter, time.Hour)
	}()
	<-getter.called
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWaitBasicClusterTaskServicesRejectsMissingCN(t *testing.T) {
	err := waitBasicClusterTaskServices(context.Background(), &cluster{}, 1)
	require.ErrorContains(t, err, "service not found")
}

func TestWaitBasicClusterTaskServicesHonorsCNCount(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	service := taskservice.NewTaskService(
		mruntime.DefaultRuntime(), taskservice.NewMemTaskStorage())
	defer func() { require.NoError(t, service.Close()) }()
	getter.set(service)

	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = getter
	c := &cluster{services: []*operator{cn}}

	err := waitBasicClusterTaskServices(context.Background(), c, 2)
	require.ErrorContains(t, err, "service not found")
}

func TestWaitBasicClusterTaskServicesRejectsUnsupportedService(t *testing.T) {
	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = &closeTrackingService{}
	c := &cluster{services: []*operator{cn}}

	err := waitBasicClusterTaskServices(context.Background(), c, 1)
	require.ErrorContains(t, err, "does not expose its task service")
}

func TestWaitBasicClusterTaskServicesReportsReadinessCancellation(t *testing.T) {
	getter := newDelayedTaskServiceGetter()
	cn := &operator{sid: "cn-0", serviceType: metadata.ServiceType_CN}
	cn.reset.svc = getter
	c := &cluster{services: []*operator{cn}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitBasicClusterTaskServices(ctx, c, 1)
	require.ErrorContains(t, err, "task service did not become ready")
}

func TestBasicClusterUsesShortStartupRetryIntervals(t *testing.T) {
	services := []*operator{
		{serviceType: metadata.ServiceType_LOG, cfg: newServiceConfig()},
		{serviceType: metadata.ServiceType_TN, cfg: newServiceConfig()},
		{serviceType: metadata.ServiceType_CN, cfg: newServiceConfig()},
	}
	for _, service := range services {
		adjustBasicClusterService(service)
	}

	assert.Equal(t, time.Second, services[0].cfg.LogService.HAKeeperCheckInterval.Duration)
	assert.Equal(t, 500*time.Millisecond, services[0].cfg.LogService.HAKeeperBootstrapRetryInterval.Duration)
	assert.Equal(t, 100*time.Millisecond, services[1].cfg.HAKeeperRunningRetryInterval.Duration)
	assert.Equal(t, 100*time.Millisecond, services[2].cfg.TNShardReadyRetryInterval.Duration)
	assert.True(t, services[2].cfg.CN.AutoIncrement.EnableAutoIDCache)
}

type panicTestReporter struct{}

func TestSharedTestClusterCompletedErrorDropsOwnership(t *testing.T) {
	for _, action := range []string{"close", "reset", "rollback", "init"} {
		t.Run(action, func(t *testing.T) {
			failure := errors.New("withdrawal failed")
			svc := &completedCloseService{closeTrackingService: closeTrackingService{closeErr: failure}, complete: true}
			op := &operator{state: started}
			op.reset.svc = svc
			value := &cluster{state: started, services: []*operator{op}}
			state := SharedTestCluster{cluster: value}
			switch action {
			case "close":
				require.ErrorIs(t, state.Close(), failure)
				require.Nil(t, state.cluster)
				require.True(t, state.closed)
			case "reset":
				require.ErrorIs(t, state.CloseIfActive(), failure)
				require.Nil(t, state.cluster)
				require.False(t, state.closed)
			case "rollback":
				owner, err := cleanupClusterOnError(value, failure)
				require.Nil(t, owner)
				require.ErrorIs(t, err, failure)
			case "init":
				require.Panics(t, func() {
					state.Run(panicTestReporter{}, func() (Cluster, error) { return value, failure }, func(Cluster) { t.Fatal("failed init callback") })
				})
				require.Nil(t, state.cluster)
				require.ErrorIs(t, state.err, failure)
			}
			require.True(t, value.CloseComplete())
			require.NoError(t, value.Close())
			require.Equal(t, int32(1), svc.closeCount.Load())
		})
	}
}

func (panicTestReporter) Helper() {}

func (panicTestReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

type loggingTestReporter struct {
	panicTestReporter
	logs []string
}

func (r *loggingTestReporter) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

type syntheticFailureReporter struct {
	panicTestReporter
	failed   bool
	cleanups []func()
	errors   []string
}

func (r *syntheticFailureReporter) Failed() bool { return r.failed }

func (r *syntheticFailureReporter) Cleanup(fn func()) {
	r.cleanups = append(r.cleanups, fn)
}

func (r *syntheticFailureReporter) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

type namedPanicTestReporter struct {
	panicTestReporter
	name string
}

func (r namedPanicTestReporter) Name() string { return r.name }

func TestSharedTestClusterFailureCleanupCannotCloseReplacement(t *testing.T) {
	state := SharedTestCluster{}
	first := &cluster{}
	firstReporter := &syntheticFailureReporter{failed: true}
	dependentCleanupSawFixture := false

	state.Run(firstReporter, func() (Cluster, error) {
		return first, nil
	}, func(Cluster) {
		// This cleanup is registered after SharedTestCluster's fallback cleanup;
		// the actual testing.T LIFO order runs it first while the fixture is
		// still available.
		firstReporter.Cleanup(func() {
			dependentCleanupSawFixture = state.cluster == first
		})
	})
	require.Same(t, first, state.cluster, "failure must seal before cleanup, not close before it")
	require.True(t, state.sealed)
	require.Len(t, firstReporter.cleanups, 2)
	oldCleanup := firstReporter.cleanups[0]
	require.NoError(t, state.CloseIfActive(), "an outer defer must not close before callback cleanup")
	require.Same(t, first, state.cluster)
	for index := len(firstReporter.cleanups) - 1; index >= 0; index-- {
		firstReporter.cleanups[index]()
	}
	require.True(t, dependentCleanupSawFixture)
	require.Nil(t, state.cluster)

	second := &cluster{}
	secondReporter := &syntheticFailureReporter{}
	state.Run(secondReporter, func() (Cluster, error) {
		return second, nil
	}, func(Cluster) {})
	require.Same(t, second, state.cluster)

	// Simulate a delayed duplicate Cleanup callback from generation one. It must not
	// close or invalidate generation two after the shared state was reset.
	oldCleanup()
	require.Same(t, second, state.cluster)
	require.NoError(t, state.CloseIfActive())
}

func TestSharedTestClusterRegistersOneFailureCleanupPerTest(t *testing.T) {
	state := SharedTestCluster{}
	reporter := &syntheticFailureReporter{}
	fixture := &cluster{}
	cleanupSawFixture := make([]bool, 0, 2)

	state.Run(reporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {
		reporter.Cleanup(func() { cleanupSawFixture = append(cleanupSawFixture, state.cluster == fixture) })
	})

	reporter.failed = true
	state.Run(reporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {
		reporter.Cleanup(func() { cleanupSawFixture = append(cleanupSawFixture, state.cluster == fixture) })
	})

	// One shared finalizer is registered before both callback-owned cleanups.
	// LIFO cleanup therefore lets both callbacks use the live fixture before it
	// is closed.
	require.Len(t, reporter.cleanups, 3)
	for index := len(reporter.cleanups) - 1; index >= 0; index-- {
		reporter.cleanups[index]()
	}
	require.Equal(t, []bool{true, true}, cleanupSawFixture)
	require.Nil(t, state.cluster)
}

func TestSharedTestClusterTracksParentAndChildCleanupScopes(t *testing.T) {
	state := SharedTestCluster{}
	parentReporter := &syntheticFailureReporter{}
	childReporter := &syntheticFailureReporter{}
	fixture := &cluster{}
	cleanupSawFixture := make([]bool, 0, 2)

	state.Run(parentReporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {
		parentReporter.Cleanup(func() { cleanupSawFixture = append(cleanupSawFixture, state.cluster == fixture) })
	})
	state.Run(childReporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {})
	for index := len(childReporter.cleanups) - 1; index >= 0; index-- {
		childReporter.cleanups[index]()
	}

	parentReporter.failed = true
	state.Run(parentReporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {
		parentReporter.Cleanup(func() { cleanupSawFixture = append(cleanupSawFixture, state.cluster == fixture) })
	})

	// The child scope has already released its registration. The parent owns
	// one finalizer for both parent callbacks, so both dependent cleanups run
	// before destruction.
	require.Len(t, parentReporter.cleanups, 3)
	for index := len(parentReporter.cleanups) - 1; index >= 0; index-- {
		parentReporter.cleanups[index]()
	}
	require.Equal(t, []bool{true, true}, cleanupSawFixture)
	require.Nil(t, state.cluster)
}

func TestSharedTestClusterAllowsChildCleanupWhileParentCallbackRuns(t *testing.T) {
	state := SharedTestCluster{}
	fixture := &cluster{}

	require.True(t, t.Run("parent", func(parent *testing.T) {
		state.Run(parent, func() (Cluster, error) {
			return fixture, nil
		}, func(Cluster) {
			// Register a real testing.T cleanup scope while the parent callback
			// waits for the child. The child cleanup must acquire lifecycle state;
			// holding c.mu across the parent callback would deadlock here.
			require.True(parent, parent.Run("child", func(child *testing.T) {
				state.mu.Lock()
				state.registerFailureCleanup(child, state.generation)
				state.mu.Unlock()
			}))
		})
	}))

	require.Same(t, fixture, state.cluster, "successful nested cleanup must not close the shared fixture")
	require.NoError(t, state.CloseIfActive())
}

func TestSharedTestClusterRejectsNestedRun(t *testing.T) {
	state := SharedTestCluster{}
	parent := namedPanicTestReporter{name: "TestSharedTestClusterRejectsNestedRun"}
	child := namedPanicTestReporter{name: "TestSharedTestClusterRejectsNestedRun/child"}
	fixture := &cluster{}

	state.Run(parent, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {
		require.PanicsWithValue(t,
			"shared cluster cannot be reacquired from a nested test; use the inherited cluster",
			func() {
				state.Run(child, func() (Cluster, error) {
					t.Fatal("nested Run must fail before initialization")
					return nil, nil
				}, func(Cluster) {
					t.Fatal("nested Run must not invoke its callback")
				})
			})
	})
	require.NoError(t, state.CloseIfActive())
}

func TestSharedTestClusterSealsOnCallbackPanic(t *testing.T) {
	state := SharedTestCluster{}
	reporter := &syntheticFailureReporter{}
	fixture := &cluster{}

	require.Panics(t, func() {
		state.Run(reporter, func() (Cluster, error) {
			return fixture, nil
		}, func(Cluster) {
			panic("synthetic callback failure")
		})
	})
	require.True(t, state.sealed, "a non-normal callback exit must seal the generation")
	require.Same(t, fixture, state.cluster)

	reporter.failed = true
	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
	require.Nil(t, state.cluster)
}

func TestSharedTestClusterFailedCleanupAttemptsCloseOnce(t *testing.T) {
	closeErr := errors.New("cleanup still in progress")
	service := &closeTrackingService{closeErr: closeErr}
	op := &operator{state: started}
	op.reset.svc = service
	fixture := &cluster{state: started, services: []*operator{op}}
	state := SharedTestCluster{}
	reporter := &syntheticFailureReporter{failed: true}

	state.Run(reporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {})
	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
	reporter.cleanups[0]()

	require.EqualValues(t, 1, service.closeCount.Load(), "automatic failure cleanup must not retry itself")
	require.True(t, state.closed)
	require.Same(t, fixture, state.cluster)
	require.Len(t, reporter.errors, 1)

	service.closeErr = nil
	require.NoError(t, state.CloseIfActive(), "an explicit retry may complete retained cleanup")
	require.False(t, state.closed)
}

func TestSharedTestClusterCloseIntentSurvivesDeferredFailureCleanup(t *testing.T) {
	state := SharedTestCluster{}
	fixture := &cluster{}
	reporter := &syntheticFailureReporter{failed: true}

	state.Run(reporter, func() (Cluster, error) {
		return fixture, nil
	}, func(Cluster) {})
	require.True(t, state.sealed)

	// Close is terminal even when it must defer destruction until the test's
	// callback-owned cleanup has finished.
	require.NoError(t, state.Close())
	require.True(t, state.closed)
	require.True(t, state.terminalClose)
	require.Same(t, fixture, state.cluster)

	require.Len(t, reporter.cleanups, 1)
	reporter.cleanups[0]()
	require.Nil(t, state.cluster)
	require.True(t, state.closed)
	require.PanicsWithValue(t, "shared cluster is closed", func() {
		state.Run(reporter, func() (Cluster, error) {
			t.Fatal("terminal Close must not permit a replacement fixture")
			return nil, nil
		}, func(Cluster) {})
	})
}

func TestSharedTestClusterSuccessfulCallbackKeepsFixture(t *testing.T) {
	state := SharedTestCluster{}
	reporter := &syntheticFailureReporter{}
	fixture := &cluster{}
	initCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return fixture, nil
	}

	state.Run(reporter, init, func(Cluster) {})
	state.Run(reporter, init, func(Cluster) {})
	for _, cleanup := range reporter.cleanups {
		cleanup()
	}

	require.Equal(t, 1, initCalls, "a successful shared fixture remains reusable")
	require.Same(t, fixture, state.cluster, "successful cleanup callbacks must not close the fixture")
	require.NoError(t, state.CloseIfActive())
}

func TestSharedTestClusterLogsInitializationOnce(t *testing.T) {
	state := SharedTestCluster{}
	reporter := &loggingTestReporter{}
	value := &cluster{}

	state.Run(reporter, func() (Cluster, error) {
		return value, nil
	}, func(Cluster) {})
	state.Run(reporter, func() (Cluster, error) {
		t.Fatal("initializer must not run after sync.Once")
		return nil, nil
	}, func(Cluster) {})

	require.Len(t, reporter.logs, 1)
	require.True(t, strings.Contains(reporter.logs[0],
		"MO_UT_SETUP fixture=shared-cluster phase=initialize"))
	require.True(t, strings.Contains(reporter.logs[0], "status=ready"))
	require.NoError(t, state.Close())
}

func TestSharedTestClusterReportsInitializationError(t *testing.T) {
	state := SharedTestCluster{}
	wantErr := errors.New("cluster startup failed")
	initCalls := 0
	testCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return nil, wantErr
	}
	test := func(Cluster) {
		testCalls++
	}

	reporter := panicTestReporter{}
	for range 2 {
		require.PanicsWithValue(t,
			"failed to initialize shared cluster: cluster startup failed",
			func() {
				state.Run(reporter, init, test)
			},
		)
	}
	require.Equal(t, 1, initCalls)
	require.Zero(t, testCalls)
}

func TestSharedTestClusterRejectsNilInitialization(t *testing.T) {
	state := SharedTestCluster{}
	reporter := panicTestReporter{}

	require.PanicsWithValue(t,
		"failed to initialize shared cluster: internal error: cluster initializer returned nil without an error",
		func() {
			state.Run(reporter, func() (Cluster, error) {
				return nil, nil
			}, func(Cluster) {
				t.Fatal("test callback must not run")
			})
		},
	)
}

func TestSharedTestClusterClosesFailedInitialization(t *testing.T) {
	lease, err := acquireClusterPortLease()
	require.NoError(t, err)
	value := &cluster{
		portLease:     lease,
		portLeaseBase: lease.base,
		portLeaseNext: lease.base,
	}
	t.Cleanup(func() {
		if value.portLease != nil {
			require.NoError(t, value.releasePortLeaseLocked())
		}
	})

	state := SharedTestCluster{}
	require.PanicsWithValue(t,
		"failed to initialize shared cluster: cluster startup failed",
		func() {
			state.Run(panicTestReporter{}, func() (Cluster, error) {
				return value, errors.New("cluster startup failed")
			}, func(Cluster) {
				t.Fatal("test callback must not run")
			})
		},
	)
	require.Nil(t, value.portLease)
	require.Nil(t, state.cluster)
}

func TestSharedTestClusterRetainsFailedCleanupOwner(t *testing.T) {
	startErr := errors.New("cluster startup failed")
	closeErr := errors.New("cluster cleanup failed")
	service := &closeTrackingService{closeErr: closeErr}
	op := &operator{state: started}
	op.reset.svc = service
	value := &cluster{state: started, services: []*operator{op}}
	state := SharedTestCluster{}
	initCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return value, startErr
	}

	require.Panics(t, func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run")
		})
	})
	require.Same(t, value, state.cluster)
	require.ErrorIs(t, state.err, startErr)
	require.ErrorIs(t, state.err, closeErr)
	require.Equal(t, int32(1), service.closeCount.Load())

	service.closeErr = nil
	require.Panics(t, func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run")
		})
	})
	require.Equal(t, 1, initCalls)
	require.Equal(t, int32(2), service.closeCount.Load())
	require.Nil(t, state.cluster)
	require.NoError(t, state.Close())
}

func TestStartTestClusterReturnsCleanupOwnerOnRollbackFailure(t *testing.T) {
	startErr := errors.New("cluster startup failed")
	closeErr := errors.New("cluster cleanup failed")
	service := &closeTrackingService{closeErr: closeErr}

	value, err := StartTestCluster(Option(func(c *cluster) {
		// The package's shared base can already be active when this rollback
		// test runs. Bypass admission only to reach the injected partial-start
		// cleanup path; no second complete cluster is created.
		c.options.allowConcurrentTestClusters = true
		c.startFn = func(op *operator) error {
			op.state = started
			op.reset.svc = service
			return startErr
		}
	}))
	if value != nil {
		cleanupOwner := value
		t.Cleanup(func() {
			service.closeErr = nil
			require.NoError(t, cleanupOwner.Close())
		})
	}

	require.ErrorIs(t, err, startErr)
	require.ErrorIs(t, err, closeErr)
	require.NotNil(t, value)
	require.Equal(t, int32(2), service.closeCount.Load())

	service.closeErr = nil
	require.NoError(t, value.Close())
	require.Equal(t, int32(3), service.closeCount.Load())
}

func TestSharedTestClusterCloseIsTerminal(t *testing.T) {
	state := SharedTestCluster{}
	value := &cluster{}
	initCalls := 0
	init := func() (Cluster, error) {
		initCalls++
		return value, nil
	}

	state.Run(panicTestReporter{}, init, func(cluster Cluster) {
		require.Same(t, value, cluster)
	})
	require.NoError(t, state.Close())
	require.True(t, state.closed)
	require.Nil(t, state.cluster)

	require.PanicsWithValue(t, "shared cluster is closed", func() {
		state.Run(panicTestReporter{}, init, func(Cluster) {
			t.Fatal("test callback must not run after close")
		})
	})
	require.Equal(t, 1, initCalls)
	require.NoError(t, state.Close())
}

func TestSharedTestClusterCloseIfActiveLeavesUnusedFixtureReusable(t *testing.T) {
	state := SharedTestCluster{}
	first := &cluster{}
	second := &cluster{}
	initCalls := 0

	// A package may have a specialized cluster before its optional shared suite.
	// Releasing an unused shared fixture must not make that later suite terminal.
	require.NoError(t, state.CloseIfActive())
	state.Run(panicTestReporter{}, func() (Cluster, error) {
		initCalls++
		return first, nil
	}, func(cluster Cluster) {
		require.Same(t, first, cluster)
	})

	require.NoError(t, state.CloseIfActive())
	require.False(t, state.closed)
	require.Nil(t, state.cluster)

	state.Run(panicTestReporter{}, func() (Cluster, error) {
		initCalls++
		return second, nil
	}, func(cluster Cluster) {
		require.Same(t, second, cluster)
	})
	require.Equal(t, 2, initCalls)
	require.NoError(t, state.CloseIfActive())
}

func TestSharedTestClusterDisposableGeneration(t *testing.T) {
	for _, outcome := range []string{"success", "canceled", "partial-setup-goexit"} {
		t.Run(outcome, func(t *testing.T) {
			state := SharedTestCluster{}
			service := &closeTrackingService{}
			op := &operator{state: started}
			op.reset.svc = service
			fixture := &cluster{state: started, services: []*operator{op}}
			reporter := &syntheticFailureReporter{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Register disposal before Run, as a scenario owning a disposable
			// generation does. Callback defers and cleanups must run first.
			require.NoError(t, state.CloseIfActive())
			reporter.Cleanup(func() {
				if err := state.CloseIfActive(); err != nil {
					reporter.Errorf("dispose fixture: %v", err)
				}
			})
			deferred := false
			cleanupSawLiveFixture := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				state.Run(reporter, func() (Cluster, error) {
					return fixture, nil
				}, func(Cluster) {
					defer func() { deferred = true }()
					reporter.Cleanup(func() {
						cleanupSawLiveFixture = state.cluster == fixture && service.closeCount.Load() == 0
					})
					if outcome != "success" {
						cancel()
					}
					if outcome == "partial-setup-goexit" {
						// Fatal assertions use Goexit after partial setup. The
						// fixture admission must unwind before scope cleanup.
						reporter.failed = true
						runtime.Goexit()
					}
				})
			}()
			<-done
			require.True(t, deferred)
			require.False(t, state.runActive)
			require.Zero(t, service.closeCount.Load())
			if outcome != "success" {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			}
			for i := len(reporter.cleanups) - 1; i >= 0; i-- {
				reporter.cleanups[i]()
			}
			require.True(t, cleanupSawLiveFixture)
			require.Empty(t, reporter.errors)
			require.EqualValues(t, 1, service.closeCount.Load())
			require.Nil(t, state.cluster)
			require.False(t, state.closed)
			require.False(t, state.sealed)

			// A subsequent scenario must initialize a new fixture, even after
			// cancellation or an assertion aborted the previous callback.
			next := &cluster{}
			state.Run(panicTestReporter{}, func() (Cluster, error) {
				return next, nil
			}, func(c Cluster) { require.Same(t, next, c) })
			require.NoError(t, state.CloseIfActive())
			require.EqualValues(t, 1, service.closeCount.Load())
		})
	}
}

func TestSharedTestClusterCloseIfActiveBlocksReuseAfterFailure(t *testing.T) {
	closeErr := errors.New("cluster cleanup failed")
	service := &closeTrackingService{closeErr: closeErr}
	op := &operator{state: started}
	op.reset.svc = service
	value := &cluster{state: started, services: []*operator{op}}
	state := SharedTestCluster{}

	state.Run(panicTestReporter{}, func() (Cluster, error) {
		return value, nil
	}, func(Cluster) {})
	require.ErrorIs(t, state.CloseIfActive(), closeErr)
	require.True(t, state.closed)
	require.Same(t, value, state.cluster)
	require.PanicsWithValue(t, "shared cluster is closed", func() {
		state.Run(panicTestReporter{}, func() (Cluster, error) {
			t.Fatal("initializer must not run while cleanup is incomplete")
			return nil, nil
		}, func(Cluster) {})
	})

	service.closeErr = nil
	require.NoError(t, state.CloseIfActive())
	require.False(t, state.closed)
	require.Nil(t, state.cluster)
}
