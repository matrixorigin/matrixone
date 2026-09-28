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
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

var (
	basicClusterState    SharedTestCluster
	singleCNClusterState SharedTestCluster
)

// SharedTestCluster serializes tests that reuse an expensive embedded cluster
// and preserves the first initialization result. Initialization callbacks must
// return errors instead of failing the current test from inside sync.Once, so a
// failed startup is reported consistently to every later caller.
type SharedTestCluster struct {
	mu          sync.Mutex
	runCondOnce sync.Once
	runCond     *sync.Cond
	runActive   bool
	runScope    sharedRunScope
	once        sync.Once
	cluster     Cluster
	err         error
	closed      bool
	// terminalClose preserves an explicit Close request while failed-test
	// cleanup is deferred until callback-owned cleanup handlers finish.
	terminalClose        bool
	sealed               bool
	cleanupTried         bool
	generation           uint64
	cleanupSequence      uint64
	cleanupRegistrations map[sharedCleanupRegistration]struct{}
}

type sharedCleanupRegistration struct {
	generation uint64
	owner      uintptr
	sequence   uint64
}

type sharedRunScope struct {
	owner uintptr
	name  string
}

type testReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// testLogger is optional so the shared fixture helpers remain usable with the
// small reporters used by unit tests. *testing.T implements it, which lets
// integration-test output explain where fixture setup time was spent without
// coupling the lifecycle code to testing.T.
type testLogger interface {
	Logf(format string, args ...any)
}

// testFailureReporter and testCleanupRegistrar are optional extensions of
// testReporter. Keeping them optional preserves the small reporter contract
// used by the lifecycle unit tests while allowing *testing.T to quarantine a
// failed shared fixture even when FailNow/Goexit prevents Run from returning.
type testFailureReporter interface {
	Failed() bool
}

type testCleanupRegistrar interface {
	Cleanup(func())
}

type testNameReporter interface {
	Name() string
}

type testErrorReporter interface {
	Errorf(format string, args ...any)
}

func logTestSetup(t testReporter, format string, args ...any) {
	t.Helper()
	if logger, ok := t.(testLogger); ok {
		logger.Logf(format, args...)
	}
}

func (c *SharedTestCluster) initializeRunCondition() {
	c.runCondOnce.Do(func() {
		c.runCond = sync.NewCond(&c.mu)
	})
}

func sharedRunScopeFor(t testReporter) sharedRunScope {
	scope := sharedRunScope{owner: testReporterIdentity(t)}
	if named, ok := t.(testNameReporter); ok {
		scope.name = named.Name()
	}
	return scope
}

// isNestedRunScope identifies a same-fixture re-entry from a parent test's
// callback. A nested callback cannot acquire the serialized fixture while the
// parent is waiting for t.Run to finish; treating it as an ordinary waiter
// would deadlock the test. The parent callback already owns the Cluster, so a
// nested test should use that value directly.
func isNestedRunScope(active, next sharedRunScope) bool {
	if active.owner != 0 && active.owner == next.owner {
		return true
	}
	if active.name == "" || next.name == "" || active.name == next.name {
		return false
	}
	return strings.HasPrefix(next.name, active.name+"/")
}

func (c *SharedTestCluster) releaseRunLocked() {
	c.runActive = false
	c.runScope = sharedRunScope{}
	if c.runCond != nil {
		c.runCond.Broadcast()
	}
}

func (c *SharedTestCluster) finishRun(
	t testReporter,
	generation uint64,
	callbackReturned bool,
	failureAware testFailureReporter,
) {
	c.mu.Lock()
	if !callbackReturned || (failureAware != nil && failureAware.Failed()) {
		c.sealFailedGenerationLocked(t, generation)
	}
	c.releaseRunLocked()
	c.mu.Unlock()
}

func (c *SharedTestCluster) Run(
	t testReporter,
	init func() (Cluster, error),
	fn func(Cluster),
) {
	t.Helper()
	c.initializeRunCondition()
	scope := sharedRunScopeFor(t)

	var (
		fixture           Cluster
		fixtureGeneration uint64
		initErr           error
		fatalMessage      string
		activeClaimed     bool
		setupComplete     bool
	)

	// Initialization and state transitions are protected by c.mu, but the user
	// callback is not. Keeping the callback outside c.mu lets a real child
	// testing.T run its own Cleanup handlers while the parent waits in t.Run.
	// Callback admission remains exclusive, so independent tests cannot mutate
	// the shared fixture concurrently.
	func() {
		c.mu.Lock()
		defer func() {
			if activeClaimed && !setupComplete {
				c.releaseRunLocked()
			}
			c.mu.Unlock()
		}()

		for c.runActive {
			if isNestedRunScope(c.runScope, scope) {
				fatalMessage = "shared cluster cannot be reacquired from a nested test; use the inherited cluster"
				return
			}
			c.runCond.Wait()
		}
		if c.closed || c.terminalClose {
			fatalMessage = "shared cluster is closed"
			return
		}
		if c.sealed {
			fatalMessage = "shared cluster is quarantined after a test failure"
			return
		}

		c.runActive = true
		c.runScope = scope
		activeClaimed = true

		initialized := false
		initStarted := time.Now()
		c.once.Do(func() {
			initialized = true
			c.generation++
			c.cluster, c.err = init()
			if c.err == nil && c.cluster == nil {
				c.err = moerr.NewInternalErrorNoCtx("cluster initializer returned nil without an error")
			}
		})
		if initialized {
			status := "ready"
			if c.err != nil {
				status = "error"
			}
			logTestSetup(t,
				"MO_UT_SETUP fixture=shared-cluster phase=initialize-total duration=%s status=%s",
				time.Since(initStarted), status)
		}
		if c.err != nil && c.cluster != nil {
			cleanupStarted := time.Now()
			cleanupErr := c.cluster.Close()
			cleanupStatus := "ready"
			if cleanupErr != nil {
				cleanupStatus = "error"
			}
			logTestSetup(t,
				"MO_UT_SETUP fixture=shared-cluster phase=rollback-cleanup duration=%s status=%s",
				time.Since(cleanupStarted), cleanupStatus)
			complete := closeComplete(c.cluster, cleanupErr)
			c.err = errors.Join(c.err, cleanupErr)
			if complete {
				c.cluster = nil
			}
		}
		if c.err != nil {
			initErr = c.err
			return
		}

		fixture = c.cluster
		fixtureGeneration = c.generation
		c.registerFailureCleanup(t, fixtureGeneration)
		setupComplete = true
	}()

	if fatalMessage != "" {
		t.Fatalf("%s", fatalMessage)
		return
	}
	if initErr != nil {
		t.Fatalf("failed to initialize shared cluster: %v", initErr)
		return
	}

	callbackReturned := false
	var failureAware testFailureReporter
	if reporter, ok := t.(testFailureReporter); ok {
		failureAware = reporter
	}
	defer func() {
		c.finishRun(t, fixtureGeneration, callbackReturned, failureAware)
	}()
	fn(fixture)
	callbackReturned = true
}

// registerFailureCleanup makes failure isolation work for callbacks that do
// not return normally. testing.T runs Cleanup after Run has released callback
// admission, so the cleanup can safely acquire c.mu. A generation guard is
// essential: a later test may already have started a replacement fixture by
// the time an older cleanup callback is invoked.
func (c *SharedTestCluster) registerFailureCleanup(t testReporter, generation uint64) {
	failed, failureAware := t.(testFailureReporter)
	registrar, cleanupAware := t.(testCleanupRegistrar)
	if !failureAware || !cleanupAware {
		return
	}
	owner := testReporterIdentity(t)
	if c.cleanupRegistrations == nil {
		c.cleanupRegistrations = make(map[sharedCleanupRegistration]struct{})
	}
	registration := sharedCleanupRegistration{generation: generation, owner: owner}
	if owner == 0 {
		c.cleanupSequence++
		registration.sequence = c.cleanupSequence
	}
	if _, exists := c.cleanupRegistrations[registration]; exists {
		return
	}
	c.cleanupRegistrations[registration] = struct{}{}
	registrar.Cleanup(func() {
		if err := c.finishFailureCleanup(t, failed, registration); err != nil {
			reportFailureQuarantineError(t, generation, err)
		}
	})
}

// testing.T is a pointer-backed test scope. Use its identity to register one
// finalizer for all Run calls made by the same test and generation; otherwise
// a later Run's finalizer could close the fixture before an earlier callback's
// cleanup. A non-pointer test reporter keeps the conservative legacy behavior.
func testReporterIdentity(t testReporter) uintptr {
	v := reflect.ValueOf(t)
	if v.IsValid() && v.Kind() == reflect.Pointer {
		return v.Pointer()
	}
	return 0
}

func reportFailureQuarantineError(t testReporter, generation uint64, err error) {
	if reporter, ok := t.(testErrorReporter); ok {
		reporter.Errorf("failed to quarantine shared cluster generation %d: %v", generation, err)
		return
	}
	logTestSetup(t,
		"MO_UT_SETUP fixture=shared-cluster phase=failed-test-quarantine generation=%d status=error error=%v",
		generation, err)
}

// sealFailedGenerationLocked is deliberately separate from destruction. The
// callback's own defers and t.Cleanup handlers may still need the cluster for
// database/connection cleanup. Sealing is enough to prevent another callback
// from reusing the generation while those handlers finish.
func (c *SharedTestCluster) sealFailedGenerationLocked(t testReporter, generation uint64) {
	if c.cluster == nil || c.generation != generation || c.sealed {
		return
	}
	c.sealed = true
	logTestSetup(t,
		"MO_UT_SETUP fixture=shared-cluster phase=failed-test-seal generation=%d status=ready",
		generation)
}

// finishFailureCleanup releases one test-scope cleanup registration. The
// generation is destroyed only after every registered scope has finished; this
// preserves callback-owned cleanup when a parent and child both call Run.
// Exactly one automatic close attempt is made after the final registration;
// incomplete cleanup retains ownership until an explicit CloseIfActive retry.
func (c *SharedTestCluster) finishFailureCleanup(
	t testReporter,
	failed testFailureReporter,
	registration sharedCleanupRegistration,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != registration.generation {
		return nil
	}
	if _, exists := c.cleanupRegistrations[registration]; !exists {
		return nil
	}
	delete(c.cleanupRegistrations, registration)
	if c.cluster == nil {
		return nil
	}
	if !failed.Failed() && !c.sealed {
		return nil
	}
	if !c.sealed {
		c.sealFailedGenerationLocked(t, registration.generation)
	}
	for outstanding := range c.cleanupRegistrations {
		if outstanding.generation == registration.generation {
			return nil
		}
	}
	if c.cleanupTried {
		return nil
	}
	c.cleanupTried = true

	started := time.Now()
	fixture := c.cluster
	err := fixture.Close()
	complete := closeComplete(fixture, err)
	status := "ready"
	if !complete || err != nil {
		status = "error"
	}
	logTestSetup(t,
		"MO_UT_SETUP fixture=shared-cluster phase=failed-test-quarantine generation=%d duration=%s status=%s complete=%t",
		registration.generation, time.Since(started), status, complete)

	if !complete {
		c.closed = true
		return errors.Join(err, moerr.NewInvalidStateNoCtx("failed shared cluster cleanup is incomplete"))
	}

	// The old generation has released all resources, including admission and
	// ports. Reset only after CloseComplete; a later Run can safely initialize a
	// new generation without paying this cost on successful tests.
	c.cluster = nil
	c.err = nil
	c.once = sync.Once{}
	c.closed = c.terminalClose
	c.sealed = false
	c.cleanupTried = false
	c.cleanupRegistrations = nil
	return err
}

// Close releases the shared cluster or retries cleanup retained from a failed
// initialization. Ownership is cleared only after the underlying Close has
// completed locally, independently of any returned diagnostic.
func (c *SharedTestCluster) Close() error {
	c.initializeRunCondition()
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.runActive {
		c.runCond.Wait()
	}
	if c.cluster == nil {
		c.closed = true
		c.terminalClose = true
		return nil
	}
	c.terminalClose = true
	if c.sealed && !c.cleanupTried && len(c.cleanupRegistrations) > 0 {
		c.closed = true
		return nil
	}
	err := c.cluster.Close()
	if !closeComplete(c.cluster, err) {
		c.closed = true
		return err
	}
	c.cluster = nil
	c.closed = true
	return err
}

// CloseIfActive releases an initialized shared fixture and resets a successful
// release for a later test invocation. It is useful for a test package that
// combines a short shared-cluster suite with later scenarios that need their
// own topology. Callers must invoke it only after Run has returned; Run holds
// callback admission while the scenario body is executing.
func (c *SharedTestCluster) CloseIfActive() error {
	c.initializeRunCondition()
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.runActive {
		c.runCond.Wait()
	}
	if c.cluster == nil {
		return nil
	}
	// A failed callback seals the generation before its callback-owned
	// t.Cleanup handlers run. Defer destruction to the registered failure
	// cleanup so an outer test defer cannot close the fixture first. Once that
	// automatic attempt has happened, this method remains the explicit retry
	// path for incomplete cleanup.
	if c.sealed && !c.cleanupTried && len(c.cleanupRegistrations) > 0 {
		return nil
	}
	err := c.cluster.Close()
	if !closeComplete(c.cluster, err) {
		// A failed close leaves ownership with this state, but prevents a later
		// scenario from observing a partially closed fixture. A subsequent
		// CloseIfActive call can retry the underlying cleanup.
		c.closed = true
		return err
	}
	c.cluster = nil
	c.err = nil
	c.once = sync.Once{}
	c.closed = c.terminalClose
	c.sealed = false
	c.cleanupTried = false
	c.cleanupRegistrations = nil
	return err
}

func init() {
	stats.SkipPanicONDuplicate.Store(true)
}

// StartTestCluster constructs and starts an embedded cluster with test mode
// enabled. If startup fails, it closes the partially started cluster before
// returning the original error. If rollback itself fails, the returned cluster
// is non-nil solely so the caller can retain it and retry Close.
func StartTestCluster(opts ...Option) (Cluster, error) {
	opts = append([]Option{WithTesting()}, opts...)
	// Keep every embedded UT cluster on the short test-only readiness cadence.
	// Shared base clusters already use this callback, but dedicated scenarios
	// commonly provide their own pre-start adjustment and would otherwise fall
	// back to the production one-second polling intervals. Apply the cadence
	// first so an explicit scenario-specific value can still override it.
	opts = append(opts, func(c *cluster) {
		preStart := c.options.preStart
		c.options.preStart = func(svc ServiceOperator) {
			adjustClusterStartupRetryIntervals(svc)
			if preStart != nil {
				preStart(svc)
			}
		}
	})
	c, err := NewCluster(opts...)
	if err != nil {
		return cleanupClusterOnError(c, err)
	}
	if err := c.Start(); err != nil {
		return cleanupClusterOnError(c, err)
	}
	return c, nil
}

const (
	// Two CNs cover the multi-CN behavior exercised by the shared test
	// cluster. Tests that specifically validate three-CN topology start their
	// own cluster with an explicit CN count.
	basicClusterCNCount                        = 2
	basicClusterHAKeeperStoreTimeout           = 60 * time.Second
	basicClusterHAKeeperCheckInterval          = time.Second
	basicClusterHAKeeperBootstrapRetryInterval = 500 * time.Millisecond
	basicClusterServiceStartupRetryInterval    = 100 * time.Millisecond
	basicClusterTaskServiceReadyTimeout        = 30 * time.Second
)

func startBasicCluster(
	cnCount int,
	trace func(phase string, duration time.Duration, err error),
) (Cluster, error) {
	started := time.Now()
	c, err := StartTestCluster(
		WithCNCount(cnCount),
		WithPreStart(adjustBasicClusterService),
	)
	if trace != nil {
		trace("cluster-start", time.Since(started), err)
	}
	if err != nil {
		return c, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), basicClusterTaskServiceReadyTimeout)
	defer cancel()
	readyStarted := time.Now()
	if err := waitBasicClusterTaskServices(ctx, c, cnCount); err != nil {
		if trace != nil {
			trace("task-services-ready", time.Since(readyStarted), err)
		}
		return cleanupClusterOnError(c, err)
	}
	if trace != nil {
		trace("task-services-ready", time.Since(readyStarted), nil)
	}
	return c, nil
}

func adjustBasicClusterService(svc ServiceOperator) {
	adjustClusterStartupRetryIntervals(svc)

	switch svc.ServiceType() {
	case metadata.ServiceType_CN:
		svc.Adjust(
			func(config *ServiceConfig) {
				config.CN.LockService.MaxFixedSliceSize = 10001
				config.CN.LockService.MaxLockRowCount = 10000
				config.CN.Frontend.SkipCheckUser = false
				// The shared feature-test fixture is a fully upgraded cluster.
				config.CN.AutoIncrement.EnableAutoIDCache = true
				config.CN.Frontend.Iceberg.Enable = true
				config.CN.Frontend.Iceberg.EnableWrite = true
				config.CN.Frontend.Iceberg.EnableDelete = true
				config.CN.Frontend.Iceberg.EnableDML = true
				config.CN.Frontend.Iceberg.EnableMaintenance = true
			},
		)
	case metadata.ServiceType_LOG:
		svc.Adjust(
			func(config *ServiceConfig) {
				config.LogService.HAKeeperCheckInterval.Duration = basicClusterHAKeeperCheckInterval
				config.LogService.HAKeeperConfig.TNStoreTimeout.Duration =
					basicClusterHAKeeperStoreTimeout
				config.LogService.HAKeeperConfig.CNStoreTimeout.Duration =
					basicClusterHAKeeperStoreTimeout
			},
		)
	}
}

// adjustClusterStartupRetryIntervals keeps test-only cluster startup
// responsive while services are converging. These intervals only affect the
// polling cadence; readiness is still gated by the same HAKeeper state and
// shard conditions.
func adjustClusterStartupRetryIntervals(svc ServiceOperator) {
	switch svc.ServiceType() {
	case metadata.ServiceType_LOG:
		svc.Adjust(func(config *ServiceConfig) {
			config.LogService.HAKeeperBootstrapRetryInterval.Duration =
				basicClusterHAKeeperBootstrapRetryInterval
		})
	case metadata.ServiceType_TN:
		svc.Adjust(func(config *ServiceConfig) {
			config.HAKeeperRunningRetryInterval.Duration = basicClusterServiceStartupRetryInterval
		})
	case metadata.ServiceType_CN:
		svc.Adjust(func(config *ServiceConfig) {
			config.TNShardReadyRetryInterval.Duration = basicClusterServiceStartupRetryInterval
		})
	}
}

type taskServiceGetter interface {
	GetTaskService() (taskservice.TaskService, bool)
}

func waitBasicClusterTaskServices(ctx context.Context, c Cluster, cnCount int) error {
	for index := 0; index < cnCount; index++ {
		svc, err := c.GetCNService(index)
		if err != nil {
			return err
		}
		getter, ok := svc.RawService().(taskServiceGetter)
		if !ok {
			return moerr.NewInternalErrorNoCtxf(
				"CN %s does not expose its task service", svc.ServiceID())
		}
		if err := waitTaskServiceReady(ctx, getter, basicClusterServiceStartupRetryInterval); err != nil {
			return moerr.NewInternalErrorf(
				ctx, "CN %s task service did not become ready: %v", svc.ServiceID(), err)
		}
	}
	return nil
}

func waitTaskServiceReady(
	ctx context.Context,
	getter taskServiceGetter,
	retryInterval time.Duration,
) error {
	for {
		if service, ok := getter.GetTaskService(); ok && service != nil {
			return nil
		}
		if err := waitStartupRetry(ctx, retryInterval); err != nil {
			return err
		}
	}
}

func basicClusterSetupTracer(t testReporter, cnCount int) func(string, time.Duration, error) {
	return func(phase string, duration time.Duration, err error) {
		status := "ready"
		if err != nil {
			status = "error"
		}
		logTestSetup(t,
			"MO_UT_SETUP fixture=shared-cluster phase=%s cn_count=%d duration=%s status=%s",
			phase, cnCount, duration, status)
	}
}

// RunBaseClusterTests starting an integration test for a 1 log, 1tn, 2cn base cluster is very slow
// due to the amount of time it takes to start a cluster (10-20s) when there are a very large number
// of test cases. So for some special cases that don't need to be restarted, a basicCluster can be
// reused to run the test cases. in summary, the basic cluster will only be started once!
func RunBaseClusterTests(
	t testReporter,
	fn func(Cluster),
) {
	t.Helper()
	basicClusterState.Run(t, func() (Cluster, error) {
		return startBasicCluster(basicClusterCNCount, basicClusterSetupTracer(t, basicClusterCNCount))
	}, func(c Cluster) {
		fn(c)
	})
}

// CloseBaseClusterTests releases and resets the process-local shared two-CN
// fixture if it was initialized. Call only after RunBaseClusterTests returns,
// because Run holds the fixture mutex for the duration of its callback.
func CloseBaseClusterTests() error {
	return basicClusterState.CloseIfActive()
}

// RunSingleCNBaseClusterTests reuses the same base-cluster configuration with
// one CN. A test package should use this only when every shared-cluster case in
// that package is single-CN; mixing both helpers would start two fixtures and
// defeat the lifecycle saving.
func RunSingleCNBaseClusterTests(
	t testReporter,
	fn func(Cluster),
) {
	t.Helper()
	singleCNClusterState.Run(t, func() (Cluster, error) {
		return startBasicCluster(1, basicClusterSetupTracer(t, 1))
	}, func(c Cluster) {
		fn(c)
	})
}

// CloseSingleCNBaseClusterTests releases the process-local one-CN fixture if
// it was initialized. A successful release leaves the fixture reusable for a
// later test invocation, which keeps -count and shuffled test order valid. It
// is a lifecycle boundary for packages that mix the canonical shared one-CN
// suite with specialized clusters; unused fixtures are left reusable so a
// later shared test still initializes normally.
func CloseSingleCNBaseClusterTests() error {
	return singleCNClusterState.CloseIfActive()
}
