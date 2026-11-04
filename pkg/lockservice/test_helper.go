// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap/zapcore"
)

// RunLockServicesForTest is used to start a lock table allocator and some
// lock services for test
func RunLockServicesForTest(
	level zapcore.Level,
	serviceIDs []string,
	lockTableBindTimeout time.Duration,
	fn func(LockTableAllocator, []LockService),
	adjustConfig func(*Config),
	opts ...Option,
) {
	runLockServicesForTest(level, serviceIDs, lockTableBindTimeout,
		func(allocator LockTableAllocator, services []LockService, _ func(...AllocatorOption) (LockTableAllocator, error)) {
			fn(allocator, services)
		}, adjustConfig, opts...)
}

// runLockServicesForTest keeps replacement allocators under the same teardown
// owner as the original. Restart is called only by the scenario goroutine.
func runLockServicesForTest(
	level zapcore.Level,
	serviceIDs []string,
	lockTableBindTimeout time.Duration,
	fn func(LockTableAllocator, []LockService, func(...AllocatorOption) (LockTableAllocator, error)),
	adjustConfig func(*Config),
	opts ...Option,
) {
	defaultLazyCheckDuration.Store(time.Millisecond * 50)
	testSocketDir, err := createTestSocketDir()
	if err != nil {
		panic(err)
	}
	cleanup := testTopologyCleanup{socketDir: testSocketDir}
	defer func() {
		panicValue := recover()
		cleanupErr := cleanup.close()
		if panicValue != nil {
			panic(panicValue)
		}
		if cleanupErr != nil {
			panic(cleanupErr)
		}
	}()
	testSockets := testSocketAddress(testSocketDir, "allocator.sock")
	services := make([]LockService, 0, len(serviceIDs))
	cns := make([]metadata.CNService, 0, len(serviceIDs))
	configs := make([]Config, 0, len(serviceIDs))
	for idx, v := range serviceIDs {
		runtime.SetupServiceBasedRuntime(v, runtime.ServiceRuntime(""))
		address := testSocketAddress(testSocketDir, "service-"+strconv.Itoa(idx)+".sock")
		cns = append(cns, metadata.CNService{
			ServiceID:          v,
			LockServiceAddress: address,
		})
		configs = append(configs, Config{ServiceID: v, ListenAddress: address})
	}

	cluster := clusterservice.NewMOCluster(
		"",
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			cns,
			[]metadata.TNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, cluster)
	cleanup.clusterCloser = cluster.Close

	var removeDisconnectDuration time.Duration
	for idx := range configs {
		cfg := &configs[idx]
		if adjustConfig != nil {
			adjustConfig(cfg)
			removeDisconnectDuration = cfg.removeDisconnectDuration
		}
	}

	// Service keepers start during construction and may immediately contact the
	// allocator. Publish its listener first so initial RPCs cannot race startup.
	constructAllocator := func(extra ...AllocatorOption) LockTableAllocator {
		allocatorOpts := make([]AllocatorOption, 0, 1+len(extra))
		allocatorOpts = append(allocatorOpts, func(lta *lockTableAllocator) {
			lta.options.removeDisconnectDuration = removeDisconnectDuration
		})
		allocatorOpts = append(allocatorOpts, extra...)
		return NewLockTableAllocator("", testSockets, lockTableBindTimeout, morpc.Config{},
			func(owner LockTableAllocator) { cleanup.allocatorCloser = owner.Close }, allocatorOpts...)
	}
	allocator := constructAllocator()
	restart := func(extra ...AllocatorOption) (LockTableAllocator, error) {
		if err := cleanup.allocatorCloser(); err != nil {
			return nil, err
		}
		// The closed allocator leaves every service with at least one pooled
		// rpc backend whose connection the close severed. Probe each service in
		// the dead-listener window until the probe provably reaches the dialer,
		// so the first scenario RPC after restart cannot ride the dead
		// connection while the rebind is still unpublished to the client.
		if err := drainPooledAllocatorBackends(services); err != nil {
			return nil, err
		}
		return constructAllocator(extra...), nil
	}
	for _, cfg := range configs {
		lockService := NewLockService(cfg, opts...)
		services = append(services, lockService)
		cleanup.serviceClosers = append(cleanup.serviceClosers, lockService.Close)
	}
	fn(allocator, services, restart)
}

const (
	// restartDrainDeadline bounds one restart's wait for every service to
	// observe the closed allocator. Healthy legs resolve in microseconds, so
	// the bound only guards a wedged backend against hanging a restart.
	restartDrainDeadline = 10 * time.Second
	// restartDrainInterval is the pause between drain probes. It trades a
	// little latency for giving the rpc client's read loop the first chance
	// to observe the severed connection itself.
	restartDrainInterval = time.Millisecond
	// allocatorDrainProbeTable names a table far outside every scenario's
	// range so drain probes stay distinguishable from real fixtures even when
	// a probe is issued outside the dead-listener window.
	allocatorDrainProbeTable = uint64(1) << 62
)

// drainPooledAllocatorBackends removes every service's pooled connection to
// the just-closed allocator. It must run between the old allocator's close and
// the new allocator's construction, the only window in which the probe below
// cannot reach any server and therefore has no allocator-visible effect. The
// raw write error a severed backend first reports ("write: broken pipe") is
// not distinguishable from a backend the client has already retired, so the
// loop continues until a probe returns the dialer's typed verdict
// (moerr.ErrBackendCannotConnect on the unbound socket path). That verdict
// proves the stale backend has left the client pool, so the next
// allocator-directed RPC dials the replacement listener instead of writing
// through the dead connection.
func drainPooledAllocatorBackends(services []LockService) error {
	ctx, cancel := context.WithTimeout(context.Background(), restartDrainDeadline)
	defer cancel()
	for _, s := range services {
		if err := drainPooledAllocatorBackendForService(ctx, s.(*service)); err != nil {
			return err
		}
	}
	return nil
}

func drainPooledAllocatorBackendForService(ctx context.Context, s *service) error {
	return drainUntilProbeDials(ctx, func() error {
		return sendAllocatorDrainProbe(ctx, s)
	})
}

// drainUntilProbeDials keeps issuing one dead-path probe until the probe
// error proves a fresh dial failed. Any connection-level failure on the old
// backend leads to a retry; only the dialer's verdict completes the drain.
func drainUntilProbeDials(ctx context.Context, probe func() error) error {
	var lastErr error
	for {
		lastErr = probe()
		if lastErr == nil {
			return moerr.NewInternalErrorNoCtx(
				"allocator drain probe unexpectedly succeeded before the replacement listener was constructed")
		}
		if moerr.IsMoErrCode(lastErr, moerr.ErrBackendCannotConnect) {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-time.After(restartDrainInterval):
		}
	}
}

// sendAllocatorDrainProbe issues one allocator get-bind request through the
// service's own client. During the drain window the allocator socket path has
// no listener, so the request can only fail on the connection or dial leg and
// can never reach an allocator.
func sendAllocatorDrainProbe(ctx context.Context, s *service) error {
	req := acquireRequest()
	defer releaseRequest(req)
	req.Method = pb.Method_GetBind
	req.GetBind.ServiceID = s.serviceID
	req.GetBind.Table = allocatorDrainProbeTable
	req.GetBind.OriginTable = allocatorDrainProbeTable
	req.GetBind.Group = 0
	req.GetBind.Sharding = pb.Sharding_None
	resp, err := s.remote.client.Send(ctx, req)
	if err != nil {
		return err
	}
	defer releaseResponse(resp)
	return nil
}

type testTopologyCleanup struct {
	serviceClosers  []func() error
	allocatorCloser func() error
	clusterCloser   func()
	socketDir       string
}

func (c *testTopologyCleanup) close() error {
	var cleanupErr error
	for _, closeService := range c.serviceClosers {
		cleanupErr = errors.Join(cleanupErr, closeService())
	}
	if c.allocatorCloser != nil {
		cleanupErr = errors.Join(cleanupErr, c.allocatorCloser())
	}
	if c.clusterCloser != nil {
		c.clusterCloser()
	}
	if c.socketDir != "" {
		cleanupErr = errors.Join(cleanupErr, removeTestSocketDir(c.socketDir))
	}
	return cleanupErr
}

func createTestSocketDir() (string, error) {
	return os.MkdirTemp("/tmp", "mo-lockservice-")
}

func removeTestSocketDir(dir string) error {
	return os.RemoveAll(dir)
}

func testSocketAddress(dir, name string) string {
	return "unix://" + filepath.Join(dir, name)
}

// WaitWaiters wait waiters
func WaitWaiters(
	ls LockService,
	group uint32,
	table uint64,
	key []byte,
	waitersCount int) error {
	s := ls.(*service)
	v, err := s.getLockTable(context.Background(), group, table)
	if err != nil {
		return err
	}

	return waitLocalWaiters(v.(*localLockTable), key, waitersCount)
}

func waitLocalWaiters(
	lt *localLockTable,
	key []byte,
	waitersCount int) error {
	return waitLocalWaitersWithTimeout(lt, key, waitersCount, 10*time.Second)
}

func waitLocalWaitersWithTimeout(
	lt *localLockTable,
	key []byte,
	waitersCount int,
	waitTimeout time.Duration) error {
	observedWaiters := 0
	fn := func() bool {
		lt.mu.Lock()
		defer lt.mu.Unlock()

		lock, ok := lt.mu.store.Get(key)
		if waitersCount == 0 && !ok {
			return true
		}

		if !ok {
			observedWaiters = 0
			return false
		}

		observedWaiters = 0
		lock.waiters.iter(func(*waiter) bool {
			observedWaiters++
			return true
		})
		return observedWaiters == waitersCount
	}

	timeout := time.NewTimer(waitTimeout)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if fn() {
			return nil
		}
		select {
		case <-timeout.C:
			return moerr.NewInternalErrorNoCtxf(
				"timed out waiting for %d local lock waiters, observed %d",
				waitersCount,
				observedWaiters)
		case <-ticker.C:
		}
	}
}
