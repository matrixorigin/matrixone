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
	for _, cfg := range configs {
		if adjustConfig != nil {
			adjustConfig(&cfg)
			removeDisconnectDuration = cfg.removeDisconnectDuration
		}
		lockService := NewLockService(cfg, opts...)
		services = append(services, lockService)
		cleanup.serviceClosers = append(cleanup.serviceClosers, lockService.Close)
	}

	allocator := NewLockTableAllocator(
		"",
		testSockets,
		lockTableBindTimeout,
		morpc.Config{},
		func(lta *lockTableAllocator) {
			lta.options.removeDisconnectDuration = removeDisconnectDuration
		},
	)
	cleanup.allocatorCloser = allocator.Close
	fn(allocator.(*lockTableAllocator), services)
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

func checkLocalWaitersStatus(
	lt *localLockTable,
	key []byte,
	status []waiterStatus) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lock, ok := lt.mu.store.Get(key)
	if !ok {
		panic("missing lock")
	}

	if lock.waiters.size() != len(status) {
		return false
	}

	i := 0
	statusCheckOK := true
	lock.waiters.iter(func(w *waiter) bool {
		if statusCheckOK {
			statusCheckOK = w.getStatus() == status[i]
		}
		i++
		return true
	})
	return statusCheckOK
}
