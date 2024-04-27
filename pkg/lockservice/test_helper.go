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
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
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
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntimeWithLevel(level))
	services := make([]LockService, 0, len(serviceIDs))
	cns := make([]metadata.CNService, 0, len(serviceIDs))
	configs := make([]Config, 0, len(serviceIDs))
	for _, v := range serviceIDs {
		address := fmt.Sprintf("unix:///tmp/service-%d-%s.sock",
			time.Now().Nanosecond(), v)
		if err := os.RemoveAll(address[7:]); err != nil {
			panic(err)
		}
		cns = append(cns, metadata.CNService{
			ServiceID:          v,
			LockServiceAddress: address,
		})
		configs = append(configs, Config{ServiceID: v, ListenAddress: address})
	}
	cluster := clusterservice.NewMOCluster(
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
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	var removeDisconnectDuration time.Duration
	for _, cfg := range configs {
		if adjustConfig != nil {
			adjustConfig(&cfg)
			removeDisconnectDuration = cfg.removeDisconnectDuration
		}
		services = append(services,
			NewLockService(cfg, opts...).(*service))
	}
	allocator := NewLockTableAllocator(testSockets, lockTableBindTimeout, morpc.Config{}, func(lta *lockTableAllocator) {
		lta.options.removeDisconnectDuration = removeDisconnectDuration
	})
	fn(allocator.(*lockTableAllocator), services)

	for _, s := range services {
		if err := s.Close(); err != nil {
			panic(err)
		}
	}
	if err := allocator.Close(); err != nil {
		panic(err)
	}
}

// WaitWaiters wait waiters
func WaitWaiters(
	ls LockService,
	group uint32,
	table uint64,
	key []byte,
	waitersCount int) error {
	s := ls.(*service)
	v, err := s.getLockTable(group, table)
	if err != nil {
		return err
	}

	return waitLocalWaiters(v.(*localLockTable), key, waitersCount)
}

func waitLocalWaiters(
	lt *localLockTable,
	key []byte,
	waitersCount int) error {
	fn := func() bool {
		lt.mu.Lock()
		defer lt.mu.Unlock()

		lock, ok := lt.mu.store.Get(key)
		if waitersCount == 0 && !ok {
			return true
		}

		if !ok {
			panic("missing lock")
		}

		waiters := make([]*waiter, 0)
		lock.waiters.iter(func(w *waiter) bool {
			waiters = append(waiters, w)
			return true
		})
		return len(waiters) == waitersCount
	}

	for {
		if fn() {
			return nil
		}
		time.Sleep(time.Millisecond * 10)
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
