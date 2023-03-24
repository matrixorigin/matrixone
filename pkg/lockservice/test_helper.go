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
	adjustConfig func(*Config)) {
	testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntimeWithLevel(level))
	allocator := NewLockTableAllocator(testSockets, lockTableBindTimeout, morpc.Config{})
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
			[]metadata.DNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	for _, cfg := range configs {
		if adjustConfig != nil {
			adjustConfig(&cfg)
		}
		services = append(services,
			NewLockService(cfg).(*service))
	}
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
	table uint64,
	key []byte,
	waitersCount int) error {
	s := ls.(*service)
	v, err := s.getLockTable(table)
	if err != nil {
		return err
	}

	lb := v.(*localLockTable)
	lb.mu.Lock()
	lock, ok := lb.mu.store.Get(key)
	if !ok {
		panic("missing lock")
	}
	lb.mu.Unlock()
	for {
		if lock.waiter.waiters.len() == waitersCount {
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
}
