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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

func TestGetWithNoBind(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s1", Table: 1, Version: 1},
				a.Get("s1", 1))
		})
}

func TestGetWithBinded(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// register s1 first
			a.Get("s1", 1)
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s1", Table: 1, Version: 1},
				a.Get("s2", 1))
		})
}

func TestGetWithBindInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// register s1 first
			a.Get("s1", 1)
			a.disableTableBinds(a.getServiceBinds("s1"))
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s2", Table: 1, Version: 2},
				a.Get("s2", 1))
		})
}

func TestGetWithBindAndServiceBothInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// invalid table 1 bind
			a.Get("s1", 1)
			a.disableTableBinds(a.getServiceBinds("s1"))

			// invalid s2
			a.Get("s2", 2)
			a.getServiceBinds("s2").disable()

			assert.Equal(t,
				pb.LockTable{Valid: false, ServiceID: "s1", Table: 1, Version: 1},
				a.Get("s2", 1))
		})
}

func TestCheckTimeoutServiceTask(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Millisecond,
		func(a *lockTableAllocator) {
			// create s1 bind
			a.Get("s1", 1)

			// wait bind timeout
			for {
				time.Sleep(time.Millisecond * 10)
				binds := a.getServiceBinds("s1")
				if binds != nil {
					continue
				}
				a.mu.Lock()
				if len(a.mu.lockTables) > 0 {
					assert.Equal(t,
						pb.LockTable{ServiceID: "s1", Table: 1, Version: 1, Valid: false},
						a.mu.lockTables[1])
				}
				a.mu.Unlock()
				return
			}
		})
}

func TestKeepaliveBind(t *testing.T) {
	interval := time.Millisecond * 100
	runLockTableAllocatorTest(
		t,
		interval,
		func(a *lockTableAllocator) {
			c, err := NewClient(morpc.Config{})
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, err)
			}()

			a.Get("s1", 1)
			k := NewLockTableKeeper("s1", c, interval/5, interval/5, &sync.Map{})

			binds := a.getServiceBinds("s1")
			assert.NotNil(t, binds)

			time.Sleep(interval * 2)
			binds = a.getServiceBinds("s1")
			assert.NotNil(t, binds)

			assert.NoError(t, k.Close())

			for {
				a.mu.Lock()
				valid := a.mu.lockTables[1].Valid
				a.mu.Unlock()
				if !valid {
					break
				}
				time.Sleep(time.Millisecond * 20)
			}

			assert.False(t, a.KeepLockTableBind("s1"))
		})
}

func TestValid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 1)
			assert.True(t, a.Valid([]pb.LockTable{b}))
		})
}

func TestValidWithServiceInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 1)
			b.ServiceID = "s2"
			assert.False(t, a.Valid([]pb.LockTable{b}))
		})
}

func TestValidWithVersionChanged(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 1)
			b.Version++
			assert.False(t, a.Valid([]pb.LockTable{b}))
		})
}

func BenchmarkValid(b *testing.B) {
	runValidBenchmark(b, "1-table", 1)
	runValidBenchmark(b, "10-table", 10)
	runValidBenchmark(b, "100-table", 100)
	runValidBenchmark(b, "1000-table", 1000)
	runValidBenchmark(b, "10000-table", 10000)
	runValidBenchmark(b, "100000-table", 100000)
}

func runValidBenchmark(b *testing.B, name string, tables int) {
	b.Run(name, func(b *testing.B) {
		runtime.SetupProcessLevelRuntime(runtime.NewRuntime(
			metadata.ServiceType_CN,
			"",
			logutil.GetPanicLoggerWithLevel(zap.InfoLevel),
			runtime.WithClock(clock.NewHLCClock(func() int64 {
				return time.Now().UTC().UnixNano()
			}, 0))))
		a := NewLockTableAllocator(testSockets, time.Hour, morpc.Config{})
		defer func() {
			assert.NoError(b, a.Close())
		}()
		var binds []pb.LockTable
		for i := 0; i < tables; i++ {
			binds = append(binds, a.Get(fmt.Sprintf("s-%d", i), uint64(i)))
		}
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(p *testing.PB) {
			values := []pb.LockTable{{}}
			rand := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
			for p.Next() {
				v := rand.Intn(tables)
				values[0] = binds[v]
				a.Valid(values)
			}
		})
	})
}

func runLockTableAllocatorTest(
	t *testing.T,
	timeout time.Duration,
	fn func(*lockTableAllocator)) {
	require.NoError(t, os.RemoveAll(testSockets[7:]))
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID:          "s1",
					LockServiceAddress: testSockets,
				},
			},
			[]metadata.DNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	a := NewLockTableAllocator(testSockets, timeout, morpc.Config{})
	defer func() {
		assert.NoError(t, a.Close())
	}()
	fn(a.(*lockTableAllocator))
}
