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
	"math"
	"os"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

type fenceTestClock struct {
	upper timestamp.Timestamp
}

func (c *fenceTestClock) HasNetworkLatency() bool  { return false }
func (c *fenceTestClock) MaxOffset() time.Duration { return 0 }
func (c *fenceTestClock) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	return timestamp.Timestamp{}, c.upper
}
func (c *fenceTestClock) Update(timestamp.Timestamp) {}
func (c *fenceTestClock) SetNodeID(uint16)           {}

func TestGetBindInRestartService(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			a.setRestartService("s1")
			l := a.getLockTablesLocked(0)[1]
			assert.False(t, a.getServiceBinds(l.ServiceID).isStatus(pb.Status_ServiceLockEnable))
		})
}

func TestSetRestartService(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			a.setRestartService("s1")
			assert.True(t, a.canRestartService("s1"))
		})
}

func TestCanRestartService(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			a.setRestartService("s1")
			assert.False(t, a.canRestartService("s1"))
		})
}

func TestRemainTxnInService(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			a.setRestartService("s1")
			assert.Equal(t, int32(-1), a.remainTxnInService("s1"))
		})
}

func TestGetWithNoBind(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s1", Table: 1, OriginTable: 1, Version: a.version, AllocatorID: a.allocatorID},
				a.Get("s1", 0, 1, 0, pb.Sharding_None))
		})
}

func TestGetWithAlreadyBind(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// register s1 first
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s1", Table: 1, OriginTable: 1, Version: a.version, AllocatorID: a.allocatorID},
				a.Get("s2", 0, 1, 0, pb.Sharding_None))
		})
}

func TestGetWithBindInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// register s1 first
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			a.disableTableBinds(a.getServiceBinds("s1"))
			assert.Equal(t,
				pb.LockTable{Valid: true, ServiceID: "s2", Table: 1, OriginTable: 1, Version: a.version + 1, AllocatorID: a.allocatorID},
				a.Get("s2", 0, 1, 0, pb.Sharding_None))
		})
}

func TestGetWithBindAndServiceBothInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// invalid table 1 bind
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			a.disableTableBinds(a.getServiceBinds("s1"))

			// invalid s2
			a.Get("s2", 0, 2, 0, pb.Sharding_None)
			a.getServiceBinds("s2").disable()

			assert.Equal(t,
				pb.LockTable{Valid: false, ServiceID: "s1", Table: 1, OriginTable: 1, Version: a.version, AllocatorID: a.allocatorID},
				a.Get("s2", 0, 1, 0, pb.Sharding_None))
		})
}

func TestCheckTimeoutServiceTask(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Millisecond,
		func(a *lockTableAllocator) {
			// create s1 bind
			a.Get("s2", 0, 1, 0, pb.Sharding_None)

			for {
				bind := a.GetLatest(0, 1)
				if !bind.Valid {
					return
				}
				time.Sleep(time.Millisecond * 10)
			}
		})
}

func TestKeepaliveBind(t *testing.T) {
	interval := time.Millisecond * 100
	runLockTableAllocatorTest(
		t,
		interval,
		func(a *lockTableAllocator) {
			c, err := NewClient("", morpc.Config{})
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, c.Close())
			}()

			bind := a.Get("s1", 0, 1, 0, pb.Sharding_None)
			m := &lockTableHolders{service: "s1", holders: map[uint32]*lockTableHolder{}}
			m.set(
				0,
				1,
				newRemoteLockTable(
					"s1",
					time.Second,
					bind,
					c,
					func(lt pb.LockTable) {},
					runtime.DefaultRuntime().Logger(),
				),
			)
			k := NewLockTableKeeper("s1", c, interval/5, interval/5, m, &service{logger: runtime.GetLogger("")})

			binds := a.getServiceBinds("s1")
			assert.NotNil(t, binds)

			time.Sleep(interval * 2)
			binds = a.getServiceBinds("s1")
			assert.NotNil(t, binds)

			assert.NoError(t, k.Close())

			for {
				a.mu.Lock()
				valid := a.getLockTablesLocked(0)[1].Valid
				a.mu.Unlock()
				if valid {
					break
				}
				time.Sleep(time.Millisecond * 20)
			}

			assert.True(t, a.KeepLockTableBind("s1"))
		})
}

func TestGetTimeoutBindsRequiresGracePeriod(t *testing.T) {
	interval := time.Millisecond * 100
	runLockTableAllocatorTest(
		t,
		interval,
		func(a *lockTableAllocator) {
			a.Get("s1", 0, 1, 0, pb.Sharding_None)
			binds := a.getServiceBinds("s1")
			require.NotNil(t, binds)

			now := time.Now()
			binds.Lock()
			binds.lastKeepaliveTime = now.Add(-interval - time.Millisecond)
			binds.Unlock()
			require.Empty(t, a.getTimeoutBinds(now))

			binds.Lock()
			binds.lastKeepaliveTime = now.Add(-2*interval - time.Millisecond)
			binds.Unlock()
			require.Len(t, a.getTimeoutBinds(now), 1)
		})
}

func TestValid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			valid, _ := a.Valid("s1", []byte{}, []pb.LockTable{b})
			assert.Empty(t, valid)

			c := a.getCtl("s1")
			state, ok := c.getCommitState(string([]byte{}))
			require.True(t, ok)
			require.Equal(t, committingState, state.state)
			require.Equal(t, uint32(1), state.inflight)
		})
}

func TestCannotCommitWaitsForAllCommitAttempts(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			txnID := []byte("txn1")
			bind := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			for i := 0; i < 2; i++ {
				invalid, err := a.Valid("s1", txnID, []pb.LockTable{bind})
				require.NoError(t, err)
				require.Empty(t, invalid)
			}

			orphan := []pb.OrphanTxn{{Service: "s1", Txn: [][]byte{txnID}}}
			require.Equal(t, [][]byte{txnID}, a.AddCannotCommit(orphan))
			a.FinishCommit("s1", txnID)
			require.Equal(t, [][]byte{txnID}, a.AddCannotCommit(orphan))
			a.FinishCommit("s1", txnID)
			require.Empty(t, a.AddCannotCommit(orphan))

			_, err := a.Valid("s1", txnID, []pb.LockTable{bind})
			require.Error(t, err)
		})
}

func TestCleanCommitStateKeepsInflightCommit(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			txnID := []byte("txn1")
			bind := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			_, err := a.Valid("s1", txnID, []pb.LockTable{bind})
			require.NoError(t, err)

			a.ctlMu.Lock()
			a.cleanCtlLocked("s1", nil, false, a.getCtl("s1").currentGeneration())
			a.ctlMu.Unlock()

			state, ok := a.getCtl("s1").getCommitState(string(txnID))
			require.True(t, ok)
			require.Equal(t, uint32(1), state.inflight)
			require.Equal(t, [][]byte{txnID}, a.AddCannotCommit([]pb.OrphanTxn{{
				Service: "s1",
				Txn:     [][]byte{txnID},
			}}))
		})
}

func TestCleanCommitStatePreservesCannotCommitWhenServiceStateUnknown(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			c := a.getCtl("s1")
			require.Equal(t, cannotCommitState, c.tryCannotCommit("txn1"))

			a.ctlMu.Lock()
			a.cleanCtlLocked("s1", nil, true, c.currentGeneration())
			a.ctlMu.Unlock()
			state, ok := c.getCommitState("txn1")
			require.True(t, ok)
			require.Equal(t, cannotCommitState, state.state)

			a.ctlMu.Lock()
			a.cleanCtlLocked("s1", nil, false, c.currentGeneration())
			a.ctlMu.Unlock()
			_, ok = c.getCommitState("txn1")
			require.False(t, ok)
		})
}

func TestCleanerPreservesCannotCommitOnActiveTxnQueryError(t *testing.T) {
	ready := make(chan struct{})
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			c := a.getCtl("s1")
			require.Equal(t, cannotCommitState, c.tryCannotCommit("cannot"))
			require.Equal(t, committingState, c.beginCommit("inflight"))
			close(ready)

			require.Eventually(t, func() bool {
				return a.HasInvalidService("s1")
			}, time.Second*3, time.Millisecond*10)
			state, exists := c.getCommitState("cannot")
			require.True(t, exists)
			require.Equal(t, cannotCommitState, state.state)
			require.Equal(t, [][]byte{[]byte("inflight")}, a.AddCannotCommit([]pb.OrphanTxn{{
				Service: "s1",
				Txn:     [][]byte{[]byte("inflight")},
			}}))
			a.FinishCommit("s1", []byte("inflight"))
		},
		func(lta *lockTableAllocator) {
			lta.keepBindTimeout = time.Millisecond * 20
			lta.options.getActiveTxnFunc = func(string) (bool, [][]byte, error) {
				<-ready
				return false, nil, moerr.NewBackendClosedNoCtx()
			}
		})
}

func TestCannotCommitGenerationRefresh(t *testing.T) {
	c := &commitCtl{}
	require.Equal(t, cannotCommitState, c.tryCannotCommit("refreshed"))
	require.Equal(t, cannotCommitState, c.tryCannotCommit("rejected"))
	watermark := c.currentGeneration()

	require.Equal(t, cannotCommitState, c.tryCannotCommit("refreshed"))
	require.Equal(t, cannotCommitState, c.beginCommit("rejected"))
	c.clean(nil, false, watermark, getLogger(""))

	_, ok := c.getCommitState("refreshed")
	require.True(t, ok)
	_, ok = c.getCommitState("rejected")
	require.True(t, ok)
}

func TestCommitCtlGenerationOverflowFailsClosed(t *testing.T) {
	c := &commitCtl{generation: math.MaxUint64 - 1}
	require.Equal(t, cannotCommitState, c.tryCannotCommit("txn1"))
	require.Equal(t, uint64(math.MaxUint64), c.currentGeneration())

	c.clean(nil, false, math.MaxUint64, getLogger(""))
	_, ok := c.getCommitState("txn1")
	require.True(t, ok)
	require.Equal(t, uint64(math.MaxUint64), c.currentGeneration())
}

func TestGetCtlGenerationWithMissingService(t *testing.T) {
	a := &lockTableAllocator{}
	_, ok := a.getCtlGeneration("missing")
	require.False(t, ok)
}

func TestNewFenceTSDominatesUpperLogicalTimestamp(t *testing.T) {
	upper := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7, NodeID: 3}
	a := &lockTableAllocator{clock: &fenceTestClock{upper: upper}}
	fenceTS := a.newFenceTS()
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 101, NodeID: 3}, fenceTS)
	require.True(t, upper.Less(fenceTS))
}

func TestNewFenceTSFailsClosedAtPhysicalLimit(t *testing.T) {
	a := &lockTableAllocator{clock: &fenceTestClock{
		upper: timestamp.Timestamp{PhysicalTime: math.MaxInt64, LogicalTime: 7},
	}}
	require.True(t, a.newFenceTS().IsEmpty())

	a.clock = nil
	require.True(t, a.newFenceTS().IsEmpty())
}

func TestValidWithServiceInvalid(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			b.ServiceID = "s2"
			valid, _ := a.Valid("", []byte{}, []pb.LockTable{b})
			assert.NotEmpty(t, valid)
		})
}

func TestValidWithVersionChanged(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			b := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			b.Version++
			valid, _ := a.Valid("", []byte{}, []pb.LockTable{b})
			assert.NotEmpty(t, valid)
		})
}

func TestValidWithCannotCommitState(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			txn := []byte{1}

			c := a.getCtl("s1")
			require.Equal(t, cannotCommitState, c.tryCannotCommit(string(txn)))

			b := a.Get("s1", 0, 4, 0, pb.Sharding_None)
			_, err := a.Valid("s1", txn, []pb.LockTable{b})
			assert.Error(t, err)
		})
}

func TestCtlCanRemovedByInvalidService(t *testing.T) {
	ch := make(chan struct{})
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			c := a.getCtl("s1")
			c.beginCommit("t1")
			require.True(t, c.finishCommit("t1"))
			close(ch)
			for {
				n := 0
				a.ctl.Range(func(key, value any) bool {
					n++
					return true
				})
				if n == 0 {
					return
				}
			}
		},
		func(lta *lockTableAllocator) {
			lta.keepBindTimeout = time.Millisecond * 100
			lta.options.getActiveTxnFunc = func(sid string) (bool, [][]byte, error) {
				<-ch
				return false, nil, nil
			}
		})
}

func TestCtlCanRemovedByNotActiveTxn(t *testing.T) {
	ch := make(chan struct{})
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			c := a.getCtl("s1")
			c.tryCannotCommit("t1")
			c.tryCannotCommit("t2")
			close(ch)
			for {
				if c.size() == 1 {
					return
				}
			}
		},
		func(lta *lockTableAllocator) {
			lta.keepBindTimeout = time.Millisecond * 100
			lta.options.getActiveTxnFunc = func(sid string) (bool, [][]byte, error) {
				<-ch
				return true, [][]byte{[]byte("t2")}, nil
			}
		})
}

func TestTryRebindLocked(t *testing.T) {
	runLockTableAllocatorTest(
		t,
		time.Hour,
		func(a *lockTableAllocator) {
			// Test case 1: New service has newer timestamp and same UUID
			oldServiceID := "1234567890123456789uuid1"
			newServiceID := "1234567890123456790uuid1" // newer timestamp
			old := pb.LockTable{
				Table:     1,
				ServiceID: oldServiceID,
				Valid:     true,
				Version:   1,
			}
			binds := a.registerService(newServiceID)
			result := a.tryRebindLocked(binds, 0, old, 1)
			assert.True(t, result.Valid)
			assert.Equal(t, newServiceID, result.ServiceID)
			assert.Equal(t, uint64(2), result.Version)

			// Test case 2: Old binding is still valid
			old = pb.LockTable{
				Table:     2,
				ServiceID: "s1",
				Valid:     true,
				Version:   1,
			}
			binds = a.registerService("s2")
			result = a.tryRebindLocked(binds, 0, old, 2)
			assert.True(t, result.Valid)
			assert.Equal(t, "s1", result.ServiceID)
			assert.Equal(t, uint64(1), result.Version)

			// Test case 3: Old binding is invalid and new service can bind
			old = pb.LockTable{
				Table:     3,
				ServiceID: "s3",
				Valid:     false,
				Version:   1,
			}
			binds = a.registerService("s4")
			result = a.tryRebindLocked(binds, 0, old, 3)
			assert.True(t, result.Valid)
			assert.Equal(t, "s4", result.ServiceID)
			assert.Equal(t, uint64(2), result.Version)

			// Test case 4: Old binding is invalid and new service cannot bind
			old = pb.LockTable{
				Table:     4,
				ServiceID: "s5",
				Valid:     false,
				Version:   1,
			}
			binds = a.registerService("s6")
			binds.disable() // Make the new service unable to bind
			result = a.tryRebindLocked(binds, 0, old, 4)
			assert.False(t, result.Valid)
			assert.Equal(t, "s5", result.ServiceID)
			assert.Equal(t, uint64(1), result.Version)
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
		runtime.SetupServiceBasedRuntime(
			"",
			runtime.NewRuntime(
				metadata.ServiceType_CN,
				"",
				logutil.GetPanicLoggerWithLevel(zap.InfoLevel),
				runtime.WithClock(clock.NewHLCClock(func() int64 {
					return time.Now().UTC().UnixNano()
				}, 0))),
		)
		testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
		a := NewLockTableAllocator("", testSockets, time.Hour, morpc.Config{})
		defer func() {
			assert.NoError(b, a.Close())
		}()
		var binds []pb.LockTable
		for i := 0; i < tables; i++ {
			binds = append(binds, a.Get(fmt.Sprintf("s-%d", i), 0, uint64(i), 0, pb.Sharding_None))
		}
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(p *testing.PB) {
			values := []pb.LockTable{{}}
			rand := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
			for p.Next() {
				v := rand.Intn(tables)
				values[0] = binds[v]
				a.Valid("", []byte{}, values)
			}
		})
	})
}

func runLockTableAllocatorTest(
	t *testing.T,
	timeout time.Duration,
	fn func(*lockTableAllocator),
	opts ...AllocatorOption,
) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			reuse.RunReuseTests(func() {
				defer leaktest.AfterTest(t)()
				testSockets := fmt.Sprintf("unix:///tmp/%d.sock", time.Now().Nanosecond())
				require.NoError(t, os.RemoveAll(testSockets[7:]))
				cluster := clusterservice.NewMOCluster(
					sid,
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
						[]metadata.TNService{
							{
								LockServiceAddress: testSockets,
							},
						}))
				runtime.ServiceRuntime(sid).SetGlobalVariables(runtime.ClusterService, cluster)
				defer cluster.Close()

				a := NewLockTableAllocator(sid, testSockets, timeout, morpc.Config{}, opts...)
				defer func() {
					assert.NoError(t, a.Close())
				}()
				fn(a.(*lockTableAllocator))
			})
		},
	)

}
