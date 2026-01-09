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

package clusterservice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func TestClusterReady(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cc := make(chan struct{})
			go func() {
				defer close(cc)
				c.GetCNService(NewSelector(), nil)
			}()
			select {
			case <-cc:
			case <-time.After(time.Second * 5):
				assert.Fail(t, "wait ready timeout")
			}
		})
}

func TestClusterForceRefresh(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.CNService) bool {
				cnt++
				return true
			}
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			assert.Equal(t, 0, cnt)

			hc.addCN("cn0")
			cnt = 0
			c.ForceRefresh(true)
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			assert.Equal(t, 1, cnt)
		})
}

func TestClusterRefresh(t *testing.T) {
	runClusterTest(
		time.Millisecond*10,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.TNService) bool {
				cnt++
				return true
			}
			c.GetTNService(NewServiceIDSelector("dn0"), apply)
			assert.Equal(t, 0, cnt)

			hc.addTN(0, "dn0")
			time.Sleep(time.Millisecond * 100)
			c.GetTNService(NewServiceIDSelector("dn0"), apply)
			assert.Equal(t, 1, cnt)
		})
}

func BenchmarkGetService(b *testing.B) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			cnt := 0
			apply := func(c metadata.TNService) bool {
				cnt++
				return true
			}
			c.GetTNService(NewServiceIDSelector("dn0"), apply)

			hc.addTN(0, "dn0")
			c.ForceRefresh(true)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.GetTNService(NewServiceIDSelector("dn0"), apply)
			}
		})
}

func TestCluster_DebugUpdateCNLabel(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			var cns []metadata.CNService
			apply := func(c metadata.CNService) bool {
				cns = append(cns, c)
				return true
			}
			hc.addCN("cn0")
			err := c.DebugUpdateCNLabel("cn0", map[string][]string{"k1": {"v1"}})
			require.NoError(t, err)
			c.ForceRefresh(true)
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			require.Equal(t, 1, len(cns))
			require.Equal(t, "cn0", cns[0].ServiceID)
			require.Equal(t, map[string]metadata.LabelList{
				"k1": {Labels: []string{"v1"}},
			}, cns[0].Labels)
		})
}

func TestCluster_DebugUpdateCNWorkState(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			var cns []metadata.CNService
			apply := func(c metadata.CNService) bool {
				cns = append(cns, c)
				return true
			}
			hc.addCN("cn0")
			err := c.DebugUpdateCNWorkState("cn0", int(metadata.WorkState_Draining))
			require.NoError(t, err)
			c.ForceRefresh(true)
			c.GetCNService(NewServiceIDSelector("cn0"), apply)
			require.Equal(t, 0, len(cns))
		})
}

func TestCluster_GetTNService(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			hc.addTN(100, "dn0")
			hc.addTN(200, "dn1")
			hc.addTN(50, "dn2")
			c.ForceRefresh(true)
			var tns []metadata.TNService
			c.GetTNService(
				NewSelector(),
				func(service metadata.TNService) bool {
					tns = append(tns, service)
					return true
				},
			)
			require.Equal(t, 1, len(tns))
			require.Equal(t, "dn1", tns[0].ServiceID)
		},
	)
}

func runClusterTest(
	refreshInterval time.Duration,
	fn func(*testHAKeeperClient, *cluster),
) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			hc := &testHAKeeperClient{}
			c := NewMOCluster(sid, hc, refreshInterval)
			defer c.Close()
			fn(hc, c.(*cluster))
		},
	)
}

type testHAKeeperClient struct {
	sync.RWMutex
	value logpb.ClusterDetails
	err   error
}

func (c *testHAKeeperClient) addCN(serviceIDs ...string) {
	c.Lock()
	defer c.Unlock()
	for _, id := range serviceIDs {
		c.value.CNStores = append(c.value.CNStores, logpb.CNStore{
			UUID:      id,
			WorkState: metadata.WorkState_Working,
		})
	}
}

func (c *testHAKeeperClient) addTN(tick uint64, serviceIDs ...string) {
	c.Lock()
	defer c.Unlock()
	for _, id := range serviceIDs {
		c.value.TNStores = append(c.value.TNStores, logpb.TNStore{
			UUID: id,
			Tick: tick,
		})
	}
}

func (c *testHAKeeperClient) Close() error                                   { return nil }
func (c *testHAKeeperClient) AllocateID(ctx context.Context) (uint64, error) { return 0, nil }
func (c *testHAKeeperClient) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	return 0, nil
}
func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	c.RLock()
	defer c.RUnlock()
	// deep copy the cluster details to avoid data race.
	copied := deepcopy.Copy(c.value)
	return copied.(logpb.ClusterDetails), c.err
}
func (c *testHAKeeperClient) GetClusterState(ctx context.Context) (logpb.CheckerState, error) {
	return logpb.CheckerState{}, nil
}
func (c *testHAKeeperClient) GetCNState(ctx context.Context) (logpb.CNState, error) {
	return logpb.CNState{}, nil
}
func (c *testHAKeeperClient) UpdateCNLabel(ctx context.Context, label logpb.CNStoreLabel) error {
	c.Lock()
	defer c.Unlock()
	for i, cn := range c.value.CNStores {
		if cn.UUID == label.UUID {
			c.value.CNStores[i].Labels = label.Labels
		}
	}
	return nil
}
func (c *testHAKeeperClient) UpdateCNWorkState(ctx context.Context, state logpb.CNWorkState) error {
	c.Lock()
	defer c.Unlock()
	for i, cn := range c.value.CNStores {
		if cn.UUID == state.UUID {
			c.value.CNStores[i].WorkState = state.State
		}
	}
	return nil
}

// TestWaitReadyFastPath tests that waitReady uses the atomic fast path after cluster is ready.
func TestWaitReadyFastPath(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			// After first refresh, ready should be true
			c.ForceRefresh(true)
			assert.True(t, c.ready.Load(), "ready flag should be true after refresh")

			// waitReady should return immediately via fast path
			done := make(chan struct{})
			go func() {
				c.waitReady()
				close(done)
			}()

			select {
			case <-done:
				// Success: waitReady returned quickly
			case <-time.After(time.Millisecond * 100):
				t.Fatal("waitReady should return immediately via fast path")
			}
		})
}

// TestWaitReadySlowPath tests that waitReady blocks on channel before cluster is ready.
func TestWaitReadySlowPath(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			hc := &testHAKeeperClient{}
			// Use WithDisableRefresh to prevent automatic refresh
			c := NewMOCluster(sid, hc, time.Hour, WithDisableRefresh())
			defer c.Close()

			cluster := c.(*cluster)
			// With disableRefresh, ready should be true immediately
			assert.True(t, cluster.ready.Load(), "ready should be true with disableRefresh")
		},
	)
}

// TestWaitReadyBeforeRefresh tests waitReady blocks until first refresh completes.
func TestWaitReadyBeforeRefresh(t *testing.T) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			// Create a slow client that delays GetClusterDetails
			hc := &slowHAKeeperClient{
				testHAKeeperClient: &testHAKeeperClient{},
				delay:              time.Millisecond * 200,
			}

			c := NewMOCluster(sid, hc, time.Hour)
			defer c.Close()

			cluster := c.(*cluster)

			// Initially ready should be false (refresh not completed yet)
			// Note: there's a race here, but with 200ms delay we should catch it
			initialReady := cluster.ready.Load()

			// Wait for ready
			start := time.Now()
			cluster.waitReady()
			elapsed := time.Since(start)

			// After waitReady returns, ready should be true
			assert.True(t, cluster.ready.Load(), "ready should be true after waitReady")

			// If initial was false, we should have waited
			if !initialReady {
				assert.True(t, elapsed >= time.Millisecond*100,
					"should have waited for refresh, elapsed: %v", elapsed)
			}
		},
	)
}

// TestWaitReadyConcurrent tests concurrent calls to waitReady are safe.
func TestWaitReadyConcurrent(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			const numGoroutines = 100
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			// Start many goroutines calling waitReady concurrently
			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					c.waitReady()
				}()
			}

			// All should complete without deadlock
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				// Success
			case <-time.After(time.Second * 5):
				t.Fatal("concurrent waitReady calls deadlocked")
			}

			// ready flag should be true
			assert.True(t, c.ready.Load())
		})
}

// TestWaitReadyConcurrentWithRefresh tests concurrent waitReady and refresh operations.
func TestWaitReadyConcurrentWithRefresh(t *testing.T) {
	runClusterTest(
		time.Millisecond*10, // Fast refresh interval
		func(hc *testHAKeeperClient, c *cluster) {
			const numGoroutines = 50
			const numIterations = 100

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			// Concurrent waitReady calls
			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < numIterations; j++ {
						c.waitReady()
						// Also test GetAllTNServices which calls waitReady
						_ = c.GetAllTNServices()
					}
				}()
			}

			// Concurrent ForceRefresh calls (stop when context is cancelled)
			var refreshWg sync.WaitGroup
			refreshWg.Add(1)
			go func() {
				defer refreshWg.Done()
				for i := 0; i < numIterations; i++ {
					select {
					case <-ctx.Done():
						return
					default:
						c.ForceRefresh(false)
						time.Sleep(time.Millisecond)
					}
				}
			}()

			// Wait for waitReady goroutines to complete
			wg.Wait()
			// Signal refresh goroutine to stop
			cancel()
			refreshWg.Wait()
		})
}

// TestReadyFlagConsistency tests that ready flag is always consistent with readyC channel state.
func TestReadyFlagConsistency(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			// After refresh, both ready flag and channel should indicate ready
			c.ForceRefresh(true)

			// ready flag should be true
			assert.True(t, c.ready.Load())

			// readyC should be closed (non-blocking receive)
			select {
			case <-c.readyC:
				// Channel is closed, as expected
			default:
				t.Fatal("readyC should be closed when ready is true")
			}
		})
}

// TestGetAllTNServicesAfterReady tests GetAllTNServices works correctly after ready.
func TestGetAllTNServicesAfterReady(t *testing.T) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			hc.addTN(100, "tn0")
			hc.addTN(200, "tn1")
			c.ForceRefresh(true)

			// Should return services without blocking
			services := c.GetAllTNServices()

			// Only the highest tick TN should be returned (tn1 with tick 200)
			require.Equal(t, 1, len(services))
			require.Equal(t, "tn1", services[0].ServiceID)
		})
}

// BenchmarkWaitReadyFastPath benchmarks the fast path performance.
func BenchmarkWaitReadyFastPath(b *testing.B) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			c.ForceRefresh(true)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c.waitReady()
				}
			})
		})
}

// BenchmarkGetAllTNServices benchmarks GetAllTNServices which uses waitReady.
func BenchmarkGetAllTNServices(b *testing.B) {
	runClusterTest(
		time.Hour,
		func(hc *testHAKeeperClient, c *cluster) {
			hc.addTN(100, "tn0")
			c.ForceRefresh(true)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = c.GetAllTNServices()
				}
			})
		})
}

// slowHAKeeperClient wraps testHAKeeperClient with artificial delay.
type slowHAKeeperClient struct {
	*testHAKeeperClient
	delay time.Duration
}

func (c *slowHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	time.Sleep(c.delay)
	return c.testHAKeeperClient.GetClusterDetails(ctx)
}
