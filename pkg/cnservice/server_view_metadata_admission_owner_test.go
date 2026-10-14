// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The fake clock and quiescence barriers distinguish catalog commit from the
// later authority acknowledgement without racing a short real-world timeout.
func TestCNViewMetadataAdmissionOwnerAfterCatalogCommit(t *testing.T) {
	for _, test := range []struct {
		name      string
		ingress   bool
		preparing bool
		protocol  uint64
		terminal  string
	}{
		{name: "local initialization", terminal: "admit"},
		{name: "preparing epoch", preparing: true, terminal: "admit"},
		{name: "ingress publication", ingress: true, terminal: "admit"},
		{name: "derived definitions", protocol: uint64(defines.MORPCVersion98), terminal: "admit"},
		{name: "owner failure", terminal: "failure"},
		{name: "owner cancellation", terminal: "cancel"},
		{name: "owner deadline", terminal: "deadline"},
		{name: "completed owner without admission", terminal: "complete"},
		{name: "superseded generation", terminal: "supersede"},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				result := make(chan error, 1)
				s := newAdmissionOwnerTestService(t, ctx, result)
				if test.preparing {
					snapshot := *s.viewMetadataAdmission.Load()
					snapshot.Preparing = true
					snapshot.Enabled = false
					s.viewMetadataAdmission.Store(&snapshot)
				}
				var catalogCommitted atomic.Bool
				s.sqlExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
					if !catalogCommitted.Load() {
						return executor.Result{}, moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_view_refresh")
					}
					if sql == catalog.ViewMetadataLifecycleGateSQL {
						return viewMetadataLifecycleGateTestResult(), nil
					}
					return executor.Result{}, nil
				})
				done := make(chan error, 1)
				go func() { done <- s.waitForViewMetadataAdmissionHandoff(test.ingress, test.protocol) }()
				synctest.Wait()
				require.Zero(t, s.viewMetadataCatalogFencedEpoch.Load())
				catalogCommitted.Store(true)
				time.Sleep(2 * time.Second) // Fake time: discovery expires, bootstrap does not.
				synctest.Wait()
				require.Equal(t, uint64(5), s.viewMetadataCatalogFencedEpoch.Load())
				require.False(t, s.viewMetadataIngressReady.Load())
				select {
				case err := <-done:
					t.Fatalf("catalog commit returned admission to the expired discovery deadline: %v", err)
				default:
				}

				var wantErr error
				switch test.terminal {
				case "admit":
					snapshot := *s.viewMetadataAdmission.Load()
					snapshot.Preparing = false
					snapshot.Enabled = true
					snapshot.Admitted = true
					snapshot.CatalogFencedEpoch = snapshot.Epoch
					require.NoError(t, s.applyViewMetadataAdmission(ctx, &snapshot))
				case "failure":
					wantErr = errors.New("upgrade owner failed after catalog commit")
					result <- wantErr
				case "cancel":
					wantErr = context.Canceled
					cancel()
				case "deadline":
					wantErr = context.DeadlineExceeded
					time.Sleep(time.Minute)
				case "complete":
					result <- nil
				case "supersede":
					snapshot := *s.viewMetadataAdmission.Load()
					snapshot.Generation++
					s.viewMetadataAdmission.Store(&snapshot)
					s.notifyViewMetadataAdmissionUpdated()
				}
				synctest.Wait()
				select {
				case err := <-done:
					switch test.terminal {
					case "admit":
						require.NoError(t, err)
						require.Equal(t, test.ingress, s.viewMetadataIngressReady.Load())
					case "complete":
						require.ErrorContains(t, err, "was not admitted before startup deadline")
					case "supersede":
						require.ErrorContains(t, err, "generation was superseded")
					default:
						require.ErrorIs(t, err, wantErr)
					}
				default:
					t.Fatal("admission did not observe its terminal event")
				}
				if test.terminal != "admit" {
					require.False(t, s.viewMetadataIngressReady.Load())
				}
			})
		})
	}
}

func TestCNViewMetadataAdmissionDiscoveryRemainsBounded(t *testing.T) {
	for _, withOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "no owner", true: "no authority"}[withOwner], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var ctx context.Context
				var result chan error
				if withOwner {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					result = make(chan error, 1)
				}
				s := newAdmissionOwnerTestService(t, ctx, result)
				if withOwner {
					s.viewMetadataAdmission.Store(nil)
				}
				done := make(chan error, 1)
				go func() { done <- s.waitForViewMetadataIngressAdmission() }()
				synctest.Wait()
				time.Sleep(2 * time.Second)
				synctest.Wait()
				select {
				case err := <-done:
					require.ErrorContains(t, err, "was not admitted before startup deadline")
				default:
					t.Fatal("discovery lost its deadline without an owner or authority")
				}
				require.False(t, s.viewMetadataIngressReady.Load())
			})
		})
	}
}

func TestCNViewMetadataAdmissionCancelledOwnerCannotPublishIngress(t *testing.T) {
	for _, authority := range []bool{false, true} {
		t.Run(map[bool]string{false: "no authority", true: "admitted snapshot"}[authority], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := newAdmissionOwnerTestService(t, ctx, make(chan error, 1))
				if authority {
					snapshot := *s.viewMetadataAdmission.Load()
					snapshot.Admitted = true
					s.viewMetadataAdmission.Store(&snapshot)
					require.NoError(t, s.viewMetadataEpochFence.Advance(t.Context(), snapshot.Epoch))
					s.viewMetadataCatalogFencedEpoch.Store(snapshot.Epoch)
				} else {
					s.viewMetadataAdmission.Store(nil)
				}
				cancel()
				start := time.Now()
				require.ErrorIs(t, s.waitForViewMetadataIngressAdmission(), context.Canceled)
				require.Equal(t, start, time.Now(), "owner cancellation must not wait for discovery")
				require.False(t, s.viewMetadataIngressReady.Load())
			})
		})
	}
}

func newAdmissionOwnerTestService(t *testing.T, ctx context.Context, result chan error) *service {
	t.Helper()
	serviceID := t.Name()
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		logger:                          zap.NewNop(),
		viewMetadataAdmissionGeneration: 11,
		viewMetadataEpochFence:          compile.NewViewMetadataEpochFence(),
		viewMetadataAdmissionUpdated:    make(chan struct{}, 1),
		bootstrapUpgradeContext:         ctx,
		bootstrapUpgradeResult:          result,
	}
	t.Cleanup(s.viewMetadataEpochFence.Close)
	s.cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	s.viewMetadataCatalogFenceReady.Store(true)
	s.viewMetadataAdmission.Store(&logservicepb.ViewMetadataAdmission{
		Enabled: true, Epoch: 5, Generation: 11, RevalidationRequired: true,
		PersistedExpressionRequiredProtocolVersion: uint64(defines.MORPCVersion98),
	})
	return s
}
