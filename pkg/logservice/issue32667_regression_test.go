package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestBootstrapLeaderRetakeStartsNewTermBudget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		interval      time.Duration
		beforeB       time.Duration
		arrivalMargin time.Duration
	}{
		{name: "short", interval: 30 * time.Millisecond, beforeB: time.Second, arrivalMargin: 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runHAKeeperClusterTest(t, func(t *testing.T, services []*Service) {
				stores := []*store{services[0].store, services[1].store, services[2].store}
				for _, s := range stores {
					s.cfg.HAKeeperCheckInterval.Duration = tc.interval
				}
				require.NoError(t, stores[0].setInitialClusterInfo(1, 1, 3, hakeeper.K8SIDRangeEnd+10, nil, nil))
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				for _, s := range stores {
					_, err := s.addLogStoreHeartbeat(ctx, s.getHeartbeatMessage())
					require.NoError(t, err)
				}
				_, err := stores[0].addTNStoreHeartbeat(ctx, pb.TNStoreHeartbeat{UUID: "retake-diagnostic-tn"})
				require.NoError(t, err)
				first := -1
				var term uint64
				for i, s := range stores {
					ok, currentTerm, err := s.isLeaderHAKeeper()
					require.NoError(t, err)
					if ok {
						first, term = i, currentTerm
						break
					}
				}
				require.NotEqual(t, -1, first)
				a := stores[first]
				second := (first + 1) % len(stores)
				b := stores[second]
				state, err := a.getCheckerState()
				require.NoError(t, err)
				require.NoError(t, a.bootstrap(term, state))
				state, err = a.getCheckerState()
				require.NoError(t, err)
				require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
				originalDeadline := a.bootstrapCheckDeadline
				require.False(t, originalDeadline.IsZero())
				a.hakeeperCheck()
				require.Equal(t, originalDeadline, a.bootstrapCheckDeadline)
				t.Logf("A deadline=%s, interval=%s", originalDeadline.Format(time.RFC3339Nano), tc.interval)
				time.Sleep(tc.beforeB)
				require.NoError(t, a.requestLeaderTransfer(hakeeper.DefaultHAKeeperShardID, uint64(second+1)))
				require.Eventually(t, func() bool { ok, _, err := b.isLeaderHAKeeper(); return err == nil && ok }, 10*time.Second, 20*time.Millisecond)
				state, err = b.getCheckerState()
				require.NoError(t, err)
				require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
				require.NoError(t, b.checkBootstrap(state))
				bDeadline := b.bootstrapCheckDeadline
				require.True(t, bDeadline.After(originalDeadline))
				t.Logf("B deadline=%s", bDeadline.Format(time.RFC3339Nano))
				if wait := time.Until(originalDeadline.Add(tc.arrivalMargin)); wait > 0 {
					time.Sleep(wait)
				}
				require.True(t, time.Now().Before(bDeadline), "B's budget must still be live")
				require.NoError(t, b.requestLeaderTransfer(hakeeper.DefaultHAKeeperShardID, uint64(first+1)))
				require.Eventually(t, func() bool { ok, _, err := a.isLeaderHAKeeper(); return err == nil && ok }, 10*time.Second, 20*time.Millisecond)
				state, err = a.getCheckerState()
				require.NoError(t, err)
				require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
				a.hakeeperCheck()
				state, err = a.getCheckerState()
				require.NoError(t, err)
				require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
				require.True(t, a.bootstrapCheckDeadline.After(originalDeadline))
				require.True(t, time.Now().Before(a.bootstrapCheckDeadline))
			})
		})
	}
}
