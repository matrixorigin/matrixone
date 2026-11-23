package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestThreeVoterGetWALTruncateRetriesBlackholedPeer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logger, _ := captureLogger()
	s1, s2, s3, cfg1, cfg2, cfg3 := buildZombieCluster(t, logger)
	services := []*Service{s1, s2, s3}
	configs := []Config{cfg1, cfg2, cfg3}
	defer func() {
		for i, service := range services {
			require.NoError(t, service.Close())
			vfs.ReportLeakedFD(configs[i].FS, t)
		}
	}()
	peers := map[uint64]dragonboat.Target{1: s1.ID(), 2: s2.ID(), 3: s3.ID()}
	for i, service := range services {
		require.NoError(t, service.store.startReplica(1, uint64(i+1), peers, false))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var leader uint64
	require.Eventually(t, func() bool {
		id, _, ok, err := s1.store.nh.GetLeaderID(1)
		leader = id
		return err == nil && ok && id != 0
	}, 10*time.Second, 20*time.Millisecond)
	addresses := []string{cfg1.LogServiceServiceAddr(), cfg2.LogServiceServiceAddr(), cfg3.LogServiceServiceAddr()}
	proxy, drop, cleanup := startDroppingResponseProxy(t, addresses[leader-1])
	defer cleanup()
	cfg := ClientConfig{LogShardID: 1, TNReplicaID: 100, ServiceAddresses: []string{proxy}}
	client, err := NewClient(ctx, "", cfg)
	require.NoError(t, err)
	defer client.Close()
	healthyCfg := cfg
	healthyCfg.ServiceAddresses = addresses
	healthy, err := NewClient(ctx, "", healthyCfg)
	require.NoError(t, err)
	defer healthy.Close()
	_, _, err = client.GetWALTruncate(ctx)
	require.NoError(t, err)
	client.(*managedClient).cfg.ServiceAddresses = addresses
	drop.Store(true)
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	firstResult := make(chan outcome, 1)
	started := make(chan struct{})
	go func() {
		start := time.Now()
		close(started)
		_, _, readErr := client.GetWALTruncate(ctx)
		firstResult <- outcome{readErr, time.Since(start)}
	}()
	<-started
	time.Sleep(50 * time.Millisecond)
	healthyStart := time.Now()
	_, _, err = healthy.GetWALTruncate(ctx)
	require.NoError(t, err)
	require.Less(t, time.Since(healthyStart), 2*time.Second)
	select {
	case result := <-firstResult:
		t.Fatalf("blackholed call returned before healthy voter read: %v", result.err)
	default:
	}
	result := <-firstResult
	require.NoError(t, result.err)
	require.Less(t, result.elapsed, 12*time.Second)
	// The same client remains usable after failing over.
	_, _, err = client.GetWALTruncate(ctx)
	require.NoError(t, err)
	// A short parent deadline must end the read even though the internal
	// failover budget is longer.
	drop.Store(false)
	shortClient, err := NewClient(ctx, "", ClientConfig{
		LogShardID: 1, TNReplicaID: 100, ServiceAddresses: []string{proxy},
	})
	require.NoError(t, err)
	defer shortClient.Close()
	_, _, err = shortClient.GetWALTruncate(ctx)
	require.NoError(t, err)
	drop.Store(true)
	shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer shortCancel()
	_, _, err = shortClient.GetWALTruncate(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
