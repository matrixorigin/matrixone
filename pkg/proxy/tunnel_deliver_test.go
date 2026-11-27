// Copyright 2021 - 2024 Matrix Origin
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

package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTunnelDeliver_Deliver(t *testing.T) {
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	queue := make(chan *tunnel, 10)
	deliver := newTunnelDeliver(queue, logger)

	// a nil tunnel
	deliver.Deliver(nil, transferByRebalance)
	assert.Equal(t, 0, deliver.Count())

	tun1 := &tunnel{cc: &clientConn{connID: 12}}
	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 0, deliver.Count())

	startTunnel := func(tun *tunnel) {
		tun.mu.Lock()
		defer tun.mu.Unlock()
		tun.mu.started = true
	}

	// start the tunnel
	startTunnel(tun1)
	tun1.deferMigration(moerr.GetOkExpectedNotSafeToStartTransfer(), time.Now())
	for range 2 {
		deliver.Deliver(tun1, transferByScaling)
		assert.Equal(t, 0, deliver.Count())
		assert.False(t, tun1.tryStartTransferAttempt())
		tun1.observeMigrationClientActivity(frontend.COM_PING)
	}
	tun1.observeMigrationClientActivity(frontend.COM_STMT_CLOSE)
	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 1, deliver.Count())

	// deliver again, but cannot deliver it.
	deliver.Deliver(tun1, transferByRebalance)
	assert.Equal(t, 1, deliver.Count())

	for i := 0; i < 15; i++ {
		tun := &tunnel{cc: &clientConn{connID: uint32(20 + i)}}
		startTunnel(tun)
		deliver.Deliver(tun, transferByRebalance)
		if i < 9 {
			assert.Equal(t, i+2, deliver.Count())
		} else {
			assert.Equal(t, 10, deliver.Count())
		}
	}
}

func TestMigrationRefusalClearedAfterForwardedStmtClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	clientProxy, client := net.Pipe()
	defer clientProxy.Close()
	defer client.Close()
	backendProxy, backend := net.Pipe()
	defer backendProxy.Close()
	defer backend.Close()
	deadline, _ := ctx.Deadline()
	require.NoError(t, client.SetDeadline(deadline))
	require.NoError(t, backend.SetDeadline(deadline))

	logger := runtime.DefaultRuntime().Logger()
	tun := newTunnel(ctx, logger, nil)
	defer tun.Close()
	tun.mu.started = true
	csp := tun.newPipe(pipeClientToServer,
		newMySQLConn(connClientName, clientProxy, 0, nil, nil, false, 1),
		newMySQLConn(connServerName, backendProxy, 0, nil, nil, false, 2))
	csp.wg.Add(1) // A synchronous transfer owns the backend until it refuses.
	var release sync.Once
	resume := func() { release.Do(csp.wg.Done) }
	defer resume()
	ready := make(chan struct{})
	csp.testHelper.beforeSend = func() { close(ready) }
	done := make(chan error, 1)
	go func() { done <- csp.kickoff(ctx, nil) }()
	defer func() {
		resume()
		_ = client.Close()
		_ = backend.Close()
		select {
		case <-done:
		case <-ctx.Done():
			t.Error("client pipe did not terminate")
		}
	}()

	packet := []byte{5, 0, 0, 0, byte(frontend.COM_STMT_CLOSE), 1, 0, 0, 0}
	_, err := client.Write(packet)
	require.NoError(t, err)
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("close packet did not reach the transfer barrier")
	}
	tun.deferMigration(moerr.GetOkExpectedNotSafeToStartTransfer(), time.Now())
	require.True(t, tun.migrationDeferred(time.Now()))
	resume()

	got := make([]byte, len(packet))
	_, err = io.ReadFull(backend, got)
	require.NoError(t, err)
	require.Equal(t, packet, got)
	// No command or server response follows the close.
	require.Eventually(t, func() bool { return !tun.migrationDeferred(time.Now()) }, time.Second, time.Millisecond)
	queue := make(chan *tunnel, 1)
	newTunnelDeliver(queue, logger).Deliver(tun, transferByScaling)
	require.Len(t, queue, 1)
}

func TestMigrationCatalogRetryIsBounded(t *testing.T) {
	for name, refusal := range map[string]error{
		"catalog":         moerr.GetOkExpectedMigrationPolicyChanged(),
		"prepared":        moerr.GetOkExpectedMigrationPreparedState(),
		"role":            moerr.GetOkExpectedMigrationRoleChanged(),
		"table-name mode": moerr.GetOkExpectedMigrationTableNameModeChanged(),
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			tun := &tunnel{}
			tun.deferMigration(refusal, now)
			tun.observeMigrationClientActivity(frontend.COM_PING)
			tun.observeMigrationClientActivity(frontend.COM_STMT_CLOSE)
			require.True(t, tun.migrationDeferred(now))
			require.True(t, tun.migrationDeferred(now.Add(migrationRetryInterval-time.Nanosecond)))
			require.False(t, tun.migrationDeferred(now.Add(migrationRetryInterval)))

			// A later refusal establishes another bounded interval.
			next := now.Add(migrationRetryInterval)
			tun.deferMigration(refusal, next)
			require.True(t, tun.migrationDeferred(next))
			require.False(t, tun.migrationDeferred(next.Add(migrationRetryInterval)))
		})
	}
}
