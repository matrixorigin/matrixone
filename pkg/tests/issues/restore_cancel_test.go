// Copyright 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

type cancelRestoreExec struct {
	frontend.BackgroundExec
	after func(context.Context, string) error
}

func (b *cancelRestoreExec) Exec(ctx context.Context, q string) error {
	if err := b.BackgroundExec.Exec(ctx, q); err != nil {
		return err
	}
	return b.after(ctx, q)
}

// SharedTestCluster serializes these tests. Pause only after the real executor
// mutates the restore transaction, then cancel its request through public KILL.
func cancelRestoreAtGrantDeletion(t *testing.T, ctx context.Context, sys *sql.DB, statement string) {
	t.Helper()
	conn, err := sys.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var id uint64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&id))
	reached, release := make(chan struct{}), make(chan struct{})
	var fired atomic.Bool
	original := frontend.NewBackgroundExec
	stub := gostub.Stub(&frontend.NewBackgroundExec, func(c context.Context, s frontend.FeSession, o ...*frontend.BackgroundExecOption) frontend.BackgroundExec {
		return &cancelRestoreExec{BackgroundExec: original(c, s, o...), after: func(c context.Context, q string) error {
			if q != "delete from mo_catalog.mo_role_privs" || !fired.CompareAndSwap(false, true) {
				return nil
			}
			close(reached)
			select {
			case <-c.Done():
				return c.Err()
			case <-release:
				return context.Canceled
			}
		}}
	})
	defer stub.Reset()
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, e := conn.ExecContext(deadline, statement); done <- e }()
	// Join the request before resetting the shared factory, even if an
	// assertion fails while its executor is paused at the barrier.
	defer func() {
		cancel()
		close(release)
		select {
		case <-finished:
		case <-time.After(30 * time.Second):
			t.Error("restore request did not stop during cleanup")
		}
	}()
	select {
	case <-reached:
	case e := <-done:
		t.Fatalf("restore ended before cancellation barrier: %v", e)
	case <-deadline.Done():
		t.Fatal("restore did not reach grant deletion")
	}
	_, err = sys.ExecContext(ctx, fmt.Sprintf("kill query %d", id))
	require.NoError(t, err)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-deadline.Done():
		t.Fatal("canceled restore did not release the request")
	}
}
