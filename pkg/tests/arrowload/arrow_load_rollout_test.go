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

package arrowload

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestArrowLoadRolloutRollbackDrain exercises the operational transition, not
// just static gate values. It starts from the default-on policy, stops a cluster
// while an Arrow statement is admitted, restarts with every Arrow gate disabled,
// and finally rolls forward with distributed execution disabled. Shutdown may
// finish the admitted transaction or cancel it; either result must be atomic
// and bounded.
func TestArrowLoadRolloutRollbackDrain(t *testing.T) {
	c := startArrowLoadClusterWithDefaults(t, 1)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_rollout")
	mustExec(t, db, "use arrow_rollout")
	path, ddl := fixtureLarge(t)
	mustExec(t, db, fmt.Sprintf("create table rollout_drain(%s)", ddl))

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "use arrow_rollout")
	require.NoError(t, err)
	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
	loadErrCh := make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table rollout_drain parallel 'true'", path))
		loadErrCh <- execErr
	}()
	waitUntilStatementRunning(t, db, connID, "load data", 30*time.Second)
	require.NoError(t, c.Close())
	_ = conn.Close()
	_ = db.Close()

	var loadErr error
	select {
	case loadErr = <-loadErrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for admitted Arrow LOAD during cluster shutdown")
	}

	adjustArrowLoadCluster(c, arrowLoadClusterOptions{cnCount: 1})
	require.NoError(t, c.Start())
	rollbackDB := openArrowLoadDB(t, c, 0)
	rows := queryCount(t, rollbackDB, "select count(*) from arrow_rollout.rollout_drain")
	if loadErr == nil {
		require.Equal(t, int64(largeFixtureRows), rows,
			"a drained statement must commit the complete fixture")
	} else {
		require.Zero(t, rows, "a shutdown-canceled statement must commit no rows")
	}

	missing := filepath.Join(t.TempDir(), "must-not-be-read.arrow")
	_, err = rollbackDB.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table arrow_rollout.rollout_drain", missing))
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "disabled by configuration")

	require.NoError(t, rollbackDB.Close())
	require.NoError(t, c.Close())
	adjustArrowLoadCluster(c, arrowLoadClusterOptions{
		cnCount: 1, enabled: true, s3Enabled: false, distributedEnabled: false,
	})
	require.NoError(t, c.Start())
	rolledForwardDB := openArrowLoadDB(t, c, 0)
	mustExec(t, rolledForwardDB, "truncate table arrow_rollout.rollout_drain")
	mustExec(t, rolledForwardDB, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table arrow_rollout.rollout_drain parallel 'true'", path))
	require.Equal(t, int64(largeFixtureRows), queryCount(t, rolledForwardDB,
		"select count(*) from arrow_rollout.rollout_drain"))
}
