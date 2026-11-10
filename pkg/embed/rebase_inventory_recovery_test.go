// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/stretchr/testify/require"
)

type rebaseInventoryRecoveryIdentity struct {
	SourceID, RetiredID, ReplacementID uint64
	CheckpointCompleted                bool
}

// These qualify storage primitives only, not catalog producers or publication.
func TestRebaseInventoryWALRecovery(t *testing.T) {
	runRebaseCrashRecoveryScenario(t, false, "wal")
}

func TestRebaseInventoryCheckpointRecovery(t *testing.T) {
	runRebaseCrashRecoveryScenario(t, false, "checkpoint")
}

type rebaseSourceRecoveryIdentity struct {
	ChangedID, ControlID, ChangedEpoch, ControlEpoch uint64
	CheckpointCompleted                              bool
}

func TestRebaseSourceDependencyWALRecovery(t *testing.T) {
	runRebaseCrashRecoveryScenario(t, false, "dependency-wal")
}

func TestRebaseSourceDependencyCheckpointRecovery(t *testing.T) {
	runRebaseCrashRecoveryScenario(t, false, "dependency-checkpoint")
}

func runRebaseInventoryRecoveryPhase(t *testing.T, phase, mode, root string, cn ServiceOperator, db *sql.DB, ctx context.Context, identity rebaseCrashIdentity) {
	t.Helper()
	if strings.HasPrefix(mode, "dependency-") {
		runRebaseSourceDependencyRecovery(t, phase, mode, root, db, ctx, identity)
		return
	}
	value, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	internal := value.(executor.SQLExecutor)
	opts := executor.Options{}.WithAccountID(catalog.System_Account).
		WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC).
		WithWaitCommittedLogApplied()
	execute := func(query string) {
		t.Helper()
		result, err := internal.Exec(ctx, query, opts)
		require.NoError(t, err, query)
		result.Close()
	}
	scalar := func(query string) uint64 {
		t.Helper()
		result, err := internal.Exec(ctx, query, opts)
		require.NoError(t, err, query)
		defer result.Close()
		require.Len(t, result.Batches, 1)
		require.Equal(t, 1, result.Batches[0].RowCount())
		return vector.GetFixedAtNoTypeCheck[uint64](result.Batches[0].Vecs[0], 0)
	}
	databaseID := func(name string) uint64 {
		t.Helper()
		return scalar(fmt.Sprintf("select cast(dat_id as unsigned) from mo_catalog.mo_database where account_id=0 and datname='%s'", name))
	}
	version := func(id uint64) uint64 {
		t.Helper()
		return scalar(fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id))
	}
	if phase == "verify" {
		require.NotNil(t, identity.Inventory)
		inv := identity.Inventory
		require.Equal(t, mode == "checkpoint", inv.CheckpointCompleted)
		require.Equal(t, inv.SourceID, databaseID("nr_inventory_recovery_source"))
		require.Equal(t, inv.ReplacementID, databaseID("nr_inventory_recovery_retired"))
		require.NotEqual(t, inv.RetiredID, inv.ReplacementID)
		require.EqualValues(t, 2, scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where database_id in (%d,%d,%d)", inv.SourceID, inv.RetiredID, inv.ReplacementID)))
		require.EqualValues(t, 2, version(inv.SourceID))
		require.EqualValues(t, 1, version(inv.ReplacementID))
		require.Zero(t, scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where database_id=%d", inv.RetiredID)))
		rows, err := db.QueryContext(ctx, "select id,v from nr_inventory_recovery_source.payload order by id")
		require.NoError(t, err)
		defer rows.Close()
		var got [][2]int
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]))
			got = append(got, row)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int{{1, 20}}, got, "uncommitted epoch/business writes must both disappear")
		return
	}

	execute("create database nr_inventory_recovery_source")
	execute("create table nr_inventory_recovery_source.payload(id int primary key,v int)")
	execute("insert into nr_inventory_recovery_source.payload values(1,10)")
	execute("create database nr_inventory_recovery_retired")
	inv := &rebaseInventoryRecoveryIdentity{
		SourceID: databaseID("nr_inventory_recovery_source"), RetiredID: databaseID("nr_inventory_recovery_retired"),
	}
	execute(fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=1 where account_id=0 and database_id=%d", inv.SourceID))
	advance := fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=0 and database_id=%d and epoch=1", inv.SourceID)
	require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		for _, query := range []string{advance, "update nr_inventory_recovery_source.payload set v=20 where id=1"} {
			result, err := tx.Exec(query, executor.StatementOption{})
			if err != nil {
				return err
			}
			affected := result.AffectedRows
			result.Close()
			if affected != 1 {
				return fmt.Errorf("committed update affected %d rows", affected)
			}
		}
		return nil
	}, opts))
	require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		for _, query := range []string{
			fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=0 and database_id=%d and epoch=1", inv.RetiredID),
			"drop database nr_inventory_recovery_retired",
		} {
			result, err := tx.Exec(query, executor.StatementOption{})
			if err != nil {
				return err
			}
			result.Close()
		}
		return nil
	}, opts))
	execute("create database nr_inventory_recovery_retired")
	inv.ReplacementID = databaseID("nr_inventory_recovery_retired")
	require.NotEqual(t, inv.RetiredID, inv.ReplacementID)
	require.EqualValues(t, 1, version(inv.ReplacementID))
	if mode == "checkpoint" {
		// The TN control waits for ForceCheckpoint to finish flushing and
		// persisting the checkpoint; a plain asynchronous trigger is insufficient.
		var response string
		require.NoError(t, db.QueryRowContext(ctx, "select mo_ctl('dn','checkpoint','')").Scan(&response))
		var receipt struct {
			Method string            `json:"method"`
			Result []json.RawMessage `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(response), &receipt))
		require.Equal(t, "Checkpoint", receipt.Method)
		require.Len(t, receipt.Result, 1, "checkpoint must reach the fixture's TN, not an empty target set")
		inv.CheckpointCompleted = true
	}
	identity.Inventory = inv
	data, err := json.Marshal(identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "identity.json"), data, 0600))
	ready := os.NewFile(3, "ready")
	require.NotNil(t, ready)
	defer ready.Close()
	err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		meta := tx.Txn().Txn()
		if meta.Mode != pbtxn.TxnMode_Pessimistic || meta.Isolation != pbtxn.TxnIsolation_RC {
			return fmt.Errorf("unexpected inventory transaction mode=%s isolation=%s", meta.Mode, meta.Isolation)
		}
		for _, query := range []string{
			fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=0 and database_id=%d and epoch=2", inv.SourceID),
			"update nr_inventory_recovery_source.payload set v=30 where id=1",
		} {
			result, err := tx.Exec(query, executor.StatementOption{})
			if err != nil {
				return err
			}
			affected := result.AffectedRows
			result.Close()
			if affected != 1 {
				return fmt.Errorf("uncommitted update affected %d rows", affected)
			}
		}
		if err := json.NewEncoder(ready).Encode(identity); err != nil {
			return err
		}
		// Only a proven pending transaction reaches the parent's SIGKILL barrier.
		<-ctx.Done()
		return ctx.Err()
	}, opts)
	t.Fatalf("inventory child returned before SIGKILL: %v", err)
}

// Reuse the existing kill/reap/restart owner. The changed no-PK table is omitted
// from TABLES deliberately: its post-F write must still forbid source retirement.
func runRebaseSourceDependencyRecovery(t *testing.T, phase, mode, root string, db *sql.DB, ctx context.Context, identity rebaseCrashIdentity) {
	t.Helper()
	restore := frontend.SetRebaseSourceExpectationForTest(true)
	defer restore()
	execute := func(query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	scalar := func(query string) uint64 {
		t.Helper()
		var value uint64
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value), query)
		return value
	}
	id := func(name string) uint64 {
		return scalar("select dat_id from mo_catalog.mo_database where account_id=0 and datname='" + name + "'")
	}
	epoch := func(database uint64) uint64 {
		return scalar(fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", database))
	}
	query := func(source, target string, sourceID, version uint64) string {
		return fmt.Sprintf("data branch rebase database %s from %s{snapshot='%s_fh'} against nr_guard_u{snapshot='nr_guard_uh'} between %s{snapshot='%s_bf'} and nr_guard_u{snapshot='nr_guard_bu'} tables(t) for update expect(%d,%d)", target, source, source, source, source, sourceID, version)
	}
	assertPayload := func(database string) {
		t.Helper()
		rows, err := db.QueryContext(ctx, "select id,v from "+database+".t order by id")
		require.NoError(t, err)
		defer rows.Close()
		got := [][2]int{}
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]))
			got = append(got, row)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int{{1, 11}}, got)
	}
	if phase == "verify" {
		observe := txnimpl.SetRebaseSnapshotValidationHookForTest(func(table uint64, fixed, latest types.TS, reject bool) {
			t.Logf("source dependency TN table=%d fixed=%s knownDML=%s rejected=%t", table, fixed.ToString(), latest.ToString(), reject)
		})
		defer observe()
		state := identity.SourceDependency
		require.NotNil(t, state)
		require.Equal(t, mode == "dependency-checkpoint", state.CheckpointCompleted)
		for _, target := range []string{"nr_guard_pending", "nr_guard_rejected", "nr_guard_committed"} {
			require.Zero(t, scalar("select count(*) from mo_catalog.mo_database where datname='"+target+"'"))
		}
		require.Equal(t, state.ChangedID, id("nr_guard_changed"))
		require.Equal(t, state.ControlID, id("nr_guard_control"))
		require.Equal(t, state.ChangedEpoch, epoch(state.ChangedID))
		require.Equal(t, state.ControlEpoch, epoch(state.ControlID))
		assertPayload("nr_guard_changed")
		assertPayload("nr_guard_control")
		require.EqualValues(t, 4, scalar("select count(*) from nr_guard_changed.history"))
		require.EqualValues(t, 2, scalar("select count(*) from nr_guard_changed.history where v=1"))
		require.EqualValues(t, 1, scalar("select count(*) from nr_guard_changed.history where v is null"))
		require.EqualValues(t, 1, scalar("select count(*) from nr_guard_changed.history where v=9"))
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
		}()
		for _, statement := range []string{"set session transaction isolation level read committed", "begin"} {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		var rejected error
		for _, statement := range []string{query("nr_guard_changed", "nr_guard_rejected", state.ChangedID, state.ChangedEpoch), "drop database nr_guard_changed", "commit"} {
			_, rejected = conn.ExecContext(ctx, statement)
			if rejected != nil {
				break
			}
		}
		require.ErrorContains(t, rejected, "def changed", "post-F source DML must remain protected after process loss")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		require.Equal(t, state.ChangedID, id("nr_guard_changed"))
		require.Equal(t, state.ChangedEpoch, epoch(state.ChangedID))
		require.Zero(t, scalar("select count(*) from mo_catalog.mo_database where datname='nr_guard_rejected'"))
		require.EqualValues(t, 4, scalar("select count(*) from nr_guard_changed.history"))
		assertPayload("nr_guard_changed")
		require.EqualValues(t, 1, scalar("select count(*) from nr_guard_changed.history where v=9"))
		for _, statement := range []string{"begin", query("nr_guard_control", "nr_guard_committed", state.ControlID, state.ControlEpoch), "drop database nr_guard_control", "commit"} {
			_, err = conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		require.Zero(t, scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", state.ControlID)))
		require.Zero(t, scalar("select count(*) from mo_catalog.mo_database where datname='nr_guard_control'"))
		rows, err := db.QueryContext(ctx, "select id,v from nr_guard_committed.t order by id")
		require.NoError(t, err)
		defer rows.Close()
		var got [][2]int
		for rows.Next() {
			var row [2]int
			require.NoError(t, rows.Scan(&row[0], &row[1]))
			got = append(got, row)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int{{1, 11}, {2, 20}}, got)
		require.EqualValues(t, 3, scalar("select count(*) from nr_guard_committed.history"))
		require.EqualValues(t, 2, scalar("select count(*) from nr_guard_committed.history where v=1"))
		require.EqualValues(t, 1, scalar("select count(*) from nr_guard_committed.history where v is null"))
		require.EqualValues(t, 2, scalar("select count(*) from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on t.rel_id=b.table_id where t.reldatabase='nr_guard_committed' and b.clone_ts=(select ts from mo_catalog.mo_snapshots where sname='nr_guard_uh')"))
		return
	}
	execute("create database nr_guard_u")
	execute("create table nr_guard_u.t(id int primary key,v int)")
	execute("create table nr_guard_u.history(v int)")
	execute("insert into nr_guard_u.t values(1,10)")
	execute("insert into nr_guard_u.history values(1),(1),(NULL)")
	execute("create snapshot nr_guard_bu for database nr_guard_u")
	state := &rebaseSourceRecoveryIdentity{}
	for _, source := range []string{"nr_guard_changed", "nr_guard_control"} {
		execute("data branch create database " + source + " from nr_guard_u{snapshot='nr_guard_bu'}")
		execute("create snapshot " + source + "_bf for database " + source)
		execute("update " + source + ".t set v=11 where id=1")
		execute("create snapshot " + source + "_fh for database " + source)
	}
	execute("insert into nr_guard_u.t values(2,20)")
	execute("create snapshot nr_guard_uh for database nr_guard_u")
	state.ChangedID, state.ControlID = id("nr_guard_changed"), id("nr_guard_control")
	state.ChangedEpoch, state.ControlEpoch = epoch(state.ChangedID), epoch(state.ControlID)
	execute("insert into nr_guard_changed.history values(9)")
	require.Equal(t, state.ChangedEpoch, epoch(state.ChangedID), "directory epoch cannot replace the DML dependency")
	if mode == "dependency-checkpoint" {
		var response string
		require.NoError(t, db.QueryRowContext(ctx, "select mo_ctl('dn','checkpoint','')").Scan(&response))
		var receipt struct {
			Method string            `json:"method"`
			Result []json.RawMessage `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(response), &receipt))
		require.Equal(t, "Checkpoint", receipt.Method)
		require.Len(t, receipt.Result, 1)
		state.CheckpointCompleted = true
	}
	identity.SourceDependency = state
	data, err := json.Marshal(identity)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "identity.json"), data, 0600))
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	for _, statement := range []string{"set session transaction isolation level read committed", "begin", query("nr_guard_control", "nr_guard_pending", state.ControlID, state.ControlEpoch), "drop database nr_guard_control"} {
		_, err = conn.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	ready := os.NewFile(3, "ready")
	require.NotNil(t, ready)
	defer ready.Close()
	require.NoError(t, json.NewEncoder(ready).Encode(identity))
	<-ctx.Done()
	t.Fatal("parent did not kill the pending guarded publication")
}
