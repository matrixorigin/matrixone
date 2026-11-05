//go:build mo_m0

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

package sqlintegration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

const m0RollbackCustomerSchema = `(
	c_id int not null,
	c_d_id int not null,
	c_w_id int not null,
	c_credit_lim decimal(12,2),
	c_ytd_payment decimal(12,2),
	c_balance decimal(12,2),
	primary key(c_w_id,c_d_id,c_id)
)`

func m0SchemaProof(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) []string {
	t.Helper()
	var tableID uint64
	var version uint32
	var extraBytes []byte
	require.NoError(t, db.QueryRowContext(ctx,
		"select rel_id,rel_version,extra_info from mo_catalog.mo_tables where reldatabase=? and relname=?",
		schema, table).Scan(&tableID, &version, &extraBytes))
	proof := []string{fmt.Sprintf("table:%d:%d", tableID, version)}
	var extra api.SchemaExtra
	require.NoError(t, extra.Unmarshal(extraBytes))
	require.NotNil(t, extra.M0Evolution)
	contractBytes, err := extra.M0Evolution.Marshal()
	require.NoError(t, err)
	proof = append(proof, fmt.Sprintf("evolution:%x", sha256.Sum256(contractBytes)))

	func() {
		rows, err := db.QueryContext(ctx,
			"select column_name,column_type,is_nullable,ordinal_position from information_schema.columns "+
				"where table_schema=? and table_name=? order by ordinal_position",
			schema, table)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var name, typ, nullable string
			var position int
			require.NoError(t, rows.Scan(&name, &typ, &nullable, &position))
			proof = append(proof, fmt.Sprintf("visible:%s:%s:%s:%d", name, typ, nullable, position))
		}
		require.NoError(t, rows.Err())
	}()

	func() {
		rows, err := db.QueryContext(ctx,
			"select attname,attr_seqnum,att_is_hidden,attnotnull,attnum from mo_catalog.mo_columns "+
				"where att_database=? and att_relname=? order by attnum",
			schema, table)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var name string
			var seqnum uint16
			var hidden, notNull int8
			var attnum int
			require.NoError(t, rows.Scan(&name, &seqnum, &hidden, &notNull, &attnum))
			proof = append(proof, fmt.Sprintf("stored:%s:%d:%d:%d:%d", name, seqnum, hidden, notNull, attnum))
		}
		require.NoError(t, rows.Err())
	}()
	return proof
}

func m0ObjectHeaderProofs(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	cn embed.ServiceOperator,
	table string,
) []string {
	t.Helper()
	service := cn.RawService().(cnservice.Service)
	engine, ok := service.GetEngine().(*disttae.Engine)
	require.True(t, ok)
	fs := engine.FS()

	rows, err := db.QueryContext(ctx,
		"select object_name,obj_loc from metadata_scan(?, 'c_id') g order by object_name", table)
	require.NoError(t, err)
	defer rows.Close()
	var proof []string
	for rows.Next() {
		var objectName, objectLocation string
		require.NoError(t, rows.Scan(&objectName, &objectLocation))
		reader, readErr := objectio.NewObjectReaderWithStr(objectName, fs)
		if readErr != nil {
			reader, readErr = objectio.NewObjectReaderWithStr(objectLocation, fs)
		}
		require.NoError(t, readErr)
		mp := mpool.MustNewZero()
		header, readErr := reader.ReadHeader(ctx, mp)
		if readErr == nil {
			var meta objectio.ObjectMeta
			meta, readErr = reader.ReadAllMeta(ctx, mp)
			if readErr == nil {
				version, versionErr := objectio.ObjectSchemaVersion(header)
				require.NoError(t, versionErr)
				dataMeta := meta.MustDataMeta()
				widths := make([]uint16, 0, dataMeta.BlockCount())
				for blockIdx := uint32(0); blockIdx < dataMeta.BlockCount(); blockIdx++ {
					widths = append(widths, dataMeta.GetBlockMeta(blockIdx).GetMaxSeqnum())
				}
				proof = append(proof, fmt.Sprintf("%s:%d:%v", objectName, version, widths))
			}
		}
		mpool.DeleteMPool(mp)
		require.NoError(t, readErr)
	}
	require.NoError(t, rows.Err())
	return proof
}

func m0AddFaultPoint(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	name, action, argument string,
) {
	t.Helper()
	m0Exec(t, ctx, db, "select enable_fault_injection()")
	_, err := db.ExecContext(ctx,
		"select add_fault_point(?, ':::', ?, 0, ?)",
		name, action, argument,
	)
	require.NoError(t, err)
}

func m0RemoveFaultPoint(t *testing.T, ctx context.Context, db *sql.DB, name string) {
	t.Helper()
	_, err := db.ExecContext(ctx, "select remove_fault_point(?)", name)
	require.NoError(t, err)
}

func m0RemoveFaultPointQuiet(ctx context.Context, db *sql.DB, name string) {
	_, _ = db.ExecContext(ctx, "select remove_fault_point(?)", name)
}

func m0FaultWaiters(t *testing.T, ctx context.Context, db *sql.DB, getter string) int64 {
	t.Helper()
	var waiters int64
	require.NoError(t, db.QueryRowContext(ctx, "select trigger_fault_point(?)", getter).Scan(&waiters))
	return waiters
}

func m0WaitForFaultWaiter(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	getter string,
	want int64,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		return m0FaultWaiters(t, ctx, db, getter) == want
	}, 10*time.Second, 20*time.Millisecond)
}

func m0WaitForError(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(45 * time.Second):
		require.FailNow(t, "operation did not terminate")
		return nil
	}
}

func m0PrepareRollbackTable(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	schema string,
) (*sql.DB, string) {
	t.Helper()
	db := openM0SQL(t, cn)
	table := schema + ".customer"
	m0Exec(t, ctx, db, "create database "+schema)
	m0Exec(t, ctx, db, "create table "+table+m0RollbackCustomerSchema)
	m0Exec(t, ctx, db, "insert into "+table+" values "+
		"(1,1,1,50000,10000,10),(2,2,2,60000,6000,NULL),(3,3,3,70000,1000,30)")
	m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
	return db, table
}

func m0RunRollbackAt(
	t *testing.T,
	cluster embed.Cluster,
	schema, point string,
) {
	t.Helper()
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer func() { _ = db.Close() }()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		var committedVersion uint32
		if point == "fj/m0/alter/tn-after-apply" {
			m0Exec(t, ctx, db, "alter table "+table+" add column first_committed int")
			committedVersion = 1
		}
		observer := openM0SQL(t, cn)
		defer func() { _ = observer.Close() }()
		t.Cleanup(func() { m0RemoveFaultPointQuiet(context.Background(), db, point) })
		defer m0RemoveFaultPointQuiet(context.Background(), observer, point)

		beforeSchema := m0SchemaProof(t, ctx, observer, schema, "customer")
		beforeObjects := m0ObjectFingerprints(t, ctx, observer, table)
		beforeHeaders := m0ObjectHeaderProofs(t, ctx, observer, cn, table)
		require.NotEmpty(t, beforeObjects)
		require.NotEmpty(t, beforeHeaders)

		m0AddFaultPoint(t, ctx, db, point, "echo", "injected M0 ALTER failure")
		_, err = db.ExecContext(ctx, "alter table "+table+" add column rolled_back int")
		require.ErrorContains(t, err, "injected M0 ALTER failure")
		fresh := openM0SQL(t, cn)
		defer func() { _ = fresh.Close() }()
		m0RemoveFaultPoint(t, ctx, fresh, point)

		require.Equal(t, beforeSchema, m0SchemaProof(t, ctx, fresh, schema, "customer"))
		require.Equal(t, beforeObjects, m0ObjectFingerprints(t, ctx, fresh, table))
		require.Equal(t, beforeHeaders, m0ObjectHeaderProofs(t, ctx, fresh, cn, table))
		require.Zero(t, m0Count(t, ctx, fresh,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='rolled_back'"))

		m0Exec(t, ctx, fresh, "alter table "+table+" add column recovered int")
		_, version := m0TableGeneration(t, ctx, fresh, schema, "customer")
		require.Equal(t, committedVersion+1, version)
		require.Equal(t, 3, m0Count(t, ctx, fresh, "select count(*) from "+table+" where recovered is null"))
		require.Equal(t, 3, m0Count(t, ctx, fresh, "select count(*) from "+table))
		m0Exec(t, ctx, fresh, "insert into "+table+"(c_id,c_d_id,c_w_id) values(4,4,4)")
		m0Exec(t, ctx, fresh, "update "+table+" set recovered=c_id")
		m0Exec(t, ctx, fresh, "select mo_ctl('dn','flush','"+table+"')")
		require.Equal(t, 4, m0Count(t, ctx, fresh, "select count(*) from "+table+" where recovered=c_id"))
	})
}

func TestM0AlterRollbackAtPrivateDefs(t *testing.T) {
	m0RunRollbackAt(t, nil, "m0_rollback_private", disttae.M0FaultAfterDefs)
}

func TestM0AlterRollbackAtTNAlter(t *testing.T) {
	// This point is CN workspace registration, not TN schema application.
	m0RunRollbackAt(t, nil, "m0_rollback_tn", disttae.M0FaultAfterTNAlter)
}

func TestM0AlterRollbackAtTNApplied(t *testing.T) {
	m0RunRollbackAt(t, nil, "m0_rollback_tn_applied", "fj/m0/alter/tn-after-apply")
}

func TestM0AlterRollbackAtCatalogDelete(t *testing.T) {
	m0RunRollbackAt(t, nil, "m0_rollback_catalog", disttae.M0FaultAfterCatalogDelete)
}

func TestM0SecondAlterFailureKeepsFirstCommit(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_second_alter"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer func() { _ = db.Close() }()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		fresh := openM0SQL(t, cn)
		defer func() { _ = fresh.Close() }()
		t.Cleanup(func() { m0RemoveFaultPointQuiet(context.Background(), fresh, disttae.M0FaultAfterTNAlter) })

		objects := m0ObjectFingerprints(t, ctx, fresh, table)
		headers := m0ObjectHeaderProofs(t, ctx, fresh, cn, table)
		m0Exec(t, ctx, fresh, "alter table "+table+" add column first_committed int")
		afterFirst := m0SchemaProof(t, ctx, fresh, schema, "customer")
		require.Equal(t, uint32(1), func() uint32 {
			_, v := m0TableGeneration(t, ctx, fresh, schema, "customer")
			return v
		}())

		_, err = fresh.ExecContext(ctx, "alter table "+table+" comment='forbidden after m0'")
		require.ErrorContains(t, err, "only supports metadata-only ADD COLUMN")
		require.Equal(t, afterFirst, m0SchemaProof(t, ctx, fresh, schema, "customer"))

		m0AddFaultPoint(t, ctx, fresh, disttae.M0FaultAfterTNAlter, "echo", "injected second ALTER failure")
		_, err = fresh.ExecContext(ctx, "alter table "+table+" add column failed_second int")
		require.ErrorContains(t, err, "injected second ALTER failure")
		m0RemoveFaultPoint(t, ctx, fresh, disttae.M0FaultAfterTNAlter)

		reopened := openM0SQL(t, cn)
		defer func() { _ = reopened.Close() }()
		require.Equal(t, afterFirst, m0SchemaProof(t, ctx, reopened, schema, "customer"))
		require.Equal(t, objects, m0ObjectFingerprints(t, ctx, reopened, table))
		require.Equal(t, headers, m0ObjectHeaderProofs(t, ctx, reopened, cn, table))
		require.Equal(t, 1, m0Count(t, ctx, reopened,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='first_committed'"))
		require.Zero(t, m0Count(t, ctx, reopened,
			"select count(*) from information_schema.columns where table_schema='"+schema+
				"' and table_name='customer' and column_name='failed_second'"))

		m0Exec(t, ctx, reopened, "insert into "+table+
			"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance,first_committed) "+
			"values(4,4,4,40000,12000,40,7)")
		m0Exec(t, ctx, reopened, "update "+table+" set first_committed=9 where c_id=1")
		require.Equal(t, 1, m0Count(t, ctx, reopened, "select count(*) from "+table+" where first_committed=9"))
		m0Exec(t, ctx, reopened, "alter table "+table+" add column third_committed int")
		_, version := m0TableGeneration(t, ctx, reopened, schema, "customer")
		require.Equal(t, uint32(2), version)
		require.Equal(t, 4, m0Count(t, ctx, reopened, "select count(*) from "+table+" where third_committed is null"))
	})
}

func TestM0AlterCancelWindow(t *testing.T) {
	m0RunAlterCancelWindow(t, disttae.M0FaultAfterTNAlter, "m0_cancel_window", false)
}

func TestM0AlterDeadlineTNAppliedWindow(t *testing.T) {
	// MySQL client deadlines are socket-local. Give the CN SQL executor a real
	// server-side deadline, which MORPC propagates as remaining timeout to TN.
	m0RunAlterCancelWindow(t, "fj/m0/alter/tn-after-apply", "m0_cancel_tn_applied", true)
}

func m0RunAlterCancelWindow(t *testing.T, waitName, schema string, deadline bool) {
	t.Helper()
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer func() { _ = db.Close() }()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		fresh := openM0SQL(t, cn)
		defer func() { _ = fresh.Close() }()
		getter := waitName + "/waiters"
		t.Cleanup(func() {
			m0RemoveFaultPointQuiet(context.Background(), fresh, getter)
			m0RemoveFaultPointQuiet(context.Background(), fresh, waitName)
		})
		defer func() {
			m0RemoveFaultPointQuiet(context.Background(), fresh, getter)
			m0RemoveFaultPointQuiet(context.Background(), fresh, waitName)
		}()
		before := m0SchemaProof(t, ctx, fresh, schema, "customer")

		m0AddFaultPoint(t, ctx, fresh, waitName, "wait", "")
		m0AddFaultPoint(t, ctx, fresh, getter, "getwaiters", waitName)
		alterCtx, alterCancel := context.WithCancel(ctx)
		if deadline {
			alterCancel()
			alterCtx, alterCancel = context.WithTimeout(ctx, 5*time.Second)
		}
		defer alterCancel()
		done := make(chan error, 1)
		if deadline {
			service := cn.RawService().(cnservice.Service)
			alterCtx = defines.AttachAccountId(alterCtx, 0)
			op, createErr := service.GetTxnClient().New(alterCtx, timestamp.Timestamp{},
				client.WithBeginAutoCommit(false, true), client.WithTxnMode(txn.TxnMode_Pessimistic),
				client.WithTxnIsolation(txn.TxnIsolation_RC))
			require.NoError(t, createErr)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_ = op.Rollback(cleanupCtx)
			}()
			require.NoError(t, service.GetEngine().New(alterCtx, op))
			go func() {
				result, execErr := service.GetSQLExecutor().Exec(alterCtx,
					"alter table "+table+" add column canceled int", executor.Options{}.WithTxn(op))
				if execErr == nil {
					result.Close()
					execErr = op.Commit(alterCtx)
				}
				done <- execErr
			}()
		} else {
			go func() {
				_, execErr := db.ExecContext(alterCtx, "alter table "+table+" add column canceled int")
				done <- execErr
			}()
		}
		finished := false
		defer func() {
			alterCancel()
			m0RemoveFaultPointQuiet(context.Background(), fresh, waitName)
			if !finished {
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("ALTER cleanup failed to join")
				}
			}
		}()
		m0WaitForFaultWaiter(t, ctx, fresh, getter, 1)
		if !deadline {
			alterCancel()
		}
		execErr := m0WaitForError(t, done)
		finished = true
		require.Error(t, execErr)
		m0WaitForFaultWaiter(t, ctx, fresh, getter, 0)
		m0RemoveFaultPoint(t, ctx, fresh, getter)
		m0RemoveFaultPoint(t, ctx, fresh, waitName)

		reopened := openM0SQL(t, cn)
		defer func() { _ = reopened.Close() }()
		require.Eventually(t, func() bool {
			return fmt.Sprint(before) == fmt.Sprint(m0SchemaProof(t, ctx, reopened, schema, "customer"))
		}, 10*time.Second, 20*time.Millisecond)
		m0Exec(t, ctx, reopened, "alter table "+table+" add column retried int")
		_, version := m0TableGeneration(t, ctx, reopened, schema, "customer")
		require.Equal(t, uint32(1), version)
		require.Equal(t, 3, m0Count(t, ctx, reopened, "select count(*) from "+table+" where retried is null"))
	})
}

// The SQL executor can borrow a transaction. This checks the specific
// same-transaction interleaving; it does not assert that a normal client can
// concurrently run two commands on one connection.
func TestM0AlterBorrowedTxnInsertInterleave(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_borrowed_txn_interleave"
		control, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer control.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, control, "alter table "+table+" add column baseline_ok int")
		beforeSchema := m0SchemaProof(t, ctx, control, schema, "customer")
		beforeObjects := m0ObjectHeaderProofs(t, ctx, control, cn, table)
		service := cn.RawService().(cnservice.Service)
		txnCtx, txnCancel := context.WithCancel(defines.AttachAccountId(ctx, 0))
		defer txnCancel()
		op, err := service.GetTxnClient().New(txnCtx, timestamp.Timestamp{},
			client.WithBeginAutoCommit(false, true), client.WithTxnMode(txn.TxnMode_Pessimistic),
			client.WithTxnIsolation(txn.TxnIsolation_RC))
		require.NoError(t, err)
		require.NoError(t, service.GetEngine().New(txnCtx, op))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = op.Rollback(cleanupCtx)
		}()
		waitName := disttae.M0FaultAfterDefs
		getter := waitName + "/waiters"
		alterDone := make(chan error, 1)
		insertDone := make(chan error, 1)
		alterStarted, alterJoined, insertStarted, insertJoined := false, false, false, false
		defer func() {
			txnCancel()
			m0RemoveFaultPointQuiet(context.Background(), control, getter)
			m0RemoveFaultPointQuiet(context.Background(), control, waitName)
			if alterStarted && !alterJoined {
				select {
				case <-alterDone:
				case <-time.After(10 * time.Second):
					t.Error("ALTER cleanup failed to join")
				}
			}
			if insertStarted && !insertJoined {
				select {
				case <-insertDone:
				case <-time.After(10 * time.Second):
					t.Error("INSERT cleanup failed to join")
				}
			}
		}()
		m0AddFaultPoint(t, ctx, control, waitName, "wait", "")
		m0AddFaultPoint(t, ctx, control, getter, "getwaiters", waitName)
		alterStarted = true
		go func() {
			result, execErr := service.GetSQLExecutor().Exec(txnCtx,
				"alter table "+table+" add column borrowed_interleave int", executor.Options{}.WithTxn(op).WithKeepTxnAlive())
			if execErr == nil {
				result.Close()
			}
			alterDone <- execErr
		}()
		m0WaitForFaultWaiter(t, ctx, control, getter, 1)
		insertStarted = true
		go func() {
			result, execErr := service.GetSQLExecutor().Exec(txnCtx,
				"insert into "+table+" (c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values(4,4,4,40000,12000,40)",
				executor.Options{}.WithTxn(op).WithKeepTxnAlive().WithDisableIncrStatement())
			if execErr == nil {
				result.Close()
			}
			insertDone <- execErr
		}()
		var insertErr error
		insertFinished := false
		select {
		case insertErr = <-insertDone:
			insertFinished = true
			insertJoined = true
			t.Log("borrowed INSERT finished before ALTER was released")
		case <-time.After(2 * time.Second):
			t.Log("borrowed INSERT waits while ALTER holds its locks")
		}
		m0RemoveFaultPoint(t, ctx, control, waitName)
		alterErr := m0WaitForError(t, alterDone)
		alterJoined = true
		if !insertFinished {
			insertErr = m0WaitForError(t, insertDone)
			insertJoined = true
		}
		m0WaitForFaultWaiter(t, ctx, control, getter, 0)
		m0RemoveFaultPoint(t, ctx, control, getter)
		require.NoError(t, alterErr)
		require.NoError(t, insertErr)
		commitErr := op.Commit(txnCtx)
		observer := openM0SQL(t, cn)
		defer observer.Close()
		observer.SetMaxIdleConns(0)
		if commitErr != nil {
			require.ErrorContains(t, commitErr, "schema changed, please rollback and retry")
			t.Log("commit rejected the stale write; verifying rollback from a fresh connection")
			rollbackErr := op.Rollback(txnCtx)
			if rollbackErr != nil {
				require.Equal(t, txn.TxnStatus_Aborted, op.Status(), "failed rollback must already be terminal")
			}
			require.Equal(t, 3, m0Count(t, ctx, observer, "select count(*) from "+table))
			require.Equal(t, 0, m0Count(t, ctx, observer, "select count(*) from "+table+" where c_id=4"))
			require.Equal(t, 3, m0Count(t, ctx, observer, "select count(*) from "+table+" where baseline_ok is null"))
			require.Equal(t, 0, m0Count(t, ctx, observer,
				"select count(*) from information_schema.columns where table_schema='"+schema+"' and table_name='customer' and column_name='borrowed_interleave'"))
			require.Equal(t, beforeSchema, m0SchemaProof(t, ctx, observer, schema, "customer"))
			require.Equal(t, beforeObjects, m0ObjectHeaderProofs(t, ctx, observer, cn, table))
			m0Exec(t, ctx, observer, "alter table "+table+" add column retry_borrowed int")
			require.Equal(t, 3, m0Count(t, ctx, observer, "select count(*) from "+table+" where retry_borrowed is null"))
			m0Exec(t, ctx, observer, "insert into "+table+" (c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) values(4,4,4,40000,12000,40)")
			m0Exec(t, ctx, observer, "update "+table+" set retry_borrowed=5 where c_id=4")
			require.Equal(t, 1, m0Count(t, ctx, observer, "select count(*) from "+table+" where c_id=4 and retry_borrowed=5"))
			return
		}
		require.Equal(t, 4, m0Count(t, ctx, observer, "select count(*) from "+table))
		require.Equal(t, 4, m0Count(t, ctx, observer, "select count(*) from "+table+" where borrowed_interleave is null"))
	})
}

func TestM0AlterInsertInterleave(t *testing.T) {
	t.Run("dml before alter", func(t *testing.T) {
		runSQLIntegration(t, func(cluster embed.Cluster) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			cn, err := cluster.GetCNService(0)
			require.NoError(t, err)
			const schema = "m0_dml_before_alter"
			db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
			m0Exec(t, ctx, db, "insert into "+table+" values(4,4,4,40000,12000,40)")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
			beforeHeaders := m0ObjectHeaderProofs(t, ctx, db, cn, table)
			require.GreaterOrEqual(t, len(beforeHeaders), 2)
			m0Exec(t, ctx, db, "alter table "+table+" add column after_dml int")
			observer := openM0SQL(t, cn)
			defer func() { _ = observer.Close() }()
			require.Equal(t, beforeHeaders, m0ObjectHeaderProofs(t, ctx, observer, cn, table))
			require.Equal(t, 4, m0Count(t, ctx, db,
				"select count(*) from "+table+" where after_dml is null"))
			require.Equal(t, 4, m0Count(t, ctx, observer,
				"select count(*) from "+table+" where after_dml is null"))
		})
	})

	t.Run("alter holds lock", func(t *testing.T) {
		runSQLIntegration(t, func(cluster embed.Cluster) {
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
			defer cancel()
			cn, err := cluster.GetCNService(0)
			require.NoError(t, err)
			const schema = "m0_alter_holds_lock"
			control, table := m0PrepareRollbackTable(t, ctx, cn, schema)
			defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
			writer := openM0SQL(t, cn)
			defer func() { _ = writer.Close() }()
			waitName := disttae.M0FaultAfterDefs
			getter := waitName + "/waiters"
			t.Cleanup(func() {
				m0RemoveFaultPointQuiet(context.Background(), control, getter)
				m0RemoveFaultPointQuiet(context.Background(), control, waitName)
			})

			m0AddFaultPoint(t, ctx, control, waitName, "wait", "")
			m0AddFaultPoint(t, ctx, control, getter, "getwaiters", waitName)
			alterDone := make(chan error, 1)
			go func() {
				_, execErr := control.ExecContext(ctx, "alter table "+table+" add column lock_interleave int")
				alterDone <- execErr
			}()
			m0WaitForFaultWaiter(t, ctx, control, getter, 1)

			insertDone := make(chan error, 1)
			go func() {
				_, execErr := writer.ExecContext(ctx,
					"insert into "+table+
						"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) "+
						"values(4,4,4,40000,12000,40)")
				insertDone <- execErr
			}()
			require.Eventually(t, func() bool {
				return m0Count(t, ctx, control,
					"select count(*) from mo_locks() l where l.lock_wait<>''") > 0
			}, 10*time.Second, 20*time.Millisecond)

			m0RemoveFaultPoint(t, ctx, control, waitName)
			require.NoError(t, m0WaitForError(t, alterDone))
			insertErr := m0WaitForError(t, insertDone)
			if insertErr != nil {
				require.True(t,
					moerr.IsMoErrCode(insertErr, moerr.ErrTxnNeedRetry) ||
						moerr.IsMoErrCode(insertErr, moerr.ErrTxnNeedRetryWithDefChanged),
					"%v", insertErr,
				)
				m0Exec(t, ctx, writer, "insert into "+table+
					"(c_id,c_d_id,c_w_id,c_credit_lim,c_ytd_payment,c_balance) "+
					"values(4,4,4,40000,12000,40)")
			}
			m0WaitForFaultWaiter(t, ctx, control, getter, 0)
			m0RemoveFaultPoint(t, ctx, control, getter)

			observer := openM0SQL(t, cn)
			defer func() { _ = observer.Close() }()
			_, version := m0TableGeneration(t, ctx, observer, schema, "customer")
			require.Equal(t, uint32(1), version)
			require.Equal(t, 4, m0Count(t, ctx, control, "select count(*) from "+table))
			require.Equal(t, 4, m0Count(t, ctx, control,
				"select count(*) from "+table+" where lock_interleave is null"))
			require.Equal(t, 4, m0Count(t, ctx, observer, "select count(*) from "+table))
			require.Equal(t, 4, m0Count(t, ctx, observer,
				"select count(*) from "+table+" where lock_interleave is null"))
			m0Exec(t, ctx, observer, "update "+table+" set lock_interleave=5 where c_id in (1,4)")
			require.Equal(t, 2, m0Count(t, ctx, observer,
				"select count(*) from "+table+" where lock_interleave=5"))
		})
	})
}

func TestM0MergeReadsOldObjectsWithoutCallerProof(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_merge_old_objects"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "insert into "+table+" values(4,4,4,40000,12000,40)")
		m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
		objects := m0ObjectFingerprints(t, ctx, db, table)
		require.GreaterOrEqual(t, len(objects), 2)
		m0Exec(t, ctx, db, "alter table "+table+" add column merged_old int")
		require.Equal(t, objects, m0ObjectFingerprints(t, ctx, db, table))
		m0RunL0Merge(t, ctx, db, table)
		mergeDeadline := time.NewTimer(30 * time.Second)
		defer mergeDeadline.Stop()
		poll := time.NewTicker(100 * time.Millisecond)
		defer poll.Stop()
		for {
			after := m0ObjectFingerprints(t, ctx, db, table)
			if len(after) != 0 && !slices.Equal(objects, after) {
				break
			}
			select {
			case <-ctx.Done():
				require.FailNow(t, "L0 merge did not replace source objects", ctx.Err())
			case <-mergeDeadline.C:
				require.FailNow(t, "L0 merge did not replace source objects")
			case <-poll.C:
			}
		}
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+table))
		require.Equal(t, 4, m0Count(t, ctx, db, "select count(*) from "+table+" where merged_old is null"))
		m0Exec(t, ctx, db, "update "+table+" set merged_old=c_id where c_id in (2,4)")
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where merged_old=c_id"))
	})
}

// Mutate only the freshly opened transaction-owned definition, keeping the
// actual persisted relation and old append-shaped objects unchanged.
func TestM0EvolutionAuthorityControlsRealRelation(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_contract_authority"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		before := m0SchemaProof(t, ctx, db, schema, "customer")
		service := cn.RawService().(cnservice.Service)
		for _, kind := range []string{"missing", "format", "binding", "semantics"} {
			t.Run(kind, func(t *testing.T) {
				op, err := service.GetTxnClient().New(ctx, timestamp.Timestamp{}, client.WithBeginAutoCommit(false, true), client.WithTxnMode(txn.TxnMode_Pessimistic), client.WithTxnIsolation(txn.TxnIsolation_RC))
				require.NoError(t, err)
				defer op.Rollback(ctx)
				require.NoError(t, service.GetEngine().New(ctx, op))
				database, err := service.GetEngine().Database(ctx, schema, op)
				require.NoError(t, err)
				rel, err := database.Relation(ctx, "customer", nil)
				require.NoError(t, err)
				extra := rel.GetExtraInfo()
				require.NotNil(t, extra.M0Evolution)
				switch kind {
				case "missing":
					extra.M0Evolution = nil
				case "format":
					extra.M0Evolution.FormatVersion++
				case "binding":
					extra.M0Evolution.RelationId++
				case "semantics":
					extra.M0Evolution.Columns[0].Semantics[0] ^= 1
				}
				err = rel.AlterTable(ctx, nil, []*api.AlterTableReq{api.NewAddColumnReq(rel.GetDBID(ctx), rel.GetTableID(ctx), "forbidden", plan.Type{Id: int32(types.T_int32)}, -1)})
				require.Error(t, err)
				require.ErrorContains(t, err, "evolution")
			})
		}
		require.Equal(t, before, m0SchemaProof(t, ctx, db, schema, "customer"))
		m0Exec(t, ctx, db, "alter table "+table+" add column accepted int")
	})
}
