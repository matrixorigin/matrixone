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

package upgrade

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_8"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type catalogReceiptTestTransport struct {
	coordinator compile.ViewMetadataCoordinator
	calls       int
}

func (t *catalogReceiptTestTransport) ApplyCatalogReceipt(ctx context.Context, r *pb.CatalogMetadataReceipt) (*pb.CatalogMetadataBarrierState, error) {
	// Required/started receipts are sent outside the catalog transaction and can
	// prove committed visibility through another CN. COMPLETE is deliberately
	// submitted while holding the coordinator lock, so another catalog read
	// would deadlock instead of strengthening its publication fence.
	if r.Action != pb.CATALOG_ACTION_COMPLETE {
		s, err := t.coordinator.Read(ctx)
		if err != nil {
			return nil, err
		}
		present := false
		for _, entry := range s.Outbox {
			if entry != nil && entry.Action == r.Action && entry.RequiredGeneration == r.RequiredGeneration {
				present = true
			}
		}
		if !present {
			return nil, fmt.Errorf("outbox was not committed")
		}
	}
	t.calls++
	a := &pb.CatalogMetadataArbitration{ClaimID: r.ClaimID}
	switch r.Action {
	case pb.CATALOG_ACTION_CATALOG_REQUIRED:
		a.RequiredReceipt = r
	case pb.CATALOG_ACTION_RECOVERY_STARTED:
		a.StartedReceipt = r
	case pb.CATALOG_ACTION_COMPLETE:
		a.CompletedReceipt = r
	}
	return &pb.CatalogMetadataBarrierState{MembershipEpoch: r.MembershipEpoch, RequiredGeneration: r.RequiredGeneration, EvidenceInitialized: true, Arbitration: a}, nil
}

type lateCompletionTransport struct {
	receipt  *pb.CatalogMetadataReceipt
	accepted bool
}

func (t *lateCompletionTransport) ApplyCatalogReceipt(_ context.Context, receipt *pb.CatalogMetadataReceipt) (*pb.CatalogMetadataBarrierState, error) {
	t.receipt = receipt
	if !t.accepted {
		return nil, context.DeadlineExceeded
	}
	return &pb.CatalogMetadataBarrierState{MembershipEpoch: receipt.MembershipEpoch, RequiredGeneration: receipt.RequiredGeneration, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{ClaimID: receipt.ClaimID, CompletedReceipt: receipt}}, nil
}

func TestViewMetadataCoordinatorDurableRecovery(t *testing.T) {
	// This fixture needs two independent CNs. The package's reusable base is
	// single-CN and remains alive between cases; explicitly budget this private
	// topology rather than trying to start a second incompatible shared base.
	cluster, err := embed.NewCluster(embed.WithCNCount(2), embed.WithTesting(), embed.WithConcurrentTestClusters())
	require.NoError(t, err)
	defer func() { require.NoError(t, cluster.Close()) }()
	require.NoError(t, cluster.Start())
	func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
		control := func(q string) {
			t.Helper()
			r, err := testutils.GetSQLExecutor(cn0).Exec(ctx, q, executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied())
			require.NoError(t, err, q)
			r.Close()
		}
		exec("create database recovery_sources")
		defer func() {
			exec("drop database recovery_sources")
			control("delete from mo_catalog.mo_view_dependencies where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers')")
			control("delete from mo_catalog.mo_view_refresh where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers')")
			control("delete from mo_catalog.mo_view_recovery_work")
			control("update mo_catalog.mo_view_recovery set state='{\"version\":1}',revision=0,lease_expires_at=null where id=1")
		}()
		exec("create database recovery_consumers")
		defer func() { exec("drop database recovery_consumers") }()
		exec("create table recovery_sources.source_t (x int)")
		exec("create view recovery_sources.v0 as select x from recovery_sources.source_t")
		// Minimum cardinality crossing the 32-row page boundary.
		for i := 1; i < 33; i++ {
			exec(fmt.Sprintf("create view recovery_sources.v%d as select %d as x", i, i))
		}
		exec("create view recovery_consumers.direct_v as select x from recovery_sources.v0")
		exec("create view recovery_consumers.transitive_v as select x from recovery_consumers.direct_v")
		// Same names in another account must not enter a system-account closure.
		exec("create account recovery_tenant admin_name='admin' identified by '111'")
		defer func() { exec("drop account recovery_tenant") }()
		tenantDB, err := sql.Open("mysql", fmt.Sprintf("recovery_tenant#admin#accountadmin:111@tcp(127.0.0.1:%d)/", cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, tenantDB.Close()) }()
		for _, q := range []string{"create database recovery_sources", "create table recovery_sources.source_t(x int)", "create view recovery_sources.v0 as select x from recovery_sources.source_t"} {
			_, err := tenantDB.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		var tenantAccount uint32
		require.NoError(t, db.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name='recovery_tenant'").Scan(&tenantAccount))
		first := compile.ViewMetadataCoordinator{SQL: testutils.GetSQLExecutor(cn0)}
		second := compile.ViewMetadataCoordinator{SQL: testutils.GetSQLExecutor(cn1)}
		for i := 0; i < 2; i++ {
			require.NoError(t, first.SQL.ExecTxn(ctx, func(txn executor.TxnExecutor) error { return v4_0_8.Handler.HandleClusterUpgrade(ctx, txn) }, executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied()))
		}
		transport := &catalogReceiptTestTransport{coordinator: second}
		publish := func(c compile.ViewMetadataCoordinator) {
			t.Helper()
			for i := 0; i < 3; i++ {
				more, err := c.Publish(ctx, transport)
				require.NoError(t, err)
				if !more {
					return
				}
			}
		}
		run := func(c compile.ViewMetadataCoordinator, claim compile.ViewRecoveryClaim) {
			t.Helper()
			finished := false
			for i := 0; i < 180; i++ {
				progress, err := c.Page(ctx, claim)
				require.NoError(t, err)
				if !progress {
					finished = true
					break
				}
			}
			require.True(t, finished, "bounded fixture did not converge")
			require.NoError(t, c.Complete(ctx, claim))
		}
		rollback := errors.New("injected outer transaction rollback")
		err = first.SQL.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			if err := compile.RequireViewMetadataRecoveryInTxn(txn, 1, 1, compile.ViewRecoveryScope{All: true}); err != nil {
				return err
			}
			return rollback
		}, executor.Options{}.WithAccountID(catalog.System_Account))
		require.ErrorIs(t, err, rollback)
		rolledBack, err := second.Read(ctx)
		require.NoError(t, err)
		require.Zero(t, rolledBack.Generation)
		require.Zero(t, rolledBack.WorkRows)
		require.NoError(t, first.Require(ctx, 1, 1, compile.ViewRecoveryScope{All: true}))
		require.NoError(t, first.Require(ctx, 1, 1, compile.ViewRecoveryScope{All: true}))
		require.Error(t, first.Require(ctx, 1, 1, compile.ViewRecoveryScope{Database: "recovery_sources"}))
		publish(first)
		canceled, stop := context.WithCancel(ctx)
		stop()
		canceledClaim, err := first.Claim(canceled, 1, 1, 11, "canceled-worker")
		require.Error(t, err)
		require.Empty(t, canceledClaim)
		a, err := first.Claim(ctx, 1, 1, 11, "cn-a")
		require.NoError(t, err)
		require.Error(t, first.Complete(ctx, a))
		oldSameOwner := a
		a, err = first.Claim(ctx, 1, 1, 11, "cn-a")
		require.NoError(t, err)
		require.Greater(t, a.LeaseEpoch, oldSameOwner.LeaseEpoch)
		_, err = first.Page(ctx, oldSameOwner)
		require.Error(t, err, "reusing an owner name after restart must fence its old token")
		_, err = second.Claim(ctx, 1, 1, 11, "cn-b")
		require.Error(t, err)
		for i := 0; i < 4; i++ {
			_, err = first.Page(ctx, a)
			require.NoError(t, err)
		}
		before, err := second.Read(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, before.WorkRows, uint64(36))
		var cursor uint64
		require.NoError(t, db.QueryRowContext(ctx, "select cursor_relation from mo_catalog.mo_view_recovery_work where generation=1 and kind='scan'").Scan(&cursor))
		require.NotZero(t, cursor)
		// No sleeps: expire the durable lease, then construct a fresh coordinator on
		// another CN. It must retain the cursor/visited set and fence the old worker.
		control("update mo_catalog.mo_view_recovery set lease_expires_at=date_sub(now(),interval 1 second) where id=1")
		b, err := second.Claim(ctx, 1, 1, 11, "cn-b")
		require.NoError(t, err)
		require.Greater(t, b.LeaseEpoch, a.LeaseEpoch)
		after, err := second.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, before.WorkRows, after.WorkRows)
		_, err = first.Page(ctx, a)
		require.Error(t, err)
		require.Error(t, first.Complete(ctx, a))
		run(second, b)
		publish(second)
		var current int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_refresh where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers') and status='CURRENT' and completed_generation=target_generation").Scan(&current))
		require.Equal(t, 35, current)
		var tenantRelation, tenantGeneration uint64
		require.NoError(t, db.QueryRowContext(ctx, "select target_relation_id,target_generation from mo_catalog.mo_view_refresh where account_id=? and target_database_name='recovery_sources' and target_relation_name='v0'", tenantAccount).Scan(&tenantRelation, &tenantGeneration))
		var target, siblingGeneration uint64
		require.NoError(t, db.QueryRowContext(ctx, "select target_relation_id from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and target_relation_name='v0'").Scan(&target))
		require.NoError(t, db.QueryRowContext(ctx, "select target_generation from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and target_relation_name='v1'").Scan(&siblingGeneration))
		ok, err := first.IsCurrent(ctx, 1, 1, 0, target)
		require.NoError(t, err)
		require.True(t, ok)
		err = first.SQL.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			r, err := txn.Exec("alter table recovery_sources.source_t modify column x bigint", executor.StatementOption{})
			if err != nil {
				return err
			}
			r.Close()
			return compile.RequireViewMetadataRecoveryInTxn(txn, 2, 2, compile.ViewRecoveryScope{Database: "recovery_sources", Relation: "source_t"})
		}, executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied())
		require.NoError(t, err)
		ok, err = first.IsCurrent(ctx, 2, 2, 0, target)
		require.NoError(t, err)
		require.False(t, ok, "another CN cannot publish restored metadata before completion")
		require.Error(t, second.Complete(ctx, b))
		publish(second)
		next, err := first.Claim(ctx, 2, 2, 22, "cn-a-restarted")
		require.NoError(t, err)
		run(first, next)
		publish(first)
		var siblingAfter, affected uint64
		require.NoError(t, db.QueryRowContext(ctx, "select target_generation from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and target_relation_name='v1'").Scan(&siblingAfter))
		require.Equal(t, siblingGeneration, siblingAfter, "table scope must not reset its account or siblings")
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_recovery_work where generation=2 and kind='node'").Scan(&affected))
		require.Equal(t, uint64(3), affected, "the exact transitive reverse closure is durable")
		ok, err = second.IsCurrent(ctx, 2, 2, 0, target)
		require.NoError(t, err)
		require.True(t, ok)
		// A real public restore while lifecycle admission is disabled must still
		// invalidate the independent coordinator's old proof on another CN.
		exec("create snapshot recovery_snapshot for database recovery_sources")
		defer func() { exec("drop snapshot recovery_snapshot") }()
		exec("alter table recovery_sources.source_t modify column x smallint")
		ok, err = second.IsCurrent(ctx, 2, 2, 0, target)
		require.NoError(t, err)
		require.False(t, ok)
		exec("restore table recovery_sources.source_t{snapshot='recovery_snapshot'}")
		ok, err = second.IsCurrent(ctx, 2, 2, 0, target)
		require.NoError(t, err)
		require.False(t, ok)
		require.NoError(t, first.Require(ctx, 3, 3, compile.ViewRecoveryScope{Database: "recovery_sources", Relation: "source_t"}))
		publish(first)
		restored, err := second.Claim(ctx, 3, 3, 33, "cn-b-restored")
		require.NoError(t, err)
		run(second, restored)
		publish(second)
		ok, err = first.IsCurrent(ctx, 3, 3, 0, target)
		require.NoError(t, err)
		require.True(t, ok)
		var bigColumns int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where lower(data_type)='bigint' and ((table_schema='recovery_sources' and table_name='v0') or table_schema='recovery_consumers')").Scan(&bigColumns))
		require.Equal(t, 3, bigColumns)
		// INVALID is terminal recovery work, not evidence that its View is usable.
		exec("alter table recovery_sources.source_t add column y int")
		exec("alter table recovery_sources.source_t drop column x")
		// Model first recovery with no durable graph. The planner-level legacy
		// fixture separately removes dependencies from ViewData itself.
		control("delete from mo_catalog.mo_view_dependencies where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers')")
		require.NoError(t, first.Require(ctx, 4, 4, compile.ViewRecoveryScope{}))
		publish(first)
		invalid, err := second.Claim(ctx, 4, 4, 44, "cn-b-invalid")
		require.NoError(t, err)
		run(second, invalid)
		publish(second)
		ok, err = first.IsCurrent(ctx, 4, 4, 0, target)
		require.NoError(t, err)
		require.False(t, ok)
		var invalidViews int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_refresh where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers') and status='INVALID'").Scan(&invalidViews))
		require.Equal(t, 3, invalidViews)
		var tenantAfter uint64
		require.NoError(t, db.QueryRowContext(ctx, "select target_generation from mo_catalog.mo_view_refresh where account_id=? and target_relation_id=?", tenantAccount, tenantRelation).Scan(&tenantAfter))
		require.Equal(t, tenantGeneration, tenantAfter)
		ok, err = first.IsCurrent(ctx, 4, 4, tenantAccount, tenantRelation)
		require.NoError(t, err)
		require.True(t, ok)
		var retainedInvalidEdges int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_dependencies where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers') and source_database_name='recovery_sources' and source_relation_name='source_t'").Scan(&retainedInvalidEdges))
		require.Equal(t, 1, retainedInvalidEdges, "the first INVALID pass must retain its direct reverse-discovery edge")
		exec("alter table recovery_sources.source_t add column x bigint")
		require.NoError(t, first.Require(ctx, 5, 5, compile.ViewRecoveryScope{Database: "recovery_sources", Relation: "source_t"}))
		publish(first)
		repaired, err := second.Claim(ctx, 5, 5, 55, "cn-b-invalid-repair")
		require.NoError(t, err)
		run(second, repaired)
		progress, err := second.Publish(ctx, transport) // committed RECOVERY_STARTED
		require.NoError(t, err)
		require.True(t, progress)
		late := &lateCompletionTransport{}
		progress, err = second.Publish(ctx, late)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, progress)
		require.NotNil(t, late.receipt)
		mutationSQL := compile.ViewMetadataRequireRevalidationSQL()
		_, err = first.SQL.Exec(ctx, mutationSQL[len(mutationSQL)-1], executor.Options{}.WithAccountID(catalog.System_Account))
		require.Error(t, err, "an unresolved COMPLETE proposal must fail catalog mutation closed")
		late.accepted = true // the timed-out Dragonboat proposal commits after its caller returned
		progress, err = second.Publish(ctx, late)
		require.NoError(t, err)
		require.True(t, progress)
		ok, err = first.IsCurrent(ctx, 5, 5, 0, target)
		require.NoError(t, err)
		require.True(t, ok, "repairing only the source table must rediscover the previously INVALID View")
		mutationResult, err := first.SQL.Exec(ctx, mutationSQL[len(mutationSQL)-1], executor.Options{}.WithAccountID(catalog.System_Account))
		require.NoError(t, err)
		mutationResult.Close()
		ok, err = first.IsCurrent(ctx, 5, 5, 0, target)
		require.NoError(t, err)
		require.False(t, ok, "the first mutation after resolving COMPLETE must invalidate its proof")
		// The lowest-ID View now has a temporarily unavailable dependency. Its
		// durable backoff must not starve the healthy Views on the next page.
		exec("drop table recovery_sources.source_t")
		require.NoError(t, first.Require(ctx, 6, 6, compile.ViewRecoveryScope{}))
		publish(first)
		waiting, err := first.Claim(ctx, 6, 6, 66, "cn-a-backoff")
		require.NoError(t, err)
		idle := false
		for i := 0; i < 180; i++ {
			progress, err := first.Page(ctx, waiting)
			require.NoError(t, err)
			if !progress {
				idle = true
				break
			}
		}
		require.True(t, idle)
		require.Error(t, first.Complete(ctx, waiting))
		var healthy, retrying int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and status='CURRENT'").Scan(&healthy))
		require.Equal(t, 32, healthy)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_refresh where account_id=0 and target_database_name in ('recovery_sources','recovery_consumers') and status='DISCOVERING' and failure_code=2 and next_retry_at>now()").Scan(&retrying))
		require.Equal(t, 3, retrying)
		publish(first)
		require.Error(t, first.Require(ctx, 7, 7, compile.ViewRecoveryScope{Database: "recovery_sources", Relation: "source_t"}), "supersession cannot discard unfinished account work")
		exec("create table recovery_sources.source_t(x bigint)")
		_, err = first.Page(ctx, waiting)
		require.Error(t, err, "old mutation snapshot cannot continue")
		require.NoError(t, first.Require(ctx, 7, 7, compile.ViewRecoveryScope{}))
		publish(first)
		resumed, err := second.Claim(ctx, 7, 7, 77, "cn-b-resumed")
		require.NoError(t, err)
		run(second, resumed)
		publish(second)
		var beforeViewRestore uint64
		require.NoError(t, db.QueryRowContext(ctx, "select target_generation from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and target_relation_name='v1'").Scan(&beforeViewRestore))
		exec("restore table recovery_sources.v0{snapshot='recovery_snapshot'}")
		ok, err = first.IsCurrent(ctx, 7, 7, 0, target)
		require.NoError(t, err)
		require.False(t, ok)
		var restoredViewID uint64
		require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='recovery_sources' and relname='v0'").Scan(&restoredViewID))
		require.NotEqual(t, target, restoredViewID)
		require.NoError(t, first.Require(ctx, 8, 8, compile.ViewRecoveryScope{Database: "recovery_sources", Relation: "v0"}))
		publish(first)
		viewClaim, err := second.Claim(ctx, 8, 8, 88, "cn-b-view-restore")
		require.NoError(t, err)
		run(second, viewClaim)
		publish(second)
		ok, err = first.IsCurrent(ctx, 8, 8, 0, restoredViewID)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = first.IsCurrent(ctx, 8, 8, 0, target)
		require.NoError(t, err)
		require.False(t, ok)
		var orphanRows int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_refresh where account_id=0 and target_relation_id=?", target).Scan(&orphanRows))
		require.Zero(t, orphanRows)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_view_dependencies where account_id=0 and target_relation_id=?", target).Scan(&orphanRows))
		require.Zero(t, orphanRows)
		require.NoError(t, db.QueryRowContext(ctx, "select target_generation from mo_catalog.mo_view_refresh where account_id=0 and target_database_name='recovery_sources' and target_relation_name='v1'").Scan(&siblingAfter))
		require.Equal(t, beforeViewRestore, siblingAfter)
		for i := 0; i < 4; i++ {
			progress, err := second.Cleanup(ctx)
			require.NoError(t, err)
			if !progress {
				break
			}
		}
		final, err := first.Read(ctx)
		require.NoError(t, err)
		require.Zero(t, final.WorkRows)
		require.Equal(t, uint64(8), final.Completed)
		require.False(t, compile.ViewMetadataRefreshEnabled(cn0.ServiceID()))
	}(cluster)
}
