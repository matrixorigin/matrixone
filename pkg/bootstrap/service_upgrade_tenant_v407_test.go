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

package bootstrap

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_18"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_19"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestV407TaskCompletionAfterLastAccountCommit(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		accountVersions := []string{"4.0.7", "4.0.6"}
		taskReady := int32(versions.No)
		taskUpdates := 0
		exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			switch {
			case strings.HasPrefix(sql, "select from_account_id, to_account_id, upgrade_id, ready"):
				mp := mpool.MustNewZeroNoFixed()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				result := executor.NewMemResult([]types.Type{types.T_int32.ToType(), types.T_int32.ToType(), types.T_uint64.ToType(), types.T_int32.ToType()}, mp)
				result.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendFixedRows(result, 0, []int32{1}))
				require.NoError(t, executor.AppendFixedRows(result, 1, []int32{2}))
				require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{10}))
				require.NoError(t, executor.AppendFixedRows(result, 3, []int32{taskReady}))
				return result.GetResult(), nil
			case strings.HasPrefix(sql, "select account_id, create_version from mo_account"):
				return ownRecoveryResult(t, buildUpgradeTenantAccountRows([]int32{1, 2}, accountVersions)), nil
			case strings.Contains(sql, "from mo_upgrade") && strings.Contains(sql, "for update"):
				return ownRecoveryResult(t, buildUpgradeVersionResult(10, versions.StateUpgradingTenant, "4.0.6", "4.0.7", 41, 0, versions.Yes, versions.Yes, 2, 2)), nil
			case strings.HasPrefix(sql, "update mo_upgrade_tenant set ready = 1"):
				taskReady = versions.Yes
				taskUpdates++
				return executor.Result{}, nil
			default:
				t.Fatalf("unexpected SQL: %s", sql)
				return executor.Result{}, nil
			}
		}, &testTxnOperator{})
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(*service) {})
		t.Cleanup(s.stopper.Stop)
		selected := tenantUpgradeSelection{upgrade: versions.VersionUpgrade{ID: 10, FromVersion: "4.0.6", ToVersion: "4.0.7"}, taskID: 20}
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Zero(t, taskUpdates, "matching counters must not publish a task containing an old account")
		accountVersions[1] = "4.0.7"
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Equal(t, 1, taskUpdates, "a restart after the final account commit must finish the task")
		require.NoError(t, s.finishV407TenantTask(t.Context(), selected))
		require.Equal(t, 1, taskUpdates, "a repeated completion must be a no-op")
	})
}

// A ready semantic version has no offset progress to replay. On the
// merged chain the final handle is 4.0.19, so the converged leg uses a
// cluster READY at 4.0.19/2 with the account already at 4.0.19: the manual
// path reads its route state, runs no migration SQL, and caches the account.
// The frozen-4.0.7 staging pins the other reachable branch of the same
// boundary: a CN at final 4.0.19 refuses a manual tenant upgrade while the
// cluster still reports 4.0.7 as its latest version, because its own cluster
// route has not been created yet. The final==4.0.7 arm inside
// UpgradeOneTenant can no longer fire on this binary; the frozen offset-only
// rejection now lives in the bootstrap guards.
func TestManualV407ReadyAccountDoesNotReplay(t *testing.T) {
	final := v4_0_7.Handler.Metadata()
	for _, midRoll := range []bool{false, true} {
		t.Run(fmt.Sprint(midRoll), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				accountVersion := "4.0.19"
				latest := versions.Version{Version: "4.0.19", VersionOffset: v4_0_19.Handler.Metadata().VersionOffset, State: versions.StateReady}
				wantReads := 3
				if midRoll {
					accountVersion = final.Version
					latest = versions.Version{Version: final.Version, VersionOffset: final.VersionOffset, State: versions.StateReady}
					wantReads = 2
				}
				reads := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					reads++
					switch {
					case sql == "select create_version from mo_account where account_id = 7":
						return newBootstrapStringResult(accountVersion), nil
					case sql == versions.LatestVersionSQL:
						return statisticsLatestVersionResult(t, latest, versions.StateReady), nil
					case strings.Contains(sql, "from mo_upgrade") && !midRoll:
						return executor.Result{}, nil
					default:
						t.Fatalf("ready account must not run migrations: %s", sql)
						return executor.Result{}, nil
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				err := s.UpgradeOneTenant(t.Context(), 7)
				if midRoll {
					require.ErrorContains(t, err,
						"tenant upgrade requires current cn version 4.0.19 to match cluster latest version 4.0.7")
					require.Empty(t, s.mu.tenants)
				} else {
					require.NoError(t, err)
					require.True(t, s.mu.tenants[7])
					require.NoError(t, s.UpgradeOneTenant(t.Context(), 7))
				}
				require.Equal(t, wantReads, reads)
			})
		})
	}
}

// An account at the cluster's final version has no migration to replay. A
// restarted CN must skip DDL rather than silently rebuilding existing objects
// and grants - including a differently stamped final row from another
// generation, which never reaches the tenant walk.
func TestManualFinalReadyAccountDoesNotReplay(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				final := v4_0_19.Handler.Metadata()
				offset := final.VersionOffset
				if mismatch {
					offset++
				}
				reads := 0
				exec := executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
					reads++
					switch {
					case sql == "select create_version from mo_account where account_id = 7":
						return newBootstrapStringResult(final.Version), nil
					case sql == versions.LatestVersionSQL:
						return statisticsLatestVersionResult(t, versions.Version{Version: final.Version, VersionOffset: offset}, versions.StateReady), nil
					case strings.Contains(sql, "from mo_upgrade"):
						return executor.Result{}, nil
					default:
						t.Fatalf("ready account must not run migrations: %s", sql)
						return executor.Result{}, nil
					}
				}, &testTxnOperator{})
				s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) { s.initUpgrade() })
				t.Cleanup(s.stopper.Stop)
				err := s.UpgradeOneTenant(t.Context(), 7)
				require.NoError(t, err)
				require.True(t, s.mu.tenants[7])
				// A differently stamped final row skips the walk entirely; the
				// matching row reads the retained route and replays nothing.
				if mismatch {
					require.Equal(t, 2, reads)
				} else {
					require.Equal(t, 3, reads)
				}
				require.NoError(t, s.UpgradeOneTenant(t.Context(), 7))
				if mismatch {
					require.Equal(t, 2, reads)
				} else {
					require.Equal(t, 3, reads)
				}
			})
		})
	}
}

type systemAccountExecutor struct {
	executor.SQLExecutor
	t     *testing.T
	calls int
}

func (e *systemAccountExecutor) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	e.calls++
	require.Equal(e.t, uint32(7), ctx.Value(defines.TenantIDKey{}))
	require.True(e.t, opts.HasAccountID(), "system catalog access must not inherit the calling tenant")
	require.Equal(e.t, uint32(catalog.System_Account), opts.AccountID())
	return e.SQLExecutor.ExecTxn(ctx, fn, opts)
}

func TestV407AccountTransactionsOverrideCallingTenant(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		exec := &systemAccountExecutor{t: t, SQLExecutor: executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
			require.Equal(t, "select create_version from mo_account where account_id = 7 for update", sql)
			return newBootstrapStringResult("4.0.6"), nil
		}, &testTxnOperator{})}
		s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(*service) {})
		t.Cleanup(s.stopper.Stop)
		ctx := context.WithValue(t.Context(), defines.TenantIDKey{}, uint32(7))
		worked := false
		active, err := s.withV407Account(ctx, versions.VersionUpgrade{FromVersion: "4.0.6", ToVersion: "4.0.7"}, 7, func(executor.TxnExecutor) error { worked = true; return nil })
		require.NoError(t, err)
		require.True(t, active)
		require.True(t, worked)
		require.Equal(t, 1, exec.calls)
	})
}

// successionTxnOperator answers the transaction-identity questions the
// frozen-stage entry logging asks through a mem executor's transaction.
type successionTxnOperator struct {
	testTxnOperator
}

func (tTxnOp *successionTxnOperator) TxnOptions() txn.TxnOptions {
	return txn.TxnOptions{}
}

// ---------------------------------------------------------------------------
// Design revision 12 continuation oracles (D12-3). Every case registers the
// real v4_0_7, upstream v4_0_18 and renumbered v4_0_19 handles through
// initUpgrade: the frozen stage keeps its bounded transactions and
// completion, and the login and manual walks must both step the successor
// chain and reach the final version before caching success.

// successionState is the mem catalog one late account observes: its recorded
// create_version, its persisted information_schema identities, and every
// account-observable write position in temporal order.
type successionState struct {
	version          string
	reads            int
	views            map[string]string
	stageStamps      int // update mo_account set create_version = '4.0.7'
	upstreamStamps   int // update mo_account set create_version = '4.0.18' inside the walking transaction
	successorStamps  int // update mo_account set create_version = final
	columnsWrites    int
	statisticsWrites int
	failStatistics   bool
	injected         error
	SQL              []string
}

// newSuccessionState records what a late account carries before the upgrade:
// the frozen stage's current definitions for the views it migrates, the base
// STATISTICS projection, and the pre-correction COLUMNS identity.
func newSuccessionState(version string) *successionState {
	views := map[string]string{
		"COLLATION_CHARACTER_SET_APPLICABILITY": sysview.InformationSchemaCollationCharacterSetApplicabilityDDL,
		"TABLE_PRIVILEGES":                      sysview.InformationSchemaTablePrivilegesDDL,
		"KEY_COLUMN_USAGE":                      sysview.InformationSchemaKeyColumnUsageDDL,
		"REFERENTIAL_CONSTRAINTS":               sysview.InformationSchemaReferentialConstraintsDDL,
		"CHECK_CONSTRAINTS":                     sysview.InformationSchemaCheckConstraintsDDL,
		"TABLE_CONSTRAINTS":                     sysview.InformationSchemaTableConstraintsDDL,
		"VIEWS":                                 sysview.InformationSchemaViewsDDL,
		"PARTITIONS":                            sysview.InformationSchemaPartitionsDDL,
		"SCHEMATA":                              sysview.InformationSchemaSchemataDDL,
		"TABLES":                                sysview.InformationSchemaTablesV118DDL,
		"STATISTICS":                            sysview.InformationSchemaStatisticsDDL,
		"COLUMNS":                               sysview.InformationSchemaColumnsBinaryFamilyNullDDL(),
	}
	return &successionState{
		version:  version,
		views:    views,
		injected: moerr.NewInternalErrorNoCtxf("successor STATISTICS leg failed"),
	}
}

func successionAuthoredCOLUMNS() string {
	return strings.Replace(sysview.InformationSchemaColumnsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
}

func successionAuthoredSTATISTICS() string {
	return strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
}

// successionCreateTableWithCurrentIndexes maps the guarded-index checks of
// the frozen stage to current catalog definitions, so the stage probes report
// every guarded index installed and never try to rebuild one inside the paged
// upgrade.
var successionCreateTableWithCurrentIndexes = map[string]string{
	"SHOW CREATE TABLE `mo_catalog`.`mo_user_defined_function`": "create table mo_user_defined_function (name varchar(100), " +
		"db varchar(100), arg_types text, unique key name_db_arg_types(name, db, arg_types))",
	"SHOW CREATE TABLE `mo_catalog`.`mo_role_grant`": "create table mo_role_grant (grantee_id bigint, " +
		"key idx_mo_role_grant_grantee_id(grantee_id))",
	"SHOW CREATE TABLE `mo_catalog`.`mo_iceberg_catalogs`": "create table mo_iceberg_catalogs (catalog_id bigint, " +
		"key catalog_id_allocator(catalog_id))",
}

// successionShowCreateResult answers SHOW CREATE TABLE with one row of two
// columns (table name, create statement).
func successionShowCreateResult(t *testing.T, table, createSQL string) executor.Result {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	memRes := executor.NewMemResult(
		[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
	memRes.NewBatchWithRowCount(1)
	executor.AppendStringRows(memRes, 0, []string{table})
	executor.AppendStringRows(memRes, 1, []string{createSQL})
	return memRes.GetResult()
}

// successionIndexCatalogCount serves a physically consistent index catalog
// triplet: one index id, one index table, one hidden index-table row.
func successionIndexCatalogCount(t *testing.T) executor.Result {
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	memRes := executor.NewMemResult([]types.Type{
		types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(memRes, 0, []int64{1}))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, []int64{1}))
	require.NoError(t, executor.AppendFixedRows(memRes, 2, []int64{1}))
	return memRes.GetResult()
}

// successionExecutor serves the catalog this account observes: frozen-stage
// probes read the recorded identities, writes update them, and every SQL
// keeps its position in the ordered log.
func successionExecutor(t *testing.T, state *successionState, final versions.Version) executor.SQLExecutor {
	return executor.NewMemExecutor2(func(sql string) (executor.Result, error) {
		state.SQL = append(state.SQL, sql)
		switch {
		case sql == fmt.Sprintf("select create_version from mo_account where account_id = %d", 7),
			sql == fmt.Sprintf("select create_version from mo_account where account_id = %d for update", 7):
			state.reads++
			return newBootstrapStringResult(state.version), nil
		case strings.HasPrefix(sql, "select version, version_offset, state from mo_version"):
			return statisticsLatestVersionResult(t, final, versions.StateReady), nil
		case sql == "SELECT mo_ctl('cn', 'GetProtocolVersion', '')":
			return newBootstrapStringResult(`{"result":"cn:134"}`), nil
		case strings.HasPrefix(sql, "SELECT tbl.rel_createsql"):
			name := successionViewName(sql)
			definition, known := state.views[name]
			if !known {
				return executor.Result{}, moerr.NewInternalErrorNoCtxf("unexpected view read %q", name)
			}
			return newBootstrapStringResult(definition), nil
		case sql == successionAuthoredCOLUMNS():
			state.columnsWrites++
			state.views["COLUMNS"] = sysview.InformationSchemaColumnsDDL
			return executor.Result{}, nil
		case sql == successionAuthoredSTATISTICS():
			if state.failStatistics {
				return executor.Result{}, state.injected
			}
			state.statisticsWrites++
			state.views["STATISTICS"] = sysview.InformationSchemaStatisticsPrefixSubPartDDL
			return executor.Result{}, nil
		case strings.HasPrefix(sql, "update mo_account set create_version = '4.0.6' where account_id = 7"):
			// Upstream's `>=` register comparison in the manual walk re-executes
			// the account's own-version handler and re-stamps it before
			// reaching the frozen stage; the re-stamp changes nothing.
			return executor.Result{AffectedRows: 1}, nil
		case sql == successionV407Stamp():
			state.stageStamps++
			state.version = v4_0_7.Handler.Metadata().Version
			return executor.Result{AffectedRows: 1}, nil
		case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", v4_0_18.Handler.Metadata().Version, 7):
			// The merged register walk steps the upstream 4.0.18 handle once
			// before the final vehicle. An aborted walking transaction deletes
			// this stamp with the transaction, so it never becomes part of the
			// durable recorded version: a failed pass simply counts it again
			// when it re-runs.
			state.upstreamStamps++
			return executor.Result{AffectedRows: 1}, nil
		case sql == fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d", final.Version, 7):
			state.successorStamps++
			state.version = final.Version
			return executor.Result{AffectedRows: 1}, nil
		default:
			if createSQL, ok := successionCreateTableWithCurrentIndexes[sql]; ok {
				table := strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(sql,
					"SHOW CREATE TABLE `mo_catalog`."), "`"), "`")
				return successionShowCreateResult(t, table, createSQL), nil
			}
			// Every remaining frozen-stage read probes a catalog that has
			// nothing left to reconcile: no legacy index, no foreign-key
			// table, no pending backfill row, no stale metadata column.
			if strings.HasPrefix(sql, "select count(distinct i.id), count(distinct i.index_table_name)") {
				return successionIndexCatalogCount(t), nil
			}
			lower := strings.ToLower(sql)
			frozenStageRead := strings.Contains(lower, "mo_show_visible_bin(atttyp, 2)") ||
				strings.HasPrefix(lower, "select reldatabase, relname, account_id from mo_catalog.mo_tables") ||
				strings.Contains(lower, "mo_indexes") ||
				strings.Contains(lower, "from mo_catalog.mo_foreign_keys") ||
				strings.Contains(lower, "mo_catalog.mo_user_defined_function") ||
				strings.Contains(lower, "mo_catalog.mo_columns") ||
				strings.Contains(lower, "from information_schema.") ||
				strings.HasPrefix(lower, "select rel_id, rel_logical_id") ||
				strings.HasPrefix(lower, "show create table")
			frozenStageWrite := strings.HasPrefix(lower, "alter table mo_catalog.") ||
				strings.HasPrefix(lower, "update mo_catalog.") ||
				strings.HasPrefix(lower, "delete from information_schema.") ||
				strings.HasPrefix(lower, "insert into information_schema.") ||
				strings.HasPrefix(lower, "create table mo_catalog.") ||
				strings.HasPrefix(lower, "create table information_schema.") ||
				strings.HasPrefix(lower, "create index") ||
				strings.HasPrefix(lower, "create unique index") ||
				strings.HasPrefix(lower, "drop index") ||
				strings.HasPrefix(lower, "drop view if exists information_schema.") ||
				strings.HasPrefix(lower, "drop table if exists")
			if frozenStageRead || frozenStageWrite {
				return executor.Result{}, nil
			}
			fmt.Fprintf(os.Stderr, "succession fixture rejected SQL: %s\n", sql)
			return executor.Result{}, moerr.NewInternalErrorNoCtxf("unexpected SQL: %s", sql)
		}
	}, &successionTxnOperator{})
}

// successionViewName extracts the information_schema view name a persisted
// definition read asks for, whether the catalog predicate quotes it in
// single quotes or backticks, and normalizes the case: the shared builder
// probes the exact and the lowercase-folded name.
func successionViewName(sql string) string {
	const marker = "relname = "
	start := strings.LastIndex(sql, marker)
	if start < 0 {
		return ""
	}
	rest := sql[start+len(marker):]
	quote := byte('\'')
	if strings.HasPrefix(rest, "`") {
		quote = '`'
	}
	if len(rest) < 2 || rest[0] != quote {
		return ""
	}
	if end := strings.IndexByte(rest[1:], quote); end >= 0 {
		return strings.ToUpper(rest[1 : 1+end])
	}
	return ""
}

func successionV407Stamp() string {
	return fmt.Sprintf("update mo_account set create_version = '%s' where account_id = %d",
		v4_0_7.Handler.Metadata().Version, 7)
}

func successionNewService(t *testing.T, exec executor.SQLExecutor, completed bool) *service {
	s := newServiceForTest("", &memLocker{}, clock.NewHLCClock(func() int64 { return 0 }, 0), nil, exec, func(s *service) {
		s.initUpgrade()
	})
	t.Cleanup(s.stopper.Stop)
	if completed {
		s.upgrade.finalVersionCompleted.Store(true)
	}
	return s
}

func successionIndexOfSQL(logged []string, target string) int {
	for i, sql := range logged {
		if sql == target {
			return i
		}
	}
	return -1
}

// TestSuccessionLateLoginConvergesToFinal drives the login compensation for a
// late 4.0.6 account under the real frozen-stage plus successor registration:
// the paged 4.0.7 stage commits first, then the on-demand continuation must
// resume the register walk to the final version before success is cached.
func TestSuccessionLateLoginConvergesToFinal(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := v4_0_19.Handler.Metadata()
		state := newSuccessionState("4.0.6")
		s := successionNewService(t, successionExecutor(t, state, final), false)
		fetch := func() (int32, string, error) { return 7, "4.0.6", nil }

		upgraded, err := s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.True(t, upgraded)

		require.Equal(t, 1, state.stageStamps, "the paged stage stamps 4.0.7 exactly once")
		require.Equal(t, 1, state.upstreamStamps, "the login walk steps the upstream 4.0.18 handle once")
		require.Equal(t, 1, state.successorStamps, "the continuation must stamp the final before caching")
		require.Equal(t, final.Version, state.version)

		require.Equal(t, 1, state.statisticsWrites)
		require.Greater(t, successionIndexOfSQL(state.SQL, successionAuthoredSTATISTICS()),
			successionIndexOfSQL(state.SQL, successionV407Stamp()),
			"no successor SQL before the frozen stage's committed stamp")

		require.Equal(t, sysview.InformationSchemaColumnsDDL, state.views["COLUMNS"])
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, state.views["STATISTICS"])

		// A completed second login must be an idempotent no-op.
		reads, writes := state.reads, state.columnsWrites+state.statisticsWrites
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Equal(t, reads, state.reads)
		require.Equal(t, writes, state.columnsWrites+state.statisticsWrites)
		require.True(t, s.mu.tenants[7])
	})
}

// TestSuccessionManualBelowFrozenChainsStages drives the manual upgrade from
// 4.0.6: the paged stage stamps 4.0.7, and the manual walk's continuation
// must chain through the successor before reporting success.
func TestSuccessionManualBelowFrozenChainsStages(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := v4_0_19.Handler.Metadata()
		state := newSuccessionState("4.0.6")
		s := successionNewService(t, successionExecutor(t, state, final), true)

		require.NoError(t, s.UpgradeOneTenant(t.Context(), 7))
		require.Equal(t, 1, state.stageStamps)
		require.Equal(t, 1, state.upstreamStamps, "the manual walk steps the upstream 4.0.18 handle once")
		require.Equal(t, 1, state.successorStamps)
		require.Equal(t, final.Version, state.version)
		require.Equal(t, 1, state.statisticsWrites)
		require.Greater(t, successionIndexOfSQL(state.SQL, successionAuthoredSTATISTICS()),
			successionIndexOfSQL(state.SQL, successionV407Stamp()))
		require.Equal(t, sysview.InformationSchemaColumnsDDL, state.views["COLUMNS"])
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, state.views["STATISTICS"])
		require.True(t, s.mu.tenants[7])
	})
}

// TestSuccessionManualAtFrozenStampRunsSuccessor drives the manual upgrade
// for an account already at 4.0.7: the v4_0_7 branch may not break the
// register walk with nothing done, the successor legs must publish, and the
// final create_version must be stamped before success is cached.
func TestSuccessionManualAtFrozenStampRunsSuccessor(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := v4_0_19.Handler.Metadata()
		state := newSuccessionState("4.0.7")
		s := successionNewService(t, successionExecutor(t, state, final), true)

		require.NoError(t, s.UpgradeOneTenant(t.Context(), 7))
		require.Zero(t, state.stageStamps, "an account already at 4.0.7 must not replay the paged stage")
		require.Equal(t, 1, state.upstreamStamps, "the manual walk steps the upstream 4.0.18 handle once")
		require.Equal(t, 1, state.successorStamps, "the manual walk must stamp the final version")
		require.Equal(t, final.Version, state.version)
		require.Equal(t, 1, state.columnsWrites, "the successor COLUMNS leg must publish")
		require.Equal(t, 1, state.statisticsWrites, "the successor STATISTICS leg must publish")
		require.Equal(t, sysview.InformationSchemaColumnsDDL, state.views["COLUMNS"])
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, state.views["STATISTICS"])
		require.True(t, s.mu.tenants[7])
	})
}

// TestSuccessionSuccessorFailureCachesNothing drives a successor-leg failure
// after the frozen stage committed: the account keeps its rolled-forward
// create_version, the success cache stays unset, and the retried login
// resumes from the committed stamp without rerunning the paged stage.
func TestSuccessionSuccessorFailureCachesNothing(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		final := v4_0_19.Handler.Metadata()
		state := newSuccessionState("4.0.6")
		state.failStatistics = true
		s := successionNewService(t, successionExecutor(t, state, final), false)
		fetch := func() (int32, string, error) { return 7, "4.0.6", nil }

		upgraded, err := s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.Error(t, err)
		require.False(t, upgraded)
		require.Empty(t, s.mu.tenants, "a successor-leg failure must not cache success")
		require.Equal(t, 1, state.stageStamps)
		require.Zero(t, state.successorStamps)
		require.Equal(t, v4_0_7.Handler.Metadata().Version, state.version,
			"the account resumes from its last committed stage")
		require.Equal(t, 1, state.upstreamStamps, "the aborted walking transaction must not persist the upstream stamp into the recorded version")

		state.failStatistics = false
		upgraded, err = s.MaybeUpgradeTenant(t.Context(), fetch, nil)
		require.NoError(t, err)
		require.True(t, upgraded)
		require.True(t, s.mu.tenants[7])
		require.Equal(t, 1, state.stageStamps, "the committed 4.0.7 stage must not run twice")
		require.Equal(t, 2, state.upstreamStamps, "the rolled-back upstream stamp re-runs exactly once on resume")
		require.Equal(t, 1, state.successorStamps)
		require.Equal(t, 1, state.statisticsWrites, "the identities publish exactly once")
		require.Equal(t, final.Version, state.version)
		require.Equal(t, sysview.InformationSchemaStatisticsPrefixSubPartDDL, state.views["STATISTICS"])
	})
}
