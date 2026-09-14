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

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

const (
	issue26114FeatureDescription       = "Branch feature 'legacy'"
	issue26114CustomFeatureDescription = `Branch feature \ 'legacy'`
)

func execIssue26114SQLRequire(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func execIssue26114SQLMaybe(ctx context.Context, db *sql.DB, statement string) error {
	_, err := db.ExecContext(ctx, statement)
	return err
}

func cleanupIssue26114Catalog(ctx context.Context, db *sql.DB, statements ...string) error {
	var cleanupErr error
	for _, statement := range statements {
		if err := execIssue26114SQLMaybe(ctx, db, statement); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("%s: %w", statement, err))
		}
	}
	return cleanupErr
}

type issue26114SnapshotSource struct {
	database string
	tables   []string
}

func verifyIssue26114NoBranchArtifacts(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	accountID int32,
	branchTableIDs []uint64,
	snapshotSources ...issue26114SnapshotSource,
) {
	t.Helper()
	checks := []struct {
		name  string
		query string
	}{
		{
			name: "feature limit",
			query: fmt.Sprintf(
				"select count(*) from mo_catalog.mo_feature_limit where account_id = %d and feature_code = 'BRANCH'",
				accountID),
		},
		{
			name: "branch metadata for account",
			query: fmt.Sprintf(
				"select count(*) from mo_catalog.mo_branch_metadata b left join mo_catalog.mo_tables t on b.table_id = t.rel_id where b.creator = %d or t.account_id = %d",
				accountID, accountID),
		},
	}
	if len(branchTableIDs) > 0 {
		ids := make([]string, 0, len(branchTableIDs))
		for _, tableID := range branchTableIDs {
			ids = append(ids, fmt.Sprintf("%d", tableID))
		}
		checks = append(checks, struct {
			name  string
			query string
		}{
			name: "branch metadata by table id",
			query: fmt.Sprintf(
				"select count(*) from mo_catalog.mo_branch_metadata where table_id in (%s)",
				strings.Join(ids, ",")),
		})
	}
	for _, source := range snapshotSources {
		tables := make([]string, 0, len(source.tables))
		for _, table := range source.tables {
			tables = append(tables, issue26114QuoteSQLString(table))
		}
		checks = append(checks, struct {
			name  string
			query string
		}{
			name: "protected branch snapshot",
			query: fmt.Sprintf(
				"select count(*) from mo_catalog.mo_snapshots where kind = 'branch' and database_name = %s and table_name in (%s)",
				issue26114QuoteSQLString(source.database), strings.Join(tables, ",")),
		})
	}
	for _, check := range checks {
		var count int
		if err := db.QueryRowContext(ctx, check.query).Scan(&count); err != nil {
			t.Errorf("check %s cleanup: %v", check.name, err)
			continue
		}
		if count != 0 {
			t.Errorf("%s cleanup left %d row(s)", check.name, count)
		}
	}
}

func readIssue26114BranchTableIDs(
	ctx context.Context,
	db *sql.DB,
	accountID int32,
	databaseNames ...string,
) ([]uint64, error) {
	databases := make([]string, 0, len(databaseNames))
	for _, databaseName := range databaseNames {
		databases = append(databases, issue26114QuoteSQLString(databaseName))
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"select rel_id from mo_catalog.mo_tables where account_id = %d and relkind = 'r' and reldatabase in (%s)",
		accountID, strings.Join(databases, ",")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func createIssue26114Account(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	accountName string,
	password string,
) int32 {
	t.Helper()
	accountName = strings.ToLower(accountName)
	statement := fmt.Sprintf(
		"create account %s ADMIN_NAME 'root' IDENTIFIED BY '%s'",
		accountName,
		password,
	)
	lookup := "select account_id from mo_catalog.mo_account where account_name = ? and admin_name = 'root'"
	var lastErr error

	for attempt := 0; attempt < 12; attempt++ {
		_, createErr := db.ExecContext(ctx, statement)
		var accountID int32
		lookupErr := db.QueryRowContext(ctx, lookup, accountName).Scan(&accountID)
		if lookupErr == nil {
			return accountID
		}
		lastErr = lookupErr
		if createErr != nil {
			lastErr = createErr
			if !isIssue26114TransientError(createErr) {
				require.NoErrorf(t, createErr, "create account %s", accountName)
				return 0
			}
		}

		timer := time.NewTimer(time.Duration(attempt+1) * 500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			require.NoError(t, ctx.Err())
			return 0
		case <-timer.C:
		}
	}

	require.NoErrorf(t, lastErr, "create account %s after retries", accountName)
	return 0
}

func isIssue26114TransientError(err error) bool {
	message := strings.ToLower(err.Error())
	for _, token := range []string{
		"timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"unexpected eof",
		"writeto",
		"already exists",
		"duplicate",
	} {
		if strings.Contains(message, token) {
			return true
		}
	}
	return false
}

// issue26114FeatureRegistryState is the complete mutable row state used by
// mo_feature_registry. The timestamps matter here: an upsert changes
// updated_at even when the test only needs the feature enabled, so restoring
// only description/scope/enabled would still pollute a shared fixture.
type issue26114FeatureRegistryState struct {
	exists      bool
	featureCode string
	description string
	scopeSpec   string
	enabled     bool
	createdAt   string
	updatedAt   string
}

func readIssue26114FeatureRegistryState(ctx context.Context, db *sql.DB) (issue26114FeatureRegistryState, error) {
	var state issue26114FeatureRegistryState
	err := db.QueryRowContext(ctx, `
		select feature_code, description, cast(scope_spec as char), enabled,
			date_format(created_at, '%Y-%m-%d %H:%i:%s.%f'),
			date_format(updated_at, '%Y-%m-%d %H:%i:%s.%f')
		from mo_catalog.mo_feature_registry
		where feature_code = 'BRANCH'`).Scan(
		&state.featureCode,
		&state.description,
		&state.scopeSpec,
		&state.enabled,
		&state.createdAt,
		&state.updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return issue26114FeatureRegistryState{}, nil
	}
	if err != nil {
		return issue26114FeatureRegistryState{}, err
	}
	state.exists = true
	return state, nil
}

func issue26114QuoteSQLString(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`'`, `''`,
	)
	return "'" + replacer.Replace(value) + "'"
}

func TestIssue26114QuoteSQLString(t *testing.T) {
	const value = `Branch feature \ 'legacy'`
	require.Equal(t, `'Branch feature \\ ''legacy'''`, issue26114QuoteSQLString(value))
}

func restoreIssue26114FeatureRegistryState(
	ctx context.Context,
	c embed.Cluster,
	state issue26114FeatureRegistryState,
) error {
	statement := "delete from mo_catalog.mo_feature_registry where feature_code = 'BRANCH'"
	if state.exists {
		statement = fmt.Sprintf(
			"insert into mo_catalog.mo_feature_registry(feature_code, description, scope_spec, enabled, created_at, updated_at) values(%s, %s, cast(%s as json), %t, %s, %s) on duplicate key update description = values(description), scope_spec = values(scope_spec), enabled = values(enabled), created_at = values(created_at), updated_at = values(updated_at)",
			issue26114QuoteSQLString(state.featureCode),
			issue26114QuoteSQLString(state.description),
			issue26114QuoteSQLString(state.scopeSpec),
			state.enabled,
			issue26114QuoteSQLString(state.createdAt),
			issue26114QuoteSQLString(state.updatedAt),
		)
	}
	return execIssue26114Internal(ctx, c, statement)
}

func execIssue26114Internal(ctx context.Context, c embed.Cluster, statement string) error {
	cn, err := c.GetCNService(0)
	if err != nil {
		return err
	}
	internalExec := testutils.GetSQLExecutor(cn)
	res, err := internalExec.Exec(ctx, statement, executor.Options{}.
		WithDatabase("mo_catalog").
		WithAccountID(0).
		WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}
	res.Close()
	return nil
}

func issue26114FeatureRegistryStateEqual(left, right issue26114FeatureRegistryState) bool {
	return left.exists == right.exists &&
		(!left.exists || (left.featureCode == right.featureCode &&
			left.description == right.description &&
			left.scopeSpec == right.scopeSpec &&
			left.enabled == right.enabled &&
			left.createdAt == right.createdAt &&
			left.updatedAt == right.updatedAt))
}

func requireIssue26114FeatureRegistryStateEqual(
	t *testing.T,
	want issue26114FeatureRegistryState,
	got issue26114FeatureRegistryState,
) {
	t.Helper()
	require.Equal(t, want.exists, got.exists)
	if !want.exists {
		return
	}
	require.Equal(t, want.featureCode, got.featureCode)
	require.Equal(t, want.description, got.description)
	require.Equal(t, want.scopeSpec, got.scopeSpec)
	require.Equal(t, want.enabled, got.enabled)
	require.Equal(t, want.createdAt, got.createdAt)
	require.Equal(t, want.updatedAt, got.updatedAt)
}

// withIssue26114FeatureRegistry snapshots and restores the full BRANCH row
// around one subtest. This keeps the two semantic quota scenarios independent
// while allowing them to share the expensive single-CN fixture.
func withIssue26114FeatureRegistry(
	t *testing.T,
	c embed.Cluster,
	db *sql.DB,
	fn func(),
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	before, err := readIssue26114FeatureRegistryState(ctx, db)
	require.NoError(t, err, "read BRANCH feature registry state")
	if err != nil {
		return
	}
	defer func() {
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer restoreCancel()
		if err := restoreIssue26114FeatureRegistryState(restoreCtx, c, before); err != nil {
			t.Errorf("restore BRANCH feature registry state: %v", err)
			return
		}
		after, err := readIssue26114FeatureRegistryState(restoreCtx, db)
		if err != nil {
			t.Errorf("read restored BRANCH feature registry state: %v", err)
			return
		}
		if !issue26114FeatureRegistryStateEqual(before, after) {
			t.Errorf("BRANCH feature registry state changed: before=%+v after=%+v", before, after)
		}
	}()

	fn()
}

func TestIssue26114CrossAccountBranchQuotaAndOwnership(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		sysDB.SetMaxOpenConns(4)

		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		execIssue26114SQLRequire(t, ctx, sysDB, "set role moadmin")
		require.NoError(t, testutils.WaitSystemBootstrap(ctx, sysDB))

		if !t.Run("feature registry non-default restore", func(t *testing.T) {
			// Exercise the existing-row branch with a deliberately non-default row.
			// The nested helper must restore every mutable field, including values
			// that need SQL escaping and timestamps that would otherwise be rewritten.
			withIssue26114FeatureRegistry(t, c, sysDB, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				custom := issue26114FeatureRegistryState{
					exists:      true,
					featureCode: "BRANCH",
					description: issue26114CustomFeatureDescription,
					scopeSpec:   `{"allowed_scope": ["database", "table"]}`,
					enabled:     false,
					createdAt:   "2020-01-02 03:04:05.000000",
					updatedAt:   "2021-02-03 04:05:06.000000",
				}
				require.NoError(t, restoreIssue26114FeatureRegistryState(ctx, c, custom))
				configured, err := readIssue26114FeatureRegistryState(ctx, sysDB)
				require.NoError(t, err)
				requireIssue26114FeatureRegistryStateEqual(t, custom, configured)

				withIssue26114FeatureRegistry(t, c, sysDB, func() {
					execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
						"select mo_feature_registry_upsert(%s, %s, %s, true)",
						issue26114QuoteSQLString("branch"),
						issue26114QuoteSQLString("temporary description"),
						issue26114QuoteSQLString(`{"allowed_scope":[]}`)))
					modified, err := readIssue26114FeatureRegistryState(ctx, sysDB)
					require.NoError(t, err)
					require.NotEqual(t, custom.description, modified.description)
				})

				restored, err := readIssue26114FeatureRegistryState(ctx, sysDB)
				require.NoError(t, err)
				requireIssue26114FeatureRegistryStateEqual(t, custom, restored)
			})
		}) {
			return
		}

		if !t.Run("feature registry absent restore", func(t *testing.T) {
			// Exercise the missing-row branch of the reset helper without starting
			// another cluster. The outer snapshot restores the fixture's original
			// row after the nested helper has proven that an upsert is removed again.
			withIssue26114FeatureRegistry(t, c, sysDB, func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, execIssue26114Internal(ctx, c,
					"delete from mo_catalog.mo_feature_registry where feature_code = 'BRANCH'"))

				missing, err := readIssue26114FeatureRegistryState(ctx, sysDB)
				require.NoError(t, err)
				require.False(t, missing.exists)

				withIssue26114FeatureRegistry(t, c, sysDB, func() {
					execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
						"select mo_feature_registry_upsert(%s, %s, %s, true)",
						issue26114QuoteSQLString("branch"),
						issue26114QuoteSQLString(issue26114FeatureDescription),
						issue26114QuoteSQLString(`{"allowed_scope":[]}`)))
					created, err := readIssue26114FeatureRegistryState(ctx, sysDB)
					require.NoError(t, err)
					require.True(t, created.exists)
					require.Equal(t, issue26114FeatureDescription, created.description)
				})

				restored, err := readIssue26114FeatureRegistryState(ctx, sysDB)
				require.NoError(t, err)
				require.False(t, restored.exists)
			})
		}) {
			return
		}

		// Each subtest mutates the global BRANCH registry row. Restore it before
		// the next subtest so a failed cleanup cannot turn fixture reuse into an
		// ordering dependency. If restoration fails, stop using this fixture;
		// runSQLIntegration will discard it after the callback releases the lock.
		if !t.Run("target quota and ownership", func(t *testing.T) {
			withIssue26114FeatureRegistry(t, c, sysDB, func() {
				runIssue26114CrossAccountBranchUsesTargetQuotaAndOwnership(t, sysDB, port)
			})
		}) {
			return
		}
		if !t.Run("legacy metadata counts toward target quota", func(t *testing.T) {
			withIssue26114FeatureRegistry(t, c, sysDB, func() {
				runIssue26114LegacyCrossAccountMetadataCountsTowardTargetQuota(t, c, sysDB, port)
			})
		}) {
			return
		}
	})
}

func runIssue26114CrossAccountBranchUsesTargetQuotaAndOwnership(
	t *testing.T,
	sysDB *sql.DB,
	port int64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	const (
		accountName   = "issue_26114_target"
		targetDB      = "issue_26114_target_db"
		sourceDB      = "issue_26114_source"
		dbSource      = "issue_26114_db_source"
		dbDestination = "issue_26114_db_destination"
		tableSnapshot = "issue_26114_table_sp"
		dbSnapshot    = "issue_26114_db_sp"
	)
	var branchTableIDs []uint64
	var accountID int32
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		cleanupErr := cleanupIssue26114Catalog(cleanupCtx, sysDB,
			"drop snapshot if exists "+tableSnapshot,
			"drop snapshot if exists "+dbSnapshot,
			"drop database if exists `"+sourceDB+"`",
			"drop database if exists `"+dbSource+"`",
			"drop account if exists "+accountName,
		)
		if cleanupErr != nil {
			t.Errorf("issue 26114 quota catalog cleanup failed: %v", cleanupErr)
		}
		if accountID != 0 {
			verifyIssue26114NoBranchArtifacts(
				t,
				cleanupCtx,
				sysDB,
				accountID,
				branchTableIDs,
				issue26114SnapshotSource{database: sourceDB, tables: []string{"base"}},
				issue26114SnapshotSource{database: dbSource, tables: []string{"t1", "t2"}},
			)
		}
	}()

	require.NoError(t, execIssue26114SQLMaybe(ctx, sysDB, "drop account if exists "+accountName))
	accountID = createIssue26114Account(t, ctx, sysDB, accountName, "111")
	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_registry_upsert(%s, %s, %s, true)",
		issue26114QuoteSQLString("branch"),
		issue26114QuoteSQLString(issue26114FeatureDescription),
		issue26114QuoteSQLString(`{"allowed_scope":[]}`)))
	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_limit_upsert(%d, 'branch', '', 0)", accountID))

	tenantDB, err := sql.Open("mysql", fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
	require.NoError(t, err)
	defer tenantDB.Close()
	execIssue26114SQLRequire(t, ctx, tenantDB, "create database `"+targetDB+"`")

	execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+sourceDB+"`")
	execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+sourceDB+"`.`base` (id int primary key)")
	execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+sourceDB+"`.`base` values (1)")
	execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+tableSnapshot+" for table `"+sourceDB+"` `base`")

	execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+dbSource+"`")
	execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+dbSource+"`.`t1` (id int primary key)")
	execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+dbSource+"`.`t2` (id int primary key)")
	execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+dbSource+"`.`t1` values (1)")
	execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+dbSource+"`.`t2` values (2)")
	execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+dbSnapshot+" for database `"+dbSource+"`")

	_, err = sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`blocked` from `"+
		sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has disabled for account "+accountName)

	_, err = sysDB.ExecContext(ctx, "data branch create database `"+dbDestination+"` from `"+
		dbSource+"`{snapshot='"+dbSnapshot+"'} to account "+accountName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has disabled for account "+accountName)

	var count int
	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname = 'blocked'").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_database where datname = '"+dbDestination+"'").Scan(&count))
	require.Zero(t, count)

	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, tableName := range []string{"race_one", "race_two"} {
		go func(name string) {
			<-start
			_, createErr := sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`"+name+"` from `"+
				sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
			results <- createErr
		}(tableName)
	}
	close(start)
	var outcomes [2]error
	for i := range outcomes {
		// Receiving both results is the join for the competing requests. Do it
		// before asserting either result so a failed assertion cannot leave the
		// other goroutine running into catalog cleanup.
		outcomes[i] = <-results
	}
	succeeded := 0
	rejected := 0
	for _, createErr := range outcomes {
		if createErr == nil {
			succeeded++
			continue
		}
		require.True(t, strings.Contains(createErr.Error(), "has reached the limit of 1"), createErr)
		rejected++
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, rejected)
	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname in ('race_one', 'race_two')").Scan(&count))
	require.Equal(t, 1, count)

	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_limit_upsert(%d, 'branch', '', 4)", accountID))
	execIssue26114SQLRequire(t, ctx, sysDB, "data branch create table `"+targetDB+"`.`allowed` from `"+
		sourceDB+"`.`base`{snapshot='"+tableSnapshot+"'} to account "+accountName)
	execIssue26114SQLRequire(t, ctx, sysDB, "data branch create database `"+dbDestination+"` from `"+
		dbSource+"`{snapshot='"+dbSnapshot+"'} to account "+accountName)

	require.NoError(t, sysDB.QueryRowContext(ctx, fmt.Sprintf(
		"select count(*) from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on b.table_id = t.rel_id "+
			"where t.account_id = %d and b.creator = %d and b.table_deleted = false", accountID, accountID)).Scan(&count))
	require.Equal(t, 4, count)
	branchTableIDs, err = readIssue26114BranchTableIDs(ctx, sysDB, accountID, targetDB, dbDestination)
	require.NoError(t, err)
	require.NotEmpty(t, branchTableIDs)
}

func runIssue26114LegacyCrossAccountMetadataCountsTowardTargetQuota(
	t *testing.T,
	c embed.Cluster,
	sysDB *sql.DB,
	port int64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	const (
		accountName = "issue_26114_legacy_target"
		targetDB    = "issue_26114_legacy_dst"
		sourceDB    = "issue_26114_legacy_src"
		snapshot    = "issue_26114_legacy_sp"
	)
	var legacyTableID uint64
	var accountID int32
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		// The scenario deliberately rewrites creator=0 to model metadata from a
		// released binary. Restore that test-only mutation before DROP ACCOUNT;
		// account cleanup intentionally keys ownership by creator, so leaving the
		// synthetic value in place would turn the fixture teardown into a false
		// leak rather than test the quota behavior under test.
		if accountID != 0 && legacyTableID != 0 {
			restoreErr := execIssue26114Internal(cleanupCtx, c, fmt.Sprintf(
				"update mo_catalog.mo_branch_metadata set creator = %d where table_id = %d",
				accountID, legacyTableID))
			if restoreErr != nil {
				t.Errorf("issue 26114 legacy metadata ownership restore failed: %v", restoreErr)
			}
		}
		cleanupErr := cleanupIssue26114Catalog(cleanupCtx, sysDB,
			"drop snapshot if exists "+snapshot,
			"drop database if exists `"+sourceDB+"`",
			"drop account if exists "+accountName,
		)
		if cleanupErr != nil {
			t.Errorf("issue 26114 legacy catalog cleanup failed: %v", cleanupErr)
		}
		if accountID != 0 {
			verifyIssue26114NoBranchArtifacts(
				t,
				cleanupCtx,
				sysDB,
				accountID,
				[]uint64{legacyTableID},
				issue26114SnapshotSource{database: sourceDB, tables: []string{"base"}},
			)
		}
	}()

	require.NoError(t, execIssue26114SQLMaybe(ctx, sysDB, "drop account if exists "+accountName))
	accountID = createIssue26114Account(t, ctx, sysDB, accountName, "111")
	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_registry_upsert(%s, %s, %s, true)",
		issue26114QuoteSQLString("branch"),
		issue26114QuoteSQLString(issue26114FeatureDescription),
		issue26114QuoteSQLString(`{"allowed_scope":[]}`)))
	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_limit_upsert(%d, 'branch', '', -1)", accountID))

	tenantDB, err := sql.Open("mysql", fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port))
	require.NoError(t, err)
	defer tenantDB.Close()
	execIssue26114SQLRequire(t, ctx, tenantDB, "create database `"+targetDB+"`")

	execIssue26114SQLRequire(t, ctx, sysDB, "create database `"+sourceDB+"`")
	execIssue26114SQLRequire(t, ctx, sysDB, "create table `"+sourceDB+"`.`base` (id int primary key)")
	execIssue26114SQLRequire(t, ctx, sysDB, "insert into `"+sourceDB+"`.`base` values (1)")
	execIssue26114SQLRequire(t, ctx, sysDB, "create snapshot "+snapshot+" for table `"+sourceDB+"` `base`")

	execIssue26114SQLRequire(t, ctx, sysDB, "data branch create table `"+targetDB+"`.`legacy` from `"+
		sourceDB+"`.`base`{snapshot='"+snapshot+"'} to account "+accountName)

	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname = 'legacy'").Scan(&legacyTableID))
	// Model the representation persisted by released binaries while retaining
	// the real target-owned table, branch metadata, and protect snapshot.
	err = execIssue26114Internal(ctx, c, fmt.Sprintf(
		"update mo_branch_metadata set creator = 0 where table_id = %d", legacyTableID))
	require.NoError(t, err)
	execIssue26114SQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"select mo_feature_limit_upsert(%d, 'branch', '', 1)", accountID))

	_, err = sysDB.ExecContext(ctx, "data branch create table `"+targetDB+"`.`should_reject` from `"+
		sourceDB+"`.`base`{snapshot='"+snapshot+"'} to account "+accountName)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has reached the limit of 1")

	var activeTargetTables int
	require.NoError(t, tenantDB.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_tables where reldatabase = '"+targetDB+"' and relname in ('legacy', 'should_reject')").Scan(&activeTargetTables))
	require.Equal(t, 1, activeTargetTables)
}
