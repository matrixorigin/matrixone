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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue28083LegacyHiddenIndexPrivilegeCleanup(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		const (
			database = "issue_28083_legacy_index_grant"
			role     = "issue_28083_legacy_index_role"
		)
		cleanup := func(cleanupCtx context.Context) {
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists `"+database+"`")
			execSQLMaybe(t, cleanupCtx, db, "drop role if exists `"+role+"`")
		}
		cleanup(ctx)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			cleanup(cleanupCtx)
		}()

		execSQLRequire(t, ctx, db, "create database `"+database+"`")
		execSQLRequire(t, ctx, db, "create role `"+role+"`")
		execSQLRequire(t, ctx, db, "create table `"+database+"`.`drop_index_t`(a int, index idx_a(a))")
		execSQLRequire(t, ctx, db, "create table `"+database+"`.`drop_table_t`(a int, index idx_a(a))")
		execSQLRequire(t, ctx, db, "grant select on table `"+database+"`.`drop_index_t` to `"+role+"`")
		execSQLRequire(t, ctx, db, "grant select on table `"+database+"`.`drop_table_t` to `"+role+"`")

		dropIndexBaseID, dropIndexHiddenID := queryIssue28083RelationIDs(t, ctx, db, database, "drop_index_t")
		dropTableBaseID, dropTableHiddenID := queryIssue28083RelationIDs(t, ctx, db, database, "drop_table_t")
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropIndexBaseID, 1)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropTableBaseID, 1)

		cnService, ok := cn.RawService().(cnservice.Service)
		require.True(t, ok)
		sqlExec := cnService.GetSQLExecutor()
		require.NotNil(t, sqlExec)
		insertIssue28083LegacyGrant(t, ctx, sqlExec, role, dropIndexBaseID, dropIndexHiddenID)
		insertIssue28083LegacyGrant(t, ctx, sqlExec, role, dropTableBaseID, dropTableHiddenID)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropIndexHiddenID, 1)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropTableHiddenID, 1)

		execSQLRequire(t, ctx, db, "drop index idx_a on `"+database+"`.`drop_index_t`")
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropIndexHiddenID, 0)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropIndexBaseID, 1)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropTableHiddenID, 1)

		execSQLRequire(t, ctx, db, "drop table `"+database+"`.`drop_table_t`")
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropTableHiddenID, 0)
		requireIssue28083PrivilegeCount(t, ctx, db, role, dropTableBaseID, 0)
	})
}

func queryIssue28083RelationIDs(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
) (uint64, uint64) {
	t.Helper()
	const query = `
select base.rel_logical_id, hidden.rel_logical_id
from mo_catalog.mo_tables base
join mo_catalog.mo_indexes idx on idx.table_id = base.rel_id and idx.name = 'idx_a'
join mo_catalog.mo_tables hidden
  on hidden.reldatabase_id = base.reldatabase_id and hidden.relname = idx.index_table_name
where base.reldatabase = ? and base.relname = ?`
	var baseID, hiddenID uint64
	err := db.QueryRowContext(ctx, query, database, table).Scan(&baseID, &hiddenID)
	require.NoError(t, err)
	require.NotZero(t, baseID)
	require.NotZero(t, hiddenID)
	require.NotEqual(t, baseID, hiddenID)
	return baseID, hiddenID
}

func insertIssue28083LegacyGrant(
	t *testing.T,
	ctx context.Context,
	sqlExec executor.SQLExecutor,
	role string,
	baseID uint64,
	hiddenID uint64,
) {
	t.Helper()
	statement := fmt.Sprintf(
		"insert into mo_catalog.mo_role_privs "+
			"select role_id, role_name, obj_type, %d, privilege_id, privilege_name, "+
			"privilege_level, operation_user_id, granted_time, with_grant_option "+
			"from mo_catalog.mo_role_privs "+
			"where role_name = '%s' and obj_id = %d and privilege_name = 'select'",
		hiddenID,
		role,
		baseID,
	)
	result, err := sqlExec.Exec(
		defines.AttachAccountId(ctx, 0),
		statement,
		executor.Options{}.
			WithAccountID(0).
			WithDatabase("mo_catalog").
			WithWaitCommittedLogApplied().
			WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	)
	require.NoError(t, err)
	result.Close()
}

func requireIssue28083PrivilegeCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	role string,
	objectID uint64,
	expected int,
) {
	t.Helper()
	var count int
	err := db.QueryRowContext(
		ctx,
		"select count(*) from mo_catalog.mo_role_privs where role_name = ? and obj_id = ?",
		role,
		objectID,
	).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, expected, count)
}
