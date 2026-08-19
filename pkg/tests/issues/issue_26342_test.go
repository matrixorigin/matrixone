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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue26342MoSubsModernUpdate(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB := openIssue26342DB(t, fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", port))
		defer func() { require.NoError(t, sysDB.Close()) }()

		suffix := time.Now().UnixNano()
		tenantName := fmt.Sprintf("acc26342_%d", suffix)
		execSQLRequire(t, ctx, sysDB, fmt.Sprintf(
			"create account `%s` admin_name 'admin' identified by '111'", tenantName,
		))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, fmt.Sprintf("drop account if exists `%s`", tenantName))
		}()

		tenantID := queryIssue26095AccountID(t, ctx, sysDB, tenantName)
		require.NotZero(t, tenantID)
		tenantDB := openIssue26342DB(t, fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", tenantName, port,
		))
		defer func() { require.NoError(t, tenantDB.Close()) }()

		prefix := fmt.Sprintf("p26342_%d", suffix)
		pubAccount := prefix + "_publisher"
		pubA := prefix + "_pub_a"
		pubA2 := prefix + "_pub_a2"
		pubB := prefix + "_pub_b"
		subA := prefix + "_sub_a"
		subA2 := prefix + "_sub_a2"
		subB := prefix + "_sub_b"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_ = execIssue26342InternalSQL(cleanupCtx, cn, fmt.Sprintf(
				"delete from mo_catalog.mo_subs where pub_account_name = '%s'", pubAccount))
		}()

		assertIssue26342MoSubsDDL(t, ctx, sysDB)
		hiddenTable := queryIssue26342MoSubsUniqueIndexTable(t, ctx, sysDB)
		hiddenBefore := queryIssue26342TableCount(t, ctx, sysDB, hiddenTable)

		insertIssue26342MoSub(t, ctx, cn, tenantID, tenantName, subA, pubAccount, pubA)
		insertIssue26342MoSub(t, ctx, cn, tenantID, tenantName, subB, pubAccount, pubB)
		require.Equal(t, 2, queryIssue26342MoSubsCount(t, ctx, sysDB, tenantID, pubAccount))
		require.Equal(t, hiddenBefore+2, queryIssue26342TableCount(t, ctx, sysDB, hiddenTable))

		logicalPlan, err := execIssue26342InternalSQLWithPlan(ctx, cn, fmt.Sprintf(
			"update mo_catalog.mo_subs set pub_name = '%s', sub_name = '%s' "+
				"where pub_account_name = '%s' and pub_name = '%s' and sub_account_id = %d",
			pubA2, subA2, pubAccount, pubA, tenantID,
		))
		require.NoError(t, err)
		require.NotNil(t, logicalPlan)
		assertIssue26342MultiUpdateContexts(t, logicalPlan, hiddenTable)
		require.Equal(t, 0, queryIssue26342MoSubKeyCountInternal(t, ctx, cn, tenantID, pubAccount, pubA, subA))
		require.Equal(t, 1, queryIssue26342MoSubKeyCountInternal(t, ctx, cn, tenantID, pubAccount, pubA2, subA2))
		require.Equal(t, hiddenBefore+2, queryIssue26342TableCount(t, ctx, sysDB, hiddenTable))

		err = execIssue26342InternalSQL(ctx, cn, fmt.Sprintf(
			"update mo_catalog.mo_subs set pub_name = '%s', sub_name = '%s' "+
				"where pub_account_name = '%s' and pub_name = '%s' and sub_account_id = %d",
			pubB, subB, pubAccount, pubA2, tenantID,
		))
		require.Error(t, err)
		require.Equal(t, 1, queryIssue26342MoSubKeyCountInternal(t, ctx, cn, tenantID, pubAccount, pubA2, subA2))
		require.Equal(t, 1, queryIssue26342MoSubKeyCountInternal(t, ctx, cn, tenantID, pubAccount, pubB, subB))
		require.Equal(t, hiddenBefore+2, queryIssue26342TableCount(t, ctx, sysDB, hiddenTable))

		require.NoError(t, execIssue26342InternalSQL(ctx, cn, fmt.Sprintf(
			"update mo_catalog.mo_subs set sub_account_name = 'stale' "+
				"where pub_account_name = '%s' and sub_account_id = %d",
			pubAccount, tenantID,
		)))
		require.NoError(t, execIssue26342InternalSQL(ctx, cn,
			"update mo_catalog.mo_subs t1 inner join mo_catalog.mo_account t2 "+
				"on t1.sub_account_id = t2.account_id set t1.sub_account_name = t2.account_name"))
		require.Equal(t, tenantName, queryIssue26342SubAccountNameInternal(t, ctx, cn, tenantID, pubAccount, pubA2))

		_, err = tenantDB.ExecContext(ctx, fmt.Sprintf(
			"update mo_catalog.mo_subs set status = status + 1 "+
				"where pub_account_name = '%s' and pub_name = '%s' and sub_account_id = %d",
			pubAccount, pubA2, tenantID,
		))
		require.Error(t, err)
		require.Equal(t, int64(1), queryIssue26342MoSubStatusInternal(t, ctx, cn, tenantID, pubAccount, pubA2))
	})
}

func queryIssue26342MoSubKeyCountInternal(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	tenantID uint32,
	pubAccount string,
	pubName string,
	subName string,
) int {
	t.Helper()
	result, err := testutils.GetSQLExecutor(cn).Exec(
		ctx,
		fmt.Sprintf(`
			select count(*) from mo_catalog.mo_subs
			where sub_account_id = %d and pub_account_name = '%s' and pub_name = '%s' and sub_name = '%s'`,
			tenantID, pubAccount, pubName, subName),
		executor.Options{}.
			WithDatabase("mo_catalog").
			WithAccountID(0).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err)
	count := testutils.ReadCount(result)
	result.Close()
	return count
}

func queryIssue26342SubAccountNameInternal(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	tenantID uint32,
	pubAccount string,
	pubName string,
) string {
	t.Helper()
	result, err := testutils.GetSQLExecutor(cn).Exec(
		ctx,
		fmt.Sprintf(`
			select sub_account_name from mo_catalog.mo_subs
			where sub_account_id = %d and pub_account_name = '%s' and pub_name = '%s'`,
			tenantID, pubAccount, pubName),
		executor.Options{}.
			WithDatabase("mo_catalog").
			WithAccountID(0).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err)
	var name string
	result.ReadRows(func(_ int, cols []*vector.Vector) bool {
		values := executor.GetStringRows(cols[0])
		if len(values) > 0 {
			name = values[0]
		}
		return false
	})
	result.Close()
	require.NotEmpty(t, name)
	return name
}

func queryIssue26342MoSubStatusInternal(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	tenantID uint32,
	pubAccount string,
	pubName string,
) int64 {
	t.Helper()
	result, err := testutils.GetSQLExecutor(cn).Exec(
		ctx,
		fmt.Sprintf(`
			select status from mo_catalog.mo_subs
			where sub_account_id = %d and pub_account_name = '%s' and pub_name = '%s'`,
			tenantID, pubAccount, pubName),
		executor.Options{}.
			WithDatabase("mo_catalog").
			WithAccountID(0).
			WithWaitCommittedLogApplied(),
	)
	require.NoError(t, err)
	var status int64
	result.ReadRows(func(_ int, cols []*vector.Vector) bool {
		values := executor.GetFixedRows[int8](cols[0])
		if len(values) > 0 {
			status = int64(values[0])
		}
		return false
	})
	result.Close()
	return status
}

func openIssue26342DB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	return db
}

func assertIssue26342MoSubsDDL(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var tableName, createSQL string
	require.NoError(t, db.QueryRowContext(ctx, "show create table mo_catalog.mo_subs").Scan(&tableName, &createSQL))
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.ReplaceAll(createSQL, "`", "")), " "))
	require.Contains(t, normalized, "primary key (pub_account_name,pub_name,sub_account_id)")
	require.Contains(t, normalized, "unique key sub_account_id (sub_account_id,sub_name)")
}

func queryIssue26342MoSubsUniqueIndexTable(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	var tableName string
	err := db.QueryRowContext(ctx, `
		select distinct i.index_table_name
		from mo_catalog.mo_indexes i
		join mo_catalog.mo_tables t on i.table_id = t.rel_id
		where t.reldatabase = 'mo_catalog'
		  and t.relname = 'mo_subs'
		  and i.name <> 'PRIMARY'
		  and i.index_table_name <> ''
		limit 1`).Scan(&tableName)
	require.NoError(t, err)
	require.NotEmpty(t, tableName)
	return tableName
}

func queryIssue26342TableCount(t *testing.T, ctx context.Context, db *sql.DB, tableName string) int {
	t.Helper()
	require.NotContains(t, tableName, "`")
	var count int
	err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from mo_catalog.`%s`", tableName)).Scan(&count)
	require.NoError(t, err)
	return count
}

func insertIssue26342MoSub(
	t *testing.T,
	ctx context.Context,
	cn embed.ServiceOperator,
	tenantID uint32,
	tenantName string,
	subName string,
	pubAccount string,
	pubName string,
) {
	t.Helper()
	err := execIssue26342InternalSQL(ctx, cn, fmt.Sprintf(`
		insert into mo_catalog.mo_subs (
			sub_account_id, sub_account_name, sub_name, sub_time,
			pub_account_id, pub_account_name, pub_name, pub_database,
			pub_tables, pub_time, pub_comment, status
		) values (%d, '%s', '%s', now(), 0, '%s', '%s', 'db26342', '*', now(), 'test', 1)`,
		tenantID, tenantName, subName, pubAccount, pubName))
	require.NoError(t, err)
}

func execIssue26342InternalSQL(ctx context.Context, cn embed.ServiceOperator, sql string) error {
	_, err := execIssue26342InternalSQLWithPlan(ctx, cn, sql)
	return err
}

func execIssue26342InternalSQLWithPlan(
	ctx context.Context,
	cn embed.ServiceOperator,
	sql string,
) (*plan.Query, error) {
	result, err := testutils.GetSQLExecutor(cn).Exec(
		ctx,
		sql,
		executor.Options{}.
			WithDatabase("mo_catalog").
			WithAccountID(0).
			WithWaitCommittedLogApplied(),
	)
	logicalPlan := result.LogicalPlan
	result.Close()
	return logicalPlan, err
}

func assertIssue26342MultiUpdateContexts(t *testing.T, query *plan.Query, hiddenTable string) {
	t.Helper()
	var multiUpdate *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_MULTI_UPDATE {
			multiUpdate = node
			break
		}
	}
	require.NotNil(t, multiUpdate)
	require.Len(t, multiUpdate.UpdateCtxList, 2)

	contexts := make(map[string]*plan.UpdateCtx, len(multiUpdate.UpdateCtxList))
	for _, updateCtx := range multiUpdate.UpdateCtxList {
		require.NotNil(t, updateCtx.TableDef)
		require.NotEmpty(t, updateCtx.InsertCols)
		require.NotEmpty(t, updateCtx.DeleteCols)
		contexts[updateCtx.TableDef.Name] = updateCtx
	}
	baseCtx := contexts["mo_subs"]
	require.NotNil(t, baseCtx)
	require.NotNil(t, baseCtx.TableDef.Pkey)
	require.Equal(t,
		[]string{"pub_account_name", "pub_name", "sub_account_id"},
		baseCtx.TableDef.Pkey.Names,
	)
	require.NotNil(t, contexts[hiddenTable])
}

func queryIssue26342MoSubsCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID uint32,
	pubAccount string,
) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx,
		"select count(*) from mo_catalog.mo_subs where sub_account_id = ? and pub_account_name = ?",
		tenantID, pubAccount).Scan(&count)
	require.NoError(t, err)
	return count
}

func queryIssue26342MoSubKeyCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID uint32,
	pubAccount string,
	pubName string,
	subName string,
) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
		select count(*) from mo_catalog.mo_subs
		where sub_account_id = ? and pub_account_name = ? and pub_name = ? and sub_name = ?`,
		tenantID, pubAccount, pubName, subName).Scan(&count)
	require.NoError(t, err)
	return count
}

func queryIssue26342SubAccountName(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID uint32,
	pubAccount string,
	pubName string,
) string {
	t.Helper()
	var name string
	err := db.QueryRowContext(ctx, `
		select sub_account_name from mo_catalog.mo_subs
		where sub_account_id = ? and pub_account_name = ? and pub_name = ?`,
		tenantID, pubAccount, pubName).Scan(&name)
	require.NoError(t, err)
	return name
}

func queryIssue26342MoSubStatus(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	tenantID uint32,
	pubAccount string,
	pubName string,
) int64 {
	t.Helper()
	var status int64
	err := db.QueryRowContext(ctx, `
		select status from mo_catalog.mo_subs
		where sub_account_id = ? and pub_account_name = ? and pub_name = ?`,
		tenantID, pubAccount, pubName).Scan(&status)
	require.NoError(t, err)
	return status
}
