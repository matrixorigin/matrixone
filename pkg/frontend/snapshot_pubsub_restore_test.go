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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type subscriptionRestoreExec struct {
	*backgroundExecTest
	accounts []uint32
}

func (b *subscriptionRestoreExec) Exec(ctx context.Context, sql string) error {
	account, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	b.accounts = append(b.accounts, account)
	return b.backgroundExecTest.Exec(ctx, sql)
}

func TestRestoreSubscriptionPropagatesErrors(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 10)
	accountSQL, err := getSqlForAccountIdAndStatus(ctx, "publisher", true)
	require.NoError(t, err)
	queries := []string{
		fmt.Sprintf(getSubsSqlFmt, "{MO_TS = 42}") + " and sub_account_id = 10 and sub_name = 'subdb'",
		accountSQL,
		"select 1 from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_pubs' and attname = 'account_name'",
		fmt.Sprintf(getPubInfoSql, 30) + " and pub_name = 'p'",
		"select datname, '', cast(creator as char), cast(owner as char) from mo_catalog.mo_database {MO_TS = 42} where account_id = 10",
		"select reldatabase, relname, cast(creator as char), cast(owner as char), relkind from mo_catalog.mo_tables {MO_TS = 42} where account_id = 10",
		"create database subdb from publisher publication p",
	}
	fixture := func() *subscriptionRestoreExec {
		bh := &backgroundExecTest{}
		bh.init()
		rows := [][][]interface{}{
			{{int64(10), "subscriber", "subdb", "", int64(1), "publisher", "p", "app", "*", "", "", int64(0)}},
			{{int64(30), "open", uint64(0)}},
			{{int64(1)}},
			{{int64(30), "publisher", "p", "app", uint64(300), "*", "subscriber", "", nil, uint64(2), uint64(0), ""}},
			{{"subdb", "", "2", "2"}},
			{{"subdb", "t", "2", "2", "r"}},
		}
		for i, row := range rows {
			names := make([]string, len(row[0]))
			bh.sql2result[queries[i]] = newMrsForRestoreStringRows(names, row)
		}
		return &subscriptionRestoreExec{backgroundExecTest: bh}
	}
	record := NewSubDbRestoreRecord("subdb", 10, 20, queries[6], 42)
	t.Run("source metadata and target creation", func(t *testing.T) {
		bh := fixture()
		require.NoError(t, restoreToSubDb(ctx, "", bh, "s", record))
		require.Equal(t, queries, bh.executedSQLs)
		require.Equal(t, []uint32{0, 0, 0, 0, 20}, bh.accounts)
	})
	for _, failure := range []error{errors.New("catalog read failed"), context.Canceled, context.DeadlineExceeded} {
		for i, query := range queries {
			t.Run(fmt.Sprintf("step %d %v", i, failure), func(t *testing.T) {
				bh := fixture()
				bh.sql2err[query] = failure
				require.ErrorIs(t, restoreToSubDb(ctx, "", bh, "s", record), failure)
				require.Equal(t, queries[:i+1], bh.executedSQLs)
			})
		}
	}
	for _, i := range []int{1, 3} {
		t.Run(fmt.Sprintf("missing publication dependency %d", i), func(t *testing.T) {
			bh := fixture()
			bh.sql2result[queries[i]] = newMrsForRestoreStringRows(nil, nil)
			exists, err := checkPubValid(ctx, "", bh, "publisher", "p")
			require.NoError(t, err)
			require.False(t, exists)
			bh.executedSQLs = nil
			require.NoError(t, restoreToSubDb(ctx, "", bh, "s", record))
			require.NotContains(t, bh.executedSQLs, queries[6])
		})
	}
	t.Run("missing snapshot subscription metadata is not absent publication", func(t *testing.T) {
		bh := fixture()
		bh.sql2result[queries[0]] = newMrsForRestoreStringRows(nil, nil)
		require.ErrorContains(t, restoreToSubDb(ctx, "", bh, "s", record), "there is no subscription")
		require.Equal(t, queries[:1], bh.executedSQLs)
	})
}

func TestRestorePublicationsRejectsMissingTarget(t *testing.T) {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getAccountIdNamesSql] = newMrsForRestoreStringRows(nil, nil)
	pub := &pubsub.PubInfo{PubAccountId: 1, PubAccountName: "publisher", PubName: "p"}
	require.ErrorContains(t, createPubs(t.Context(), "", bh, "s", 42, []*pubsub.PubInfo{pub}), "target account publisher does not exist")
	require.Equal(t, []string{getAccountIdNamesSql}, bh.executedSQLs)
	failure := errors.New("account lookup failed")
	bh.sql2err[getAccountIdNamesSql] = failure
	require.ErrorIs(t, createPubs(t.Context(), "", bh, "s", 42, []*pubsub.PubInfo{pub}), failure)
}

// Test_recreatePublicationsAfterDbRestoreTS pins #30610: a snapshot-time
// database restore drops the database and its mo_pubs rows; the restore must
// replay the publications so subscribers keep working, and must stay silent
// when the restore crosses accounts (the source account keeps its rows).
func Test_recreatePublicationsAfterDbRestoreTS(t *testing.T) {
	pubTs := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()

	fixture := func() *backgroundExecTest {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[getPubInfoWithPitr(pubTs, 0, "test")] = newMrsForSqlForGetPubs([][]interface{}{
			{int64(sysAccountID), sysAccountName, "pub01", pubsub.TableAll, uint64(0), pubsub.TableAll, pubsub.AccountAll, "", "", uint64(17), uint64(29), ""},
		})
		bh.sql2result[getAccountIdNamesSql] = newMrsForGetAllAccounts([][]interface{}{
			{int64(sysAccountID), sysAccountName, "open", uint64(1), nil},
			{int64(1), "acc01", "open", uint64(1), nil},
		})
		bh.sql2result[getSubsSql+" and pub_account_name = 'sys' and pub_name = 'pub01'"] =
			newMrsForSqlForGetSubs([][]interface{}{})
		return bh
	}
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)

	t.Run("cross-account restore leaves the source publications untouched", func(t *testing.T) {
		bh := fixture()
		require.NoError(t, recreatePublicationsAfterDbRestoreTS(ctx, "", bh, tree.RESTORELEVELDATABASE, "test", 0, 5, pubTs))
		require.Empty(t, bh.executedSQLs)
	})

	t.Run("same-account restore replays the publications with their principals", func(t *testing.T) {
		bh := fixture()
		require.NoError(t, recreatePublicationsAfterDbRestoreTS(ctx, "", bh, tree.RESTORELEVELDATABASE, "test", 0, 0, pubTs))
		var insertSQL string
		for _, s := range bh.executedSQLs {
			if insertSQL == "" && strings.HasPrefix(s, "insert into mo_catalog.mo_pubs") {
				insertSQL = s
			}
		}
		require.Contains(t, insertSQL, "17, 29")
	})

	t.Run("publication read failure aborts the restore", func(t *testing.T) {
		bh := fixture()
		readErr := errors.New("mo_pubs read failed")
		bh.sql2err[getPubInfoWithPitr(pubTs, 0, "test")] = readErr
		require.ErrorIs(t, recreatePublicationsAfterDbRestoreTS(ctx, "", bh, tree.RESTORELEVELDATABASE, "test", 0, 0, pubTs), readErr)
	})

	for _, level := range []tree.RestoreLevel{tree.RESTORELEVELACCOUNT, tree.RESTORELEVELCLUSTER, tree.RESTORELEVELTABLE} {
		t.Run(level.String()+" does not own database publication replay", func(t *testing.T) {
			bh := fixture()
			require.NoError(t, recreatePublicationsAfterDbRestoreTS(ctx, "", bh, level, "test", 0, 0, pubTs))
			require.Empty(t, bh.executedSQLs)
		})
	}
}

func TestSnapshotDatabaseWorkerReportsRecreation(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), uint32(sysAccountID))
	const db, ts = "worker_result", int64(100)
	masterSQL := fmt.Sprintf(checkDatabaseIsMasterFormat, quoteSQLStringLiteral(db), quoteSQLStringLiteral(db))
	failure := errors.New("database recreation failed")
	for _, tc := range []struct {
		name, failSQL string
		externalFK    bool
		wantRecreated bool
	}{
		{name: "recreated", wantRecreated: true},
		{name: "external_fk_skipped", externalFK: true},
		{name: "master_check_failed", failSQL: masterSQL},
		{name: "drop_failed", failSQL: dropDatabaseIfExistsSQL(db)},
		{name: "create_failed", failSQL: createDatabaseIfNotExistsSQL(db)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[fmt.Sprintf("select datname, dat_createsql, dat_type from mo_catalog.mo_database {MO_TS = %d} where datname = '%s' and account_id = 0", ts, db)] =
				newMrsForRestoreStringRows([]string{"datname", "dat_createsql", "dat_type"}, [][]interface{}{{db, "create database " + db, ""}})
			bh.sql2result[masterSQL] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
			if tc.externalFK {
				bh.sql2result[masterSQL] = newMrsForRestoreStringRows([]string{"db_name"}, [][]interface{}{{"external"}})
			}
			bh.sql2result[fmt.Sprintf(getPubInfoSql, uint32(sysAccountID))+" and database_name = '"+db+"'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
			if tc.failSQL != "" {
				bh.sql2err[tc.failSQL] = failure
			}
			recreated, err := restoreToDatabase(ctx, "", bh, "s", db, 0, map[string]*tableInfo{}, map[string]*tableInfo{}, ts, 0, false, nil, newPartialRestoreSource(nil))
			if tc.failSQL == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, tc.wantRecreated, recreated)
			if tc.externalFK {
				require.NotContains(t, bh.executedSQLs, dropDatabaseIfExistsSQL(db))
				require.NotContains(t, bh.executedSQLs, createDatabaseIfNotExistsSQL(db))
			}
		})
	}
	bh := &backgroundExecTest{}
	bh.init()
	recreated, err := restoreToDatabase(ctx, "", bh, "s", moCatalog, 0, nil, nil, ts, 0, false, nil, nil)
	require.NoError(t, err)
	require.False(t, recreated)
	require.Empty(t, bh.executedSQLs)
}
