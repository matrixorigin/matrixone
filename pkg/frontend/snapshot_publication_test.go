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

package frontend

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	client "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// snapshotPublicationPubSQL is the byte-exact mo_pubs read of
// readSnapshotPublicationRow; tests must mock it with the same trailing
// whitespace the production query carries.
func snapshotPublicationPubSQL(pubAccountName, pubName string, forUpdate bool) string {
	sql := fmt.Sprintf("SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list \n"+
		"\t\t\tFROM mo_catalog.mo_pubs \n"+
		"\t\t\tWHERE account_name = '%s' AND pub_name = '%s'", pubAccountName, pubName)
	if forUpdate {
		sql += " for update"
	}
	return sql
}

func stubPublicationRow(bh *backgroundExecTest, pubAccountName, pubName string, forUpdate bool, accountID uint64, accountName, dbName string, dbID uint64, tableList, accountList string) {
	bh.sql2result[snapshotPublicationPubSQL(pubAccountName, pubName, forUpdate)] =
		newMrsForPublicationInfo(accountID, accountName, pubName, dbName, dbID, tableList, accountList)
}

func stubDatabaseOwnerByID(bh *backgroundExecTest, dbName string, dbID uint64, ownerID uint64) {
	bh.sql2result[fmt.Sprintf(getDbAccountIdAndTypByIdFormat, dbID, dbName)] =
		newMrsForPublicationColumns([]string{"account_id", "dat_type"}, [][]interface{}{{ownerID, ""}})
}

func stubAccountNameByID(bh *backgroundExecTest, accountID uint32, accountName string) {
	bh.sql2result[fmt.Sprintf("select account_name from mo_catalog.mo_account where account_id = %d", accountID)] =
		newMrsForPublicationColumns([]string{"account_name"}, [][]interface{}{{accountName}})
}

// TestSnapshotPublicationBindingResolution is design §9 U4/U5 for the
// shape-aware resolver of design §4.4 step 1: database-scoped rows resolve
// the physical owner through the persisted (D, database_id); account-covering
// rows bind the publisher and never resolve id 0 through the catalog.
func TestSnapshotPublicationBindingResolution(t *testing.T) {
	ctx := context.Background()

	t.Run("ordinary database-scoped publication binds its publisher", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubPublicationRow(bh, "pubacc", "pub1", false, 100, "pubacc", "tdb", 42, "*", "sub_acc")
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)

		binding, err := resolveSnapshotPublicationBinding(ctx, bh, 3, "sub_acc", "pubacc", "pub1")
		require.NoError(t, err)
		require.Equal(t, uint32(100), binding.row.accountID)
		require.Equal(t, uint64(42), binding.row.databaseID)
		require.Equal(t, uint32(100), binding.physID)
		require.Equal(t, "pubacc", binding.physName,
			"an ordinary publication's physical owner equals the publisher: one name, no extra resolution")
		require.Equal(t, []string{
			snapshotPublicationPubSQL("pubacc", "pub1", false),
			fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(42), "tdb"),
		}, bh.executedSQLs)
	})

	t.Run("account-covering publication resolves to the publisher without an id-0 probe", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubPublicationRow(bh, "pubacc", "pub1", false, 100, "pubacc", "*", 0, "*", "all")

		binding, err := resolveSnapshotPublicationBinding(ctx, bh, 3, "sub_acc", "pubacc", "pub1")
		require.NoError(t, err)
		require.Equal(t, uint32(100), binding.physID)
		require.Equal(t, "pubacc", binding.physName)
		require.Equal(t, []string{snapshotPublicationPubSQL("pubacc", "pub1", false)}, bh.executedSQLs,
			"no getDbAccountIdAndTypeById call runs for database_id 0 (design §9 U5)")
	})

	t.Run("sys-published foreign source records under the physical owner", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubPublicationRow(bh, "sys", "pub1", false, 0, "sys", "fsrc_db", 42, "base,v_x", "sub_acc")
		stubDatabaseOwnerByID(bh, "fsrc_db", 42, 2)
		stubAccountNameByID(bh, 2, "owner_acc")

		binding, err := resolveSnapshotPublicationBinding(ctx, bh, 3, "sub_acc", "sys", "pub1")
		require.NoError(t, err)
		require.Equal(t, uint32(0), binding.row.accountID, "P_pub stays sys")
		require.Equal(t, uint32(2), binding.physID, "P_src is the physical owner (design §4.4 step 1)")
		require.Equal(t, "owner_acc", binding.physName)
	})
}

// TestDeriveSnapshotPublicationSourceGate is design §9 U7: the shared
// foreign-source gate of design §4.4 step 2 (finding 10), mirroring the
// persisted mo_pubs rows of publication_view_scope_test.go foreign-source
// cases.
func TestDeriveSnapshotPublicationSourceGate(t *testing.T) {
	ctx := context.Background()

	targetStatus := func(bh *backgroundExecTest, name string, id int64) {
		bh.sql2result[fmt.Sprintf(getAccountIdAndStatusFormat, name)] =
			newMrsForPublicationColumns([]string{"account_id", "status"}, [][]interface{}{{id, "open"}})
	}

	t.Run("general publisher foreign source with cross-account targets is rejected", func(t *testing.T) {
		// (publisher=2, owner=3, targets={1}) — the finding-10 row shape.
		bh := &backgroundExecTest{}
		bh.init()
		stubDatabaseOwnerByID(bh, "tdb", 42, 3)
		targetStatus(bh, "acc1", 1)
		row := &snapshotPublicationRow{
			accountID: 2, accountName: "pubacc", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "acc1",
		}
		_, err := deriveSnapshotPublicationSource(ctx, bh, row)
		require.ErrorContains(t, err, "cannot publish database 'tdb' owned by account 3",
			"a general account may not publish another account's database to third parties")
	})

	t.Run("ACCOUNT ALL targets never admit a general publisher's foreign source", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubDatabaseOwnerByID(bh, "tdb", 42, 3)
		row := &snapshotPublicationRow{
			accountID: 2, accountName: "pubacc", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "all",
		}
		_, err := deriveSnapshotPublicationSource(ctx, bh, row)
		require.ErrorContains(t, err, "cannot publish database 'tdb' owned by account 3")
		require.Equal(t, []string{fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(42), "tdb")}, bh.executedSQLs,
			"ACCOUNT ALL is rejected without per-target resolution")
	})

	t.Run("sys publisher over a foreign source is admitted", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubDatabaseOwnerByID(bh, "tdb", 42, 3)
		row := &snapshotPublicationRow{
			accountID: 0, accountName: "sys", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "acc1",
		}
		physID, err := deriveSnapshotPublicationSource(ctx, bh, row)
		require.NoError(t, err)
		require.Equal(t, uint32(3), physID)
	})

	t.Run("owner-only targets admit a general publisher's foreign source", func(t *testing.T) {
		// (publisher=2, owner=3, targets={3}): the publication only returns
		// the owner's own visibility, so the foreign source stays admitted.
		bh := &backgroundExecTest{}
		bh.init()
		stubDatabaseOwnerByID(bh, "tdb", 42, 3)
		targetStatus(bh, "acc3", 3)
		row := &snapshotPublicationRow{
			accountID: 2, accountName: "acc2", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "acc3",
		}
		physID, err := deriveSnapshotPublicationSource(ctx, bh, row)
		require.NoError(t, err)
		require.Equal(t, uint32(3), physID)
	})

	t.Run("ordinary publication needs no gate", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubDatabaseOwnerByID(bh, "tdb", 42, 2)
		row := &snapshotPublicationRow{
			accountID: 2, accountName: "acc2", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "acc1",
		}
		physID, err := deriveSnapshotPublicationSource(ctx, bh, row)
		require.NoError(t, err)
		require.Equal(t, uint32(2), physID)
		require.Equal(t, []string{fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(42), "tdb")}, bh.executedSQLs)
	})
}

// TestSnapshotPublicationCoverageCheck keeps the decoy behavior of design
// §9 U4: the coverage read pins the persisted (database, database_id) pair,
// so a same-named replacement never satisfies the target's coverage.
func TestSnapshotPublicationCoverageCheck(t *testing.T) {
	ctx := context.Background()
	coverageSQL := "select account_name, database_name, database_id, table_list, account_list from mo_catalog.mo_pubs where account_id = 100 and pub_name = 'pub1'"
	stubCoverage := func(bh *backgroundExecTest, account, db string, dbID uint64, tableList, accountList string) {
		bh.sql2result[coverageSQL] = newMrsForPublicationColumns(
			[]string{"account_name", "database_name", "database_id", "table_list", "account_list"},
			[][]interface{}{{account, db, dbID, tableList, accountList}})
	}

	t.Run("persisted database identity covers the matching target", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCoverage(bh, "pubacc", "tdb", 42, "*", "all")
		require.NoError(t, checkSnapshotPublicationCoverage(
			ctx, bh, 100, "pubacc", "pub1", "sub_acc", "tdb", "t1", 42))
	})

	t.Run("a same-named replacement database id fails coverage (decoy)", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCoverage(bh, "pubacc", "tdb", 42, "*", "all")
		err := checkSnapshotPublicationCoverage(ctx, bh, 100, "pubacc", "pub1", "sub_acc", "tdb", "t1", 43)
		require.ErrorContains(t, err, "outside current publication coverage")
	})

	t.Run("table-limited coverage rejects unlisted tables and database snapshots", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCoverage(bh, "pubacc", "tdb", 42, "t1", "all")
		require.NoError(t, checkSnapshotPublicationCoverage(ctx, bh, 100, "pubacc", "pub1", "sub_acc", "tdb", "t1", 42))
		require.ErrorContains(t,
			checkSnapshotPublicationCoverage(ctx, bh, 100, "pubacc", "pub1", "sub_acc", "tdb", "t2", 42),
			"outside current publication coverage")
		require.ErrorContains(t,
			checkSnapshotPublicationCoverage(ctx, bh, 100, "pubacc", "pub1", "sub_acc", "tdb", "", 42),
			"outside current publication coverage",
			"a database-level snapshot passes no table-limited publication (finding 11 create side)")
	})

	t.Run("account-covering coverage admits any concrete database", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCoverage(bh, "pubacc", "*", 0, "*", "all")
		require.NoError(t, checkSnapshotPublicationCoverage(ctx, bh, 100, "pubacc", "pub1", "sub_acc", "anydb", "t1", 77))
	})
}

// TestLifecycleAdmissionPlanValues is design §9 U5 for the split-identity
// key plan of design §4.4 step 3: ordering A→D→T→Q→P, the alias-only A key,
// and the byte-identity of the single-account forms.
func TestLifecycleAdmissionPlanValues(t *testing.T) {
	const (
		sub  uint32 = 3
		phys uint32 = 2
		pub  uint32 = 0
	)

	t.Run("missing database fails before any step", func(t *testing.T) {
		_, err := planLifecycleAdmission(0, "", 7, 7, "", "table", lock.LockMode_Shared, "", "t1")
		require.ErrorContains(t, err, "missing snapshot database")
	})

	t.Run("publisher outside the catalog key range fails before the P step", func(t *testing.T) {
		_, err := planLifecycleAdmission(0, "", 7, math.MaxInt32+1, "tdb", "table", lock.LockMode_Shared, "pub1", "t1")
		require.ErrorContains(t, err, "publication account id exceeds catalog key range")
	})

	t.Run("local table admission keeps the pre-split plan value", func(t *testing.T) {
		local, err := planLifecycleAdmission(0, "", 7, 7, "tdb", "table", lock.LockMode_Shared, "", "t1")
		require.NoError(t, err)
		require.Equal(t, lifecycleAdmissionPlan{
			aliasAccount: 0, aliasDb: "",
			physAccount: 7, databaseName: "tdb", databaseMode: lock.LockMode_Shared,
			tableName: "t1", quotaScope: "table", pitr: false,
			publisher: 7, publicationName: "",
		}, local,
			"the single-account form is pinned against a literal golden plan, and its executable lock-key sequence is pinned in TestAdmitLifecycleRCSplitKeyOrder")
	})

	t.Run("publication forms bind D T Q to the physical owner and P to the publisher", func(t *testing.T) {
		alias, err := planLifecycleAdmission(sub, "sub3", phys, pub, "tdb", "table", lock.LockMode_Exclusive, "pub1", "t1")
		require.NoError(t, err)
		require.Equal(t, sub, alias.aliasAccount)
		require.Equal(t, "sub3", alias.aliasDb, "the A key (S, N) heads the alias order (finding 12)")
		require.Equal(t, phys, alias.physAccount, "D and T keys bind P_src")
		require.Equal(t, lock.LockMode_Exclusive, alias.databaseMode)
		require.Equal(t, "table", alias.quotaScope, "Q binds P_src")
		require.Equal(t, pub, alias.publisher, "the mo_pubs key binds P_pub, never P_src")
		require.Equal(t, "pub1", alias.publicationName)

		explicit, err := planLifecycleAdmission(0, "", phys, pub, "tdb", "table", lock.LockMode_Exclusive, "pub1", "t1")
		require.NoError(t, err)
		require.Empty(t, explicit.aliasDb, "the explicit form carries no A key")
		require.Equal(t, alias.publisher, explicit.publisher)
		require.Equal(t, alias.physAccount, explicit.physAccount)
	})

	t.Run("database-less scopes keep the system PITR retention key", func(t *testing.T) {
		plan, err := planLifecycleAdmission(0, "", 7, 7, "tdb", "", lock.LockMode_Shared, "", "")
		require.NoError(t, err)
		require.True(t, plan.pitr)
		require.Empty(t, plan.quotaScope)
	})
}

// snapshotPublicationTestBinding builds a resolved ordinary binding over a
// database-scoped publication for the revalidation tests.
func snapshotPublicationTestBinding() *snapshotPublicationBinding {
	return &snapshotPublicationBinding{
		row: &snapshotPublicationRow{
			accountID: 100, accountName: "pubacc", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: "*", accountList: "sub_acc",
		},
		physID:         100,
		physName:       "pubacc",
		subscriberID:   3,
		subscriberName: "sub_acc",
	}
}

func stubBindingReread(bh *backgroundExecTest, row *snapshotPublicationRow) {
	stubPublicationRow(bh, row.accountName, row.pubName, false,
		uint64(row.accountID), row.accountName, row.databaseName, row.databaseID, row.tableList, row.accountList)
}

// TestRevalidateSnapshotPublicationBinding is design §9 U5 for the
// post-frontier revalidation of design §4.4 step 5: membership removal and
// every drift shape fails closed (retry-class) before any write, and the
// alias binding re-reads pin (S, N) to its captured publication (finding 12).
func TestRevalidateSnapshotPublicationBinding(t *testing.T) {
	ctx := context.Background()

	requireRetry := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged),
			"binding drift fails with the retry-class error, got: %v", err)
	}

	t.Run("unchanged locked row revalidates", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)
		require.NoError(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
		require.Equal(t, []string{
			snapshotPublicationPubSQL("pubacc", "pub1", false),
			fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(42), "tdb"),
		}, bh.executedSQLs, "the revalidation performs reads only")
	})

	t.Run("missing locked row fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		bh.sql2result[snapshotPublicationPubSQL("pubacc", "pub1", false)] = newMrsEmpty()
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("lost membership fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		stubPublicationRow(bh, "pubacc", "pub1", false, 100, "pubacc", "tdb", 42, "*", "other_acc")
		require.ErrorContains(t, revalidateSnapshotPublicationBinding(ctx, bh, binding),
			"does not have permission to access publication")
	})

	t.Run("retargeted database id fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		drifted := *binding.row
		drifted.databaseID = 43
		stubBindingReread(bh, &drifted)
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("shape retarget to account-covering fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		drifted := *binding.row
		drifted.databaseName = "*"
		drifted.databaseID = 0
		stubBindingReread(bh, &drifted)
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("table dropped from the table list fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		drifted := *binding.row
		drifted.tableList = "t1"
		stubBindingReread(bh, &drifted)
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("physical owner drift fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := snapshotPublicationTestBinding()
		binding.row.accountID = 0
		binding.row.accountName = "sys"
		binding.physID = 2
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 9)
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})
}

// TestRevalidateSnapshotPublicationAliasBinding is design §9 U5 for the
// alias-form post-frontier binding re-reads (finding 12): N rebound to
// another publication, recreated as a local database, or dropped must each
// fail closed with the retry-class error while the intact binding passes.
func TestRevalidateSnapshotPublicationAliasBinding(t *testing.T) {
	ctx := context.Background()

	aliasBinding := func() *snapshotPublicationBinding {
		binding := snapshotPublicationTestBinding()
		binding.aliasDb = "sub3"
		binding.aliasDbID = 555
		return binding
	}
	aliasIDSQL := fmt.Sprintf(getDbIdAndTypFormat, "sub3", 3)
	colSQL := "select 1 from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
	subsSQL := getSubsSql + " and sub_account_id = 3 and sub_name = 'sub3'" +
		fmt.Sprintf(" and status = %d and sub_name is not null and sub_name <> ''", pubsub.SubStatusNormal) + " limit 2"

	stubAliasRowReads := func(bh *backgroundExecTest, aliasID uint64) {
		bh.sql2result[aliasIDSQL] = newMrsForPublicationColumns(
			[]string{"dat_id", "dat_type"}, [][]interface{}{{aliasID, "subscription"}})
		bh.sql2result[colSQL] = newMrsForPublicationColumns([]string{"1"}, [][]interface{}{{int64(1)}})
	}
	stubSubs := func(bh *backgroundExecTest, pubAccountName, pubName string) {
		bh.sql2result[subsSQL] = newMrsForPublicationColumns(
			[]string{"sub_account_id", "sub_account_name", "sub_name", "sub_time",
				"pub_account_id", "pub_account_name", "pub_name", "pub_database",
				"pub_tables", "pub_time", "pub_comment", "status"},
			[][]interface{}{{int64(3), "sub_acc", "sub3", "2026-01-01 00:00:00",
				int64(100), pubAccountName, pubName, "tdb", "*", "2026-01-01 00:00:00", "", int64(0)}})
	}

	requireRetry := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged),
			"alias binding drift fails with the retry-class error, got: %v", err)
	}

	t.Run("intact alias binding revalidates", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := aliasBinding()
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)
		stubAliasRowReads(bh, 555)
		stubSubs(bh, "pubacc", "pub1")
		require.NoError(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("N rebound to another publication fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := aliasBinding()
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)
		stubAliasRowReads(bh, 555)
		stubSubs(bh, "pubacc", "other_pub")
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("N recreated as a local database fails closed", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := aliasBinding()
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)
		stubAliasRowReads(bh, 555)
		bh.sql2result[subsSQL] = newMrsEmpty()
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})

	t.Run("N dropped and recreated keeps no stale database id", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		binding := aliasBinding()
		stubBindingReread(bh, binding.row)
		stubDatabaseOwnerByID(bh, "tdb", 42, 100)
		stubAliasRowReads(bh, 556)
		stubSubs(bh, "pubacc", "pub1")
		requireRetry(t, revalidateSnapshotPublicationBinding(ctx, bh, binding))
	})
}

// snapshotDropRecord builds a publication-visible snapshot record for the
// drop-binding table tests.
func snapshotDropRecord(level, accountName, dbName, tableName string, objID uint64, ts int64) *snapshotRecord {
	return &snapshotRecord{
		snapshotId:   "00000000-0000-0000-0000-0000000000s1",
		snapshotName: "sp1",
		ts:           ts,
		level:        level,
		accountName:  accountName,
		databaseName: dbName,
		tableName:    tableName,
		objId:        objID,
	}
}

// TestBindPublicationSnapshotRecordForDropDatabase is design §9 U6 for the
// DATABASE level of design §4.5: whole-database coverage AND recorded
// identity are both required (finding 11), and the account-covering anchor
// resolves through the publisher's current catalog.
func TestBindPublicationSnapshotRecordForDropDatabase(t *testing.T) {
	ctx := context.Background()
	dbRow := func(tableList string, dbID uint64) *snapshotPublicationRow {
		return &snapshotPublicationRow{
			accountID: 100, accountName: "pubacc", pubName: "pub1",
			databaseName: "tdb", databaseID: dbID, tableList: tableList, accountList: "sub_acc",
		}
	}

	t.Run("table-limited publication never deletes a database snapshot", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "tdb", "", 42, 10)
		before := *record
		err := bindPublicationSnapshotRecordForDrop(ctx, bh, dbRow("t1", 42), 100, record)
		require.ErrorContains(t, err, "outside the publication's scope")
		require.Equal(t, before, *record, "a rejected record stays byte-identical (finding 11)")
	})

	t.Run("whole-database publication deletes the matching database record", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "tdb", "", 42, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, dbRow("*", 42), 100, record))
	})

	t.Run("a same-named replaced database's stale objId is rejected", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "tdb", "", 41, 10)
		before := *record
		err := bindPublicationSnapshotRecordForDrop(ctx, bh, dbRow("*", 42), 100, record)
		require.ErrorContains(t, err, "outside the publication's scope")
		require.Equal(t, before, *record, "the replaced database retains its records (finding 8)")
	})

	t.Run("account-covering publication anchors through the publisher's catalog", func(t *testing.T) {
		anchorSQL := fmt.Sprintf(getDbIdAndTypFormat, "otherdb", 100)
		row := &snapshotPublicationRow{
			accountID: 100, accountName: "pubacc", pubName: "pub1",
			databaseName: "*", databaseID: 0, tableList: "*", accountList: "all",
		}
		t.Run("current anchor admits the matching objId", func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[anchorSQL] = newMrsForPublicationColumns(
				[]string{"dat_id", "dat_type"}, [][]interface{}{{uint64(88), ""}})
			record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "otherdb", "", 88, 10)
			require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row, 100, record))
		})
		t.Run("stale objId after replacement is rejected", func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[anchorSQL] = newMrsForPublicationColumns(
				[]string{"dat_id", "dat_type"}, [][]interface{}{{uint64(89), ""}})
			record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "otherdb", "", 88, 10)
			before := *record
			err := bindPublicationSnapshotRecordForDrop(ctx, bh, row, 100, record)
			require.ErrorContains(t, err, "outside the publication's scope")
			require.Equal(t, before, *record)
		})
		t.Run("dropped anchor database fails closed", func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[anchorSQL] = newMrsEmpty()
			record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "pubacc", "otherdb", "", 88, 10)
			require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row, 100, record),
				"outside the publication's scope")
		})
	})

	t.Run("account snapshots delete only for account-covering rows of the publisher", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		covering := &snapshotPublicationRow{
			accountID: 100, accountName: "pubacc", pubName: "pub1",
			databaseName: "*", databaseID: 0, tableList: "*", accountList: "all",
		}
		record := snapshotDropRecord(tree.SNAPSHOTLEVELACCOUNT.String(), "pubacc", "", "", 100, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, covering, 100, record))
		record.objId = 5
		before := *record
		require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, covering, 100, record),
			"outside the publication's scope")
		require.Equal(t, before, *record, "another account's account snapshot stays intact")
		require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, dbRow("*", 42), 100, record),
			"outside the publication's scope",
			"a database-scoped row never binds an account snapshot")
	})

	t.Run("cluster-level records are never deletable through a publication", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELCLUSTER.String(), "pubacc", "", "", 0, 10)
		require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, dbRow("*", 42), 100, record),
			"outside the publication's scope")
	})
}

// TestBindPublicationSnapshotRecordForDropTable is design §9 U6 for the
// TABLE level of design §4.5: coverage by the persisted list, anchoring by
// the physical owner, and binding by logical table identity (findings 13,
// 14), with the finding-15 name-absent fail-closed rule and the
// whole-database historical fallback.
func TestBindPublicationSnapshotRecordForDropTable(t *testing.T) {
	ctx := context.Background()
	const phys uint32 = 100
	row := func(tableList string) *snapshotPublicationRow {
		return &snapshotPublicationRow{
			accountID: phys, accountName: "pubacc", pubName: "pub1",
			databaseName: "tdb", databaseID: 42, tableList: tableList, accountList: "sub_acc",
		}
	}
	currentSQL := func(db, table string) string {
		sql, err := getSqlForCheckDatabaseTableWithSnapshot(ctx, db, table, phys, 0)
		require.NoError(t, err)
		return sql
	}
	histSQL := func(logicalID uint64, ts int64) string {
		return fmt.Sprintf(
			"select reldatabase_id from mo_catalog.mo_tables {MO_TS = %d} where rel_logical_id = %d and account_id = %d;",
			ts, logicalID, phys)
	}
	stubCurrent := func(bh *backgroundExecTest, db, table string, logicalIDs ...uint64) {
		rows := make([][]interface{}, 0, len(logicalIDs))
		for _, id := range logicalIDs {
			rows = append(rows, []interface{}{id})
		}
		if len(rows) == 0 {
			bh.sql2result[currentSQL(db, table)] = newMrsEmpty()
			return
		}
		bh.sql2result[currentSQL(db, table)] =
			newMrsForPublicationColumns([]string{"rel_logical_id"}, rows)
	}
	stubHistory := func(bh *backgroundExecTest, logicalID uint64, ts int64, dbIDs ...uint64) {
		rows := make([][]interface{}, 0, len(dbIDs))
		for _, id := range dbIDs {
			rows = append(rows, []interface{}{id})
		}
		if len(rows) == 0 {
			bh.sql2result[histSQL(logicalID, ts)] = newMrsEmpty()
			return
		}
		bh.sql2result[histSQL(logicalID, ts)] =
			newMrsForPublicationColumns([]string{"reldatabase_id"}, rows)
	}

	t.Run("coverage pins the recorded table name", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t3", 777, 10)
		before := *record
		err := bindPublicationSnapshotRecordForDrop(ctx, bh, row("t1,t2"), phys, record)
		require.ErrorContains(t, err, "outside the publication's scope")
		require.Equal(t, before, *record)
	})

	t.Run("the record's database qualifier pins the publication database", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "otherdb", "t1", 777, 10)
		require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row("*"), phys, record),
			"outside the publication's scope")
	})

	t.Run("current-row binding honors the recorded logical id (COPY ALTER continuity)", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCurrent(bh, "tdb", "t1", 777)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row("*"), phys, record),
			"WithKeepLogicalId keeps the recorded identity across the physical replacement")
	})

	t.Run("a drop/recreate replacement with a fresh logical id is rejected", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCurrent(bh, "tdb", "t1", 900)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
		before := *record
		err := bindPublicationSnapshotRecordForDrop(ctx, bh, row("*"), phys, record)
		require.ErrorContains(t, err, "outside the publication's scope")
		require.Equal(t, before, *record,
			"the old record survives until the owner cleans it up (findings 13, 14)")
	})

	t.Run("table-limited publication admits its listed table through logical identity", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCurrent(bh, "tdb", "t1", 777)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row("t1,t2"), phys, record))
	})

	t.Run("name-absent state under a table-limited list fails closed (finding 15)", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCurrent(bh, "tdb", "t1")
		stubHistory(bh, 777, 10, 42)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
		before := *record
		err := bindPublicationSnapshotRecordForDrop(ctx, bh, row("t1"), phys, record)
		require.ErrorContains(t, err, "outside the publication's scope",
			"a stale name-only list cannot testify which logical identity it authorized")
		require.Equal(t, before, *record, "the record reverts to owner cleanup")
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), "MO_TS",
			"the historical fallback never runs under table-limited coverage")
	})

	t.Run("name-absent state under whole-database coverage uses the historical fallback", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubCurrent(bh, "tdb", "t1")
		stubHistory(bh, 777, 10, 42)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row("*"), phys, record))
	})

	t.Run("historical fallback validates the exactly-one visible row", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			dbIDs []uint64
		}{
			{"zero rows fail closed", nil},
			{"multiple rows fail closed", []uint64{42, 42}},
			{"a foreign-database row fails closed", []uint64{43}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				bh := &backgroundExecTest{}
				bh.init()
				stubCurrent(bh, "tdb", "t1")
				stubHistory(bh, 777, 10, tc.dbIDs...)
				record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "tdb", "t1", 777, 10)
				before := *record
				require.ErrorContains(t,
					bindPublicationSnapshotRecordForDrop(ctx, bh, row("*"), phys, record),
					"outside the publication's scope")
				require.Equal(t, before, *record)
			})
		}
	})

	t.Run("account-covering table records anchor through the recorded database", func(t *testing.T) {
		anchorSQL := fmt.Sprintf(getDbIdAndTypFormat, "otherdb", phys)
		covering := &snapshotPublicationRow{
			accountID: phys, accountName: "pubacc", pubName: "pub1",
			databaseName: "*", databaseID: 0, tableList: "*", accountList: "all",
		}
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[anchorSQL] = newMrsForPublicationColumns(
			[]string{"dat_id", "dat_type"}, [][]interface{}{{uint64(88), ""}})
		stubCurrent(bh, "otherdb", "t9", 555)
		record := snapshotDropRecord(tree.SNAPSHOTLEVELTABLE.String(), "pubacc", "otherdb", "t9", 555, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, covering, phys, record))
	})
}

// TestBindPublicationSnapshotRecordForDropForeignSource is design §9 U6 for
// the foreign-source drop: only the physical owner's rows bind; the
// publisher's own same-named rows never resolve through the publication
// (findings 4, 7, 9).
func TestBindPublicationSnapshotRecordForDropForeignSource(t *testing.T) {
	ctx := context.Background()
	row := &snapshotPublicationRow{
		accountID: 0, accountName: "sys", pubName: "pub1",
		databaseName: "fsrc_db", databaseID: 42, tableList: "*", accountList: "sub_acc",
	}

	t.Run("records bind under the physical owner's name", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubAccountNameByID(bh, 2, "owner_acc")
		record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "owner_acc", "fsrc_db", "", 42, 10)
		require.NoError(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row, 2, record))
	})

	t.Run("the publisher's own same-named record is rejected and intact", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		stubAccountNameByID(bh, 2, "owner_acc")
		record := snapshotDropRecord(tree.SNAPSHOTLEVELDATABASE.String(), "sys", "fsrc_db", "", 42, 10)
		before := *record
		require.ErrorContains(t, bindPublicationSnapshotRecordForDrop(ctx, bh, row, 2, record),
			"outside the publication's scope")
		require.Equal(t, before, *record, "the decoy row stays byte-identical")
	})
}

// ---------------------------------------------------------------------------
// Review finding 3 fixtures: executable admission and statement-path proof of
// the design §4.4 lifecycle protocol (lock-controlled rebind/revocation
// interleavings and snapshot/quota rollback assertions of design §9 U5/U7).
// ---------------------------------------------------------------------------

// admissionLockStep is one lock acquisition as seen by the recording fake.
type admissionLockStep struct {
	relation string
	mode     lock.LockMode
	group    uint32
	encoded  string
	sqlCount int
}

// recordingAdmissionLockOps fakes lifecycleAdmissionLockOps: every physical
// lock step is recorded (relation, mode, group, encoded key, and the number
// of SQL statements issued against the shared executor so far), an optional
// hook deterministically injects drift at named steps (e.g., a rebind while
// the alias key is notionally held), and finish/free are counted so lock
// release is asserted on every outcome.
type recordingAdmissionLockOps struct {
	proc      *process.Process
	bh        *backgroundExecTest
	steps     []admissionLockStep
	hook      func(stepIndex int, relationName string) error
	finishes  int32
	frees     int32
	finishErr error
	mu        sync.Mutex
}

func (o *recordingAdmissionLockOps) lockProcess() *process.Process { return o.proc }

func (o *recordingAdmissionLockOps) lockKey(
	relationName string, ctx context.Context, keys *batch.Batch, mode lock.LockMode, group uint32,
) error {
	defer keys.Vecs[0].Free(o.proc.Mp())
	o.mu.Lock()
	idx := len(o.steps)
	o.mu.Unlock()
	if o.hook != nil {
		if err := o.hook(idx, relationName); err != nil {
			return err
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.steps = append(o.steps, admissionLockStep{
		relation: relationName, mode: mode, group: group,
		encoded: string(keys.Vecs[0].GetBytesAt(0)), sqlCount: len(o.bh.executedSQLs),
	})
	return nil
}

func (o *recordingAdmissionLockOps) finish(ctx context.Context, ses *Session, bh BackgroundExec) error {
	atomic.AddInt32(&o.finishes, 1)
	return o.finishErr
}

func (o *recordingAdmissionLockOps) free() {
	atomic.AddInt32(&o.frees, 1)
}

func (o *recordingAdmissionLockOps) recordedSteps() []admissionLockStep {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]admissionLockStep(nil), o.steps...)
}

// snapshotQuotaAdmissionSurface cans the owner's SNAPSHOT quota pair reads
// the admission Q step performs: the registry row, the quota select, and the
// FOR UPDATE locking read (issued twice by lockFeatureQuota).
func snapshotQuotaAdmissionSurface(bh *backgroundExecTest, accountID uint32, scope string) {
	if err := mockSnapshotQuotaResult(bh, pubCreatePhysName, accountID, scope); err != nil {
		panic(err)
	}
	lockedQuotaSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = '%s' for update",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeSnapshot, scope,
	)
	bh.sql2result[lockedQuotaSQL] = newMrsForFeatureLimit([][]interface{}{{int64(defaultSnapshotLimit)}})
}

// TestAdmitLifecycleRCSplitKeyOrder is the executable half of design §4.4
// step 3 that the plan-value test cannot express: the recorded key sequence
// proves A→D→T→Q(SQL)→P ordering, the alias-only A key, and no step runs
// after an injected lock failure.
func TestAdmitLifecycleRCSplitKeyOrder(t *testing.T) {
	ctx := context.Background()

	quotaSQLs := func(accountID uint32) []string {
		registrySQL := fmt.Sprintf(
			"select enabled, scope_spec from %s.%s where feature_code = '%s'",
			catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeSnapshot)
		quotaSQL := fmt.Sprintf(
			"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = 'table'",
			catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountID, featureCodeSnapshot)
		return []string{registrySQL, quotaSQL, quotaSQL + " for update", quotaSQL + " for update"}
	}

	newRun := func(t *testing.T) (*Session, *backgroundExecTest, *recordingAdmissionLockOps) {
		t.Helper()
		ses := newFeatureLimitTestSession(t)
		bh := &backgroundExecTest{}
		bh.init()
		snapshotQuotaAdmissionSurface(bh, 7, "table")
		fake := &recordingAdmissionLockOps{proc: testutil.NewProc(t), bh: bh}
		stub := gostub.Stub(&beginLifecycleAdmission,
			func(context.Context, *Session, BackgroundExec) (lifecycleAdmissionLockOps, error) {
				return fake, nil
			})
		t.Cleanup(stub.Reset)
		return ses, bh, fake
	}

	t.Run("local form keeps the pre-split golden sequence byte for byte", func(t *testing.T) {
		ses, bh, fake := newRun(t)
		require.NoError(t, admitLocalLifecycleRC(
			ctx, ses, bh, 7, "tdb", "table", lock.LockMode_Shared, "", "t1"))
		steps := fake.recordedSteps()
		require.Len(t, steps, 2)
		require.Equal(t, admissionLockStep{relation: catalog.MO_DATABASE, mode: lock.LockMode_Shared, group: 7, encoded: steps[0].encoded, sqlCount: 0}, steps[0])
		require.Equal(t, admissionLockStep{relation: catalog.MO_TABLES, mode: lock.LockMode_Shared, group: 7, encoded: steps[1].encoded, sqlCount: 0}, steps[1])
		require.Equal(t, quotaSQLs(7), bh.executedSQLs,
			"the Q step (quota pair) follows the target locks and precedes finish")
		require.Equal(t, int32(1), atomic.LoadInt32(&fake.finishes))
		require.Equal(t, int32(1), atomic.LoadInt32(&fake.frees))
	})

	t.Run("alias form prepends A and appends P with identical D T keys", func(t *testing.T) {
		ses, bh, fake := newRun(t)
		require.NoError(t, admitPublicationLifecycleRC(
			ctx, ses, bh, 3, "sub3", 7, 100, "tdb", "table", lock.LockMode_Exclusive, "pub1", "t1"))
		aliasSteps := fake.recordedSteps()
		require.Len(t, aliasSteps, 4)
		require.Equal(t, catalog.MO_DATABASE, aliasSteps[0].relation)
		require.Equal(t, uint32(3), aliasSteps[0].group, "A = (S, N) heads the alias lock order (finding 12)")
		require.Equal(t, lock.LockMode_Shared, aliasSteps[0].mode)
		require.Equal(t, admissionLockStep{relation: catalog.MO_DATABASE, mode: lock.LockMode_Exclusive, group: 7, encoded: aliasSteps[1].encoded, sqlCount: 0}, aliasSteps[1])
		require.Equal(t, admissionLockStep{relation: catalog.MO_TABLES, mode: lock.LockMode_Shared, group: 7, encoded: aliasSteps[2].encoded, sqlCount: 0}, aliasSteps[2])
		require.Equal(t, catalog.MO_PUBS, aliasSteps[3].relation, "P binds the publication account")
		require.Equal(t, catalog.System_Account, aliasSteps[3].group)
		require.Equal(t, lock.LockMode_Shared, aliasSteps[3].mode)
		require.Equal(t, 4, aliasSteps[3].sqlCount,
			"the Q step SQL (registry, quota, locked quota x2) precedes the publication key")
		require.Equal(t, quotaSQLs(7), bh.executedSQLs)
		require.Equal(t, int32(1), atomic.LoadInt32(&fake.finishes))

		ses2, bh2, fake2 := newRun(t)
		require.NoError(t, admitPublicationLifecycleRC(
			ctx, ses2, bh2, 0, "", 7, 100, "tdb", "table", lock.LockMode_Exclusive, "pub1", "t1"))
		explicitSteps := fake2.recordedSteps()
		require.Len(t, explicitSteps, 3, "no A key without the alias form")
		for i := range explicitSteps {
			require.Equal(t, explicitSteps[i].encoded, aliasSteps[i+1].encoded,
				"D/T/P keys are byte-identical between the alias and explicit forms")
			require.Equal(t, explicitSteps[i].mode, aliasSteps[i+1].mode)
			require.Equal(t, explicitSteps[i].group, aliasSteps[i+1].group)
		}

		ses3, _, fake3 := newRun(t)
		require.NoError(t, admitLocalLifecycleRC(
			ctx, ses3, fake3.bh, 7, "tdb", "table", lock.LockMode_Exclusive, "", "t1"))
		localSteps := fake3.recordedSteps()
		require.Len(t, localSteps, 2)
		for i := range localSteps {
			require.Equal(t, localSteps[i].encoded, explicitSteps[i].encoded,
				"the local single-account D/T keys are byte-identical to the publication form's (pre-split compatibility)")
		}
	})

	t.Run("an injected D-step failure stops the sequence and releases the admission", func(t *testing.T) {
		ses, bh, fake := newRun(t)
		injected := moerr.NewInternalErrorNoCtx("database lock conflict")
		fake.hook = func(stepIndex int, relationName string) error {
			if stepIndex == 1 {
				return injected
			}
			return nil
		}
		err := admitPublicationLifecycleRC(
			ctx, ses, bh, 3, "sub3", 7, 100, "tdb", "table", lock.LockMode_Shared, "pub1", "t1")
		require.ErrorIs(t, err, injected)
		steps := fake.recordedSteps()
		require.Len(t, steps, 1, "only the alias key was acquired before the failure")
		require.Empty(t, bh.executedSQLs, "the owner's quota pair is never touched after a lock failure")
		require.Equal(t, int32(0), atomic.LoadInt32(&fake.finishes))
		require.Equal(t, int32(1), atomic.LoadInt32(&fake.frees), "the admission is released exactly once")
	})
}

// publicationCreateFixture drives doCreateSnapshot through its statement path
// with the admission operations, statement transaction, and persisted db-id
// reads faked at the package seams (the auth test precedence).
type publicationCreateFixture struct {
	ses   *Session
	ctx   context.Context
	bh    *backgroundExecTest
	fake  *recordingAdmissionLockOps
	txnOp *mock_frontend.MockTxnOperator
	dbID  uint64
}

const (
	pubCreateSubTenant   = "tenant_b"
	pubCreateSubID       = uint32(3)
	pubCreatePhysName    = "pubacc"
	pubCreatePhysID      = uint32(100)
	pubCreateDatabase    = "tdb"
	pubCreateDatabaseID  = uint64(42)
	pubCreateTable       = "t1"
	pubCreateTableID     = uint64(777)
	pubCreateAliasDb     = "sub3"
	pubCreateAliasDbID   = uint64(555)
	pubCreateTSBase      = int64(1700000000000000000)
	pubCreateInsertPfx   = "insert into mo_catalog.mo_snapshots("
	pubCreateRegistrySQL = "select enabled, scope_spec from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT'"
)

func pubCreateSubMeta(pubName string) *plan.SubscriptionMeta {
	return &plan.SubscriptionMeta{
		AccountName: pubCreatePhysName, Name: pubName,
		AccountId: int32(pubCreatePhysID), DbName: pubCreateDatabase, Tables: "*",
	}
}

func pubCreateSubRows(pubAccountName, pubName string) *MysqlResultSet {
	return newMrsForPublicationColumns(
		[]string{"sub_account_id", "sub_account_name", "sub_name", "sub_time",
			"pub_account_id", "pub_account_name", "pub_name", "pub_database",
			"pub_tables", "pub_time", "pub_comment", "status"},
		[][]interface{}{{int64(pubCreateSubID), pubCreateSubTenant, pubCreateAliasDb, "2026-01-01 00:00:00",
			int64(pubCreatePhysID), pubAccountName, pubName, pubCreateDatabase, "*", "2026-01-01 00:00:00", "", int64(0)}})
}

// publicationCreateHarness dispatches the package seams per session so two
// concurrent statement executions each run against their own fixture.
type publicationCreateHarness struct {
	mu    sync.Mutex
	bySes map[FeSession]*publicationCreateFixture
	byBH  map[BackgroundExec]*publicationCreateFixture
	meta  map[FeSession]*plan.SubscriptionMeta
}

func (h *publicationCreateHarness) add(f *publicationCreateFixture) {
	h.bySes[f.ses] = f
	h.byBH[f.bh] = f
}

func (h *publicationCreateHarness) forSes(ses FeSession) *publicationCreateFixture {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bySes[ses]
}

func (h *publicationCreateHarness) forBh(bh BackgroundExec) *publicationCreateFixture {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.byBH[bh]
}

func (h *publicationCreateHarness) metaFor(ses FeSession) *plan.SubscriptionMeta {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.meta[ses]
}

func (h *publicationCreateHarness) install(t *testing.T) {
	t.Helper()
	stub := gostub.Stub(&NewBackgroundExec,
		func(_ context.Context, fes FeSession, _ ...*BackgroundExecOption) BackgroundExec {
			return h.forSes(fes).bh
		})
	t.Cleanup(stub.Reset)
	stub = gostub.Stub(&beginLifecycleAdmission,
		func(_ context.Context, ses *Session, _ BackgroundExec) (lifecycleAdmissionLockOps, error) {
			return h.forSes(ses).fake, nil
		})
	t.Cleanup(stub.Reset)
	stub = gostub.Stub(&snapshotBackgroundTxnOperator,
		func(bh BackgroundExec) client.TxnOperator {
			return h.forBh(bh).txnOp
		})
	t.Cleanup(stub.Reset)
	stub = gostub.Stub(&resolvePublicationSnapshotDatabaseID,
		func(_ context.Context, _ *Session, bh BackgroundExec, _ string) (uint64, error) {
			return h.forBh(bh).dbID, nil
		})
	t.Cleanup(stub.Reset)
	stub = gostub.Stub(&resolveBranchSubscriptionMeta,
		func(_ context.Context, _ string, fes FeSession, _ TxnOperator, _ BackgroundExec) (*plan.SubscriptionMeta, error) {
			return h.metaFor(fes), nil
		})
	t.Cleanup(stub.Reset)
}

func newPublicationCreateFixture(
	t *testing.T, ctrl *gomock.Controller, databaseID uint64,
) *publicationCreateFixture {
	t.Helper()
	ses := newTestSession(t, ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm
	ses.SetTenantInfo(&TenantInfo{
		Tenant:      pubCreateSubTenant,
		User:        rootName,
		DefaultRole: accountAdminRoleName,
		TenantID:    pubCreateSubID, UserID: rootID, DefaultRoleID: moAdminRoleID,
	})

	// The transaction snapshot is state, not a call script: reads always see
	// the current frontier, and an RC refresh stores a frontier strictly above
	// the requested one (the AdvanceLineageSnapshot contract), so a statement
	// may read and refresh any number of times.
	currentTS := &atomic.Int64{}
	currentTS.Store(pubCreateTSBase)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(pbtxn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SnapshotTS().DoAndReturn(func() timestamp.Timestamp {
		return timestamp.Timestamp{PhysicalTime: currentTS.Load()}
	}).AnyTimes()
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().AdvanceSnapshot(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, ts timestamp.Timestamp) error {
			currentTS.Store(ts.PhysicalTime + int64(time.Microsecond))
			return nil
		}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	ses.proc = testutil.NewProc(t)
	ses.proc.Base.TxnOperator = txnOp

	bh := &backgroundExecTest{}
	bh.init()
	return &publicationCreateFixture{
		ses:   ses,
		ctx:   ctx,
		bh:    bh,
		fake:  &recordingAdmissionLockOps{proc: testutil.NewProc(t), bh: bh},
		txnOp: txnOp,
		dbID:  databaseID,
	}
}

// canPublicationWorld cans every catalog answer the statement path reads for
// the given publication name; mutating single keys later simulates drift.
func (f *publicationCreateFixture) canPublicationWorld(pubName string) {
	f.bh.sql2result[snapshotPublicationPubSQL(pubCreatePhysName, pubName, false)] =
		newMrsForPublicationInfo(uint64(pubCreatePhysID), pubCreatePhysName, pubName, pubCreateDatabase, pubCreateDatabaseID, "*", pubCreateSubTenant)
	stubDatabaseOwnerByID(f.bh, pubCreateDatabase, pubCreateDatabaseID, uint64(pubCreatePhysID))
	snapshotQuotaAdmissionSurface(f.bh, pubCreatePhysID, "table")
	f.bh.sql2result["select account_id from mo_catalog.mo_account where account_name = '"+pubCreateSubTenant+"'"] =
		newMrsForCheckTenant([][]interface{}{{uint64(pubCreateSubID), pubCreateSubTenant, "open", 0}})
	f.bh.sql2result[f.tableIDSQL()] = newMrsForPublicationColumns(
		[]string{"rel_logical_id"}, [][]interface{}{{pubCreateTableID}})
	f.bh.sql2result[f.coverageSQL(pubName)] = newMrsForPublicationColumns(
		[]string{"account_name", "database_name", "database_id", "table_list", "account_list"},
		[][]interface{}{{pubCreatePhysName, pubCreateDatabase, pubCreateDatabaseID, "*", pubCreateSubTenant}})
	// The pre-write duplicate-name probe runs on every publication-routed
	// create; both test snapshot names stay claimable.
	for _, name := range []string{"sp_sub_t1", "sp_pub_t1"} {
		checkSQL, err := getSqlForCheckSnapshot(f.ctx, name)
		if err != nil {
			panic(err)
		}
		f.bh.sql2result[checkSQL] = newMrsForPasswordOfUser([][]interface{}{})
	}
}

// canAliasBinding adds the alias-form subscriber binding reads.
func (f *publicationCreateFixture) canAliasBinding(pubName string, aliasID uint64) {
	f.bh.sql2result[fmt.Sprintf(getDbIdAndTypFormat, pubCreateAliasDb, pubCreateSubID)] =
		newMrsForPublicationColumns([]string{"dat_id", "dat_type"}, [][]interface{}{{aliasID, "subscription"}})
	f.bh.sql2result["select 1 from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"] =
		newMrsForPublicationColumns([]string{"1"}, [][]interface{}{{int64(1)}})
	f.bh.sql2result[f.subsSQL()] = pubCreateSubRows(pubCreatePhysName, pubName)
}

func (f *publicationCreateFixture) subsSQL() string {
	return getSubsSql + " and sub_account_id = 3 and sub_name = 'sub3'" +
		fmt.Sprintf(" and status = %d and sub_name is not null and sub_name <> ''", pubsub.SubStatusNormal) + " limit 2"
}

func (f *publicationCreateFixture) tableIDSQL() string {
	return fmt.Sprintf(
		"select rel_logical_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s' and account_id = %d;",
		pubCreateTable, pubCreateDatabase, pubCreatePhysID)
}

func (f *publicationCreateFixture) coverageSQL(pubName string) string {
	return fmt.Sprintf(
		"select account_name, database_name, database_id, table_list, account_list from mo_catalog.mo_pubs where account_id = %d and pub_name = '%s'",
		pubCreatePhysID, pubName)
}

func snapshotCreateTableStatement(name, objName, pubAccount, pubName string) *tree.CreateSnapShot {
	return &tree.CreateSnapShot{
		Name: tree.Identifier(name),
		Object: tree.ObjectInfo{
			SLevel:      tree.SnapshotLevelType{Level: tree.SNAPSHOTLEVELTABLE},
			ObjName:     tree.Identifier(objName),
			AccountName: tree.Identifier(pubAccount),
			PubName:     tree.Identifier(pubName),
		},
	}
}

func moSnapshotsInserts(bh *backgroundExecTest) []string {
	var out []string
	for _, s := range bh.executedSQLs {
		if strings.HasPrefix(s, pubCreateInsertPfx) {
			out = append(out, s)
		}
	}
	return out
}

func requireCommittedSnapshotRow(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	inserts := moSnapshotsInserts(bh)
	require.Len(t, inserts, 1, "exactly one mo_snapshots row is written")
	require.Contains(t, inserts[0], "'"+pubCreatePhysName+"'", "the row is stored under the physical owner")
	require.Contains(t, inserts[0], "'"+pubCreateDatabase+"'")
	require.Contains(t, inserts[0], "'"+pubCreateTable+"'")
	require.NotContains(t, inserts[0], pubCreateSubTenant, "nothing is recorded under the subscriber")
	require.NotContains(t, inserts[0], pubCreateAliasDb)
	require.Contains(t, bh.executedSQLs, "commit;")
	require.NotContains(t, bh.executedSQLs, "rollback;")
}

func requireNoSnapshotWrite(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	require.Empty(t, moSnapshotsInserts(bh), "no snapshot row quota consumption survives a rejected create")
	require.NotContains(t, bh.executedSQLs, "commit;")
	require.Contains(t, bh.executedSQLs, "rollback;",
		"every failure path rolls the statement transaction back")
}

func requireQuotaTouchedOnlyOwner(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	ownerQuota := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = 'table'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, pubCreatePhysID, featureCodeSnapshot)
	require.Contains(t, bh.executedSQLs, ownerQuota,
		"the physical owner's Snapshot quota pair is consulted")
	ownerCount := fmt.Sprintf(
		"select count(*) from %s.%s where account_name = '%s' and level = 'table' and kind != 'branch'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, pubCreatePhysName)
	require.Contains(t, bh.executedSQLs, ownerCount)
	for _, s := range bh.executedSQLs {
		require.NotContains(t, s, "account_id = 3 and feature_code",
			"the subscriber's quota rows are never touched")
		if strings.HasPrefix(s, "select count(*)") {
			require.NotContains(t, s, pubCreateSubTenant,
				"the subscriber's snapshot usage is never counted")
		}
	}
}

// TestDoCreateSnapshotPublicationAdmission is the executable statement-path
// proof required by review finding 3 (design §9 U5/U7): quota-admission and
// insertion failure-atomicity with committed-state and lock-release
// assertions in both accounts.
func TestDoCreateSnapshotPublicationAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runtimeCleanup := isolatePublicationCreateRuntime(t)
	defer runtimeCleanup()

	t.Run("alias create commits owner-routed state once with A D T Q P order", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		f.canAliasBinding("pub1", pubCreateAliasDbID)
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{}, byBH: map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{f.ses: pubCreateSubMeta("pub1")},
		}
		h.add(f)
		h.install(t)

		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
		require.NoError(t, err)
		requireCommittedSnapshotRow(t, f.bh)
		requireQuotaTouchedOnlyOwner(t, f.bh)

		steps := f.fake.recordedSteps()
		require.Len(t, steps, 4)
		require.Equal(t, catalog.MO_DATABASE, steps[0].relation)
		require.Equal(t, pubCreateSubID, steps[0].group, "A heads the alias order")
		require.Equal(t, admissionLockStep{relation: catalog.MO_DATABASE, mode: lock.LockMode_Shared, group: pubCreatePhysID, encoded: steps[1].encoded, sqlCount: 4}, steps[1],
			"D follows A; the pre-admission reads are begin; alias-id, mo_pubs, db-owner")
		require.Equal(t, admissionLockStep{relation: catalog.MO_TABLES, mode: lock.LockMode_Shared, group: pubCreatePhysID, encoded: steps[2].encoded, sqlCount: 4}, steps[2])
		require.Equal(t, catalog.MO_PUBS, steps[3].relation)
		require.Equal(t, catalog.System_Account, steps[3].group)
		require.Equal(t, 8, steps[3].sqlCount,
			"the owner's quota pair SQL (registry, quota, locked x2) precedes the publication key")
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.finishes))
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	})

	t.Run("explicit form commits with D T Q P and never classifies", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{}, byBH: map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{},
		}
		h.add(f)
		h.install(t)

		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_pub_t1", pubCreateDatabase+"."+pubCreateTable, pubCreatePhysName, "pub1"))
		require.NoError(t, err)
		requireCommittedSnapshotRow(t, f.bh)
		steps := f.fake.recordedSteps()
		require.Len(t, steps, 3, "no A key without the alias form")
		require.Equal(t, 3, steps[0].sqlCount, "begin; mo_pubs and db-owner reads precede admission")
		require.Equal(t, 7, steps[2].sqlCount)
		require.Equal(t, catalog.MO_PUBS, steps[2].relation)
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	})

	t.Run("quota-admission failure rolls back everything", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		f.canAliasBinding("pub1", pubCreateAliasDbID)
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{}, byBH: map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{f.ses: pubCreateSubMeta("pub1")},
		}
		h.add(f)
		h.install(t)

		injected := moerr.NewInternalErrorNoCtx("quota catalog unavailable")
		ownerQuota := fmt.Sprintf(
			"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = 'table'",
			catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, pubCreatePhysID, featureCodeSnapshot)
		f.bh.sql2err[ownerQuota] = injected

		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
		require.ErrorContains(t, err, "quota catalog unavailable")
		requireNoSnapshotWrite(t, f.bh)
		assertAliasAdmissionAbortedBeforeWrites(t, f)
	})

	t.Run("coverage mismatch fails before the write", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		f.bh.sql2result[f.coverageSQL("pub1")] = newMrsForPublicationColumns(
			[]string{"account_name", "database_name", "database_id", "table_list", "account_list"},
			[][]interface{}{{pubCreatePhysName, pubCreateDatabase, uint64(7), "*", pubCreateSubTenant}})
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{}, byBH: map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{},
		}
		h.add(f)
		h.install(t)

		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_pub_t1", pubCreateDatabase+"."+pubCreateTable, pubCreatePhysName, "pub1"))
		require.ErrorContains(t, err, "outside current publication coverage")
		requireNoSnapshotWrite(t, f.bh)
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	})

	t.Run("insert failure is failure-atomic at the quota-consumption point", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		f.canAliasBinding("pub1", pubCreateAliasDbID)
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{}, byBH: map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{f.ses: pubCreateSubMeta("pub1")},
		}
		h.add(f)
		h.install(t)

		injected := moerr.NewInternalErrorNoCtx("mo_snapshots insert conflict")
		f.bh.sqlPrefixErr[pubCreateInsertPfx] = injected
		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
		require.ErrorContains(t, err, "mo_snapshots insert conflict")
		require.Len(t, moSnapshotsInserts(f.bh), 1,
			"the mo_snapshots insert (the quota consumption itself) was attempted and failed")
		require.NotContains(t, f.bh.executedSQLs, "commit;")
		require.Contains(t, f.bh.executedSQLs, "rollback;",
			"the statement rolls back, consuming no quota and recording nothing")
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	})
}

// assertAliasAdmissionAbortedBeforeWrites pins the admission-killed paths:
// A/D/T ran, the Q step failed, the publication key was never touched, no
// registry re-check ran, and the admission was released exactly once.
func assertAliasAdmissionAbortedBeforeWrites(t *testing.T, f *publicationCreateFixture) {
	t.Helper()
	steps := f.fake.recordedSteps()
	require.Len(t, steps, 3, "A D T ran; the Q step failed before the publication key")
	require.Equal(t, catalog.MO_TABLES, steps[2].relation)
	require.Equal(t, int32(0), atomic.LoadInt32(&f.fake.finishes))
	require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	require.NotContains(t, f.bh.executedSQLs,
		"select account_id from mo_catalog.mo_account where account_name = '"+pubCreateSubTenant+"'",
		"no post-admission statement work runs after the admission failure")
}

// isolatePublicationCreateRuntime detaches the service-global internal SQL
// executor for the duration of one statement-path test (the TestDoCreateSnapshot
// precedent): the fixtures replace NewBackgroundExec with backgroundExecTest
// and must not meet another fixture's executor.
func isolatePublicationCreateRuntime(t *testing.T) func() {
	t.Helper()
	rt := moruntime.ServiceRuntime("")
	oldExecutor, hadOldExecutor := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if hadOldExecutor {
		require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor))
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			if hadOldExecutor {
				rt.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
				return
			}
			if current, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor); ok {
				rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, current)
			}
		})
	}
	t.Cleanup(cleanup)
	return cleanup
}

// singleFixtureHarness builds the one-session harness for a statement-path
// subtest; passing a nil meta keeps the subscription classifier inert.
func singleFixtureHarness(t *testing.T, f *publicationCreateFixture, meta *plan.SubscriptionMeta) {
	t.Helper()
	h := &publicationCreateHarness{
		bySes: map[FeSession]*publicationCreateFixture{},
		byBH:  map[BackgroundExec]*publicationCreateFixture{},
		meta:  map[FeSession]*plan.SubscriptionMeta{},
	}
	if meta != nil {
		h.meta[f.ses] = meta
	}
	h.add(f)
	h.install(t)
}

func requireRetryClassError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged),
		"binding drift fails closed with the retry-class error, got: %v", err)
}

// TestDoCreateSnapshotPublicationRebind is the lock-controlled interleaving
// proof of review finding 3 / design §9 U5/U7: publication revocation and
// retargeting while the admission holds its keys fails closed pre-insert with
// a retry-class error, and the retried statement classifies and commits the
// new binding.
func TestDoCreateSnapshotPublicationRebind(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	isolatePublicationCreateRuntime(t)

	newAliasFixture := func(t *testing.T, pubName string) *publicationCreateFixture {
		t.Helper()
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld(pubName)
		f.canAliasBinding(pubName, pubCreateAliasDbID)
		return f
	}

	driftAtPublicationKey := []struct {
		name   string
		mutate func(f *publicationCreateFixture)
	}{
		{"revoked publication row", func(f *publicationCreateFixture) {
			f.bh.sql2result[snapshotPublicationPubSQL(pubCreatePhysName, "pub1", false)] = newMrsEmpty()
		}},
		{"retargeted database identity", func(f *publicationCreateFixture) {
			f.bh.sql2result[snapshotPublicationPubSQL(pubCreatePhysName, "pub1", false)] =
				newMrsForPublicationInfo(uint64(pubCreatePhysID), pubCreatePhysName, "pub1", pubCreateDatabase, 43, "*", pubCreateSubTenant)
		}},
		{"shape retarget to account-covering", func(f *publicationCreateFixture) {
			f.bh.sql2result[snapshotPublicationPubSQL(pubCreatePhysName, "pub1", false)] =
				newMrsForPublicationInfo(uint64(pubCreatePhysID), pubCreatePhysName, "pub1", "*", 0, "*", pubCreateSubTenant)
		}},
		{"table dropped from the publication list", func(f *publicationCreateFixture) {
			f.bh.sql2result[snapshotPublicationPubSQL(pubCreatePhysName, "pub1", false)] =
				newMrsForPublicationInfo(uint64(pubCreatePhysID), pubCreatePhysName, "pub1", pubCreateDatabase, pubCreateDatabaseID, "t2", pubCreateSubTenant)
		}},
	}
	for _, tc := range driftAtPublicationKey {
		t.Run("publication "+tc.name+" while the publication key is held", func(t *testing.T) {
			f := newAliasFixture(t, "pub1")
			singleFixtureHarness(t, f, pubCreateSubMeta("pub1"))
			f.fake.hook = func(stepIndex int, relationName string) error {
				if relationName == catalog.MO_PUBS {
					tc.mutate(f)
				}
				return nil
			}
			err := doCreateSnapshot(f.ctx, f.ses,
				snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
			requireRetryClassError(t, err)
			requireNoSnapshotWrite(t, f.bh)
			require.Len(t, f.fake.recordedSteps(), 4,
				"the admission completed; the locked post-frontier revalidation caught the drift")
			require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
		})
	}

	t.Run("N rebound to another publication: first attempt fails closed, retry commits the new binding", func(t *testing.T) {
		f := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f.canPublicationWorld("pub1")
		f.canPublicationWorld("pub2")
		f.canAliasBinding("pub1", pubCreateAliasDbID)
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{},
			byBH:  map[BackgroundExec]*publicationCreateFixture{},
			meta:  map[FeSession]*plan.SubscriptionMeta{f.ses: pubCreateSubMeta("pub1")},
		}
		h.add(f)
		h.install(t)

		var rebindOnce sync.Once
		f.fake.hook = func(stepIndex int, relationName string) error {
			if relationName == catalog.MO_PUBS {
				rebindOnce.Do(func() {
					f.bh.sql2result[f.subsSQL()] = pubCreateSubRows(pubCreatePhysName, "pub2")
				})
			}
			return nil
		}
		err := doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
		requireRetryClassError(t, err)
		requireNoSnapshotWrite(t, f.bh)
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))

		// The retried statement classifies the rebound subscriber: the create
		// commits under the new publication with the same physical owner.
		h.meta[f.ses] = pubCreateSubMeta("pub2")
		f.fake.hook = nil
		f.bh.executedSQLs = nil
		f.fake = &recordingAdmissionLockOps{proc: testutil.NewProc(t), bh: f.bh}
		err = doCreateSnapshot(f.ctx, f.ses,
			snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", ""))
		require.NoError(t, err)
		requireCommittedSnapshotRow(t, f.bh)
		require.Len(t, f.fake.recordedSteps(), 4)
		require.Equal(t, int32(1), atomic.LoadInt32(&f.fake.frees))
	})

	t.Run("rebind while the alias key is held by a concurrent create", func(t *testing.T) {
		// G1 classifies N over pub1 and holds the alias key; while it holds it,
		// the world publishes the rebound binding — modeled by G2, an
		// independent concurrent create against pub2's binding, running to
		// completion — and G1's post-frontier revalidation must catch the
		// post-rebind catalog instead of writing with its stale binding.
		f1 := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f1.canPublicationWorld("pub1")
		f1.canAliasBinding("pub1", pubCreateAliasDbID)
		f2 := newPublicationCreateFixture(t, ctrl, pubCreateDatabaseID)
		f2.canPublicationWorld("pub2")
		f2.canAliasBinding("pub2", pubCreateAliasDbID)
		h := &publicationCreateHarness{
			bySes: map[FeSession]*publicationCreateFixture{},
			byBH:  map[BackgroundExec]*publicationCreateFixture{},
			meta: map[FeSession]*plan.SubscriptionMeta{
				f1.ses: pubCreateSubMeta("pub1"),
				f2.ses: pubCreateSubMeta("pub2"),
			},
		}
		h.add(f1)
		h.add(f2)
		h.install(t)

		held := make(chan struct{})
		release := make(chan struct{})
		var blockOnce sync.Once
		f1.fake.hook = func(stepIndex int, relationName string) error {
			if stepIndex == 1 {
				// The alias key (recorded step 0) is held when the D step runs.
				blockOnce.Do(func() {
					close(held)
					<-release
				})
			}
			return nil
		}

		stmt := snapshotCreateTableStatement("sp_sub_t1", pubCreateAliasDb+"."+pubCreateTable, "", "")
		var err1 error
		done := make(chan struct{})
		go func() {
			defer close(done)
			err1 = doCreateSnapshot(f1.ctx, f1.ses, stmt)
		}()
		<-held

		require.NoError(t, doCreateSnapshot(f2.ctx, f2.ses, stmt),
			"the concurrent create against the rebound binding commits")
		requireCommittedSnapshotRow(t, f2.bh)
		f1.bh.sql2result[f1.subsSQL()] = pubCreateSubRows(pubCreatePhysName, "pub2")
		close(release)
		<-done

		requireRetryClassError(t, err1)
		requireNoSnapshotWrite(t, f1.bh)
		require.Equal(t, int32(1), atomic.LoadInt32(&f1.fake.frees))
		require.Equal(t, int32(1), atomic.LoadInt32(&f2.fake.frees))
		require.Len(t, append(moSnapshotsInserts(f1.bh), moSnapshotsInserts(f2.bh)...), 1,
			"exactly one owner-routed row is committed across the interleaving")
	})
}

// TestLockPublicationSnapshotOwnerSequence is design §9 U6 for the drop
// spine of design §4.5 step 1: the generation match precedes the FOR UPDATE
// membership row, whose locked row then resolves the physical owner.
func TestLockPublicationSnapshotOwnerSequence(t *testing.T) {
	ctx := context.Background()

	t.Run("generation then locked row then owner resolution", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		liveSQL := "select account_id from mo_catalog.mo_account where account_name = 'sub_acc'"
		bh.sql2result[liveSQL] = newMrsForCheckTenant([][]interface{}{{uint64(3), "sub_acc", "open", 0}})
		stubPublicationRow(bh, "sys", "pub1", true, 0, "sys", "fsrc_db", 42, "*", "sub_acc")
		stubDatabaseOwnerByID(bh, "fsrc_db", 42, 2)

		row, physID, err := lockPublicationSnapshotOwner(ctx, bh, "sys", "pub1", "sub_acc", 3)
		require.NoError(t, err)
		require.Equal(t, uint32(0), row.accountID)
		require.Equal(t, uint32(2), physID)
		require.Equal(t, []string{
			liveSQL,
			snapshotPublicationPubSQL("sys", "pub1", true),
			fmt.Sprintf(getDbAccountIdAndTypByIdFormat, uint64(42), "fsrc_db"),
		}, bh.executedSQLs)
	})

	t.Run("a retired subscriber generation fails before the publication read", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		liveSQL := "select account_id from mo_catalog.mo_account where account_name = 'sub_acc'"
		bh.sql2result[liveSQL] = newMrsForCheckTenant([][]interface{}{{uint64(9), "sub_acc", "open", 0}})
		_, _, err := lockPublicationSnapshotOwner(ctx, bh, "sys", "pub1", "sub_acc", 3)
		require.ErrorContains(t, err, "session is no longer active")
		require.Equal(t, []string{liveSQL}, bh.executedSQLs)
	})
}
