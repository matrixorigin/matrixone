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

package compile

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Retention ledger helper fabric tests (D153/D154): the keyed ledger row
// lifecycle — lookup/idempotent insert/enrich/retire — and the link
// presence probe's range and overflow contract. Everything is driven
// through the drop-lifecycle harness's SQL fixtures; the helpers are
// deliberately fail-ALIVE on the write path (a wedged ledger write must
// never fail the table drop it mirrors). Every contact attempts the wide
// (row_kind/logical_id) shape first and degrades to the legacy shape on
// the missing-column signal.

// pkMoveRetentionLookupSQL is shared with pk_move_owner_lifecycle_test.go.
const (
	pkMoveRetentionInsertSQL       = "insert into mo_catalog.mo_data_branch_link_retention(row_kind, table_id, database_name, table_name, obj_id, created_ts, logical_id) values("
	pkMoveRetentionLegacyInsertSQL = "insert into mo_catalog.mo_data_branch_link_retention(table_id, database_name, table_name, obj_id, created_ts) values("
	pkMoveRetentionUpdateSQL       = "update mo_catalog.mo_data_branch_link_retention set "
	pkMoveRetentionDeleteSQL       = "delete from mo_catalog.mo_data_branch_link_retention where row_kind = 0 and table_id = "
	pkMoveRetentionLegacyDeleteSQL = "delete from mo_catalog.mo_data_branch_link_retention where table_id = "
	pkMoveRetentionProbeSQL        = "select 1 from mo_catalog.mo_data_branch_update_link where link_id >= x'"
)

func TestLookupPkMoveLinkRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("found row yields captured identity including the logical id", func(t *testing.T) {
		var c *Compile
		c, _ = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{"db1"}, []string{"t800"}, []uint64{800}), nil
			}},
		})
		dbName, tblName, logicalID, found, err := lookupPkMoveLinkRetention(c, 7, 800)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "db1", dbName)
		require.Equal(t, "t800", tblName)
		require.Equal(t, uint64(800), logicalID)
	})

	t.Run("empty result reports not found", func(t *testing.T) {
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, nil)
		dbName, tblName, logicalID, found, err := lookupPkMoveLinkRetention(c, 7, 800)
		require.NoError(t, err)
		require.False(t, found)
		require.Empty(t, dbName)
		require.Empty(t, tblName)
		require.Zero(t, logicalID)
	})

	t.Run("missing catalog reports not found for pre-upgrade tenants", func(t *testing.T) {
		nosuch := moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_data_branch_link_retention")
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, nosuch
			}},
		})
		_, _, _, found, err := lookupPkMoveLinkRetention(c, 7, 800)
		require.NoError(t, err)
		require.False(t, found)
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionLookupLegacySQL),
			"a missing catalog is not the missing-column signal: no legacy retry")
	})

	t.Run("missing-column verdict retries the legacy projection with logical id zero", func(t *testing.T) {
		var c *Compile
		var got *[]string
		badfield := pkMoveMissingLedgerColumnErr()
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, badfield
			}},
			{pkMoveRetentionLookupLegacySQL, func(sql string) (executor.Result, error) {
				return pkMoveStrPairResult(t, c, []string{"db2"}, []string{"t801"}), nil
			}},
		})
		dbName, tblName, logicalID, found, err := lookupPkMoveLinkRetention(c, 7, 800)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "db2", dbName)
		require.Equal(t, "t801", tblName)
		require.Zero(t, logicalID, "the legacy projection carries no logical id")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionLookupSQL))
		require.Equal(t, []string{
			"select database_name, table_name from mo_catalog.mo_data_branch_link_retention where table_id = 800",
		}, collectSqlWithPrefix(*got, pkMoveRetentionLookupLegacySQL))
	})

	t.Run("other probe errors propagate to the fail-ALIVE caller", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("boom")
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		_, _, _, _, err := lookupPkMoveLinkRetention(c, 7, 800)
		require.ErrorIs(t, err, boom)
	})
}

func TestInsertPkMoveLinkRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("insert on miss carries the captured identity", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionLookupSQL))
		inserts := collectSqlWithPrefix(*got, pkMoveRetentionInsertSQL)
		require.Len(t, inserts, 1)
		require.Contains(t, inserts[0], "values(0, 800, 'db1', 't800', 800, ")
		require.Contains(t, inserts[0], ", 0)", "logical id 0 marks the legacy/unresolved lane")
	})

	t.Run("the member's logical id reaches the miss insert", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		insertPkMoveLinkRetention(c, 7, 800, 900, "db1", "t800")
		inserts := collectSqlWithPrefix(*got, pkMoveRetentionInsertSQL)
		require.Len(t, inserts, 1)
		require.Contains(t, inserts[0], "values(0, 800, 'db1', 't800', 800, ")
		require.Contains(t, inserts[0], ", 900)")
	})

	t.Run("quoted names survive the SQL literal boundary", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		insertPkMoveLinkRetention(c, 7, 800, 0, "we'ird", "t800")
		inserts := collectSqlWithPrefix(*got, pkMoveRetentionInsertSQL)
		require.Len(t, inserts, 1)
		require.Contains(t, inserts[0], `'we''ird'`)
	})

	t.Run("first write wins over a later named verdict", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{"db_winner"}, []string{"t_winner"}, []uint64{900}), nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionUpdateSQL))
	})

	t.Run("named verdict enriches an empty-name residue row", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{""}, []string{""}, []uint64{0}), nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
		updates := collectSqlWithPrefix(*got, pkMoveRetentionUpdateSQL)
		require.Len(t, updates, 1)
		require.Equal(t,
			"update mo_catalog.mo_data_branch_link_retention set database_name = 'db1', table_name = 't800' where row_kind = 0 and table_id = 800",
			updates[0])
	})

	t.Run("named verdict backfills the logical id of a residue row", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{""}, []string{""}, []uint64{0}), nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 900, "db1", "t800")
		updates := collectSqlWithPrefix(*got, pkMoveRetentionUpdateSQL)
		require.Len(t, updates, 1)
		require.Equal(t,
			"update mo_catalog.mo_data_branch_link_retention set database_name = 'db1', table_name = 't800', logical_id = 900 where row_kind = 0 and table_id = 800",
			updates[0])
	})

	t.Run("logical id alone backfills while the first-written names win", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{"db_old"}, []string{"t_old"}, []uint64{0}), nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 900, "db_new", "t_new")
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
		updates := collectSqlWithPrefix(*got, pkMoveRetentionUpdateSQL)
		require.Len(t, updates, 1)
		require.Equal(t,
			"update mo_catalog.mo_data_branch_link_retention set logical_id = 900 where row_kind = 0 and table_id = 800",
			updates[0])
	})

	t.Run("partial names never enrich residue", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{""}, []string{""}, []uint64{0}), nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "")
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionUpdateSQL))
	})

	t.Run("enrichment retries the names-only legacy shape on the missing-column verdict", func(t *testing.T) {
		var c *Compile
		var got *[]string
		badfield := pkMoveMissingLedgerColumnErr()
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return pkMoveIdentityResult(t, c, []string{""}, []string{""}, []uint64{0}), nil
			}},
			{pkMoveRetentionUpdateSQL, func(sql string) (executor.Result, error) {
				if sql == "update mo_catalog.mo_data_branch_link_retention set database_name = 'db1', table_name = 't800' where row_kind = 0 and table_id = 800" {
					return executor.Result{}, badfield
				}
				return executor.Result{}, nil
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		updates := collectSqlWithPrefix(*got, pkMoveRetentionUpdateSQL)
		require.Len(t, updates, 2, "the wide enrichment attempt plus the legacy retry")
		require.Equal(t,
			"update mo_catalog.mo_data_branch_link_retention set database_name = 'db1', table_name = 't800' where table_id = 800",
			updates[1], "the legacy retry carries neither row_kind nor logical_id")
	})

	t.Run("probe failure swallows the write without failing the drop", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("ledger probe failed")
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionLookupSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionLookupSQL))
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
	})

	t.Run("write failure including a missing catalog never escalates", func(t *testing.T) {
		nosuch := moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_data_branch_link_retention")
		boom := moerr.NewInternalErrorNoCtx("insert conflict")
		failInsert := boom
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionInsertSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, failInsert
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
		failInsert = nosuch
		*got = nil
		insertPkMoveLinkRetention(c, 7, 800, 0, "db1", "t800")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
	})

	t.Run("miss insert retries the legacy five-column shape on the missing-column verdict", func(t *testing.T) {
		badfield := pkMoveMissingLedgerColumnErr()
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionInsertSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, badfield
			}},
		})
		insertPkMoveLinkRetention(c, 7, 800, 900, "db1", "t800")
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL))
		legacy := collectSqlWithPrefix(*got, pkMoveRetentionLegacyInsertSQL)
		require.Len(t, legacy, 1)
		require.Contains(t, legacy[0], "values(800, 'db1', 't800', 800, ")
		require.NotContains(t, legacy[0], "row_kind")
		require.NotContains(t, legacy[0], "logical_id")
	})
}

func TestDeletePkMoveLinkRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("delete rides the keyed row", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		require.NoError(t, deletePkMoveLinkRetention(c, 7, 800))
		deletes := collectSqlWithPrefix(*got, pkMoveRetentionDeleteSQL)
		require.Len(t, deletes, 1)
		require.Equal(t,
			"delete from mo_catalog.mo_data_branch_link_retention where row_kind = 0 and table_id = 800",
			deletes[0])
	})

	t.Run("missing catalog is not an error", func(t *testing.T) {
		nosuch := moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_data_branch_link_retention")
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionDeleteSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, nosuch
			}},
		})
		require.NoError(t, deletePkMoveLinkRetention(c, 7, 800))
	})

	t.Run("missing-column verdict retries the legacy delete shape", func(t *testing.T) {
		badfield := pkMoveMissingLedgerColumnErr()
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionDeleteSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, badfield
			}},
		})
		require.NoError(t, deletePkMoveLinkRetention(c, 7, 800))
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionDeleteSQL))
		require.Equal(t, []string{
			"delete from mo_catalog.mo_data_branch_link_retention where table_id = 800",
		}, collectSqlWithPrefix(*got, pkMoveRetentionLegacyDeleteSQL))
	})

	t.Run("other delete errors propagate", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("ledger delete failed")
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionDeleteSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		require.ErrorIs(t, deletePkMoveLinkRetention(c, 7, 800), boom)
	})
}

func TestDeletePkMoveLinkAndRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("links retire before their ledger row", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		require.NoError(t, deletePkMoveLinkAndRetention(c, 7, 800))
		require.Len(t, *got, 2)
		require.Equal(t,
			"delete from mo_catalog.mo_data_branch_update_link where link_id >= x'0000000000000320' and link_id < x'0000000000000321'",
			(*got)[0])
		require.Equal(t,
			"delete from mo_catalog.mo_data_branch_link_retention where row_kind = 0 and table_id = 800",
			(*got)[1])
	})

	t.Run("link delete failure stops before the ledger row", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("link delete failed")
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveLinkDeletesSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		require.ErrorIs(t, deletePkMoveLinkAndRetention(c, 7, 800), boom)
		require.Zero(t, countSqlWithPrefix(*got, pkMoveRetentionDeleteSQL))
	})

	t.Run("ledger delete failure surfaces after links retired", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("ledger delete failed")
		c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionDeleteSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		require.ErrorIs(t, deletePkMoveLinkAndRetention(c, 7, 800), boom)
		require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveLinkDeletesSQL))
	})
}

func TestPkMoveLinkPresenceProbe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("live row reports present", func(t *testing.T) {
		var c *Compile
		var got *[]string
		c, got = newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionProbeSQL, func(sql string) (executor.Result, error) {
				return pkMoveStrColResult(t, c, []string{"1"}), nil
			}},
		})
		has, err := pkMoveLinkPresenceProbe(c, 7, 800)
		require.NoError(t, err)
		require.True(t, has)
		require.Equal(t, []string{
			"select 1 from mo_catalog.mo_data_branch_update_link where link_id >= x'0000000000000320' and link_id < x'0000000000000321' limit 1",
		}, *got)
	})

	t.Run("empty range reports absent", func(t *testing.T) {
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, nil)
		has, err := pkMoveLinkPresenceProbe(c, 7, 800)
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("missing link catalog reports absent", func(t *testing.T) {
		nosuch := moerr.NewNoSuchTableNoCtx("mo_catalog", "mo_data_branch_update_link")
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionProbeSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, nosuch
			}},
		})
		has, err := pkMoveLinkPresenceProbe(c, 7, 800)
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("probe errors propagate to the fail-ALIVE caller", func(t *testing.T) {
		boom := moerr.NewInternalErrorNoCtx("probe failed")
		c, _ := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
			{pkMoveRetentionProbeSQL, func(sql string) (executor.Result, error) {
				return executor.Result{}, boom
			}},
		})
		_, err := pkMoveLinkPresenceProbe(c, 7, 800)
		require.ErrorIs(t, err, boom)
	})

	t.Run("max uint64 table id skips the unrepresentable range without SQL", func(t *testing.T) {
		c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
		has, err := pkMoveLinkPresenceProbe(c, 7, ^uint64(0))
		require.NoError(t, err)
		require.False(t, has)
		require.Empty(t, *got)
	})
}
