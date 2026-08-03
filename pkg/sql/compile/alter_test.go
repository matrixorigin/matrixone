// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestShouldEnableAlterCopyPipelineFlush(t *testing.T) {
	assert.False(t, shouldEnableAlterCopyPipelineFlush(nil))
	assert.False(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: false}))
	assert.True(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: true}))
}

func TestBuildRefreshViewSQL(t *testing.T) {
	sqlMode := "ANSI_QUOTES"
	testCases := []struct {
		name     string
		view     viewMetadataRefresh
		expected string
	}{
		{
			name: "create view with explicit columns",
			view: viewMetadataRefresh{
				database: "target-db",
				name:     "target`view",
				viewData: plan.ViewData{
					Stmt:            "create view old_name (A, B) as select code, qty + 1 from source_t",
					DefaultDatabase: "source-db",
					SQLMode:         &sqlMode,
				},
			},
			expected: "alter view `target-db`.`target``view` (`a`, `b`) as select `code`, `qty` + 1 from `source_t`",
		},
		{
			name: "stored alter view",
			view: viewMetadataRefresh{
				database: "db",
				name:     "v",
				viewData: plan.ViewData{
					Stmt:            "alter view v as select * from source_t",
					DefaultDatabase: "db",
				},
			},
			expected: "alter view `db`.`v` as select * from `source_t`",
		},
		{
			name: "legacy pipes as concat",
			view: viewMetadataRefresh{
				database: "db",
				name:     "legacy_v",
				viewData: plan.ViewData{
					Stmt:            "create view legacy_v as select a || b from source_t",
					DefaultDatabase: "db",
				},
			},
			expected: "alter view `db`.`legacy_v` as select concat(`a`, `b`) from `source_t`",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			sql, err := buildRefreshViewSQL(context.Background(), 1, testCase.view)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, sql)
		})
	}

	_, err := buildRefreshViewSQL(context.Background(), 1, viewMetadataRefresh{
		database: "db",
		name:     "v",
		viewData: plan.ViewData{
			Stmt: "select 1",
		},
	})
	require.Error(t, err)
}

func TestBuildViewMetadataRefreshQueryEscapesLegacyTableName(t *testing.T) {
	query := buildViewMetadataRefreshQuery(
		7,
		24,
		42,
		"db",
		`x\')) OR 1=1 -- x`,
		128,
		128,
	)
	stmts, err := mysql.Parse(context.Background(), query, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.Len(t, stmts, 1)
	formatted := tree.String(stmts[0], dialect.MYSQL)
	require.Contains(t, formatted, "order by")
	require.Contains(t, formatted, "limit 128")
	require.Contains(t, query, `x\\'')) OR 1=1 -- x`)
	require.Contains(t, query, `\"logical_id\":24,`)
	require.Contains(t, query, `\"table_id\":42,`)
	require.Contains(t, query, `"database_name":"db","table_name"`)
	require.Contains(t, query, `"subscription_table":"x`)
	require.Contains(t, query, "pub_account_id = 7")
	require.Contains(t, query, "json_extract(viewdef, '$.dependencies') is null")
	require.NotContains(t, query, "viewdef like '\\%")
	pendingQuery := buildViewMetadataRefreshQuery(7, 24, 42, "db", "source", 0, 128, true)
	require.Contains(t, pendingQuery, "json_extract(viewdef, '$.metadata_refresh_pending')")
	require.NotContains(t, pendingQuery, "mo_catalog.mo_subs")
	pendingStmts, err := mysql.Parse(context.Background(), pendingQuery, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range pendingStmts {
			stmt.Free()
		}
	}()
}

func TestCurrentViewSubscriptionResolver(t *testing.T) {
	resolver := currentViewSubscriptionResolver{byDatabase: map[string]*plan.SubscriptionMeta{
		"subdb": {
			AccountId: 9,
			DbName:    "pubdb",
			SubName:   "subdb",
			Tables:    "allowed",
		},
	}}

	meta, err := resolver.GetSubscriptionMeta("subdb", nil)

	require.NoError(t, err)
	require.Equal(t, int32(9), meta.GetAccountId())
	require.Equal(t, "pubdb", meta.GetDbName())
	require.Equal(t, "subdb", meta.GetSubName())
	require.Equal(t, "allowed", meta.GetTables())

	meta, err = resolver.GetSubscriptionMeta("localdb", nil)
	require.NoError(t, err)
	require.Nil(t, meta)

	resolver.accountID = 7
	resolver.snapshotByIdentity = make(map[viewMetadataSnapshotSubscriptionKey]*plan.SubscriptionMeta)
	resolver.loadedSnapshots = make(map[viewMetadataSnapshotSubscriptionKey]struct{})
	var loaded []viewMetadataSnapshotSubscriptionKey
	resolver.loadSnapshot = func(accountID uint32, database string, snapshot *plan.Snapshot) (*plan.SubscriptionMeta, error) {
		loaded = append(loaded, viewMetadataSnapshotSubscriptionKey{
			accountID: accountID, database: strings.ToLower(database),
			physicalTime: snapshot.GetTS().GetPhysicalTime(),
			logicalTime:  snapshot.GetTS().GetLogicalTime(),
		})
		return &plan.SubscriptionMeta{AccountId: int32(snapshot.GetTS().GetPhysicalTime())}, nil
	}
	meta, err = resolver.GetSubscriptionMeta("historical_subdb", &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 1},
		Tenant: &plan.SnapshotTenant{TenantID: 11},
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), meta.GetAccountId())
	_, err = resolver.GetSubscriptionMeta("historical_subdb", &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 1},
		Tenant: &plan.SnapshotTenant{TenantID: 11},
	})
	require.NoError(t, err)
	meta, err = resolver.GetSubscriptionMeta("historical_subdb", &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 2},
		Tenant: &plan.SnapshotTenant{TenantID: 11},
	})
	require.NoError(t, err)
	require.Equal(t, int32(2), meta.GetAccountId())
	require.Len(t, loaded, 2)
	require.Equal(t, uint32(11), loaded[0].accountID)
}

func TestLoadSnapshotViewSubscriptionKeepsSystemPublisher(t *testing.T) {
	ctrl := gomock.NewController(t)
	mp := mpool.MustNewZero()
	query := "select sub_name, pub_account_id, pub_account_name, pub_name, " +
		"pub_database, pub_tables from mo_catalog.mo_subs " +
		"where sub_account_id = 7 and lower(sub_name) = lower('subdb') and status = 0"
	result := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_int32.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
	}, mp)
	result.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendStringRows(result, 0, []string{"subdb"}))
	require.NoError(t, executor.AppendFixedRows(result, 1, []int32{0}))
	require.NoError(t, executor.AppendStringRows(result, 2, []string{"sys"}))
	require.NoError(t, executor.AppendStringRows(result, 3, []string{"pub"}))
	require.NoError(t, executor.AppendStringRows(result, 4, []string{"pubdb"}))
	require.NoError(t, executor.AppendStringRows(result, 5, []string{"source_t"}))
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		query: result.GetResult(),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	meta, err := loadSnapshotViewSubscription(c, 7, "subdb", &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 1},
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), meta.GetAccountId())
	require.Equal(t, "pubdb", meta.GetDbName())
	require.Equal(t, "source_t", meta.GetTables())
	require.Equal(t, []string{query}, spyExec.executedSQLs)
	require.Zero(t, mp.CurrNB())
}

func TestViewDependenciesContainLiveSource(t *testing.T) {
	source := viewMetadataRefreshSource{
		accountID: 7, logicalID: 24, previousID: 41, currentID: 42,
		database: "db", tableName: "t",
	}
	snapshotOnly := []plan.ViewDependency{{
		AccountID: 7, AccountIDSet: true, LogicalID: 24, TableID: 41, Snapshot: true,
	}}
	require.False(t, viewDependenciesContainLiveSource(snapshotOnly, source, nil))
	require.True(t, viewDependenciesContainLiveSource(append(snapshotOnly, plan.ViewDependency{
		AccountID: 7, AccountIDSet: true, LogicalID: 24, TableID: 42,
	}), source, nil))
	require.False(t, viewDependenciesContainLiveSource([]plan.ViewDependency{{
		AccountID: 8, AccountIDSet: true, LogicalID: 24, TableID: 42,
	}}, source, nil))
	require.True(t, viewDependenciesContainLiveSource([]plan.ViewDependency{{
		AccountID: 7, AccountIDSet: true, LogicalID: 100, TableID: 101,
		DatabaseName: "DB", TableName: "T",
	}}, source, nil))
	require.True(t, viewDependenciesContainLiveSource([]plan.ViewDependency{{
		AccountID: 0, AccountIDSet: true, LogicalID: 100, TableID: 101,
		Subscription: true, PublisherAccountIDSet: true, PublisherAccountID: 0,
		PublisherDB: "DB", PublisherTable: "T",
	}}, viewMetadataRefreshSource{accountID: 0, database: "db", tableName: "t"}, nil))
	require.True(t, viewDependenciesContainLiveSource([]plan.ViewDependency{{
		AccountID: 9, AccountIDSet: true, LogicalID: 100, TableID: 101,
		Subscription: true, SubscriptionDB: "subdb", SubscriptionTable: "t",
		PublisherAccountIDSet: true, PublisherAccountID: 9,
		PublisherDB: "old_db", PublisherTable: "t",
	}}, viewMetadataRefreshSource{accountID: 11, database: "new_db", tableName: "t"},
		map[string]*plan.SubscriptionMeta{
			"subdb": {AccountId: 11, DbName: "new_db", Tables: "t"},
		}))
}

func TestCheckViewMetadataCandidateLimit(t *testing.T) {
	require.NoError(t, checkViewMetadataCandidateLimit(
		context.Background(), maxLegacyCandidatesPerMetadataRefresh,
	))
	err := checkViewMetadataCandidateLimit(
		context.Background(), maxLegacyCandidatesPerMetadataRefresh+1,
	)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
}

func TestViewColumnsEqual(t *testing.T) {
	col := func(name string, typ plan2.Type) *plan2.ColDef {
		return &plan2.ColDef{Name: name, Typ: typ}
	}
	base := []*plan2.ColDef{
		col("code", plan2.Type{Id: int32(types.T_varchar), Width: 5}),
		col("qty", plan2.Type{Id: int32(types.T_int32), NotNullable: true}),
	}

	require.True(t, viewColumnsEqual(
		append(base, &plan2.ColDef{Name: catalog.Row_ID, Hidden: true}),
		base,
	))
	require.False(t, viewColumnsEqual(base, []*plan2.ColDef{
		col("code", plan2.Type{Id: int32(types.T_varchar), Width: 60}),
		base[1],
	}))
	require.False(t, viewColumnsEqual(base, []*plan2.ColDef{
		base[0],
		col("qty", plan2.Type{Id: int32(types.T_int64), NotNullable: true}),
	}))
	require.False(t, viewColumnsEqual(base, base[:1]))
}

func TestCanSkipViewMetadataRefreshError(t *testing.T) {
	ctx := context.Background()
	planError := func(err error) error {
		return &viewMetadataRefreshPlanError{err: err}
	}
	require.True(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewBadFieldError(ctx, "missing_column", "field list")),
	))
	require.True(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewNoSuchTable(ctx, "db", "missing_table")),
	))
	require.True(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewViewWrongList(ctx)),
	))
	require.True(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewInvalidInput(ctx, "ambiguous column reference")),
	))
	require.True(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewConstraintViolation(ctx, "invalid view")),
	))
	require.True(t, canSkipViewMetadataRefreshError(
		planError(&viewMetadataSnapshotNotFoundError{name: "deleted_snapshot"}),
	))
	require.False(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewInternalError(ctx, "snapshot catalog read failed")),
	))
	require.False(t, canSkipViewMetadataRefreshError(
		planError(moerr.NewTxnNeedRetry(ctx)),
	))
	require.False(t, canSkipViewMetadataRefreshError(
		moerr.NewDuplicateEntry(ctx, "duplicate", "key"),
	))
	require.False(t, canSkipViewMetadataRefreshError(
		context.Canceled,
	))
	require.True(t, CanSkipViewMetadataRefreshError(
		moerr.NewNotSupported(ctx, "missing UDF"),
	))
	require.False(t, CanSkipViewMetadataRefreshError(
		moerr.NewInternalError(ctx, "catalog write failed"),
	))
}

func TestRefreshViewMetadataAfterAlter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mp := mpool.MustNewZero()
	const sourceTableID = 42
	const sourceLogicalID = 24
	query := buildViewMetadataRefreshQuery(
		7,
		sourceLogicalID,
		sourceTableID,
		"db",
		"source_t",
		0,
		128,
	)
	nextQuery := strings.Replace(query, "rel_id > 0", "rel_id > 2", 1)
	subscriptionQuery := "select sub_name, pub_account_id, pub_account_name, pub_name, " +
		"pub_database, pub_tables from mo_catalog.mo_subs " +
		"where sub_account_id = 7 and sub_name is not null and status = 0"

	bat := batch.NewWithSize(7)
	bat.SetRowCount(2)
	bat.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint32(7), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], uint32(7), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], uint64(2), false, mp))
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], uint64(1), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], uint64(2), false, mp))
	bat.Vecs[3] = vector.NewVec(types.T_uint32.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], uint32(5), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], uint32(6), false, mp))
	for i := 4; i < len(bat.Vecs); i++ {
		bat.Vecs[i] = vector.NewVec(types.T_varchar.ToType())
	}
	require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte("db"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte("v"), false, mp))
	require.NoError(t, vector.AppendBytes(
		bat.Vecs[6],
		[]byte(`{"Stmt":"create view v as select a from source_t","DefaultDatabase":"db"}`),
		false,
		mp,
	))
	require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte("db"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte("invalid_v"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[6], []byte("not-json"), false, mp))

	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		query:             {Mp: mp, Batches: []*batch.Batch{bat}},
		subscriptionQuery: {},
		nextQuery:         {},
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	c.proc.Ctx = defines.AttachAccountId(c.proc.Ctx, 7)
	require.NoError(t, refreshViewMetadataAfterAlter(
		c, 7, sourceLogicalID, sourceTableID, sourceTableID, "db", "source_t", false,
	))
	require.Equal(t, []string{
		query,
		subscriptionQuery,
		"alter view `db`.`v` as select `a` from `source_t`",
		nextQuery,
	}, spyExec.executedSQLs)
	require.Zero(t, mp.CurrNB())

	require.False(t, isViewMetadataRefresh(nil))
	require.True(t, isViewMetadataRefresh(
		context.WithValue(
			context.Background(),
			defines.ViewMetadataRefreshKey{},
			viewMetadataRefreshContext{sourceLogicalID: sourceLogicalID},
		),
	))
}

func TestAlterCopySameStatementColumnReplacement(t *testing.T) {
	tableDef := &plan2.TableDef{Cols: []*plan2.ColDef{
		{Name: "a", ColId: 1, Seqnum: 0},
		{Name: "b", ColId: 2, Seqnum: 1},
	}}
	replacement := &plan2.AlterTable{
		TableDef: tableDef,
		ChangeTblColIdMap: map[uint64]*plan2.ColDef{
			1: {Name: "a"},
		},
		CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0},
			{Name: "B", ColId: ^uint64(0), Seqnum: 0},
		}},
	}
	name, ok := alterCopySameStatementColumnReplacement(replacement)
	require.True(t, ok)
	require.Equal(t, "B", name)

	t.Run("same identity survives rename and reorder", func(t *testing.T) {
		unchanged := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
				2: {Name: "B"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "B", ColId: 2, Seqnum: 1},
				{Name: "a", ColId: 1, Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(unchanged)
		require.False(t, replaced)
	})

	t.Run("different-name drop and add is rejected", func(t *testing.T) {
		dropped := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
				{Name: "c", ColId: ^uint64(0), Seqnum: 0},
			}},
		}
		name, replaced := alterCopySameStatementColumnReplacement(dropped)
		require.True(t, replaced)
		require.Equal(t, "c", name)
	})

	t.Run("target-only add without a drop remains supported", func(t *testing.T) {
		added := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
				2: {Name: "b"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
				{Name: "b", ColId: 2, Seqnum: 1},
				{Name: "c", ColId: ^uint64(0), Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(added)
		require.False(t, replaced)
	})

	t.Run("drop without an add remains supported", func(t *testing.T) {
		dropped := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(dropped)
		require.False(t, replaced)
	})
}

func TestBuildAlterDataBranchLineageSQL(t *testing.T) {
	metadataSQL, snapshotSQL := buildAlterDataBranchLineageSQL(
		11, 22, 123456, 7,
		"alter:table", "tenant'o", "db'x", "tbl'y", "snapshot-id",
	)

	require.Equal(t,
		"insert into mo_catalog.mo_branch_metadata values(22, 123456, 11, 7, 'alter:table', false)",
		metadataSQL,
	)
	require.Contains(t, snapshotSQL, "insert into mo_catalog.mo_snapshots")
	require.Contains(t, snapshotSQL, "'snapshot-id', '__mo_branch_22', 123456")
	require.Contains(t, snapshotSQL, "'tenant''o', 'db''x', 'tbl''y', 11, 'branch'")
}

func TestAlterDataBranchHistoricalSourceSQL(t *testing.T) {
	for _, sql := range []string{
		alterDataBranchHistoricalSnapshotSourceSQL("tenant'o", "db'x", "tbl'y", 42),
		alterDataBranchHistoricalPitrSourceSQL("tenant'o", "db'x", "tbl'y", 42),
	} {
		require.Contains(t, sql, "account_name = 'tenant''o'")
		require.Contains(t, sql, "database_name = 'db''x'")
		require.Contains(t, sql, "table_name = 'tbl''y'")
		require.Contains(t, sql, "obj_id = 42")
		require.Contains(t, sql, "limit 1 for update")
	}
}

func TestAlterTableHasLatestHistoricalBranchSourceUsesFreshUnlockedProbe(t *testing.T) {
	const (
		oldTableID = uint64(42)
		database   = "test"
		table      = "dept"
	)
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	snapshotSQL := alterDataBranchHistoricalSnapshotSourceProbeSQL(
		"", database, table, oldTableID, false,
	)
	spyExec.results[snapshotSQL] = newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)

	hasHistory, err := c.alterTableHasLatestHistoricalBranchSource(oldTableID, database, table)
	require.NoError(t, err)
	require.True(t, hasHistory)
	require.NotContains(t, snapshotSQL, "for update")
	require.Equal(t, []string{snapshotSQL}, spyExec.executedSQLs)
}

func TestAlterDataBranchLineageMetadata(t *testing.T) {
	dag := databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, Creator: 9, Level: "table", TableDeleted: false},
	})

	creator, level := alterDataBranchLineageMetadata(dag, 2)
	require.Equal(t, uint32(9), creator)
	require.Equal(t, "alter:table", level)

	creator, level = alterDataBranchLineageMetadata(dag, 1)
	require.Equal(t, uint32(catalog.System_Account), creator)
	require.Equal(t, "alter", level)
}

func TestValidateAlterDataBranchLineageTxn(t *testing.T) {
	require.NoError(t, validateAlterDataBranchLineageTxn(false, true, true))
	require.NoError(t, validateAlterDataBranchLineageTxn(false, true, false))

	for _, tc := range []struct {
		name        string
		byBegin     bool
		autocommit  bool
		pessimistic bool
		want        string
	}{
		{
			name:        "explicit begin",
			byBegin:     true,
			autocommit:  true,
			pessimistic: true,
			want:        "not supported inside an explicit transaction",
		},
		{
			name:        "autocommit disabled",
			autocommit:  false,
			pessimistic: true,
			want:        "not supported inside an explicit transaction",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAlterDataBranchLineageTxn(tc.byBegin, tc.autocommit, tc.pessimistic)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestPrepareAlterDataBranchLineageAllowsHistoricalSourceTxn(t *testing.T) {
	const (
		oldTableID = uint64(42)
		database   = "test"
		table      = "dept"
	)
	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	snapshotSQL := alterDataBranchHistoricalSnapshotSourceSQL("", database, table, oldTableID)
	pitrSQL := alterDataBranchHistoricalPitrSourceSQL("", database, table, oldTableID)

	for _, tc := range []struct {
		name     string
		history  string
		wantSQLs []string
	}{
		{
			name:     "snapshot",
			history:  snapshotSQL,
			wantSQLs: []string{participationSQL, snapshotSQL},
		},
		{
			name:     "pitr",
			history:  pitrSQL,
			wantSQLs: []string{participationSQL, snapshotSQL, pitrSQL},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			spyExec.results[tc.history] = newAlterCopyFixedResult(
				t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
			)

			lineagePlan, err := c.prepareAlterDataBranchLineage(oldTableID, database, table)
			require.NoError(t, err)
			require.True(t, lineagePlan.enabled)
			require.True(t, lineagePlan.preserveHistoricalSource)
			require.Equal(t, tc.wantSQLs, spyExec.executedSQLs)
		})
	}
}

func TestShouldAdvanceAlterDataBranchLineageSnapshot(t *testing.T) {
	require.True(t, shouldAdvanceAlterDataBranchLineageSnapshot(true, true))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(true, false))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(false, true))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(false, false))
}

func TestAdvanceAlterDataBranchLineageSnapshotRejectsOverflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{
		PhysicalTime: math.MaxInt64 - int64(time.Microsecond) + 1,
	})

	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txnOp
	c := &Compile{proc: proc}
	_, err := c.advanceAlterDataBranchLineageSnapshot()
	require.ErrorContains(t, err, "timestamp limit")
}

func TestIsAlterAffectedPluginIndexMatchesIndexNamePartsAndIncludedColumns(t *testing.T) {
	indexDef := &plan2.IndexDef{
		IndexName:       "idx_vec",
		Parts:           []string{"embedding"},
		IncludedColumns: []string{"doc_id", catalog.CreateAlias("category")},
	}

	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"idx_vec"}))
	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"embedding"}))
	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"category"}))
	require.False(t, isAlterAffectedPluginIndex(indexDef, []string{"other"}))
	require.False(t, isAlterAffectedPluginIndex(indexDef, nil))
	require.False(t, isAlterAffectedPluginIndex(nil, []string{"idx_vec"}))
}

func TestReplaceRefChildTableID(t *testing.T) {
	t.Run("replace altered child and preserve siblings", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Equal(t, []uint64{10, 21, 30}, canonicalRefChildTableIDs(constraintDef))
	})

	t.Run("do not invent a missing child reference", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 30}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Equal(t, []uint64{10, 30}, canonicalRefChildTableIDs(constraintDef))
	})

	t.Run("canonicalize duplicate definitions and table ids", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 21}},
			&engine.RefChildTableDef{Tables: []uint64{20, 30, 0}},
			&engine.RefChildTableDef{Tables: []uint64{0}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{10, 21, 30, 0},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})

	t.Run("keep an empty reference list empty", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Len(t, constraintDef.Cts, 1)
		require.Empty(t, canonicalRefChildTableIDs(constraintDef))
	})
}

func TestTruncateRefChildTableIDReplacementCanonicalizesLegacyState(t *testing.T) {
	constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.RefChildTableDef{Tables: []uint64{0, 10, 20}},
		&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		&engine.RefChildTableDef{Tables: []uint64{0}},
	}}

	replaceRefChildTableID(constraintDef, 20, 21)

	require.Len(t, constraintDef.Cts, 1)
	require.Equal(
		t,
		[]uint64{0, 10, 21, 30},
		constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
	)
}

func TestCanonicalRefChildTableIDMutations(t *testing.T) {
	t.Run("add merges definitions and deduplicates sentinel", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{0, 10}},
			&engine.RefChildTableDef{Tables: []uint64{10, 20}},
		}}

		addRefChildTableIDs(constraintDef, []uint64{0, 20, 30})

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{0, 10, 20, 30},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})

	t.Run("remove deletes every duplicate and keeps other ids", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{0, 10, 20}},
			&engine.RefChildTableDef{Tables: []uint64{10, 30}},
		}}

		removeRefChildTableID(constraintDef, 10)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{0, 20, 30},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})
}

func TestReconcileParentRefChildTableID(t *testing.T) {
	t.Run("replace child in existing reverse reference", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		}}
		reconcileParentRefChildTableID(constraintDef, 20, 21)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{10, 21, 30},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})

	t.Run("restore reverse reference removed while dropping old child", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{}
		reconcileParentRefChildTableID(constraintDef, 20, 21)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{21},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})
}

func TestAlterCopyAutoIncrementCleanupDiscardsTrackedReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = context.Background()
	_, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnOperator = txnOp

	cleanupErr := errors.New("discard failed")
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(11), txnOp).Return(cleanupErr)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(12), txnOp).Return(nil)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	cleanup := newAlterCopyAutoIncrementCleanup(&Compile{proc: proc})
	cleanup.track(11)
	cleanup.track(11)
	cleanup.track(12)
	originalErr := errors.New("statement failed")
	statementErr := originalErr
	cleanup.finish(&statementErr)

	require.ErrorIs(t, statementErr, originalErr)
	require.ErrorIs(t, statementErr, cleanupErr)
}

type alterCopyInsertSpyExecutor struct {
	insertSQL    string
	insertErr    error
	insertCtx    context.Context
	insertOption executor.StatementOption
	results      map[string]executor.Result
	errs         map[string]error
	onExec       func(context.Context, string)
	executedSQLs []string
}

func TestReconcileAlterCopyAutoIncrementUsesStableIdentityAndSafeBounds(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
	renamedMaxSQL := "select cast(coalesce(max(case when `renamed_id` > 0 then `renamed_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	reusedMaxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		sourceOffsetSQL: newTableCloneOffsetResult(t, resultMP, 0, 500),
		renamedMaxSQL:   newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
		reusedMaxSQL:    newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	srcDef := &plan.TableDef{
		TblId: 1,
		Cols: []*plan.ColDef{
			{ColId: 10, Name: "id", Typ: autoType},
			{ColId: 11, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	copyDef := &plan.TableDef{
		TblId:          2,
		Name:           "dept_copy",
		AutoIncrOffset: 99,
		Cols: []*plan.ColDef{
			{ColId: 12, Name: "id", Typ: autoType},
			{ColId: 10, Name: "renamed_id", Typ: autoType},
			{ColId: 11, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	gomock.InOrder(
		autoSvc.EXPECT().SetOffset(c.proc.Ctx, copyDef.TblId, "id", uint64(99), c.proc.GetTxnOperator()),
		autoSvc.EXPECT().SetOffset(c.proc.Ctx, copyDef.TblId, "renamed_id", uint64(500), c.proc.GetTxnOperator()),
		autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), copyDef.TblId, c.proc.GetTxnOperator()).Return(nil),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	cleanup := newAlterCopyAutoIncrementCleanup(c)
	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, cleanup,
	))
	require.Equal(t, []string{sourceOffsetSQL, reusedMaxSQL, renamedMaxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB(), "all internal SQL results must be closed")
	laterErr := errors.New("later ALTER COPY step failed")
	cleanup.finish(&laterErr)
	require.ErrorContains(t, laterErr, "later ALTER COPY step failed")
}

func TestReconcileAlterCopyAutoIncrementSkipsHiddenAndRejectsNarrowedOverflow(t *testing.T) {
	t.Run("hidden only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		spyExec := &alterCopyInsertSpyExecutor{}
		c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
		copyDef := &plan.TableDef{
			TblId: 2,
			Name:  "dept_copy",
			Cols: []*plan.ColDef{{
				ColId: 1, Name: catalog.FakePrimaryKeyColName, Hidden: true,
				Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		}
		copyRel := mock_frontend.NewMockRelation(ctrl)
		autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
		autoSvc.EXPECT().SetOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
		incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

		require.NoError(t, c.reconcileAlterCopyAutoIncrement(
			"test", &plan.TableDef{}, copyDef, copyRel, newAlterCopyAutoIncrementCleanup(c),
		))
		require.Empty(t, spyExec.executedSQLs)
	})

	t.Run("source offset exceeds narrowed type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		resultMP := mpool.MustNewZero()
		sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
		maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
		spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
			sourceOffsetSQL: newTableCloneOffsetResult(t, resultMP, 0, 300),
			maxSQL:          newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
		}}
		c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
		srcDef := &plan.TableDef{TblId: 1, Cols: []*plan.ColDef{{
			ColId: 10, Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}}}
		copyDef := &plan.TableDef{TblId: 2, Name: "dept_copy", Cols: []*plan.ColDef{{
			ColId: 10, Name: "id", Typ: plan.Type{Id: int32(types.T_uint8), AutoIncr: true},
		}}}
		copyRel := mock_frontend.NewMockRelation(ctrl)
		copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
		autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
		autoSvc.EXPECT().SetOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
		incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

		err := c.reconcileAlterCopyAutoIncrement(
			"test", srcDef, copyDef, copyRel, newAlterCopyAutoIncrementCleanup(c),
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		require.Zero(t, resultMP.CurrNB(), "all internal SQL results must be closed")
	})
}

func TestReconcileAlterCopyAutoIncrementStopsAfterCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	firstMaxSQL := "select cast(coalesce(max(case when `first` > 0 then `first` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		firstMaxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)

	copyDef := &plan.TableDef{
		TblId:          2,
		Name:           "dept_copy",
		AutoIncrOffset: 99,
		Cols: []*plan.ColDef{
			{ColId: 20, Name: "first", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
			{ColId: 21, Name: "second", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(ctx, copyDef.TblId, "first", uint64(99), c.proc.GetTxnOperator()).DoAndReturn(
		func(context.Context, uint64, string, uint64, client.TxnOperator) error {
			cancel()
			return nil
		},
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	err := c.reconcileAlterCopyAutoIncrement(
		"test", &plan.TableDef{}, copyDef, copyRel, newAlterCopyAutoIncrementCleanup(c),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{firstMaxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
}

const (
	alterCopyTestPkNullCheckSQL      = "SELECT `col4` FROM `test`.`dept` WHERE `col4` IS NULL LIMIT 1"
	alterCopyTestPkDuplicateCheckSQL = "SELECT `col4` FROM `test`.`dept` GROUP BY `col4` HAVING count(*) > 1 LIMIT 1"
)

func (e *alterCopyInsertSpyExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	e.executedSQLs = append(e.executedSQLs, sql)
	if e.onExec != nil {
		e.onExec(ctx, sql)
	}
	if sql == e.insertSQL {
		e.insertCtx = ctx
		e.insertOption = opts.StatementOption()
		return executor.Result{}, e.insertErr
	}
	if e.errs != nil {
		if err, ok := e.errs[sql]; ok {
			return executor.Result{}, err
		}
	}
	if e.results != nil {
		if res, ok := e.results[sql]; ok {
			return res, nil
		}
	}
	return executor.Result{}, nil
}

func (e *alterCopyInsertSpyExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

func TestScopeAlterTableCopyInsertTmpDataPipelineFlush(t *testing.T) {
	insertErr := errors.New("stop after insert-copy")

	for _, tc := range []struct {
		name               string
		skipPkDedup        bool
		nilCtxBeforeInsert bool
		wantPipelineFlush  bool
	}{
		{
			name:               "skip pk dedup false",
			skipPkDedup:        false,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  false,
		},
		{
			name:               "skip pk dedup true",
			skipPkDedup:        true,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  true,
		},
		{
			name:               "skip pk dedup true with nil proc ctx",
			skipPkDedup:        true,
			nilCtxBeforeInsert: true,
			wantPipelineFlush:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.Buf = buffer.New()
			proc.Base.SessionInfo.TimeZone = time.Local

			serviceID := "alter-copy-pipeline-flush-" + tc.name
			lockSvc := mock_lock.NewMockLockService(ctrl)
			lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
			proc.Base.LockService = lockSvc
			require.Equal(t, serviceID, proc.GetService())

			const accountID = catalog.System_Account
			ctx := defines.AttachAccountId(context.Background(), accountID)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)

			txnCli, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp

			tableDef := &plan.TableDef{
				TblId: 1,
				Name:  "dept",
			}
			copyTableDef := &plan.TableDef{
				TblId: 2,
				Name:  "dept_copy",
			}
			alterTable := &plan2.AlterTable{
				Database:          "test",
				TableDef:          tableDef,
				CopyTableDef:      copyTableDef,
				CreateTmpTableSql: "create table dept_copy",
				InsertTmpDataSql:  "insert into dept_copy select * from dept",
				Options:           &plan2.AlterCopyOpt{SkipPkDedup: tc.skipPkDedup},
			}
			s := &Scope{
				Magic: AlterTable,
				Plan: &plan.Plan{
					Plan: &plan2.Plan_Ddl{
						Ddl: &plan2.DataDefinition{
							DdlType: plan2.DataDefinition_ALTER_TABLE,
							Definition: &plan2.DataDefinition_AlterTable{
								AlterTable: alterTable,
							},
						},
					},
				},
			}

			originRel := mock_frontend.NewMockRelation(ctrl)
			originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

			copyRel := mock_frontend.NewMockRelation(ctrl)
			if tc.nilCtxBeforeInsert {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef {
					proc.Ctx = nil
					return &plan.TableDef{
						TblId: 2,
						Name:  "dept_copy",
					}
				})
			} else {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
					TblId: 2,
					Name:  "dept_copy",
				}).AnyTimes()
			}

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
			mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

			spyExec := &alterCopyInsertSpyExecutor{
				insertSQL: alterTable.InsertTmpDataSql,
				insertErr: insertErr,
			}
			rt := moruntime.DefaultRuntime()
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
			moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

			c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
			c.pn = s.Plan
			origCtx := proc.Ctx

			err := s.AlterTableCopy(c)
			require.ErrorIs(t, err, insertErr)
			require.NotNil(t, spyExec.insertCtx)
			assert.Equal(t, tc.wantPipelineFlush, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)

			insertAccountID, err := defines.GetAccountId(spyExec.insertCtx)
			require.NoError(t, err)
			assert.Equal(t, accountID, insertAccountID)

			if tc.nilCtxBeforeInsert {
				require.NotNil(t, proc.Ctx)
				require.NotSame(t, spyExec.insertCtx, proc.Ctx)
				require.Same(t, proc.GetTopContext(), proc.Ctx)

				restoredAccountID, err := defines.GetAccountId(proc.Ctx)
				require.NoError(t, err)
				assert.Equal(t, accountID, restoredAccountID)
			} else {
				require.Same(t, origCtx, proc.Ctx)
			}
			assert.NotEqual(t, true, proc.Ctx.Value(ioutil.PipelineFlushKey))

			if tc.skipPkDedup {
				require.Same(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
			} else {
				require.Nil(t, spyExec.insertOption.AlterCopyDedupOpt())
			}
		})
	}
}

func TestGetAlterCopyPkPrecheck(t *testing.T) {
	for _, tc := range []struct {
		name             string
		tableDef         *plan.TableDef
		copyTableDef     *plan.TableDef
		skipPkDedup      bool
		wantCols         []string
		wantCheckNotNull bool
	}{
		{
			name: "add pk on nullable original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			wantCols:         []string{"col4"},
			wantCheckNotNull: true,
		},
		{
			name: "add pk on not null original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			wantCols: []string{"col4"},
		},
		{
			name: "static skip pk dedup needs no precheck",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			skipPkDedup: true,
		},
		{
			name: "pk column is not copied from original table",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "new_col", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "new_col", Names: []string{"new_col"}},
			},
		},
		{
			name: "pk column type change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 16}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
		{
			name: "pk column width change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 32}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qry := &plan2.AlterTable{
				TableDef:     tc.tableDef,
				CopyTableDef: tc.copyTableDef,
				Options: &plan2.AlterCopyOpt{
					SkipPkDedup:     tc.skipPkDedup,
					TargetTableName: "dept_copy",
				},
			}
			pkCols, checkNotNull := getAlterCopyPkPrecheck(qry)
			assert.Equal(t, tc.wantCols, pkCols)
			assert.Equal(t, tc.wantCheckNotNull, checkNotNull)
		})
	}
}

func TestScopeAlterTableCopyPrecheckPrimaryKeyThenSkipDedup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-pk-precheck"
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc
	require.Equal(t, serviceID, proc.GetService())

	const accountID = catalog.System_Account
	ctx := defines.AttachAccountId(context.Background(), accountID)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	tableDef := &plan.TableDef{
		TblId: 1,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
	}
	copyTableDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
		Cols: []*plan.ColDef{
			{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
	}
	alterTable := &plan2.AlterTable{
		Database:          "test",
		TableDef:          tableDef,
		CopyTableDef:      copyTableDef,
		CreateTmpTableSql: "create table dept_copy",
		InsertTmpDataSql:  "insert into dept_copy select * from dept",
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
	}
	s := &Scope{
		Magic: AlterTable,
		Plan: &plan.Plan{
			Plan: &plan2.Plan_Ddl{
				Ddl: &plan2.DataDefinition{
					DdlType: plan2.DataDefinition_ALTER_TABLE,
					Definition: &plan2.DataDefinition_AlterTable{
						AlterTable: alterTable,
					},
				},
			},
		},
	}

	originRel := mock_frontend.NewMockRelation(ctrl)
	originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()

	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(copyTableDef).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
	mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

	insertErr := errors.New("stop after insert-copy")
	spyExec := &alterCopyInsertSpyExecutor{
		insertSQL: alterTable.InsertTmpDataSql,
		insertErr: insertErr,
	}
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = s.Plan

	err := s.AlterTableCopy(c)
	require.ErrorIs(t, err, insertErr)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotNil(t, spyExec.insertCtx)
	assert.Equal(t, true, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)
	require.NotSame(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
	require.True(t, spyExec.insertOption.AlterCopyDedupOpt().SkipPkDedup)
	require.Equal(t, alterTable.Options.TargetTableName, spyExec.insertOption.AlterCopyDedupOpt().TargetTableName)
	assert.Equal(t, []string{
		databranchutils.LineageOwnerPublicationLockSQL(),
		alterDataBranchParticipationSQL(1),
		alterDataBranchHistoricalSnapshotSourceSQL("", "test", "dept", 1),
		alterDataBranchHistoricalPitrSourceSQL("", "test", "dept", 1),
		alterDataBranchHistoricalSnapshotSourceProbeSQL("", "test", "dept", 1, false),
		alterDataBranchHistoricalPitrSourceProbeSQL("", "test", "dept", 1, false),
		alterTable.CreateTmpTableSql,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterTable.InsertTmpDataSql,
	}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsNull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkNullCheckSQL: newAlterCopyConstNullResult(c.proc.Mp(), types.T_int32.ToType()),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkDuplicateCheckSQL: newAlterCopyFixedResult(t, c.proc.Mp(), types.T_int32.ToType(), []int32{7}),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL, alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupCanSkipNullCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	alterTable.TableDef.Cols[0].NotNull = true
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, opt)
	assert.True(t, opt.SkipPkDedup)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, alterTable.Options, opt)
	assert.Equal(t, []string{alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupDoesNotMutatePlanOption(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	firstOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, firstOpt)
	require.True(t, firstOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)

	secondOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, secondOpt)
	require.True(t, secondOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, firstOpt, secondOpt)

	assert.Equal(t, []string{
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
	}, spyExec.executedSQLs)
}

func testAlterCopyAddPrimaryKeyPlan() *plan2.AlterTable {
	return &plan2.AlterTable{
		Database: "test",
		TableDef: &plan.TableDef{
			Name: "dept",
			Cols: []*plan.ColDef{
				{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		CopyTableDef: &plan.TableDef{
			Name: "dept_copy",
			Cols: []*plan.ColDef{
				{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
		},
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
	}
}

func newAlterCopyPrecheckCompile(t *testing.T, ctrl *gomock.Controller, exec executor.SQLExecutor) *Compile {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-precheck-" + t.Name()
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	eng := mock_frontend.NewMockEngine(ctrl)
	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
			},
		},
	}
	return c
}

func newAlterCopyConstNullResult(mp *mpool.MPool, typ types.Type) executor.Result {
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	bat.Vecs[0] = vector.NewConstNull(typ, 1, mp)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func newAlterCopyFixedResult[T any](t *testing.T, mp *mpool.MPool, typ types.Type, values []T) executor.Result {
	memRes := executor.NewMemResult([]types.Type{typ}, mp)
	memRes.NewBatchWithRowCount(len(values))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, values))
	return memRes.GetResult()
}

func TestLoadAlterDataBranchHistoricalSourcesUsesPitrCatalogType(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	results := map[string]executor.Result{
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(
			t, mp, nil, nil, nil, nil, nil, nil,
		),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp,
			[]string{"database", "table"},
			[]string{"tenant", "tenant"},
			[]string{"db_hour", "db_day"},
			[]string{"", "tbl"},
			[]uint64{101, 102},
			[]uint8{1, 100},
			[]string{"h", "d"},
		),
	}

	sources, err := loadAlterDataBranchHistoricalSourcesWithQuery(
		func(sql string) (executor.Result, error) {
			res, ok := results[sql]
			require.True(t, ok, "unexpected lineage source query: %s", sql)
			return res, nil
		},
		now,
	)
	require.NoError(t, err)
	require.Equal(t, []databranchutils.HistoricalSource{
		{
			Level:        "database",
			AccountName:  "tenant",
			DatabaseName: "db_hour",
			ObjectID:     101,
			OldestTS:     now.Add(-time.Hour).UnixNano(),
		},
		{
			Level:        "table",
			AccountName:  "tenant",
			DatabaseName: "db_day",
			TableName:    "tbl",
			ObjectID:     102,
			OldestTS:     now.AddDate(0, 0, -100).UnixNano(),
		},
	}, sources)
}

func TestCompactExpiredAlterDataBranchLineage(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	const (
		metadataSQL = "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata for update"
		edgeSQL     = "select sname, ts, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind = 'branch'"
		snapshotSQL = "select ts, level, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind = 'user'"
		pitrSQL     = "select level, account_name, database_name, table_name, obj_id, pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_status = 1"
	)

	for _, tc := range []struct {
		name          string
		pitrLength    uint8
		wantDeletes   bool
		wantSQLSuffix []string
	}{
		{
			name:        "expired PITR releases ALTER edge",
			pitrLength:  24,
			wantDeletes: true,
			wantSQLSuffix: []string{
				"delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')",
				"delete from mo_catalog.mo_branch_metadata where table_id in (2) and (level = 'alter' or level like 'alter:%')",
			},
		},
		{
			name:        "active PITR retains ALTER edge",
			pitrLength:  72,
			wantDeletes: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			mp := c.proc.Mp()

			spyExec.results[metadataSQL] = newAlterLineageMetadataResult(
				t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
				[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
			)
			spyExec.results[edgeSQL] = newAlterLineageEdgeResult(
				t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
				[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
			)
			spyExec.results[snapshotSQL] = newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil)
			spyExec.results[pitrSQL] = newAlterLineagePitrSourceResult(
				t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
				[]uint64{1}, []uint8{tc.pitrLength}, []string{"h"},
			)

			require.NoError(t, c.compactExpiredAlterDataBranchLineage(now))
			want := []string{metadataSQL, edgeSQL, snapshotSQL, pitrSQL}
			if tc.wantDeletes {
				want = append(want, tc.wantSQLSuffix...)
			}
			require.Equal(t, want, spyExec.executedSQLs)
		})
	}
}

func TestCompactExpiredAlterDataBranchLineageWithExecutor(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()

	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s for update",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	results := map[string]executor.Result{
		metadataSQL: newAlterLineageMetadataResult(
			t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
			[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
		),
		alterDataBranchLineageEdgeSQL(): newAlterLineageEdgeResult(
			t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
			[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
		),
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
			[]uint64{1}, []uint8{24}, []string{"h"},
		),
	}
	var executed []string
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		return results[sql], nil
	})

	require.NoError(t, compactExpiredAlterDataBranchLineageWithExecutor(context.Background(), sqlExecutor, now))
	require.Equal(t, []string{
		metadataSQL,
		alterDataBranchLineageEdgeSQL(),
		alterDataBranchSnapshotSourceSQL(),
		alterDataBranchPitrSourceSQL(),
		"delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (2) and (level = 'alter' or level like 'alter:%')",
	}, executed)
}

type lineageGCDeadlineExecutor struct {
	deadline time.Time
}

func (e *lineageGCDeadlineExecutor) Exec(
	context.Context, string, executor.Options,
) (executor.Result, error) {
	return executor.Result{}, nil
}

func (e *lineageGCDeadlineExecutor) ExecTxn(
	ctx context.Context,
	_ func(executor.TxnExecutor) error,
	_ executor.Options,
) error {
	e.deadline, _ = ctx.Deadline()
	return nil
}

func TestDataBranchLineageGCExecutorSetsDeadline(t *testing.T) {
	spyExec := &lineageGCDeadlineExecutor{}
	started := time.Now()
	require.NoError(t, DataBranchLineageGCExecutor(spyExec)(context.Background(), nil))
	require.False(t, spyExec.deadline.IsZero())
	require.WithinDuration(t, started.Add(dataBranchLineageGCTimeout), spyExec.deadline, time.Second)

	parentDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()
	require.NoError(t, DataBranchLineageGCExecutor(spyExec)(ctx, nil))
	require.WithinDuration(t, parentDeadline, spyExec.deadline, time.Second)
}

func TestCompactExpiredAlterDataBranchLineageWithExecutorPropagatesDeleteError(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s for update",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	results := map[string]executor.Result{
		metadataSQL: newAlterLineageMetadataResult(
			t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
			[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
		),
		alterDataBranchLineageEdgeSQL(): newAlterLineageEdgeResult(
			t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
			[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
		),
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
			[]uint64{1}, []uint8{24}, []string{"h"},
		),
	}
	wantErr := errors.New("delete failed")
	snapshotDeleteSQL := "delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')"
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == snapshotDeleteSQL {
			return executor.Result{}, wantErr
		}
		return results[sql], nil
	})

	require.ErrorIs(t,
		compactExpiredAlterDataBranchLineageWithExecutor(context.Background(), sqlExecutor, now),
		wantErr,
	)
}

func newAlterLineageMetadataResult(
	t *testing.T,
	mp *mpool.MPool,
	tableIDs, parentIDs []uint64,
	cloneTSs []int64,
	creators []uint64,
	levels []string,
	deleted []bool,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType(),
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_bool.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(tableIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, tableIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, parentIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 2, cloneTSs))
	require.NoError(t, executor.AppendFixedRows(memRes, 3, creators))
	require.NoError(t, executor.AppendStringRows(memRes, 4, levels))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, deleted))
	return memRes.GetResult()
}

func newAlterLineageEdgeResult(
	t *testing.T,
	mp *mpool.MPool,
	names []string,
	cloneTSs []int64,
	accounts, databases, tables []string,
	objectIDs []uint64,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(names))
	require.NoError(t, executor.AppendStringRows(memRes, 0, names))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, cloneTSs))
	require.NoError(t, executor.AppendStringRows(memRes, 2, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 3, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 4, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, objectIDs))
	return memRes.GetResult()
}

func newAlterLineageSnapshotSourceResult(
	t *testing.T,
	mp *mpool.MPool,
	cloneTSs []int64,
	levels, accounts, databases, tables []string,
	objectIDs []uint64,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_int64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(cloneTSs))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, cloneTSs))
	require.NoError(t, executor.AppendStringRows(memRes, 1, levels))
	require.NoError(t, executor.AppendStringRows(memRes, 2, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 3, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 4, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, objectIDs))
	return memRes.GetResult()
}

func newAlterLineagePitrSourceResult(
	t *testing.T,
	mp *mpool.MPool,
	levels, accounts, databases, tables []string,
	objectIDs []uint64,
	lengths []uint8,
	units []string,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_uint8.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(levels))
	require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
	require.NoError(t, executor.AppendStringRows(memRes, 1, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 2, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 3, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 4, objectIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, lengths))
	require.NoError(t, executor.AppendStringRows(memRes, 6, units))
	return memRes.GetResult()
}

func TestScope_AlterTableInplace(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{
				ColId: 0,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 1,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	alterTable := &plan2.AlterTable{
		Database: "test",
		TableDef: tableDef,
		Actions: []*plan2.AlterTable_Action{
			{
				Action: &plan2.AlterTable_Action_AddIndex{
					AddIndex: &plan2.AlterTableAddIndex{
						DbName:                "test",
						TableName:             "dept",
						OriginTablePrimaryKey: "deptno",
						IndexTableExist:       true,
						IndexInfo: &plan2.CreateTable{
							TableDef: &plan.TableDef{
								Indexes: []*plan.IndexDef{
									{
										IndexName:      "idx",
										Parts:          []string{"dname", "__mo_alias_deptno"},
										Unique:         false,
										IndexTableName: "__mo_index_secondary_0193d918",
										TableExist:     true,
									},
								},
							},
							IndexTables: []*plan.TableDef{
								{
									Name: "__mo_index_secondary_0193d918-3e7b",
									Cols: []*plan.ColDef{
										{
											Name: "__mo_index_idx_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          61,
												NotNullable: false,
												AutoIncr:    false,
												Width:       65535,
												Scale:       0,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
										{
											Name: "__mo_index_pri_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          27,
												NotNullable: false,
												AutoIncr:    false,
												Width:       32,
												Scale:       -1,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
									},
									Pkey: &plan2.PrimaryKeyDef{
										PkeyColName: "__mo_index_idx_col",
										Names:       []string{"__mo_index_idx_col"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock mo_tables", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})
}

func TestScope_AlterTableCopy(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{
				ColId: 0,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 1,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	copyTableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept_copy_0193dcb4-4c07-77d8",
		Cols: []*plan.ColDef{
			{
				ColId: 1,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       20,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 3,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId:  4,
				Name:   "__mo_rowid",
				Hidden: true,
				Alg:    plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          101,
					NotNullable: true,
					AutoIncr:    false,
					Width:       0,
					Scale:       0,
					Table:       "dept",
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		TableType: "r",
		Createsql: `create table dept (deptno int unsigned auto_increment comment "部门编号", dname varchar(15) comment "部门名称", loc varchar(50) comment "部门所在位置", index idxloc (loc), primary key (deptno)) comment = '部门表'`,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	alterTable := &plan2.AlterTable{
		Database:     "test",
		TableDef:     tableDef,
		CopyTableDef: copyTableDef,
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})
}
