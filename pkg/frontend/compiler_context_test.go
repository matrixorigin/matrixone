// Copyright 2024 Matrix Origin
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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

var _ plan2.ViewDependencyIdentityResolver = (*TxnCompilerContext)(nil)

func TestSubscriptionMetasFromSubInfos(t *testing.T) {
	metas, err := subscriptionMetasFromSubInfos(context.Background(), []*pubsub.SubInfo{
		{
			SubName:        "Zulu",
			PubAccountId:   42,
			PubAccountName: "publisher",
			PubName:        "pub_z",
			PubDbName:      "db_z",
			PubTables:      "t1,t2",
			Status:         pubsub.SubStatusNormal,
		},
		nil,
		{SubName: "deleted", Status: pubsub.SubStatusDeleted},
		{SubName: "unauthorized", Status: pubsub.SubStatusNotAuthorized},
		{SubName: "", Status: pubsub.SubStatusNormal},
		{
			SubName:        "alpha",
			PubAccountId:   7,
			PubAccountName: "publisher_b",
			PubName:        "pub_a",
			PubDbName:      "db_a",
			PubTables:      pubsub.TableAll,
			Status:         pubsub.SubStatusNormal,
		},
	})
	require.NoError(t, err)

	require.Equal(t, []*pbplan.SubscriptionMeta{
		{
			Name:        "pub_a",
			AccountId:   7,
			DbName:      "db_a",
			AccountName: "publisher_b",
			SubName:     "alpha",
			Tables:      pubsub.TableAll,
		},
		{
			Name:        "pub_z",
			AccountId:   42,
			DbName:      "db_z",
			AccountName: "publisher",
			SubName:     "Zulu",
			Tables:      "t1,t2",
		},
	}, metas)
}

func TestSubscriptionMetadataEnumerationObservesCancellation(t *testing.T) {
	wantErr := errors.New("stop subscription metadata enumeration")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(wantErr)

	metas, err := subscriptionMetasFromSubInfos(ctx, []*pubsub.SubInfo{{
		SubName: "sub_db",
		Status:  pubsub.SubStatusNormal,
	}})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, metas)

	bh := &backgroundExecTest{}
	bh.init()
	visible, err := getVisibleSubscriptionMetadata(ctx, bh, []*pbplan.SubscriptionMeta{{
		SubName: "sub_db",
	}}, defines.MORPCVersion41)
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, visible)
	require.Empty(t, bh.executedSQLs,
		"canceled metadata enumeration must stop before constructing and executing the visibility query")

	result := &MysqlResultSet{}
	result.AddRow([]interface{}{})
	_, err = extractSubInfosFromExecResult(ctx, []ExecResult{result})
	require.ErrorIs(t, err, wantErr)
	_, err = extractSubInfosFromExecResultOld(ctx, []ExecResult{result})
	require.ErrorIs(t, err, wantErr)
}

func TestActiveSubscriptionMetadataCandidatesAreBoundedAtCatalogQuery(t *testing.T) {
	columnCheckSQL := "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"

	for _, test := range []struct {
		name          string
		modernCatalog bool
		maxCandidates int
		rowCount      int
		wantError     bool
	}{
		{name: "exact boundary", modernCatalog: true, maxCandidates: 256, rowCount: 256},
		{name: "overflow sentinel", modernCatalog: true, maxCandidates: 256, rowCount: 257, wantError: true},
		{name: "no remaining budget", modernCatalog: true, maxCandidates: 0, rowCount: 1, wantError: true},
		{name: "rolling upgrade catalog", maxCandidates: 1, rowCount: 2, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			columnExists := &MysqlResultSet{}
			candidateSQL := getSubsSqlOld
			if test.modernCatalog {
				columnExists.AddRow([]interface{}{int64(1)})
				candidateSQL = getSubsSql
			}
			candidateSQL += fmt.Sprintf(
				" and sub_account_id = 7 and status = 0 and sub_name is not null and sub_name <> '' limit %d",
				test.maxCandidates+1,
			)
			candidates := subscriptionCandidateResult(test.rowCount, test.modernCatalog)
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[columnCheckSQL] = columnExists
			bh.sql2result[candidateSQL] = candidates

			ctx := defines.AttachAccountId(context.Background(), 7)
			got, err := getActiveSubInfosFromSubBounded(ctx, bh, test.maxCandidates)
			if test.wantError {
				require.ErrorContains(t, err, fmt.Sprintf(
					"candidate enumeration exceeds planning budget of %d branches", test.maxCandidates))
				require.Nil(t, got, "overflow must not expose the bounded prefix as partial metadata")
			} else {
				require.NoError(t, err)
				require.Len(t, got, test.maxCandidates)
			}
			require.Equal(t, []string{columnCheckSQL, candidateSQL}, bh.executedSQLs)
			require.Equal(t, []uint32{catalog.System_Account, catalog.System_Account}, bh.executionAccountIDs)
			require.NotContains(t, candidateSQL, " IN (",
				"catalog admission must happen before constructing the visibility-name list")

			if !test.wantError {
				metas, convertErr := subscriptionMetasFromSubInfos(ctx, got)
				require.NoError(t, convertErr)
				names := make([]string, 0, len(metas))
				for _, meta := range metas {
					names = append(names, escapeSQLString(meta.SubName))
				}
				visibilitySQL := subscriptionMetadataVisibilitySQL(
					strings.Join(names, ","), defines.MORPCVersion41,
				)
				bh.sql2result[visibilitySQL] = &MysqlResultSet{}
				visible, visibilityErr := getVisibleSubscriptionMetadata(
					ctx, bh, metas, defines.MORPCVersion41,
				)
				require.NoError(t, visibilityErr)
				require.Empty(t, visible)
				require.Len(t, bh.executedSQLs, 3)
				require.Equal(t, visibilitySQL, bh.executedSQLs[2])
				require.Equal(t, test.maxCandidates, strings.Count(visibilitySQL, "'sub_"),
					"visibility SQL must encode only the admitted bounded candidate set")
			}
		})
	}
}

func subscriptionCandidateResult(rowCount int, modernCatalog bool) *MysqlResultSet {
	result := &MysqlResultSet{}
	columnCount := 10
	if modernCatalog {
		columnCount = 12
	}
	for i := 0; i < columnCount; i++ {
		column := &MysqlColumn{}
		column.SetName(fmt.Sprintf("column_%d", i))
		result.AddColumn(column)
	}
	for i := 0; i < rowCount; i++ {
		row := []interface{}{
			int64(7), "subscriber", fmt.Sprintf("sub_%03d", i), "2026-09-02",
			int64(42), "publisher", "publication", "database", "*",
			"2026-09-02", "", int64(pubsub.SubStatusNormal),
		}
		if !modernCatalog {
			row = []interface{}{
				int64(7), fmt.Sprintf("sub_%03d", i), "2026-09-02", "publisher",
				"publication", "database", "*", "2026-09-02", "",
				int64(pubsub.SubStatusNormal),
			}
		}
		result.AddRow(row)
	}
	return result
}

func subscriptionPublisherAccountResult(rows ...[]interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	for _, name := range []string{"account_id", "account_name"} {
		column := &MysqlColumn{}
		column.SetName(name)
		result.AddColumn(column)
	}
	for _, row := range rows {
		result.AddRow(row)
	}
	return result
}

func TestLegacySubscriptionMetadataResolvesPublisherAccountAtCatalogBoundary(t *testing.T) {
	columnCheckSQL := "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
	candidateSQL := getSubsSqlOld +
		" and sub_account_id = 7 and status = 0 and sub_name is not null and sub_name <> '' limit 2"
	lookupSQL := subscriptionPublisherAccountLookupSQL([]string{"publisher"})

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[columnCheckSQL] = &MysqlResultSet{}
	bh.sql2result[candidateSQL] = subscriptionCandidateResult(1, false)
	bh.sql2result[lookupSQL] = subscriptionPublisherAccountResult(
		[]interface{}{int64(42), "publisher"},
	)

	ctx := defines.AttachAccountId(context.Background(), 7)
	got, err := getActiveSubInfosFromSubBounded(ctx, bh, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int32(42), got[0].PubAccountId)

	metas, err := subscriptionMetasFromSubInfos(ctx, got)
	require.NoError(t, err)
	require.Len(t, metas, 1)
	require.Equal(t, int32(42), metas[0].AccountId,
		"the resolved legacy publisher identity must reach the planner metadata")
	require.Equal(t, []string{columnCheckSQL, candidateSQL, lookupSQL}, bh.executedSQLs)
	require.Equal(t, []uint32{
		catalog.System_Account, catalog.System_Account, catalog.System_Account,
	}, bh.executionAccountIDs)
}

func TestLegacySubscriptionMetadataPublisherResolutionFailsClosed(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	for _, test := range []struct {
		name       string
		lookupRows [][]interface{}
		wantError  string
	}{
		{
			name:      "missing account",
			wantError: "cannot resolve publication account publisher",
		},
		{
			name: "zero id for non-system account",
			lookupRows: [][]interface{}{
				{int64(0), "publisher"},
			},
			wantError: "invalid publication account id 0",
		},
		{
			name: "duplicate account identity",
			lookupRows: [][]interface{}{
				{int64(42), "publisher"},
				{int64(43), "publisher"},
			},
			wantError: "ambiguous publication account publisher",
		},
		{
			name: "unexpected account identity",
			lookupRows: [][]interface{}{
				{int64(42), "other"},
			},
			wantError: "unexpected publication account other",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lookupSQL := subscriptionPublisherAccountLookupSQL([]string{"publisher"})
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[lookupSQL] = subscriptionPublisherAccountResult(test.lookupRows...)
			subInfos := []*pubsub.SubInfo{{
				SubName: "sub_db", PubAccountName: "publisher",
			}}

			err := resolveMissingSubscriptionPublisherAccountIDs(ctx, bh, subInfos)
			require.ErrorContains(t, err, test.wantError)
			require.Equal(t, int32(0), subInfos[0].PubAccountId,
				"failed resolution must not leave a partially usable publisher identity")
			require.Equal(t, []string{lookupSQL}, bh.executedSQLs)
			require.Equal(t, []uint32{catalog.System_Account}, bh.executionAccountIDs)
		})
	}
}

func TestLegacySubscriptionMetadataPublisherResolutionIsBatched(t *testing.T) {
	const publisherCount = subscriptionPublisherAccountLookupBatchSize + 1
	subInfos := make([]*pubsub.SubInfo, 0, publisherCount+1)
	accountNames := make([]string, 0, publisherCount)
	for i := 0; i < publisherCount; i++ {
		accountName := fmt.Sprintf("publisher_%03d", i)
		accountNames = append(accountNames, accountName)
		subInfos = append(subInfos, &pubsub.SubInfo{
			SubName: fmt.Sprintf("sub_%03d", i), PubAccountName: accountName,
		})
	}
	// The real system publisher legitimately owns account id 0 and needs no
	// catalog lookup. It also proves that zero is not used as an unresolved
	// sentinel without considering the publisher name.
	subInfos = append(subInfos, &pubsub.SubInfo{
		SubName: "sys_sub", PubAccountName: sysAccountName,
	})

	firstBatch := accountNames[:subscriptionPublisherAccountLookupBatchSize]
	secondBatch := accountNames[subscriptionPublisherAccountLookupBatchSize:]
	firstSQL := subscriptionPublisherAccountLookupSQL(firstBatch)
	secondSQL := subscriptionPublisherAccountLookupSQL(secondBatch)
	firstRows := make([][]interface{}, 0, len(firstBatch))
	for i, accountName := range firstBatch {
		firstRows = append(firstRows, []interface{}{int64(i + 1), accountName})
	}
	secondRows := [][]interface{}{{int64(publisherCount), secondBatch[0]}}

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[firstSQL] = subscriptionPublisherAccountResult(firstRows...)
	bh.sql2result[secondSQL] = subscriptionPublisherAccountResult(secondRows...)

	err := resolveMissingSubscriptionPublisherAccountIDs(
		defines.AttachAccountId(context.Background(), 7), bh, subInfos,
	)
	require.NoError(t, err)
	require.Equal(t, []string{firstSQL, secondSQL}, bh.executedSQLs)
	require.Equal(t, []uint32{catalog.System_Account, catalog.System_Account}, bh.executionAccountIDs)
	require.Contains(t, firstSQL, " limit 65")
	require.Contains(t, secondSQL, " limit 2")
	for i := 0; i < publisherCount; i++ {
		require.Equal(t, int32(i+1), subInfos[i].PubAccountId)
	}
	require.Equal(t, int32(sysAccountID), subInfos[publisherCount].PubAccountId)
}

func TestLegacySubscriptionMetadataPublisherResolutionDoesNotPartiallyMutateAcrossBatches(t *testing.T) {
	const publisherCount = subscriptionPublisherAccountLookupBatchSize + 1
	subInfos := make([]*pubsub.SubInfo, 0, publisherCount)
	accountNames := make([]string, 0, publisherCount)
	for i := 0; i < publisherCount; i++ {
		accountName := fmt.Sprintf("publisher_%03d", i)
		accountNames = append(accountNames, accountName)
		subInfos = append(subInfos, &pubsub.SubInfo{
			SubName: fmt.Sprintf("sub_%03d", i), PubAccountName: accountName,
		})
	}

	firstBatch := accountNames[:subscriptionPublisherAccountLookupBatchSize]
	secondBatch := accountNames[subscriptionPublisherAccountLookupBatchSize:]
	firstSQL := subscriptionPublisherAccountLookupSQL(firstBatch)
	secondSQL := subscriptionPublisherAccountLookupSQL(secondBatch)
	firstRows := make([][]interface{}, 0, len(firstBatch))
	for i, accountName := range firstBatch {
		firstRows = append(firstRows, []interface{}{int64(i + 1), accountName})
	}

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[firstSQL] = subscriptionPublisherAccountResult(firstRows...)
	bh.sql2result[secondSQL] = subscriptionPublisherAccountResult()

	err := resolveMissingSubscriptionPublisherAccountIDs(
		defines.AttachAccountId(context.Background(), 7), bh, subInfos,
	)
	require.ErrorContains(t, err, "cannot resolve publication account publisher_064")
	require.Equal(t, []string{firstSQL, secondSQL}, bh.executedSQLs)
	for _, subInfo := range subInfos {
		require.Equal(t, int32(0), subInfo.PubAccountId,
			"a late batch failure must not publish identities from an earlier successful batch")
	}
}

func TestLegacySubscriptionMetadataPublisherResolutionObservesCancellation(t *testing.T) {
	wantErr := errors.New("stop legacy publisher identity resolution")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(wantErr)
	bh := &backgroundExecTest{}
	bh.init()
	err := resolveMissingSubscriptionPublisherAccountIDs(ctx, bh, []*pubsub.SubInfo{{
		SubName: "sub_db", PubAccountName: "publisher",
	}})
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, bh.executedSQLs)
}

func TestGetVisibleSubscriptionMetadata(t *testing.T) {
	metas := []*pbplan.SubscriptionMeta{
		{SubName: "all_visible", AccountId: 1, DbName: "pub_a", Tables: "*"},
		{SubName: "table_visible", AccountId: 2, DbName: "pub_b", Tables: "t1,t2"},
		{SubName: "hidden", AccountId: 3, DbName: "pub_c", Tables: "*"},
	}
	query := subscriptionMetadataVisibilitySQL(
		escapeSQLString("all_visible")+","+
			escapeSQLString("hidden")+","+
			escapeSQLString("table_visible"),
		defines.MORPCVersion41,
	)
	_, err := mysql.ParseOne(context.Background(), query, 1)
	require.NoError(t, err)
	result := &MysqlResultSet{}
	for _, name := range []string{"datname", "all_tables", "table_id"} {
		column := &MysqlColumn{}
		column.SetName(name)
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{"all_visible", int64(1), uint64(0)})
	result.AddRow([]interface{}{"all_visible", int64(0), uint64(99)})
	result.AddRow([]interface{}{"table_visible", int64(0), uint64(42)})
	result.AddRow([]interface{}{"table_visible", int64(0), uint64(7)})
	result.AddRow([]interface{}{"table_visible", int64(0), uint64(42)})
	result.AddRow([]interface{}{"hidden", int64(0), uint64(0)})
	result.AddRow([]interface{}{"unknown", int64(1), uint64(0)})

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = result
	got, err := getVisibleSubscriptionMetadata(
		context.Background(), bh, metas, defines.MORPCVersion41,
	)
	require.NoError(t, err)
	require.Equal(t, []*plan2.SubscriptionMetadata{
		{Meta: metas[0], AllTablesVisible: true},
		{Meta: metas[1], VisibleTableIDs: []uint64{7, 42}},
	}, got)
	require.Equal(t, []string{query}, bh.executedSQLs)
	require.Contains(t, query, "SELECT role_id FROM mo_current_roles() role_closure")
	require.Contains(t, query, "db.owner IN")
	require.Contains(t, query, "rp.privilege_level IN ('d.t','t')")
	require.Contains(t, query, "tbl.account_id = current_account_id()")
	require.Contains(t, query, "tbl.reldatabase_id = db.dat_id")
	require.Contains(t, query, "tbl.reldatabase = db.datname")
	require.Contains(t, query, "rp.obj_id = tbl.rel_logical_id")
}

func TestGetVisibleSubscriptionMetadataKeepsExactGrantsSubscriptionScoped(t *testing.T) {
	metas := []*pbplan.SubscriptionMeta{
		{SubName: "sub_a", AccountId: 1, DbName: "pub_a", Tables: "shared_t,secret_t"},
		{SubName: "sub_b", AccountId: 2, DbName: "pub_b", Tables: "shared_t,secret_t"},
	}
	query := subscriptionMetadataVisibilitySQL(
		escapeSQLString("sub_a")+","+escapeSQLString("sub_b"),
		defines.MORPCVersion41,
	)
	result := &MysqlResultSet{}
	for _, name := range []string{"datname", "all_tables", "table_id"} {
		column := &MysqlColumn{}
		column.SetName(name)
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{"sub_a", int64(0), uint64(11)})
	result.AddRow([]interface{}{"sub_b", int64(0), uint64(22)})
	result.AddRow([]interface{}{"sub_b", int64(0), uint64(22)})

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[query] = result
	got, err := getVisibleSubscriptionMetadata(
		context.Background(), bh, metas, defines.MORPCVersion41,
	)
	require.NoError(t, err)
	require.Equal(t, []*plan2.SubscriptionMetadata{
		{Meta: metas[0], VisibleTableIDs: []uint64{11}},
		{Meta: metas[1], VisibleTableIDs: []uint64{22}},
	}, got)
}

func TestSubscriptionMetadataVisibilitySQLUsesRollingUpgradeFallback(t *testing.T) {
	query := subscriptionMetadataVisibilitySQL("'sub_db'", defines.MORPCVersion35)
	require.NotContains(t, query, "mo_current_roles()")
	require.Contains(t, query, "SELECT current_role_id() UNION")
	require.Contains(t, query, "FROM mo_catalog.mo_role_grant rg")
	require.Contains(t, query, "rg.grantee_id = current_role_id()")
}

func TestExecCtxWithRootSQLRestoresScopedValues(t *testing.T) {
	ses := &Session{}
	ses.SetSql("session SQL")
	execCtx := &ExecCtx{ses: ses}
	tcc := &TxnCompilerContext{execCtx: execCtx}
	wantErr := errors.New("stop")

	require.NoError(t, execCtx.withRootSQL("outer SQL", func() error {
		require.Equal(t, "outer SQL", tcc.GetRootSql())
		require.ErrorIs(t, execCtx.withRootSQL("inner SQL", func() error {
			require.Equal(t, "inner SQL", tcc.GetRootSql())
			return wantErr
		}), wantErr)
		require.Equal(t, "outer SQL", tcc.GetRootSql())
		return nil
	}))
	require.Equal(t, "session SQL", tcc.GetRootSql())
}

func TestExecCtxWithRootSQLRestoresAfterPanic(t *testing.T) {
	ses := &Session{}
	ses.SetSql("session SQL")
	execCtx := &ExecCtx{ses: ses}
	tcc := &TxnCompilerContext{execCtx: execCtx}

	require.PanicsWithValue(t, "boom", func() {
		_ = execCtx.withRootSQL("prepared SQL", func() error {
			require.Equal(t, "prepared SQL", tcc.GetRootSql())
			panic("boom")
		})
	})
	require.Equal(t, "session SQL", tcc.GetRootSql())
}

func TestExecCtxCloseClearsRootSQLOverride(t *testing.T) {
	rootSQL := "prepared SQL"
	execCtx := &ExecCtx{rootSQLOverride: &rootSQL}
	execCtx.Close()
	require.Nil(t, execCtx.rootSQLOverride)
}

func TestDatabaseExistsSuppressesOnlyExpectedEOBLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	storage := mock_frontend.NewMockEngine(ctrl)
	storage.EXPECT().Database(gomock.Any(), "invisible", txnOp).
		Return(nil, moerr.GetOkExpectedEOB())
	realErr := moerr.NewInternalErrorNoCtx("database lookup failed")
	storage.EXPECT().Database(gomock.Any(), "broken", txnOp).
		Return(nil, realErr)

	ses, logs := newObservedProtocolSession()
	ses.txnHandler = InitTxnHandler("", storage, ctx, txnOp)
	tcc := &TxnCompilerContext{execCtx: &ExecCtx{reqCtx: ctx, ses: ses}}

	require.False(t, tcc.DatabaseExists("invisible", nil))
	require.Equal(t, 0, logs.Len())

	require.False(t, tcc.DatabaseExists("broken", nil))
	require.Equal(t, 1, logs.Len())
	require.Equal(t, "Failed to get database", logs.All()[0].Message)
}

func TestResolveViewDependencyAccount(t *testing.T) {
	ses := &Session{}
	ses.SetTenantInfo(&TenantInfo{TenantID: 7})
	ses.SetAccountId(7)
	tcc := &TxnCompilerContext{execCtx: &ExecCtx{ses: ses}}

	for _, test := range []struct {
		name     string
		obj      *pbplan.ObjectRef
		tableDef *pbplan.TableDef
		snapshot *pbplan.Snapshot
		want     uint32
	}{
		{name: "ordinary tenant table", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t"}, want: 7},
		{name: "snapshot tenant", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t"},
			snapshot: &pbplan.Snapshot{Tenant: &pbplan.SnapshotTenant{TenantID: 8}}, want: 8},
		{name: "subscription publisher", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t",
			PubInfo: &pbplan.PubInfo{TenantId: 9}}, want: 9},
		{name: "subscription overrides snapshot", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "t",
			PubInfo: &pbplan.PubInfo{TenantId: 9}},
			snapshot: &pbplan.Snapshot{Tenant: &pbplan.SnapshotTenant{TenantID: 8}}, want: 9},
		{name: "cluster table", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_CATALOG, ObjName: "cluster_table"}, want: 0},
		{name: "relation kind alone keeps tenant context", obj: &pbplan.ObjectRef{SchemaName: "db", ObjName: "cluster_table"},
			tableDef: &pbplan.TableDef{TableType: catalog.SystemClusterRel}, want: 7},
		{name: "publication overrides generic cluster name", obj: &pbplan.ObjectRef{
			SchemaName: catalog.MO_CATALOG, ObjName: "cluster_table",
			PubInfo: &pbplan.PubInfo{TenantId: 9}}, want: 9},
		{name: "statement info", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM, ObjName: catalog.MO_STATEMENT}, want: 0},
		{name: "system relation overrides publisher", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM,
			ObjName: catalog.MO_STATEMENT, PubInfo: &pbplan.PubInfo{TenantId: 9}}, want: 0},
		{name: "metric", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM_METRICS, ObjName: catalog.MO_METRIC}, want: 0},
		{name: "sql statement cu", obj: &pbplan.ObjectRef{SchemaName: catalog.MO_SYSTEM_METRICS, ObjName: catalog.MO_SQL_STMT_CU}, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			tableDef := test.tableDef
			if tableDef == nil {
				tableDef = &pbplan.TableDef{}
			}
			got, err := tcc.ResolveViewDependencyAccount(test.obj, tableDef, test.snapshot)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestGetConfig(t *testing.T) {
	tcc := &TxnCompilerContext{
		execCtx: &ExecCtx{
			ses: &Session{},
		},
	}

	tests := []struct {
		varName   string
		dbName    string
		tblName   string
		expected  string
		expectErr bool
	}{
		{
			varName:   "unique_check_on_autoincr",
			dbName:    "test_db",
			tblName:   "test_tbl",
			expected:  "None",
			expectErr: true,
		},
		{
			varName:  "unique_check_on_autoincr",
			dbName:   "mo_catalog",
			tblName:  "test_tbl",
			expected: "Check",
		},
		{
			varName:   "invalid_var",
			dbName:    "test_db",
			tblName:   "test_tbl",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.varName, func(t *testing.T) {
			val, err := tcc.GetConfig(tt.varName, tt.dbName, tt.tblName)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, val)
			}
			require.True(t, len(tcc.GetAccountName()) > 0)
		})
	}
}
