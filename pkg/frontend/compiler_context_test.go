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
	result := &MysqlResultSet{}
	for _, name := range []string{"datname", "all_tables", "table_id"} {
		column := &MysqlColumn{}
		column.SetName(name)
		result.AddColumn(column)
	}
	result.AddRow([]interface{}{"all_visible", int64(1), uint64(0)})
	result.AddRow([]interface{}{"table_visible", int64(1), uint64(0)})
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
		{Meta: metas[1], AllTablesVisible: true},
	}, got)
	require.Equal(t, []string{query}, bh.executedSQLs)
	require.Contains(t, query, "SELECT role_id FROM mo_current_roles() role_closure")
	require.Contains(t, query, "db.owner IN")
	require.NotContains(t, query, "rp.privilege_level IN ('d.t','t')")
	require.NotContains(t, query, "tbl.account_id")
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
