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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestShowMetadataTarget(t *testing.T) {
	for _, tc := range []struct {
		sql, database, table string
		base                 bool
	}{
		{"show tables from target", "target", "", false},
		{"show columns from target.t from ignored", "target", "t", false},
		{"show columns from t from target", "target", "t", false},
		{"show create table target.t", "target", "t", true},
		{"show columns from t", "", "t", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			db, table, base, supported := showMetadataTarget(stmt)
			require.True(t, supported)
			require.Equal(t, tc.database, db)
			require.Equal(t, tc.table, table)
			require.Equal(t, tc.base, base)
			for _, entry := range determinePrivilegeSetOfStatement(stmt).entries {
				require.Equal(t, tc.database, entry.databaseName)
			}
			local, sub := showMetadataVisibilitySQL(stmt, "session_db", defines.MORPCVersion41)
			expectedDB := tc.database
			if expectedDB == "" {
				expectedDB = "session_db"
			}
			require.Contains(t, local, "reldatabase = "+escapeSQLString(expectedDB))
			require.Contains(t, sub, "d.dat_type = 'subscription'")
			if tc.table != "" {
				require.Contains(t, local, "relname = 't'")
			}
			require.Equal(t, tc.base, strings.Contains(local, "relkind != 'v'"))
		})
	}
	for _, stmt := range []tree.Statement{&tree.ShowColumns{}, &tree.ShowCreateTable{}, &tree.ShowCreateView{}, &tree.ShowCreateDatabase{}, &tree.ShowTables{AtTsExpr: &tree.AtTimeStamp{}}, &tree.ShowCreateTable{Name: tree.NewUnresolvedObjectName("t"), AtTsExpr: &tree.AtTimeStamp{}}} {
		local, sub := showMetadataVisibilitySQL(stmt, "db", defines.MORPCVersion41)
		require.Empty(t, local)
		require.Empty(t, sub)
		require.NotNil(t, determinePrivilegeSetOfStatement(stmt))
	}
	stmt, err := mysql.ParseOne(t.Context(), "show tables from `d'\\b`", 1)
	require.NoError(t, err)
	local, _ := showMetadataVisibilitySQL(stmt, "", defines.MORPCVersion35)
	require.Contains(t, local, "reldatabase = "+escapeSQLString(stmt.(*tree.ShowTables).DBName))
	require.NotContains(t, local, "mo_current_roles()")
	require.Contains(t, local, "rg.grantee_id = current_role_id()")
}

func setShowMetadataTestProtocol(t *testing.T, version int64) {
	t.Helper()
	rt := moruntime.ServiceRuntime("")
	previous, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, version)
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
}

func TestAuthenticateShowTablesLegacyProtocol(t *testing.T) {
	setShowMetadataTestProtocol(t, defines.MORPCVersion40)
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	t.Cleanup(ses.Close)
	// No executor methods may be called: object-level fallback is disabled
	// before acquiring a catalog transaction on the compatibility path.
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer stub.Reset()
	ok, _, err := authenticateShowMetadata(t.Context(), ses, &tree.ShowTables{DBName: "db"})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestAuthenticateShowMetadataFailsClosed(t *testing.T) {
	setShowMetadataTestProtocol(t, defines.MORPCVersion41)
	sentinel := errors.New("catalog failure")
	for _, tc := range []struct {
		name       string
		sql        string
		local, sub bool
		view       bool
		failAt     string
		badResult  bool
		badProbe   bool
		want       bool
	}{
		{name: "visible local", local: true, want: true},
		{name: "subscription broad", sub: true, want: true},
		{name: "subscription columns", sql: "show columns from db.t", sub: true, want: true},
		{name: "subscription table list", sql: "show tables from db", sub: true, want: true},
		{name: "subscription view or missing table", sub: true, view: true},
		{name: "subscription type error", sub: true, failAt: "type"},
		{name: "invalid subscription type result", sub: true, badProbe: true},
		{name: "no visible object"},
		{name: "local error", failAt: "local"},
		{name: "subscription error", failAt: "subscription"},
		{name: "begin error", failAt: "begin;"},
		{name: "commit error", local: true, failAt: "commit;"},
		{name: "subscription commit error", sub: true, failAt: "commit;"},
		{name: "invalid result", badResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := tc.sql
			if sql == "" {
				sql = "show create table db.t"
			}
			stmt, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ctrl := gomock.NewController(t)
			ses := newSes(nil, ctrl)
			t.Cleanup(ses.Close)
			localSQL, subSQL := showMetadataVisibilitySQL(stmt, "", currentProtocolVersion(ses.GetProc()))
			typeSQL := showSubscriptionBaseTableSQL("db", "t")
			bh := mock_frontend.NewMockBackgroundExec(ctrl)
			bh.EXPECT().Close().Times(1)
			stats := statistic.StatsArray{}
			stats.Reset()
			stats.WithTimeConsumed(42)
			bh.EXPECT().GetExecStatsArray().Return(stats).Times(1)
			bh.EXPECT().ClearExecResultSet().AnyTimes()
			current := ""
			probeCount := 0
			bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
				current = sql
				if sql == typeSQL {
					probeCount++
				}
				if sql == tc.failAt || sql == localSQL && tc.failAt == "local" || sql == subSQL && tc.failAt == "subscription" || sql == typeSQL && tc.failAt == "type" {
					return sentinel
				}
				return nil
			}).AnyTimes()
			bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
				if tc.badResult || tc.badProbe && current == typeSQL {
					return []interface{}{123}
				}
				result := &MysqlResultSet{}
				if current == localSQL && tc.local || current == subSQL && tc.sub || current == typeSQL && !tc.view {
					result.AddRow([]interface{}{int64(1)})
				}
				return []interface{}{result}
			}).AnyTimes()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer stub.Reset()
			ok, gotStats, err := authenticateShowMetadata(t.Context(), ses, stmt)
			if tc.failAt != "" || tc.badResult || tc.badProbe {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, ok)
			}
			if tc.sub && tc.sql == "" {
				require.Equal(t, 1, probeCount)
			} else {
				require.Zero(t, probeCount)
			}
			require.Equal(t, stats, gotStats)
		})
	}
}

func TestShowSubscriptionBaseTableSQLQuotesExactTarget(t *testing.T) {
	for _, database := range []string{"sub_db", "d`'\\b"} {
		t.Run(database, func(t *testing.T) {
			table := "t'\\_%"
			stmt, err := mysql.ParseOne(t.Context(), showSubscriptionBaseTableSQL(database, table), 1)
			require.NoError(t, err)
			defer stmt.Free()
			show := stmt.(*tree.ShowTables)
			require.Equal(t, database, show.DBName)
			require.True(t, show.Full)
			filter := show.Where.Expr.(*tree.AndExpr)
			target := filter.Left.(*tree.ComparisonExpr)
			require.Equal(t, tree.EQUAL, target.Op)
			require.Equal(t, "Tables_in_"+database, target.Left.(*tree.UnresolvedName).ColNameOrigin())
			require.Equal(t, table, target.Right.(*tree.NumVal).String())
			kind := filter.Right.(*tree.ComparisonExpr)
			require.Equal(t, tree.NOT_EQUAL, kind.Op)
			require.Equal(t, "VIEW", kind.Right.(*tree.NumVal).String())
		})
	}
}
