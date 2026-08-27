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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26227RefreshesPersistedViewMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		waitForViewMetadataActivation(t, ctx, cn.ServiceID())

		const database = "issue_26227_view_metadata"
		execSQLMaybe(t, ctx, db, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, db, "create database `"+database+"`")
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists `"+database+"`")
		execSQLRequire(t, ctx, db,
			"create table `"+database+"`.source_t ("+
				"id int primary key, code varchar(5) not null, qty int not null default 1, "+
				"price decimal(10,2) not null)")
		execSQLRequire(t, ctx, db,
			"create view `"+database+"`.direct_v as "+
				"select id, code, qty, price, qty * price total from `"+database+"`.source_t")
		execSQLRequire(t, ctx, db,
			"create view `"+database+"`.chain_v as select code, total from `"+database+"`.direct_v")

		execSQLRequire(t, ctx, db,
			"alter table `"+database+"`.source_t "+
				"modify code varchar(60) not null, modify qty bigint not null default 1, "+
				"modify price decimal(20,5) not null")

		require.Eventually(t, func() bool {
			return strings.EqualFold(
				viewMetadataColumnType(ctx, db, database, "direct_v", "code"), "varchar") &&
				strings.Contains(strings.ToLower(
					viewMetadataColumnDefinition(ctx, db, database, "direct_v", "code")), "varchar(60)") &&
				strings.EqualFold(
					viewMetadataColumnType(ctx, db, database, "direct_v", "qty"), "bigint") &&
				strings.EqualFold(
					viewMetadataColumnType(ctx, db, database, "direct_v", "price"), "decimal") &&
				strings.EqualFold(
					viewMetadataColumnType(ctx, db, database, "chain_v", "code"), "varchar")
		}, 30*time.Second, 100*time.Millisecond)

		rows, err := db.QueryContext(ctx, "show columns from `"+database+"`.direct_v")
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		execSQLRequire(t, ctx, db,
			"create table `"+database+"`.copied as select * from `"+database+"`.direct_v")
		require.Equal(t, "BIGINT",
			strings.ToUpper(viewMetadataColumnType(ctx, db, database, "copied", "qty")))
		require.Contains(t,
			strings.ToLower(viewMetadataColumnDefinition(ctx, db, database, "copied", "code")),
			"varchar(60)")
	})
}

func viewMetadataColumnType(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
	column string,
) string {
	var value string
	_ = db.QueryRowContext(ctx,
		"select data_type from information_schema.columns "+
			"where table_schema=? and table_name=? and column_name=?",
		database, table, column).Scan(&value)
	return value
}

func viewMetadataColumnDefinition(
	ctx context.Context,
	db *sql.DB,
	database string,
	table string,
	column string,
) string {
	var value string
	_ = db.QueryRowContext(ctx,
		"select column_type from information_schema.columns "+
			"where table_schema=? and table_name=? and column_name=?",
		database, table, column).Scan(&value)
	return value
}
