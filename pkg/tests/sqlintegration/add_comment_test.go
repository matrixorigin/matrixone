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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestAddColumnCommentPreservedSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "add_column_comment"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"create database " + schema,
			"create table " + schema + ".customer(a int,b int,primary key(a,b))",
			"insert into " + schema + ".customer values(1,2)",
			"alter table " + schema + ".customer add column x int comment 'kept'",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		fresh, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer fresh.Close()
		var comment string
		require.NoError(t, fresh.QueryRowContext(ctx, "select column_comment from information_schema.columns where table_schema=? and table_name='customer' and column_name='x'", schema).Scan(&comment))
		require.Equal(t, "kept", comment)
		var name, create string
		require.NoError(t, fresh.QueryRowContext(ctx, "show create table "+schema+".customer").Scan(&name, &create))
		require.Contains(t, create, "COMMENT 'kept'")
		var rows int
		require.NoError(t, fresh.QueryRowContext(ctx, "select count(*) from "+schema+".customer where a=1 and b=2 and x is null").Scan(&rows))
		require.Equal(t, 1, rows)
	})
}
