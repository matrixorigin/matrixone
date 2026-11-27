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

func TestCurrentDatabaseAutoIncrementIgnoresUnrelatedDrop(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists ai_meta_a", "drop database if exists ai_meta_b")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		a, err := db.Conn(ctx)
		require.NoError(t, err)
		defer a.Close()
		exec := func(q string) {
			t.Helper()
			_, err := a.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		value := func(q string, want int) {
			t.Helper()
			var got int
			require.NoError(t, a.QueryRowContext(ctx, q).Scan(&got), q)
			require.Equal(t, want, got, q)
		}
		exec("create database ai_meta_a")
		exec("create database ai_meta_b")
		exec("create table ai_meta_a.t(id bigint auto_increment primary key) auto_increment=10")
		exec("create table ai_meta_b.t(id bigint auto_increment primary key) auto_increment=200")
		const metadata = "select auto_increment from information_schema.tables where table_schema=database() and table_name='t'"
		exec("use ai_meta_a")
		exec("prepare p from \"" + metadata + "\"")
		value("execute p", 10)
		exec("use ai_meta_b")
		value("execute p", 200)
		exec("use ai_meta_a")
		exec("set transaction isolation level repeatable read")
		exec("begin")
		// Pin A's catalog snapshot before B drops its table and allocator.
		value("select count(*) from t", 0)
		_, err = db.ExecContext(ctx, "drop table ai_meta_b.t")
		require.NoError(t, err)
		value(metadata, 10)
		exec("commit")
	})
}
