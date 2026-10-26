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

func TestVirtualTimestampColumnUsesReaderTimeZone(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "virtual_timestamp_reader_tz"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		count := func(q string, want int) {
			t.Helper()
			var got int
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
			require.Equal(t, want, got, q)
		}
		exec("create database " + schema)
		exec("use " + schema)
		exec("create table t(id int primary key, ts timestamp(6) not null, d date as (date(ts)) virtual, s date as (date(ts)) stored, key kd(d))")
		exec("set time_zone='+00:00'")
		exec("insert into t(id,ts) values(1,'2024-02-29 23:30:00.123456'),(2,'2024-02-29 12:00:00.000000')")
		prepared, err := conn.PrepareContext(ctx, "select count(*) from t force index(kd) where d=?")
		require.NoError(t, err)
		defer prepared.Close()
		exec("set time_zone='+08:00'")
		var d, computed, stored string
		require.NoError(t, conn.QueryRowContext(ctx, "select cast(d as char),cast(date(ts) as char),cast(s as char) from t where id=1").Scan(&d, &computed, &stored))
		require.Equal(t, "2024-03-01", d)
		require.Equal(t, computed, d)
		require.Equal(t, "2024-02-29", stored)
		for _, hint := range []string{"", "force index(kd)", "ignore index(kd)"} {
			count("select count(*) from t "+hint+" where d='2024-03-01'", 1)
			count("select count(*) from t "+hint+" where d='2024-02-29'", 1)
		}
		var got int
		require.NoError(t, prepared.QueryRowContext(ctx, "2024-03-01").Scan(&got))
		require.Equal(t, 1, got)
		exec("set time_zone='+00:00'")
		count("select count(*) from t force index(kd) where d='2024-03-01'", 0)
		count("select count(*) from t force index(kd) where d='2024-02-29'", 2)
	})
}
