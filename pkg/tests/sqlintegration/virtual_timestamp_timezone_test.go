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
	"strings"
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
		var starID int
		var starTS, starD, starStored string
		require.NoError(t, conn.QueryRowContext(ctx, "select * from t where id=1").Scan(&starID, &starTS, &starD, &starStored))
		require.Equal(t, 1, starID)
		require.Equal(t, d, starD)
		count("select count(*) from (select d from t group by d) grouped", 2)
		count("select count(*) from t a join t b on a.d=b.d where a.id=1", 1)
		for _, hint := range []string{"", "force index(kd)", "ignore index(kd)"} {
			count("select count(*) from t "+hint+" where d='2024-03-01'", 1)
			count("select count(*) from t "+hint+" where d='2024-02-29'", 1)
		}
		count("select id from t force index for order by (kd) where d='2024-03-01' order by d", 1)
		count("select count(*) from t outer_t where exists (select 1 from t inner_t where inner_t.id=outer_t.id and outer_t.d='2024-03-01')", 1)
		exec("create view v as select id,d from t")
		count("select count(*) from v where d='2024-03-01'", 1)
		exec("create table copied as select id,d from t where d='2024-03-01'")
		count("select count(*) from copied where d='2024-03-01'", 1)
		var got int
		require.NoError(t, prepared.QueryRowContext(ctx, "2024-03-01").Scan(&got))
		require.Equal(t, 1, got)
		exec("create table dml(id int primary key, ts timestamp(6) not null, d date as (date(ts)) virtual, key kd(d))")
		exec("set time_zone='+00:00'")
		exec("insert into dml(id,ts) values(1,'2024-02-29 23:30:00.123456'),(2,'2024-02-29 23:30:00.123456')")
		exec("set time_zone='+08:00'")
		exec("delete from dml where id=1")
		count("select count(*) from dml where id=1", 0)
		exec("update dml set ts=ts where id=2")
		count("select count(*) from dml where id=2 and d='2024-03-01'", 1)
		exec("delete from dml where id=2")
		count("select count(*) from dml", 0)
		exec("set time_zone='+00:00'")
		count("select count(*) from t force index(kd) where d='2024-03-01'", 0)
		count("select count(*) from t force index(kd) where d='2024-02-29'", 2)
	})
}

func TestVirtualTimestampGeneratedDDLAdmission(t *testing.T) {
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
		const schema = "virtual_timestamp_admission"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		_, err = conn.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		create := func(name, sourceType string) string {
			cols := []string{"source " + sourceType, "g0 date as (date(source)) virtual"}
			for level := 1; level <= 16; level++ {
				previous := fmt.Sprintf("g%d", level-1)
				expression := previous + "+" + previous
				if level == 1 {
					expression = "year(g0)+year(g0)"
				}
				cols = append(cols, fmt.Sprintf("g%d int as (%s) virtual", level, expression))
			}
			return fmt.Sprintf("create table %s(%s)", name, strings.Join(cols, ","))
		}
		_, err = conn.ExecContext(ctx, create("too_large", "timestamp(6)"))
		require.ErrorContains(t, err, "read-expression nodes")
		_, err = conn.ExecContext(ctx, create("stable", "datetime(6)"))
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "alter table stable modify column source timestamp(6)")
		require.ErrorContains(t, err, "read-expression nodes")
	})
}
