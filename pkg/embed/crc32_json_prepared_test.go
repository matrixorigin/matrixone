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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

// Reuse the package's single-CN fixture; two rows distinguish runtime JSON
// evaluation from folding and one schema change exercises prepared reprepare.
func TestCRC32JSONBinaryPrepared(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		cfg := mysql.NewConfig()
		cfg.User = "dump"
		cfg.Passwd = "111"
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) { t.Helper(); _, err := conn.ExecContext(ctx, query); require.NoError(t, err) }
		exec("create database crc32_prepared_contract")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, _ = conn.ExecContext(cleanup, "drop database crc32_prepared_contract")
		}()
		exec("use crc32_prepared_contract")
		exec("create table t(id int primary key,j json)")
		exec(`insert into t values(1,'{"t1":"a"}'),(2,'{"t1":"b"}')`)
		stmt, err := conn.PrepareContext(ctx, "select crc32(j) from t where id=?")
		require.NoError(t, err)
		defer stmt.Close()
		check := func(id int, want uint64) {
			t.Helper()
			var got uint64
			require.NoError(t, stmt.QueryRowContext(ctx, id).Scan(&got))
			require.Equal(t, want, got)
		}
		check(1, 4012824821)
		check(2, 3983042220)
		exec("alter table t add column other int")
		check(1, 4012824821)

		// Exercise the added function BVT's generated value and index contract
		// through the same live SQL frontend, including every mutation form.
		// The embedded cluster becomes query-ready before the durable catalog
		// barrier finishes. Observe the real CN admission result; do not set a
		// runtime version or floor to manufacture authoring permission.
		require.Eventually(t, func() bool {
			rt := moruntime.ServiceRuntime(cn.ServiceID())
			if rt == nil {
				return false
			}
			value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
			floor, valid := value.(int64)
			return ok && valid && floor >= defines.MORPCVersion94
		}, 90*time.Second, 100*time.Millisecond, "durable CRC32 catalog authoring admission did not complete")
		exec("create table generated_crc(id int primary key,j json,c bigint unsigned generated always as (crc32(j)) stored,index idx_crc(c))")
		exec(`insert into generated_crc(id,j) values(1,'{"t1":"a"}')`)
		exec("insert into generated_crc(id,j) select 2,j from generated_crc where id=1")
		exec(`update generated_crc set j='{"t1":"b"}' where id=2`)
		exec(`replace into generated_crc(id,j) values(1,'{"t1":"c"}')`)
		exec(`insert into generated_crc(id,j) values(2,'{"t1":"a"}') on duplicate key update j=values(j)`)
		checkGenerated := func() {
			t.Helper()
			for id, want := range map[int]uint64{1: 3970567323, 2: 4012824821} {
				var stored, fresh uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select c,crc32(j) from generated_crc where id=?", id).Scan(&stored, &fresh))
				require.Equal(t, want, stored)
				require.Equal(t, want, fresh)
			}
		}
		checkGenerated()
		exec("prepare crc32_read from 'select c,crc32(j) from generated_crc where id=?'")
		exec("set @crc32_id=2")
		var preparedStored, preparedFresh uint64
		require.NoError(t, conn.QueryRowContext(ctx, "execute crc32_read using @crc32_id").Scan(&preparedStored, &preparedFresh))
		require.Equal(t, uint64(4012824821), preparedStored)
		require.Equal(t, preparedStored, preparedFresh)
		exec("deallocate prepare crc32_read")
		exec("alter table generated_crc add column note int")
		checkGenerated()
		exec("create unique index uniq_crc on generated_crc(c)")
		exec(`insert ignore into generated_crc(id,j) values(3,'{"t1":"a"}')`)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from generated_crc").Scan(&count))
		require.Equal(t, 2, count)
	})
}
