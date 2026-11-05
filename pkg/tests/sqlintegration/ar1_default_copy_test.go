//go:build !mo_m0

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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestAR1DefaultADDRemainsCopySQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "ar1_default_copy"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err) }
		exec("create database " + schema)
		exec("create table " + schema + ".t(a int not null,b int not null,primary key(a,b))")
		exec("insert into " + schema + ".t values(1,10),(2,20)")
		exec("select mo_ctl('dn','flush','" + schema + ".t')")
		read := func() uint64 {
			t.Helper()
			var id uint64
			var raw []byte
			require.NoError(t, db.QueryRowContext(ctx, "select rel_id,extra_info from mo_catalog.mo_tables where reldatabase=? and relname='t'", schema).Scan(&id, &raw))
			var extra api.SchemaExtra
			require.NoError(t, extra.Unmarshal(raw))
			require.Nil(t, extra.M0Evolution)
			return id
		}
		before := read()
		exec("alter table " + schema + ".t add column tail int")
		require.NotEqual(t, before, read(), "default must retain COPY and cannot issue an M0 root")
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".t where tail is null and b=a*10").Scan(&count))
		require.Equal(t, 2, count)
	})
}
