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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue29160PersistedMembershipPrefix(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, db, "create database `"+name+"`")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			execSQLRequire(t, cleanup, db, "drop database if exists `"+name+"`")
		}()
		for _, q := range []string{
			"use `" + name + "`", "set experimental_ivf_index=1", "set ivf_preload_entries=0", "set probe_limit=1",
			"create table docs(id bigint primary key, category int, v vecf32(2))",
			"insert into docs values (1,1,'[1,0]'),(2,0,'[2,0]'),(3,1,'[3,0]'),(4,0,'[4,0]')",
			"create index idx using ivfflat on docs(v) lists=1 op_type 'vector_l2_ops'",
		} {
			execSQLRequire(t, ctx, db, q)
		}
		query := "select id from docs where category=1 order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=pre'"
		require.Equal(t, []int64{1, 3}, queryInt64Rows(t, ctx, db, query))
		var entries string
		require.NoError(t, db.QueryRowContext(ctx, `select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=? and t.relname='docs' and i.name='idx' and i.algo_table_type='entries'`, name).Scan(&entries))
		execSQLRequire(t, ctx, db, "select mo_ctl('dn','flush','"+name+"."+entries+"')")
		var count float64
		var objects int64
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("select table_cnt, accurate_object_number from table_stats('%s.%s','refresh','full') g", name, entries)).Scan(&count, &objects))
		require.Equal(t, float64(4), count)
		require.Positive(t, objects)
		require.Equal(t, []int64{1, 3}, queryInt64Rows(t, ctx, db, query))
		require.Equal(t, []int64{1, 3}, queryInt64Rows(t, ctx, db, strings.Replace(query, "mode=pre", "mode=force", 1)))
		text := strings.Join(querySingleStringColumn(t, ctx, db, "explain analyze "+query), "\n")
		// Assert execution fields, not a plan-text snapshot or estimated cost.
		for _, field := range []string{"entry_blocks_read", "vector_rows_scored", "block_topk_rows"} {
			match := regexp.MustCompile(field + `=(\d+)`).FindStringSubmatch(text)
			require.Len(t, match, 2, "missing execution field %s", field)
			value, err := strconv.ParseInt(match[1], 10, 64)
			require.NoError(t, err)
			require.Positive(t, value, "execution field %s", field)
		}
		match := regexp.MustCompile(`storage_filter_rows=(\d+):(\d+)`).FindStringSubmatch(text)
		require.Len(t, match, 3)
		before, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		after, err := strconv.Atoi(match[2])
		require.NoError(t, err)
		require.Positive(t, after)
		require.Greater(t, before, after)
	})
}
