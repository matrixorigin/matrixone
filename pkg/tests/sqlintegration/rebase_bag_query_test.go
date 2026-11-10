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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Compare the exact grouped/null-safe-join shape used by Rebase preflight
// against signed bag aggregation. This is not a W1 latency assertion.
func TestRebaseBagQueryPlans(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		f := rebaseSQLFixture{t: t, ctx: ctx, db: db}
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_bag_query")
		f.exec("create database nr_bag_query")
		for _, table := range []string{"l", "r"} {
			f.exec("create table nr_bag_query." + table + "(v int,s varchar(20))")
			f.exec("insert into nr_bag_query." + table + " values(NULL,NULL),(1,'A')")
		}
		bag := func(table, alias string) string {
			return "(select v as c0,serial(s) as c1,count(*) as n from nr_bag_query." + table + " group by v,serial(s)) " + alias
		}
		old := "select count(*) from " + bag("l", "l") + " full outer join " + bag("r", "r") +
			" on (l.c0 <=> r.c0 and l.c1 <=> r.c1) where not (l.n <=> r.n)"
		// Use a wide signed counter: subtracting an unsigned COUNT directly
		// could wrap. Stored values keep the existing binary serialization.
		candidate := "select count(*) from (select c0,c1 from (" +
			"select v as c0,serial(s) as c1,cast(count(*) as decimal(38,0)) as n from nr_bag_query.l group by v,serial(s) union all " +
			"select v as c0,serial(s) as c1,-cast(count(*) as decimal(38,0)) as n from nr_bag_query.r group by v,serial(s)" +
			") bags group by c0,c1 having sum(n)<>0) differences"
		explain := func(prefix, query string) []string {
			t.Helper()
			rows, err := db.QueryContext(ctx, prefix+query)
			require.NoError(t, err)
			defer rows.Close()
			var plan []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				plan = append(plan, line)
			}
			require.NoError(t, rows.Err())
			require.NotEmpty(t, plan)
			return plan
		}
		plans := map[string][]string{
			"grouped_full_join":          explain("explain ", old),
			"signed_bag":                 explain("explain ", candidate),
			"grouped_full_join_physical": explain("explain phyplan ", old),
			"signed_bag_physical":        explain("explain phyplan ", candidate),
		}
		if path := os.Getenv("MO_REBASE_PLAN_OUTPUT"); path != "" {
			data, err := json.Marshal(struct {
				Plans   map[string][]string `json:"plans"`
				Queries map[string]string   `json:"queries"`
			}{plans, map[string]string{"grouped_full_join": old, "signed_bag": candidate}})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, data, 0600))
		}
		require.NotContains(t, strings.ToLower(strings.Join(plans["signed_bag"], "\n")), " join")
		require.NotContains(t, strings.ToLower(strings.Join(plans["signed_bag_physical"], "\n")), "join")
		check := func(want int64) {
			t.Helper()
			require.Equal(t, want, f.scalar(old))
			require.Equal(t, want, f.scalar(candidate))
		}
		f.exec("truncate table nr_bag_query.l")
		f.exec("truncate table nr_bag_query.r")
		check(0)
		f.exec("insert into nr_bag_query.l values(NULL,NULL),(1,'A'),(1,'A'),(2,'a')")
		check(3)
		f.exec("insert into nr_bag_query.r values(2,'a'),(NULL,NULL),(1,'A'),(1,'A')")
		check(0)
		f.exec("insert into nr_bag_query.r values(1,'A')")
		check(1)
		f.exec("update nr_bag_query.r set s='a' where v=1")
		check(2)
	})
}
