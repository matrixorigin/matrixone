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

func TestFullPartitionAggregatesAndMixedExists(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists general_ap_optimization")
		exec := func(t *testing.T, s string) { t.Helper(); execSQLRequire(t, ctx, db, s) }
		exec(t, "create database general_ap_optimization")
		exec(t, "use general_ap_optimization")
		query := func(t *testing.T, s string) []string {
			t.Helper()
			rows, err := db.QueryContext(ctx, s)
			require.NoError(t, err, s)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			var got []string
			for rows.Next() {
				values, args := make([]sql.NullString, len(cols)), make([]any, len(cols))
				for i := range args {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				fields := make([]string, len(cols))
				for i, v := range values {
					fields[i] = "NULL"
					if v.Valid {
						fields[i] = v.String
					}
				}
				got = append(got, strings.Join(fields, ":"))
			}
			require.NoError(t, rows.Err(), s)
			return got
		}
		t.Run("full partition aggregates", func(t *testing.T) {
			exec(t, "create table amounts(id int,g int,v decimal(18,2))")
			exec(t, "insert into amounts values(1,1,1.25),(2,1,null),(3,1,2.75),(4,2,null),(5,3,-2),(6,3,1)")
			require.Equal(t, []string{
				"1:4.00:2.00:2:3:1.25:2.75", "2:4.00:2.00:2:3:1.25:2.75", "3:4.00:2.00:2:3:1.25:2.75",
				"4:NULL:NULL:0:1:NULL:NULL", "5:-1.00:-0.50:2:2:-2.00:1.00", "6:-1.00:-0.50:2:2:-2.00:1.00",
			}, query(t, `select id,sum(v) over(partition by g),cast(avg(v) over(partition by g) as decimal(18,2)),
			 count(v) over(partition by g),count(*) over(partition by g),
			 min(v) over(partition by g),max(v) over(partition by g) from amounts order by id`))
			require.Equal(t, []string{"1:3.00", "2:3.00", "3:3.00", "4:3.00", "5:3.00", "6:3.00"},
				query(t, `select id,sum(v) over(order by id rows between unbounded preceding and unbounded following) from amounts order by id`))
		})
		t.Run("mixed exists with nullable text keys", func(t *testing.T) {
			exec(t, "create table probes(row_id int primary key,k bigint,flag text)")
			exec(t, "create table choices(pk text,name text,id bigint)")
			exec(t, "create table attachments(id bigint)")
			exec(t, "insert into probes values(1,1,'x'),(2,1,'x'),(3,2,'y'),(4,3,null),(5,4,'x'),(6,null,'x'),(7,5,'x'),(8,6,null),(9,7,'x'),(10,8,'y'),(11,9,'x'),(12,10,'y')")
			exec(t, "insert into choices values('1','a',11),('1','a',11),('2','wrong',12),('3','b',13),('4',null,14),(null,'a',15),('5','wrong',null),('6','wrong',16),('7',null,17),('8','a',18),('9','b',19),('10','b',20)")
			exec(t, "insert into attachments values(12),(12),(16),(17),(null)")
			const predicate = `exists (select 1 from choices x where x.pk=cast(p.k as char)
			  and x.name=case when p.flag='x' then 'a' else 'b' end)
			 or exists (select 1 from choices y join attachments z on z.id=y.id where y.pk=cast(p.k as char))`
			const simple = "select p.row_id from probes p where " + predicate + " order by p.row_id"
			want := []string{"1", "2", "3", "4", "8", "9", "12"}
			require.Equal(t, want, query(t, simple), "outer duplicates and NULL inner keys must preserve EXISTS semantics")
			const joined = `select distinct p.row_id from probes p
			 left join choices m on m.pk=cast(p.k as char) and m.name=case when p.flag='x' then 'a' else 'b' end
			 left join attachments a on a.id=m.id where ` + predicate + ` order by p.row_id limit 500`
			require.Equal(t, want, query(t, joined))
			logical := strings.Join(query(t, "explain "+joined), "\n")
			require.Contains(t, logical, "SEMI", logical)
			require.NotContains(t, logical, "MARK", logical)
			physical := strings.Join(query(t, "explain phyplan "+simple), "\n")
			require.Contains(t, physical, "hash join", physical)
			require.NotContains(t, physical, "loop join", physical)
			// Both branches having an additional, different key exercises the
			// general shared-key rule rather than just a subset/superset pair.
			require.Equal(t, []string{"1", "2", "4", "12"}, query(t, `select p.row_id from probes p where
			 exists(select 1 from choices x where x.pk=cast(p.k as char) and x.name=case when p.flag='x' then 'a' else 'b' end) or
			 exists(select 1 from choices y where y.pk=cast(p.k as char) and y.id=p.row_id)
			 order by p.row_id`))
		})
	})
}
