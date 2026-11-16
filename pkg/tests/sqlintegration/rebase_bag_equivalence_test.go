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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestNativeRebaseFourBagEquivalence(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const common = "(1,'a'),(1,'a'),(NULL,NULL)"
		for i, tc := range []struct {
			name  string
			bags  [4]string // Bf, F, Bu, U; handwritten visible multisets.
			equal bool
		}{
			{"duplicates_null", [4]string{common, common, common, common}, true},
			{"empty", [4]string{}, true},
			{"feature_duplicate", [4]string{common, common + ",(1,'a')", common, common}, false},
			{"upstream_null_delete", [4]string{common, common, common, "(1,'a'),(1,'a')"}, false},
			{"feature_base", [4]string{"(1,'b'),(1,'a'),(NULL,NULL)", common, common, common}, false},
			{"upstream_base", [4]string{common, common, "(1,'b'),(1,'a'),(NULL,NULL)", common}, false},
			{"text_bytes", [4]string{common, "(1,'A'),(1,'a'),(NULL,NULL)", common, common}, false},
			{"opposite_counts", [4]string{"(1,'a')", "(1,'a'),(1,'a')", "", "(1,'a')"}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				prefix := fmt.Sprintf("nr_bageq_%d_", i)
				f := rebaseSQLFixture{t, ctx, db, prefix}
				u, branch, target := prefix+"u", prefix+"f", prefix+"result"
				defer cleanupSQLIntegration(t, cn,
					"drop snapshot if exists "+prefix+"bu", "drop snapshot if exists "+prefix+"bf",
					"drop snapshot if exists "+prefix+"fh", "drop snapshot if exists "+prefix+"uh",
					"drop database if exists "+target, "drop database if exists "+branch, "drop database if exists "+u)
				set := func(database, values string) {
					f.exec("delete from " + database + ".bag")
					if values != "" {
						f.exec("insert into " + database + ".bag values " + values)
					}
				}
				f.exec("create database " + u)
				f.exec("create table " + u + ".bag(n int,v varchar(16))")
				set(u, tc.bags[2])
				bu := f.snapshot(u, "bu")
				f.exec("data branch create database " + branch + " from " + bu)
				set(branch, tc.bags[0])
				bf := f.snapshot(branch, "bf")
				set(branch, tc.bags[1])
				fh := f.snapshot(branch, "fh")
				set(u, tc.bags[3])
				uh := f.snapshot(u, "uh")
				endpoints := [4]string{bf, fh, bu, uh}
				// Independent original pairwise SQL, not the production builder.
				bag := func(endpoint, sign string) string {
					return "select n,serial(v) as s," + sign + "cast(count(*) as decimal(38,0)) as m from " +
						strings.Replace(endpoint, "{", ".bag{", 1) + " group by n,serial(v)"
				}
				allEqual := true
				for _, pair := range [][2]int{{0, 2}, {0, 1}, {2, 3}} {
					query := "select count(*) from (select n,s from (" + bag(endpoints[pair[0]], "") + " union all " +
						bag(endpoints[pair[1]], "-") + ") bags group by n,s having sum(m)<>0) differences"
					allEqual = f.scalar(query) == 0 && allEqual
				}
				require.Equal(t, tc.equal, allEqual)
				_, counts, err := f.rebaseTables(target, fh, uh, bf, bu, "when conflict fail", 1)
				if tc.equal {
					require.NoError(t, err)
					require.Equal(t, [6]uint64{}, counts)
					require.Equal(t, f.scalar("select count(*) from "+u+".bag"), f.scalar("select count(*) from "+target+".bag"))
				} else {
					require.ErrorContains(t, err, "rejects changed no-PK tables")
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
				}
			})
		}
	})
}
