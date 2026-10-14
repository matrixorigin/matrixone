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

package dml

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestODKUEmptyUniqueValidation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		name := testutils.GetDatabaseName(t)
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, fmt.Sprintf("create database `%s`", name))
		execSQLDB(t, ctx, db, fmt.Sprintf("use `%s`", name))
		for _, tc := range []struct{ name, partition, unique string }{
			{"plain", "", "uk"},
			{"key_partition", " partition by key(id) partitions 2", "uk"},
			{"range", " partition by range(id)(partition p0 values less than(16), partition p1 values less than(maxvalue))", "uk"},
			{"composite", " partition by key(id) partitions 2", "uk,kind"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				execSQLDB(t, ctx, db, fmt.Sprintf("create table %s(id int primary key,uk int,kind int,w int,unique(%s))%s", tc.name, tc.unique, tc.partition))
				var values []string
				for i := 1; i <= 32; i++ {
					values = append(values, fmt.Sprintf("(%d,%d,0,0)", i, i*10))
				}
				execSQLDB(t, ctx, db, "insert into "+tc.name+" values"+strings.Join(values, ","))
				prefix := "insert into " + tc.name + " values"
				suffix := " on duplicate key update w=w+values(w)"
				stmt, err := db.PrepareContext(ctx, prefix+"(?,?,0,?)"+suffix)
				require.NoError(t, err)
				defer stmt.Close()
				// Reuse one binary-prepared generation across DROP -> IN -> DROP.
				for _, args := range [][3]int{{99, 10, 1}, {100, 1000, 2}, {99, 10, 3}} {
					_, err = stmt.ExecContext(ctx, args[0], args[1], args[2])
					require.NoError(t, err)
				}
				require.Equal(t, [][]string{{"1", "4"}, {"100", "2"}}, queryStringRows(t, ctx, db, "select id,w from "+tc.name+" where w<>0 order by id"))
				// Physical accounting distinguishes final INSERT-only validation from
				// the two-column UNIQUE target lookup, which still reads its matched row.
				planRows := queryStringRows(t, ctx, db, "explain phyplan analyze "+prefix+"(99,10,0,1)"+suffix)
				validationSource := false
				scans := 0
				zeroScan := regexp.MustCompile(`InRows:0 OutRows:0 .*ScanBytes:0bytes`)
				for _, row := range planRows {
					line := row[0]
					if strings.Contains(line, "DataSource:") {
						validationSource = strings.Contains(line, ".__mo_index_unique_") && strings.Contains(line, "[__mo_index_idx_col]")
					}
					if validationSource && strings.Contains(line, "tablescan ") {
						scans++
						require.Regexp(t, zeroScan, line)
					}
				}
				require.Positive(t, scans, "must observe final UNIQUE validation, not an empty plan")
				execSQLDB(t, ctx, db, prefix+"(200,10,0,2),(201,2010,0,3),(202,2010,0,4)"+suffix)
				require.Equal(t, [][]string{{"1", "7"}, {"100", "2"}, {"201", "7"}}, queryStringRows(t, ctx, db, "select id,w from "+tc.name+" where w<>0 order by id"))
				execSQLDB(t, ctx, db, prefix+"(203,NULL,0,9),(204,NULL,0,10)"+suffix)
				require.Equal(t, [][]string{{"2"}}, queryStringRows(t, ctx, db, "select count(*) from "+tc.name+" where uk is null"))
				execSQLDB(t, ctx, db, "insert into "+tc.name+" select 300,3000,0,1 where false"+suffix)
				// A nonempty FAIL validation must still reject ordinary duplicate inserts.
				_, err = db.ExecContext(ctx, prefix+"(500,10,0,1)")
				require.Error(t, err)
				require.Equal(t, [][]string{{"36"}}, queryStringRows(t, ctx, db, "select count(*) from "+tc.name))
				// Empty build sides must preserve outer/anti semantics and aggregation.
				require.Equal(t, [][]string{{"36", "0"}}, queryStringRows(t, ctx, db, "select count(*),count(b.id) from "+tc.name+" a left join "+tc.name+" b on a.uk=b.uk and b.id=-1"))
				require.Equal(t, [][]string{{"36"}}, queryStringRows(t, ctx, db, "select count(*) from "+tc.name+" a where not exists(select 1 from "+tc.name+" b where b.id=-1 and b.uk=a.uk)"))
			})
		}
	})
}
