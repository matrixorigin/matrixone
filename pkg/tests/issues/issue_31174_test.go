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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestIssue31174GeneralCIOrderBy covers the ORDER BY half of the
// utf8mb4_general_ci defect: row sequences must follow case-folded collation
// weights rather than raw byte values. Scalar equality ('a' = 'A') is a
// separate tracked issue and is intentionally not asserted here.
func TestIssue31174GeneralCIOrderBy(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		queryString := func(query string) string {
			t.Helper()
			var value string
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&value), "query: %s", query)
			return value
		}
		queryStrings := func(query string) []string {
			t.Helper()
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err, "query: %s", query)
			defer rows.Close()
			var values []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}

		require.Equal(t, "utf8mb4_general_ci",
			queryString("select collation('abc')"),
			"test assumes the server default string collation")

		// The minimal cases from the issue: folded base letters interleave.
		require.Equal(t, "A,b,C", queryString(
			"select group_concat(x order by x) from "+
				"(select 'b' as x union all select 'A' union all select 'C') t"))
		require.Equal(t, "a,Z", queryString(
			"select group_concat(x order by x) from "+
				"(select 'Z' as x union all select 'a') t"))
		require.Equal(t, "aaa,BBB,ccc", queryString(
			"select group_concat(x order by x) from "+
				"(select 'aaa' as x union all select 'BBB' union all select 'ccc') t"))

		name := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+name+"`")
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists `"+name+"`")
		execSQLRequire(t, ctx, db, "use `"+name+"`")

		execSQLRequire(t, ctx, db,
			"create table ci (id int primary key, v varchar(20))")
		execSQLRequire(t, ctx, db,
			"insert into ci values (1,'b'),(2,'A'),(3,'C'),(4,'Z'),(5,'a'),(6,'abc'),(7,'ABC')")

		// Folded ties ('abc' vs 'ABC', 'A' vs 'a') fall through to the
		// secondary key.
		require.Equal(t, []string{"A", "a", "abc", "ABC", "b", "C", "Z"}, queryStrings(
			"select v from ci order by v, id"))
		require.Equal(t, []string{"Z", "C", "b", "ABC", "abc", "a", "A"}, queryStrings(
			"select v from ci order by v desc, id desc"))
		// ORDER BY ... LIMIT returns the folded top-n set.
		require.Equal(t, []string{"A", "a", "abc"}, queryStrings(
			"select v from ci order by v, id limit 3"))

		// An explicit binary collation control keeps byte ordering.
		execSQLRequire(t, ctx, db,
			"create table bin (v varchar(20) collate utf8mb4_bin)")
		execSQLRequire(t, ctx, db, "insert into bin values ('b'),('A'),('C')")
		require.Equal(t, []string{"A", "C", "b"}, queryStrings(
			"select v from bin order by v"))
		// The Top operator (ORDER BY ... LIMIT) builds its comparator from the
		// scan vector's own charset; byte ordering must survive that path too.
		require.Equal(t, []string{"A", "C"}, queryStrings(
			"select v from bin order by v limit 2"))

		// Ordered GROUP_CONCAT over a table column agrees with ORDER BY.
		require.Equal(t, "A,a,abc,ABC,b,C,Z", queryString(
			"select group_concat(v order by v, id separator ',') from ci"))

		// NULL placement is orthogonal to the folded value order: NULLs sort
		// first for ASC and last for DESC.
		execSQLRequire(t, ctx, db, "create table ci_null (v varchar(20))")
		execSQLRequire(t, ctx, db, "insert into ci_null values ('b'),(null),('A')")
		require.Equal(t, []string{"<null>", "A", "b"}, queryStrings(
			"select ifnull(v,'<null>') from ci_null order by v"))
		require.Equal(t, []string{"b", "A", "<null>"}, queryStrings(
			"select ifnull(v,'<null>') from ci_null order by v desc"))

		// PAD SPACE: once a key is exhausted its missing characters keep
		// comparing against the space weight, so a trailing TAB (lighter than
		// space) sorts the longer value first. 'a' and 'a ' stay weight-equal
		// and fall through to the secondary key.
		execSQLRequire(t, ctx, db, "create table ci_pad (id int primary key, v varchar(20))")
		execSQLRequire(t, ctx, db,
			"insert into ci_pad values (1,'a'),(2,concat('a',char(9))),(3,'a b'),(4,'ab'),(5,'a ')")
		require.Equal(t, []string{"a\t", "a", "a ", "a b", "ab"}, queryStrings(
			"select v from ci_pad order by v, id"))
		require.Equal(t, "a\t", queryString(
			"select v from ci_pad order by v limit 1"))
		// The same stored values keep plain byte order under utf8mb4_bin.
		execSQLRequire(t, ctx, db,
			"create table bin_pad (id int primary key, v varchar(20) collate utf8mb4_bin)")
		execSQLRequire(t, ctx, db,
			"insert into bin_pad values (1,'a'),(2,concat('a',char(9))),(3,'a b'),(4,'ab'),(5,'a ')")
		require.Equal(t, []string{"a", "a\t", "a ", "a b", "ab"}, queryStrings(
			"select v from bin_pad order by v, id"))

		// A fixed-prefix secondary index scan must not replace the logical
		// general_ci ordering of a VARCHAR primary key with the byte-ordered
		// serialized hidden key, even under ORDER BY ... LIMIT.
		execSQLRequire(t, ctx, db,
			"create table ci_idx (id varchar(20) primary key, k int, key idx_k(k))")
		execSQLRequire(t, ctx, db, "insert into ci_idx values ('b',1),('A',1),('C',1)")
		require.Equal(t, []string{"A", "b", "C"}, queryStrings(
			"select id from ci_idx force index(idx_k) where k = 1 order by id"))
		require.Equal(t, []string{"A", "b"}, queryStrings(
			"select id from ci_idx force index(idx_k) where k = 1 order by id limit 2"))
		require.Equal(t, []string{"C", "b"}, queryStrings(
			"select id from ci_idx force index(idx_k) where k = 1 order by id desc limit 2"))
		// Binary collation control: the same plan shape keeps byte order.
		execSQLRequire(t, ctx, db,
			"create table bin_idx (id varchar(20) collate utf8mb4_bin primary key, k int, key idx_k(k))")
		execSQLRequire(t, ctx, db, "insert into bin_idx values ('b',1),('A',1),('C',1)")
		require.Equal(t, []string{"A", "C"}, queryStrings(
			"select id from bin_idx force index(idx_k) where k = 1 order by id limit 2"))

		// Flushed multi-block TopN: raw-threshold block pruning must not drop
		// blocks whose zonemap bytes misorder folded weights ('b' > 'C' by
		// bytes, 'b' < 'C' by general_ci).
		execSQLRequire(t, ctx, db, "create table ci_zm (v varchar(20))")
		execSQLRequire(t, ctx, db, "insert into ci_zm values ('C'),('z')")
		execSQLRequire(t, ctx, db,
			"select mo_ctl('dn','flush','"+strings.ToLower(name)+".ci_zm')")
		execSQLRequire(t, ctx, db, "insert into ci_zm values ('b')")
		execSQLRequire(t, ctx, db,
			"select mo_ctl('dn','flush','"+strings.ToLower(name)+".ci_zm')")
		require.Equal(t, "b", queryString("select v from ci_zm order by v limit 1"))
		require.Equal(t, "z", queryString("select v from ci_zm order by v desc limit 1"))
	})
}
