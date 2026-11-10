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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Reuse the package's single-CN fixture: session policy, statement rollback and
// warning cardinality need the frontend, while parser permutations live in UT.
func TestEnumSetAssignmentCompatibilityOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoError(t, err, s) }
		query := func(s string) [][]string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, s)
			require.NoError(t, err, s)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			result := make([][]string, 0)
			for rows.Next() {
				values := make([]sql.NullString, len(cols))
				args := make([]any, len(cols))
				for i := range values {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				row := make([]string, len(cols))
				for i, v := range values {
					row[i] = "NULL"
					if v.Valid {
						row[i] = v.String
					}
				}
				result = append(result, row)
			}
			require.NoError(t, rows.Err())
			return result
		}
		warnings := func(count, retained int) {
			t.Helper()
			require.Equal(t, [][]string{{fmt.Sprint(count)}}, query("SHOW COUNT(*) WARNINGS"))
			records := query("SHOW WARNINGS")
			require.Len(t, records, retained)
			for _, row := range records {
				require.Equal(t, "Warning", row[0])
				require.Equal(t, "1265", row[1])
			}
		}
		strictFailure := func(s string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, s)
			var e *mysql.MySQLError
			require.ErrorAs(t, err, &e, s)
			require.Equal(t, uint16(1265), e.Number, s)
			require.Equal(t, "01000", string(e.SQLState[:]), s)
		}
		exec("CREATE DATABASE enum_set_assignment_contract")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "ROLLBACK")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanup, "DROP DATABASE enum_set_assignment_contract")
			require.NoError(t, err)
		}()
		exec("USE enum_set_assignment_contract")
		exec("CREATE TABLE t(id INT PRIMARY KEY,e ENUM('xs','s','m'),s SET('a','b','c'))")
		exec("SET sql_mode='STRICT_TRANS_TABLES'")
		exec("INSERT INTO t VALUES(1,'xs ','a,c ')")
		warnings(0, 0)
		require.Equal(t, [][]string{{"1", "xs", "1", "a,c", "5"}}, query("SELECT id,e,e+0,s,s+0 FROM t ORDER BY id"))
		exec("SET sql_mode=''")
		exec("INSERT INTO t VALUES(2,'zz','a,c,x'),(3,'1_0','a,,c')")
		warnings(4, 4)
		require.Equal(t, [][]string{{"2", "", "0", "a,c", "5"}, {"3", "", "0", "a,c", "5"}}, query("SELECT id,e,e+0,s,s+0 FROM t WHERE id IN(2,3) ORDER BY id"))
		exec("SET sql_mode='STRICT_TRANS_TABLES'")
		exec("INSERT INTO t VALUES(4,'0','0')")
		warnings(0, 0)
		exec("INSERT IGNORE INTO t VALUES(5,'bad','a,2'),(6,'xs',9),(7,'xs','9')")
		warnings(4, 4)
		require.Equal(t, [][]string{{"5", "0", "1"}, {"6", "1", "1"}, {"7", "1", "0"}}, query("SELECT id,e+0,s+0 FROM t WHERE id>=5 ORDER BY id"))
		strictFailure("INSERT INTO t VALUES(20,'xs','a'),(21,'bad','b')")
		require.Equal(t, [][]string{{"0"}}, query("SELECT COUNT(*) FROM t WHERE id>=20"))
		exec("CREATE TABLE u(id INT PRIMARY KEY,e ENUM('xs','s','m'),s SET('a','b','c'))")
		exec("INSERT INTO u VALUES(1,'xs','a'),(2,'s','b')")
		strictFailure("UPDATE u SET e='bad'")
		require.Equal(t, [][]string{{"1", "xs", "a"}, {"2", "s", "b"}}, query("SELECT id,e,s FROM u ORDER BY id"))
		exec("SET sql_mode=''")
		exec("UPDATE u SET e='bad',s='a,x,y'")
		warnings(4, 4)
		require.Equal(t, [][]string{{"1", "0", "1"}, {"2", "0", "1"}}, query("SELECT id,e+0,s+0 FROM u ORDER BY id"))
		exec("CREATE TABLE source_rows(id INT PRIMARY KEY)")
		exec("INSERT INTO source_rows VALUES(1),(2),(3)")
		exec("INSERT INTO t SELECT id+100,'bad','a,x,y' FROM source_rows")
		warnings(6, 6)
		exec("CREATE TABLE one_enum(e ENUM('xs','s','m'))")
		exec("INSERT INTO one_enum SELECT 'bad' FROM source_rows")
		warnings(3, 3)
		require.Equal(t, [][]string{{"1"}, {"1"}, {"1"}}, query("SELECT IF(id=2,1,cast_set_index_value_to_index('a,b',9)) FROM source_rows ORDER BY id"))
		warnings(2, 2)
		for _, limit := range []int{0, 1} {
			exec(fmt.Sprintf("SET max_error_count=%d", limit))
			exec("DELETE FROM one_enum")
			exec("INSERT INTO one_enum SELECT 'bad' FROM source_rows")
			warnings(3, limit)
		}
		exec("SET max_error_count=1024")
		exec("PREPARE enum_mode_reuse FROM 'INSERT INTO t VALUES(200,?,?)'")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "DEALLOCATE PREPARE enum_mode_reuse")
			require.NoError(t, err)
		}()
		exec("SET @ev='bad',@sv='a,c,x'")
		for _, mode := range []string{"", "STRICT_TRANS_TABLES", ""} {
			exec("DELETE FROM t WHERE id=200")
			exec("SET sql_mode='" + mode + "'")
			if mode == "" {
				exec("EXECUTE enum_mode_reuse USING @ev,@sv")
				warnings(2, 2)
				require.Equal(t, [][]string{{"0", "5"}}, query("SELECT e+0,s+0 FROM t WHERE id=200"))
			} else {
				strictFailure("EXECUTE enum_mode_reuse USING @ev,@sv")
				require.Equal(t, [][]string{{"0"}}, query("SELECT COUNT(*) FROM t WHERE id=200"))
			}
		}
		exec("SET sql_mode='STRICT_TRANS_TABLES'")
		exec("INSERT INTO t VALUES(201,'xs','a')")
		warnings(0, 0)
		// A strict conversion failure is statement-local inside an explicit transaction.
		exec("BEGIN")
		exec("INSERT INTO u VALUES(10,'xs','a')")
		strictFailure("INSERT INTO u VALUES(11,'xs','b'),(12,'bad','c')")
		require.Equal(t, [][]string{{"10"}}, query("SELECT id FROM u WHERE id>=10 ORDER BY id"))
		exec("INSERT INTO u VALUES(13,'s','b')")
		exec("COMMIT")
		require.Equal(t, [][]string{{"10"}, {"13"}}, query("SELECT id FROM u WHERE id>=10 ORDER BY id"))
		// A second connection observes both successful statements after commit.
		var committed int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM enum_set_assignment_contract.u WHERE id IN(10,13)").Scan(&committed))
		require.Equal(t, 2, committed)
		exec("CREATE TABLE reads(id INT PRIMARY KEY,e ENUM('z','a'),ee ENUM('','2'),s SET('a','','b'))")
		exec("INSERT IGNORE INTO reads VALUES(1,'bad',9,2),(2,'z',1,7),(3,'a',2,3),(4,NULL,NULL,NULL)")
		warnings(2, 2)
		for _, q := range []string{"SELECT e FROM reads GROUP BY e ORDER BY e", "SELECT d.e FROM(SELECT e FROM reads GROUP BY e)d ORDER BY d.e"} {
			require.Equal(t, [][]string{{"NULL"}, {""}, {"z"}, {"a"}}, query(q))
			warnings(0, 0)
		}
		require.Equal(t, [][]string{{"NULL", "NULL"}, {"", "0"}, {"z", "1"}, {"a", "2"}}, query("SELECT e,e+0 FROM reads GROUP BY e ORDER BY e+0"))
		warnings(0, 0)
		require.Equal(t, [][]string{{"1", "", "0"}, {"2", "", "1"}, {"3", "2", "2"}, {"4", "NULL", "NULL"}}, query("SELECT id,ee,ee+0 FROM reads ORDER BY id"))
		warnings(0, 0)
		require.Equal(t, [][]string{{"NULL", "NULL"}, {"", "0"}, {"a,", "3"}, {"a,,b", "7"}}, query("SELECT d.s,d.s+0 FROM(SELECT DISTINCT s FROM reads)d ORDER BY d.s"))
		warnings(0, 0)
		exec("INSERT INTO one_enum SELECT e FROM t WHERE id=201")
		warnings(0, 0)
		exec("CREATE TABLE numeric_sink(n BIGINT)")
		exec("INSERT INTO numeric_sink SELECT e+0 FROM reads GROUP BY e")
		warnings(0, 0)
		require.Equal(t, [][]string{{"NULL"}, {"0"}, {"1"}, {"2"}}, query("SELECT n FROM numeric_sink ORDER BY n"))
		labels := make([]string, 64)
		for i := range labels {
			labels[i] = fmt.Sprintf("'l%02d'", i+1)
		}
		exec("CREATE TABLE full_set(id INT PRIMARY KEY,s SET(" + strings.Join(labels, ",") + "))")
		exec("INSERT INTO full_set VALUES(1,-1),(2,'-1'),(3,-9223372036854775808)")
		warnings(0, 0)
		require.Equal(t, [][]string{{"1", "18446744073709551615"}, {"2", "18446744073709551615"}, {"3", "9223372036854775808"}}, query("SELECT id,CAST(s AS UNSIGNED) FROM full_set ORDER BY id"))
		warnings(0, 0)
		exec("CREATE TABLE defaults(e ENUM('xs','s') DEFAULT 'xs ',s SET('a','b') DEFAULT 'a,x')")
		strictFailure("INSERT INTO defaults VALUES(DEFAULT,DEFAULT)")
		require.Equal(t, [][]string{{"0"}}, query("SELECT COUNT(*) FROM defaults"))
		exec("SET sql_mode=''")
		exec("INSERT INTO defaults VALUES(DEFAULT,DEFAULT)")
		warnings(1, 1)
		require.Equal(t, [][]string{{"1", "1"}}, query("SELECT e+0,s+0 FROM defaults"))
	})
}
