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
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestNumericValueKindPassengerAcrossRelationalOperators(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		database := fmt.Sprintf("numeric_kind_passenger_%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+database)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+database) })
		_, err = conn.ExecContext(ctx, "USE "+database)
		require.NoError(t, err)

		queryFor := func(t *testing.T, statement string) [][]string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer func() { require.NoError(t, rows.Close()) }()
			columns, err := rows.Columns()
			require.NoError(t, err)
			got := make([][]string, 0)
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				dest := make([]any, len(values))
				for i := range values {
					dest[i] = &values[i]
				}
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(values))
				for i, value := range values {
					row[i] = "NULL"
					if value.Valid {
						row[i] = value.String
					}
				}
				got = append(got, row)
			}
			require.NoError(t, rows.Err())
			return got
		}
		query := func(statement string) [][]string { return queryFor(t, statement) }
		requirePairedResult := func(row []string, absIndex, sqrtIndex int) {
			t.Helper()
			switch row[absIndex] {
			case "49":
				require.Equal(t, "7", row[sqrtIndex])
			case "1":
				require.Equal(t, "1", row[sqrtIndex])
			default:
				t.Fatalf("unexpected ABS result %q in %v", row[absIndex], row)
			}
		}

		ids := `(SELECT 1 AS id UNION ALL SELECT 2 AS id)`
		t.Run("projection_and_identity_limit", func(t *testing.T) {
			for _, statement := range []string{
				"SELECT id, ABS(v), SQRT(v) FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM " + ids + " AS s) AS d ORDER BY id",
				"SELECT id, ABS(v), SQRT(v) FROM (SELECT id, v FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM " + ids + " AS s) AS raw LIMIT 2) AS d ORDER BY id",
				"SELECT id, ABS(CASE WHEN id=1 THEN v ELSE '1' END), SQRT(CASE WHEN id=1 THEN v ELSE '1' END) FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s LIMIT 2) AS d ORDER BY id",
			} {
				got := query(statement)
				require.Equal(t, [][]string{{"1", "49", "7"}, {"2", "1", "1"}}, got, statement)
			}
		})

		t.Run("nested_dependent_retained_projection", func(t *testing.T) {
			statement := "SELECT id, (SELECT (SELECT ABS(d.v))) FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s LIMIT 2) AS d ORDER BY id"
			got := queryFor(t, statement)
			require.Equal(t, [][]string{{"1", "49"}, {"2", "1"}}, got, statement)
		})
		t.Run("nested_dependent_condition_source", func(t *testing.T) {
			statement := "SELECT id, (SELECT (SELECT ABS(CASE WHEN d.id=1 THEN X'31' ELSE '1' END))) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS d ORDER BY id"
			got := queryFor(t, statement)
			require.Equal(t, [][]string{{"1", "49"}, {"2", "1"}}, got, statement)
		})
		t.Run("nested_dependent_raw_result_kind_reaches_outer_math", func(t *testing.T) {
			statement := `SELECT id, v, ABS(v), SQRT(v) FROM (
				SELECT id, (SELECT (SELECT d.v)) AS v
				FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM ` + ids + ` AS s) AS d
			) AS result ORDER BY id`
			got := queryFor(t, statement)
			require.Equal(t, [][]string{{"1", "1", "49", "7"}, {"2", "1", "1", "1"}}, got, statement)
		})

		t.Run("explicit_text_cast_clears_hex_kind", func(t *testing.T) {
			for _, statement := range []string{
				"SELECT id, ABS(CASE WHEN id=1 THEN CAST(X'31' AS CHAR) ELSE '1' END) FROM " + ids + " AS s ORDER BY id",
				"SELECT id, ABS(CASE WHEN id=1 THEN CAST(v AS CHAR) ELSE '1' END) FROM (SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM " + ids + " AS s) AS d ORDER BY id",
			} {
				require.Equal(t, [][]string{{"1", "1"}, {"2", "1"}}, query(statement), statement)
			}
		})

		t.Run("insert_values_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "CREATE TABLE rowset_kind_source (n BIGINT)")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_pick = 1")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "INSERT INTO rowset_kind_source VALUES (CAST(CASE WHEN @numeric_kind_pick=1 THEN X'31' ELSE '1' END AS SIGNED))")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"49"}}, query("SELECT n FROM rowset_kind_source"))
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_pick = 2")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "INSERT INTO rowset_kind_source VALUES (CAST(CASE WHEN @numeric_kind_pick=1 THEN X'31' ELSE '1' END AS SIGNED))")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"49"}, {"1"}}, query("SELECT n FROM rowset_kind_source ORDER BY n DESC"))
		})

		t.Run("session_variable_assignment_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "SET @numeric_kind_pick = 1")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_value = CASE WHEN @numeric_kind_pick=1 THEN X'31' ELSE '1' END")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "49", "7"}}, query("SELECT @numeric_kind_value, ABS(@numeric_kind_value), SQRT(@numeric_kind_value)"))

			_, err = conn.ExecContext(ctx, "SET @numeric_kind_pick = 2")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_value = CASE WHEN @numeric_kind_pick=1 THEN X'31' ELSE '1' END")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "1", "1"}}, query("SELECT @numeric_kind_value, ABS(@numeric_kind_value), SQRT(@numeric_kind_value)"))
		})

		t.Run("prepared_session_variable_assignment_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "PREPARE numeric_kind_set_stmt FROM 'SET @numeric_kind_value = CASE WHEN ?=1 THEN X''31'' ELSE ''1'' END'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_pick = 1")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "EXECUTE numeric_kind_set_stmt USING @numeric_kind_pick")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "49", "7"}}, query("SELECT @numeric_kind_value, ABS(@numeric_kind_value), SQRT(@numeric_kind_value)"))
			_, err = conn.ExecContext(ctx, "SET @numeric_kind_pick = 2")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "EXECUTE numeric_kind_set_stmt USING @numeric_kind_pick")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "1", "1"}}, query("SELECT @numeric_kind_value, ABS(@numeric_kind_value), SQRT(@numeric_kind_value)"))
			_, err = conn.ExecContext(ctx, "DEALLOCATE PREPARE numeric_kind_set_stmt")
			require.NoError(t, err)
		})

		t.Run("recursive_cte_preserves_selected_kind", func(t *testing.T) {
			got := query(`WITH RECURSIVE r(id, v) AS (
				SELECT 1, X'31'
				UNION ALL
				SELECT id+1, '1' FROM r WHERE id < 2
			) SELECT id, ABS(v), SQRT(v) FROM r ORDER BY id`)
			require.Equal(t, [][]string{{"1", "49", "7"}, {"2", "1", "1"}}, got)
		})
		t.Run("recursive_union_distinct_excludes_kind_passenger", func(t *testing.T) {
			got := query(`WITH RECURSIVE r(id, v) AS (
				SELECT 1, X'31'
				UNION
				SELECT 1, '1' FROM r WHERE id = 1
			) SELECT id, ABS(v) FROM r`)
			require.Equal(t, [][]string{{"1", "49"}}, got)
		})
		t.Run("persisted_row_dependent_default_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "CREATE TABLE persisted_default_kind (id INT, n DOUBLE DEFAULT (ABS(CASE WHEN id=1 THEN X'31' ELSE '1' END)))")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "INSERT INTO persisted_default_kind (id) VALUES (1), (2)")
			require.NoError(t, err)
			got := query("SELECT id, n FROM persisted_default_kind ORDER BY id")
			require.Equal(t, [][]string{{"1", "49"}, {"2", "1"}}, got)
		})
		t.Run("persisted_string_default_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "CREATE TABLE persisted_string_default_kind (id INT, n VARCHAR(20) DEFAULT (ABS(CASE WHEN id=1 THEN X'31' ELSE '1' END)))")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "INSERT INTO persisted_string_default_kind (id) VALUES (1), (2)")
			require.NoError(t, err)
			got := query("SELECT id, n FROM persisted_string_default_kind ORDER BY id")
			require.Equal(t, [][]string{{"1", "49"}, {"2", "1"}}, got)
		})

		t.Run("integer_oct_maketime_and_cast_consumers", func(t *testing.T) {
			cases := []struct {
				name string
				sql  string
				want [][]string
			}{
				{
					name: "REPEAT integer argument",
					sql:  "SELECT id, LENGTH(REPEAT('a', CASE WHEN id=1 THEN X'31' ELSE '1' END)) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "49"}, {"2", "1"}},
				},
				{
					name: "OCT numeric interpretation",
					sql:  "SELECT id, OCT(CASE WHEN id=1 THEN X'31' ELSE '1' END) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "61"}, {"2", "1"}},
				},
				{
					name: "MAKETIME hour integer argument",
					sql:  "SELECT id, MAKETIME(CASE WHEN id=1 THEN X'31' ELSE '1' END, 0, 0) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "49:00:00"}, {"2", "01:00:00"}},
				},
				{
					name: "MAKETIME seconds exact source kind",
					sql:  "SELECT id, MAKETIME(1, 0, CASE WHEN id=1 THEN X'31' ELSE '1' END) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "01:00:49.000000"}, {"2", "01:00:01.000000"}},
				},
				{
					name: "explicit signed cast consumes source kind",
					sql:  "SELECT id, CAST(CASE WHEN id=1 THEN X'31' ELSE '1' END AS SIGNED) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "49"}, {"2", "1"}},
				},
				{
					name: "IF truthiness consumes source kind",
					sql:  "SELECT id, IF(CASE WHEN id=1 THEN X'30' ELSE '0' END, 1, 0) FROM " + ids + " AS s ORDER BY id",
					want: [][]string{{"1", "1"}, {"2", "0"}},
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					require.Equal(t, tc.want, query(tc.sql), tc.sql)
				})
			}
		})

		t.Run("set_distinct_representative", func(t *testing.T) {
			for _, statement := range []string{
				"SELECT ABS(v), SQRT(v) FROM (SELECT X'31' AS v UNION SELECT '1' AS v) AS d",
				"SELECT ABS(v), SQRT(v) FROM (SELECT '1' AS v UNION SELECT X'31' AS v) AS d",
			} {
				got := query(statement)
				require.Len(t, got, 1, statement)
				requirePairedResult(got[0], 0, 1)
			}
		})

		t.Run("projected_distinct_equality", func(t *testing.T) {
			got := query("SELECT ABS(v), SQRT(v) FROM (SELECT DISTINCT v FROM (SELECT CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM " + ids + " AS s) AS source) AS d")
			require.Len(t, got, 1)
			requirePairedResult(got[0], 0, 1)
		})

		t.Run("group_by_representative", func(t *testing.T) {
			got := query("SELECT ABS(v), SQRT(v), COUNT(*) FROM (SELECT CASE WHEN id=1 THEN X'31' ELSE '1' END AS v FROM " + ids + " AS s) AS d GROUP BY v")
			require.Len(t, got, 1)
			require.Equal(t, "2", got[0][2])
			requirePairedResult(got[0], 0, 1)
		})

		t.Run("shared_cte_two_readers", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "CREATE TABLE numeric_source (id INT, pad INT)")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, `INSERT INTO numeric_source
				SELECT a.i*100+b.i*10+c.i+1, 0
				FROM (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
				CROSS JOIN (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
				CROSS JOIN (SELECT 0 i UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c`)
			require.NoError(t, err)
			got := query(`WITH c AS (
				SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v, SUM(id) AS s
				FROM numeric_source GROUP BY id, CASE WHEN id=1 THEN X'31' ELSE '1' END
			) SELECT a.id, ABS(a.v), SQRT(a.v), ABS(b.v), SQRT(b.v)
			FROM c a JOIN c b ON a.id=b.id WHERE a.id IN (1,2) ORDER BY a.id`)
			require.Equal(t, [][]string{{"1", "49", "7", "49", "7"}, {"2", "1", "1", "1", "1"}}, got)
		})

		t.Run("select_into_preserves_selected_kind", func(t *testing.T) {
			_, err := conn.ExecContext(ctx, "SELECT CASE WHEN 1=1 THEN X'31' ELSE '1' END INTO @numeric_kind_selected")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "49"}}, query("SELECT @numeric_kind_selected, ABS(@numeric_kind_selected)"))

			_, err = conn.ExecContext(ctx, "PREPARE numeric_kind_stmt FROM 'SELECT ABS(?)'")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"49"}}, query("EXECUTE numeric_kind_stmt USING @numeric_kind_selected"),
				"SQL PREPARE/EXECUTE must preserve the kind stored by SELECT INTO")
			_, err = conn.ExecContext(ctx, "SELECT CASE WHEN 0=1 THEN X'31' ELSE '1' END INTO @numeric_kind_selected")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1"}}, query("EXECUTE numeric_kind_stmt USING @numeric_kind_selected"),
				"reused SQL PREPARE must read the current ordinary-text kind")
			_, err = conn.ExecContext(ctx, "DEALLOCATE PREPARE numeric_kind_stmt")
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx, "SELECT NULL INTO @numeric_kind_null")
			require.NoError(t, err)
			require.Equal(t, [][]string{{"1", "NULL"}}, query("SELECT @numeric_kind_null IS NULL, ABS(@numeric_kind_null)"))
		})
	})
}

func TestNumericValueKindPassengerCrossesRemoteCNAndBatchBoundary(t *testing.T) {
	require.NoError(t, CloseSingleCNBaseClusterTests())
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		database := fmt.Sprintf("numeric_kind_remote_%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+database)
		require.NoError(t, err)
		defer func() { _, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+database) }()
		_, err = conn.ExecContext(ctx, "CREATE TABLE "+database+".source (id INT PRIMARY KEY)")
		require.NoError(t, err)
		digits := `(SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)`
		_, err = conn.ExecContext(ctx, "INSERT INTO "+database+`.source
			SELECT a.n*1000+b.n*100+c.n*10+d.n+1 FROM `+digits+` a
			CROSS JOIN `+digits+` b CROSS JOIN `+digits+` c CROSS JOIN `+digits+` d`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "SELECT mo_ctl('dn','flush','"+database+".source')")
		require.NoError(t, err)

		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		var peerAddress string
		require.Eventually(t, func() bool {
			clusterservice.GetMOCluster(service.ID()).ForceRefresh(true)
			nodes, nodesErr := service.GetEngine().Nodes(false, "sys", "root", nil)
			for _, node := range nodes {
				if node.Id == peer.RawService().(cnservice.Service).ID() {
					peerAddress = node.Addr
				}
			}
			return nodesErr == nil && len(nodes) == 2 && peerAddress != ""
		}, 15*time.Second, 100*time.Millisecond,
			"both CN workers must be visible before forcing a remote scan")

		oldForce := plan2.GetForceScanOnMultiCN()
		plan2.SetForceScanOnMultiCN(true)
		defer plan2.SetForceScanOnMultiCN(oldForce)
		statement := "SELECT id, ABS(CASE WHEN id IN (1,8193) THEN X'31' ELSE '1' END), " +
			"SQRT(CASE WHEN id IN (1,8193) THEN X'31' ELSE '1' END) FROM " +
			database + ".source ORDER BY id"
		func() {
			rows, queryErr := conn.QueryContext(ctx, "EXPLAIN PHYPLAN ANALYZE "+statement)
			require.NoError(t, queryErr)
			defer rows.Close()
			columns, columnsErr := rows.Columns()
			require.NoError(t, columnsErr)
			var physicalPlan strings.Builder
			for rows.Next() {
				values := make([]sql.RawBytes, len(columns))
				args := make([]any, len(values))
				for i := range values {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				for _, value := range values {
					physicalPlan.Write(value)
					physicalPlan.WriteByte('\n')
				}
			}
			require.NoError(t, rows.Err())
			require.Contains(t, physicalPlan.String(), "addr:"+peerAddress,
				"the forced physical plan must schedule a scan on the peer CN")
		}()

		var remoteReceipts atomic.Int64
		previousReport := logutil.GetReportZapFunc()
		logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zap.Field) (*buffer.Buffer, error) {
			if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
				remoteReceipts.Add(1)
			}
			return previousReport(enc, entry, fields)
		}})
		defer logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReport})
		if fault.Enable() {
			defer fault.Disable()
		}
		require.NoError(t, fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "echo", 0, database+".source", false))
		defer func() {
			_, removeErr := fault.RemoveFaultPoint(ctx, "inject_send_pipeline")
			require.NoError(t, removeErr)
		}()
		rows, err := conn.QueryContext(ctx, statement)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		count := 0
		for rows.Next() {
			var id int
			var absolute, squareRoot string
			require.NoError(t, rows.Scan(&id, &absolute, &squareRoot))
			if id == 1 || id == 8193 {
				require.Equal(t, "49", absolute, "HEX provenance at row %d", id)
				require.Equal(t, "7", squareRoot, "HEX provenance at row %d", id)
			} else {
				require.Equal(t, "1", absolute, "ordinary text at row %d", id)
				require.Equal(t, "1", squareRoot, "ordinary text at row %d", id)
			}
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10000, count, "the query crosses the 8192-row vector boundary")
		require.Positive(t, remoteReceipts.Load(), "the numeric expression query must reach a peer CN pipeline server")
	})
}
