// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func orPlanningPredicate(n int, shape string) string {
	terms := make([]string, n)
	for i := range terms {
		if shape == "in" {
			terms[i] = fmt.Sprint(i + 1)
		} else {
			terms[i] = fmt.Sprintf("fact_id=%d", i+1)
		}
	}
	if shape == "in" {
		return "fact_id IN (" + strings.Join(terms, ",") + ")"
	}
	var balanced func([]string) string
	balanced = func(xs []string) string {
		if len(xs) == 1 {
			return xs[0]
		}
		mid := len(xs) / 2
		return "(" + balanced(xs[:mid]) + " OR " + balanced(xs[mid:]) + ")"
	}
	if shape == "balanced" {
		return balanced(terms)
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

func TestLargeORPlanningSemantics(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
		defer cancel()
		exec := func(statement string) {
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("CREATE DATABASE or_planning_regression")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, err := db.ExecContext(cleanup, "DROP DATABASE IF EXISTS or_planning_regression")
			require.NoError(t, err)
		}()
		exec("CREATE TABLE or_planning_regression.probe (scenario_id INT, fact_id BIGINT, amount DECIMAL(24,6))")
		exec("INSERT INTO or_planning_regression.probe SELECT MOD(result,200)+1,MOD(result,1024)+1,CAST(MOD(result,100000) AS DECIMAL(24,6))/1000 FROM generate_series(1,23790) g")
		aggregate := func(t *testing.T, query string, count int64, sum string, args ...any) {
			t.Helper()
			started := time.Now()
			var gotCount int64
			var gotSum string
			require.NoError(t, db.QueryRowContext(ctx, query, args...).Scan(&gotCount, &gotSum))
			require.Equal(t, count, gotCount)
			require.Equal(t, sum, gotSum)
			t.Logf("OR planning query elapsed=%s", time.Since(started))
		}
		for _, shape := range []string{"flat", "balanced", "in"} {
			t.Run(shape, func(t *testing.T) {
				predicate := orPlanningPredicate(2048, shape)
				query := "SELECT COUNT(*),SUM(amount) FROM or_planning_regression.probe WHERE scenario_id=191 AND " + predicate
				aggregate(t, query, 119, "1426.810000")
				aggregate(t, query, 119, "1426.810000") // Identical cached repeat must remain correct.
				rows, err := db.QueryContext(ctx, "EXPLAIN SELECT COUNT(*),SUM(p.amount) FROM or_planning_regression.probe p WHERE p.scenario_id=191 AND "+strings.ReplaceAll(predicate, "fact_id", "p.fact_id"))
				require.NoError(t, err)
				defer rows.Close()
				for rows.Next() {
					var line string
					require.NoError(t, rows.Scan(&line))
				}
				require.NoError(t, rows.Err())
				prepared, err := db.PrepareContext(ctx, "SELECT COUNT(*),SUM(amount) FROM or_planning_regression.probe WHERE scenario_id=? AND "+predicate)
				require.NoError(t, err)
				defer prepared.Close()
				for _, scenario := range []int{191, 190, 191} {
					var count int64
					var sum string
					require.NoError(t, prepared.QueryRowContext(ctx, scenario).Scan(&count, &sum))
					require.Equal(t, int64(119), count)
					expected := "1426.810000"
					if scenario == 190 {
						expected = "1426.691000"
					}
					require.Equal(t, expected, sum)
				}
			})
		}
		exec("CREATE TABLE or_planning_regression.null_probe (scenario_id INT, fact_id BIGINT, amount DECIMAL(24,6))")
		exec("INSERT INTO or_planning_regression.null_probe VALUES (191,1,1),(191,2,2),(191,3,3),(191,NULL,4),(NULL,1,5)")
		for _, predicate := range []string{"fact_id=1 OR fact_id=NULL OR fact_id=2", "fact_id IN (1,NULL,2)"} {
			aggregate(t, "SELECT COUNT(*),SUM(amount) FROM or_planning_regression.null_probe WHERE scenario_id=191 AND ("+predicate+")", 2, "3.000000")
			rows, err := db.QueryContext(ctx, "SELECT fact_id,("+predicate+") FROM or_planning_regression.null_probe WHERE scenario_id=191 ORDER BY fact_id")
			require.NoError(t, err)
			defer rows.Close()
			var seen int
			for rows.Next() {
				var id sql.NullInt64
				var truth sql.NullBool
				require.NoError(t, rows.Scan(&id, &truth))
				if id.Valid && (id.Int64 == 1 || id.Int64 == 2) {
					require.Equal(t, sql.NullBool{Bool: true, Valid: true}, truth)
				} else {
					require.False(t, truth.Valid)
				}
				seen++
			}
			require.Equal(t, 4, seen)
			require.NoError(t, rows.Err())
		}
		exec("CREATE TABLE or_planning_regression.pk_probe (id INT PRIMARY KEY,scenario_id INT,fact_id BIGINT,amount DECIMAL(24,6))")
		exec("INSERT INTO or_planning_regression.pk_probe VALUES (1,191,1,1),(2,191,2,2),(3,191,NULL,4),(4,NULL,1,8)")
		for _, shape := range []string{"flat", "balanced", "in"} {
			aggregate(t, "SELECT COUNT(*),SUM(amount) FROM or_planning_regression.pk_probe WHERE scenario_id=191 AND "+orPlanningPredicate(32, shape), 2, "3.000000")
		}
		expired, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer stop()
		_, err = db.ExecContext(expired, "SELECT COUNT(*) FROM or_planning_regression.probe WHERE scenario_id=191 AND "+orPlanningPredicate(8192, "flat"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		aggregate(t, "SELECT COUNT(*),SUM(amount) FROM or_planning_regression.probe WHERE scenario_id=191", 119, "1426.810000")
	})
}
