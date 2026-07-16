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

package embed

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceAccountingBVTSingleCN(t *testing.T) {
	db, closeCluster := startResourceAccountingCluster(t, 1)
	defer closeCluster()

	plan := queryExplainResource(t, db, "select sum(result) from generate_series(1, 10000) g")
	assertResourceOverview(t, plan)
	require.NotContains(t, plan, "REMOTE SCOPES:")

	require.NoError(t, execStatements(db,
		"drop database if exists resource_accounting_lifecycle",
		"create database resource_accounting_lifecycle",
		"create table resource_accounting_lifecycle.t (id bigint)",
		"prepare resource_accounting_stmt from 'select 1'",
		"execute resource_accounting_stmt",
		"deallocate prepare resource_accounting_stmt",
	))
	assertPersistedResourceQuality(t, db, []string{
		"create database resource_accounting_lifecycle",
		"create table resource_accounting_lifecycle.t (id bigint)",
		"deallocate prepare resource_accounting_stmt",
	})
}

func TestResourceAccountingBVTMultiCN(t *testing.T) {
	db, closeCluster := startResourceAccountingCluster(t, 2)
	defer closeCluster()
	plan2.SetForceScanOnMultiCN(true)
	defer plan2.SetForceScanOnMultiCN(false)
	waitForBackendCNs(t, db, 2)

	require.NoError(t, execStatements(db,
		"drop database if exists resource_accounting_bvt",
		"create database resource_accounting_bvt",
		"use resource_accounting_bvt",
		"create table fact (id bigint primary key, payload varchar(32))",
		"insert into fact select result, concat('row-', result) from generate_series(1, 200000) g",
	))

	plan := queryExplainResource(t, db,
		"select sum(id), count(*) from resource_accounting_bvt.fact")
	assertResourceOverview(t, plan)
	addresses := make(map[string]struct{})
	for _, match := range regexp.MustCompile(`addr:([^, )]+)`).FindAllStringSubmatch(plan, -1) {
		addresses[match[1]] = struct{}{}
	}
	require.GreaterOrEqual(t, len(addresses), 2,
		"multi-CN acceptance query did not dispatch a remote fragment")
}

func waitForBackendCNs(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		rows, err := db.Query("show backend servers")
		if err != nil {
			return false
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return false
		}
		count := 0
		for rows.Next() {
			values := make([]sql.RawBytes, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if rows.Scan(dest...) != nil {
				return false
			}
			count++
		}
		return rows.Err() == nil && count >= want
	}, 15*time.Second, 200*time.Millisecond)
}

func startResourceAccountingCluster(t *testing.T, cnCount int) (*sql.DB, func()) {
	t.Helper()
	c, err := NewCluster(
		WithCNCount(cnCount),
		WithTesting(),
		WithPreStart(func(svc ServiceOperator) {
			if svc.ServiceType() != metadata.ServiceType_CN {
				return
			}
			svc.Adjust(func(config *ServiceConfig) {
				config.Observability.TraceExportInterval = 1
				config.Observability.LongQueryTime = 0
				config.Observability.DisableStmtAggregation = true
			})
		}),
	)
	require.NoError(t, err)
	require.NoError(t, c.Start())

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=5s&readTimeout=30s&writeTimeout=30s",
		cn.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())

	return db, func() {
		require.NoError(t, db.Close())
		require.NoError(t, c.Close())
	}
}

func execStatements(db *sql.DB, statements ...string) error {
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func queryExplainResource(t *testing.T, db *sql.DB, statement string) string {
	t.Helper()
	rows, err := db.Query("explain phyplan analyze " + statement)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

func assertResourceOverview(t *testing.T, plan string) {
	t.Helper()
	require.Contains(t, plan, "Overview:")
	require.Contains(t, plan, "Attempts:1")
	require.Contains(t, plan, "Quality:complete")
	for _, field := range []string{"ActiveTime", "WaitTime", "PeakMemory"} {
		match := regexp.MustCompile(field + `:([0-9]+)`).FindStringSubmatch(plan)
		require.Len(t, match, 2, "%s missing from resource overview", field)
		value, err := strconv.ParseUint(match[1], 10, 64)
		require.NoError(t, err)
		if field == "ActiveTime" {
			require.Positive(t, value, "%s must be positive", field)
		}
	}
}

func assertPersistedResourceQuality(t *testing.T, db *sql.DB, statements []string) {
	t.Helper()
	wanted := make(map[string]struct{}, len(statements))
	for _, statement := range statements {
		wanted[statement] = struct{}{}
	}
	last := "no rows"
	satisfied := assert.Eventually(t, func() bool {
		rows, err := db.Query(`select statement, stats from system.statement_info
			where status != 'Running' and statement in (` + sqlStringList(statements) + `)`)
		if err != nil {
			last = err.Error()
			return false
		}
		defer rows.Close()
		seen := make(map[string]struct{}, len(statements))
		observed := make([]string, 0, len(statements))
		for rows.Next() {
			var statement, encoded string
			if rows.Scan(&statement, &encoded) != nil {
				return false
			}
			observed = append(observed, statement+"="+encoded)
			var stats []float64
			if json.Unmarshal([]byte(encoded), &stats) != nil || len(stats) <= 11 ||
				stats[0] != 6 || stats[11] != 0 {
				return false
			}
			seen[statement] = struct{}{}
		}
		last = strings.Join(observed, "; ")
		if rows.Err() != nil {
			return false
		}
		for statement := range wanted {
			if _, ok := seen[statement]; !ok {
				return false
			}
		}
		return true
	}, 20*time.Second, 250*time.Millisecond)
	if !satisfied {
		require.FailNowf(t, "persisted resource quality did not converge", "last observation: %s", last)
	}
}

func sqlStringList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
	}
	return strings.Join(quoted, ",")
}
