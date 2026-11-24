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
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestExplainAnalyzeCarriesRoleRule(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()

		const (
			dbName   = "explain_analyze_role_rule"
			target   = dbName + ".target"
			source   = dbName + ".authorized_source"
			role     = "explain_analyze_rule_role"
			user     = "explain_analyze_rule_user"
			password = "ExplainAnalyzeRule01"
			query    = "select id from " + target
		)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists "+user,
			"drop role if exists "+role,
			"drop database if exists "+dbName,
		)
		execSQLRequire(t, ctx, admin, "create database "+dbName)
		execSQLRequire(t, ctx, admin, "create table "+target+" (id int)")
		execSQLRequire(t, ctx, admin, "create table "+source+" (id int)")
		// The target is empty. The rule supplies one synthetic row from a table
		// the role also has permission to read.
		execSQLRequire(t, ctx, admin, "insert into "+source+" values (42)")
		var baseRows int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+target).Scan(&baseRows))
		require.Zero(t, baseRows)

		execSQLRequire(t, ctx, admin, "create role "+role)
		execSQLRequire(t, ctx, admin, "alter role "+role+
			" add rule \"select id from "+source+"\" on table "+target)
		execSQLRequire(t, ctx, admin, "create user "+user+" identified by '"+password+"' default role "+role)
		execSQLRequire(t, ctx, admin, "grant connect on account * to "+role)
		execSQLRequire(t, ctx, admin, "grant select on table "+target+" to "+role)
		execSQLRequire(t, ctx, admin, "grant select on table "+source+" to "+role)

		client, err := sql.Open("mysql", fmt.Sprintf(
			"sys#%s#%s:%s@tcp(127.0.0.1:%d)/", user, role, password, port))
		require.NoError(t, err)
		defer client.Close()
		conn, err := client.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		// Both statements go through the same connection with no parameters,
		// using the MySQL text protocol.
		queryColumn := func(statement string) []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
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
		_, err = conn.ExecContext(ctx, "set enable_remap_hint = 1")
		require.NoError(t, err)
		require.Equal(t, []string{"42"}, queryColumn(query))

		lines := queryColumn("explain analyze " + query)
		require.NotEmpty(t, lines)
		plan := strings.Join(lines, "\n")
		require.Contains(t, plan, "Table Scan on "+source)

		// Text plans list each node before its details and children. Read only
		// the initial node's Analyze field and stop at the next node.
		var rootAnalyze string
		for _, line := range lines[1:] {
			detail := strings.TrimSpace(line)
			if line == detail || strings.HasPrefix(detail, "->") {
				break
			}
			if strings.HasPrefix(detail, "Analyze:") {
				rootAnalyze = detail
				break
			}
		}
		match := regexp.MustCompile(`\boutputRows=(\d+)\b`).FindStringSubmatch(rootAnalyze)
		require.Len(t, match, 2, "missing result root outputRows in plan:\n%s", plan)
		outputRows, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equal(t, 1, outputRows, "result root outputRows")
	})
}
