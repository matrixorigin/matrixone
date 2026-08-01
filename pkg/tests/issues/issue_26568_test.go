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

const issue26568Rows = 130_000

const issue26568JoinSQL = `
	select count(*)
	from po_l as l
	join po_r as r on l.id = r.id`

type issue26568ExplainAttack struct {
	name      string
	hint      string
	statement string
}

// TestIssue26568ExplainMultiCNHashJoin exercises the same SQL-protocol path as
// the reported failure through two independent frontend coordinators, then
// attacks the same plan contract through alternative stats and renderers.
func TestIssue26568ExplainMultiCNHashJoin(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		cn0Port := cn0.GetServiceConfig().CN.Frontend.Port
		setupIssue26568Tables(t, ctx, cn0Port, dbName)

		for _, index := range []int{0, 1} {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			t.Run(fmt.Sprintf("coordinator-cn-%d", index), func(t *testing.T) {
				runIssue26568ExplainAttack(t, ctx, cn.GetServiceConfig().CN.Frontend.Port, dbName,
					issue26568ExplainAttack{
						name:      "reported logical plan",
						hint:      "execType=2",
						statement: "explain " + issue26568JoinSQL,
					})
			})
		}

		attacks := []issue26568ExplainAttack{
			{
				name:      "minimal stats replacement",
				hint:      "execType=1",
				statement: "explain " + issue26568JoinSQL,
			},
			{
				name:      "big stats verbose renderer",
				hint:      "execType=2",
				statement: "explain verbose " + issue26568JoinSQL,
			},
			{
				name:      "huge stats replacement",
				hint:      "execType=3",
				statement: "explain " + issue26568JoinSQL,
			},
			{
				name: "multiple shuffle pass markers",
				hint: "execType=2",
				statement: `explain
					select count(*)
					from po_l as l
					join po_r as r on l.id = r.id
					join po_l as l2 on l2.id = r.id`,
			},
			{
				name:      "physical plan renderer",
				hint:      "execType=2",
				statement: "explain phyplan " + issue26568JoinSQL,
			},
			{
				name:      "analyze renderer",
				hint:      "execType=2",
				statement: "explain analyze " + issue26568JoinSQL,
			},
		}
		for _, attack := range attacks {
			t.Run("attack/"+attack.name, func(t *testing.T) {
				runIssue26568ExplainAttack(t, ctx, cn0Port, dbName, attack)
			})
		}
	})
}

func runIssue26568ExplainAttack(
	t *testing.T,
	ctx context.Context,
	port int64,
	dbName string,
	attack issue26568ExplainAttack,
) {
	t.Helper()
	conn := openIssue26568Conn(t, ctx, port)
	defer conn.Close()

	issue26568Exec(t, ctx, conn, "set role moadmin")
	issue26568Exec(t, ctx, conn, "use `"+dbName+"`")
	issue26568Exec(t, ctx, conn,
		fmt.Sprintf(`set session optimizer_hints = "%s"`, attack.hint))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err := conn.ExecContext(cleanupCtx, `set session optimizer_hints = ""`)
		require.NoError(t, err)
	}()
	issue26568Exec(t, ctx, conn, "set session join_spill_mem = 1073741824")

	planText, err := testutils.QueryText(ctx, conn, attack.statement)
	require.NoErrorf(t, err, "attack failed: %s", attack.name)
	require.NotEmpty(t, planText)
	require.Contains(t, strings.ToLower(planText), "join")
	for _, line := range strings.Split(planText, "\n") {
		line = strings.TrimSpace(line)
		require.NotEqual(t, "Runtime Filter Probe:", line)
		require.NotEqual(t, "Runtime Filter Build:", line)
	}
}

func setupIssue26568Tables(t *testing.T, ctx context.Context, port int64, dbName string) {
	t.Helper()
	conn := openIssue26568Conn(t, ctx, port)
	defer conn.Close()

	issue26568Exec(t, ctx, conn, "set role moadmin")
	issue26568Exec(t, ctx, conn, "drop database if exists `"+dbName+"`")
	t.Cleanup(func() {
		cleanupIssue26568Database(t, port, dbName)
	})
	issue26568Exec(t, ctx, conn, "create database `"+dbName+"`")
	issue26568Exec(t, ctx, conn, "use `"+dbName+"`")
	issue26568Exec(t, ctx, conn, "create table po_l (id bigint primary key)")
	issue26568Exec(t, ctx, conn, "create table po_r (id bigint primary key)")
	issue26568Exec(t, ctx, conn, fmt.Sprintf(
		"insert into po_l select result from generate_series(1, %d) g", issue26568Rows))
	issue26568Exec(t, ctx, conn, fmt.Sprintf(
		"insert into po_r select result from generate_series(1, %d) g", issue26568Rows))
}

func openIssue26568Conn(t *testing.T, ctx context.Context, port int64) *sql.Conn {
	t.Helper()
	db, err := sql.Open("mysql", issue26568DSN(port))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	return conn
}

func cleanupIssue26568Database(t *testing.T, port int64, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", issue26568DSN(port))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	issue26568Exec(t, ctx, db, "set role moadmin")
	issue26568Exec(t, ctx, db, "drop database if exists `"+dbName+"`")
}

func issue26568DSN(port int64) string {
	return fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
}

func issue26568Exec(t *testing.T, ctx context.Context, conn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, statement string) {
	t.Helper()
	_, err := conn.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}
