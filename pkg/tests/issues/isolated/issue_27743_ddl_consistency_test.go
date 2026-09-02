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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// The v44 epoch is monotonic and intentionally cannot be rolled back. Keep this
// activation regression in a dedicated process so it cannot alter the shared
// issue-test cluster used by unrelated tests.
func TestIssue27743DDLConsistency(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	require.NoError(t, err)
	defer func() { require.NoError(t, cluster.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	db0 := openIssue27743DB(t, cn0.GetServiceConfig().CN.Frontend.Port)
	defer db0.Close()
	db1 := openIssue27743DB(t, cn1.GetServiceConfig().CN.Frontend.Port)
	defer db1.Close()

	const database = "issue_27743_automatic_ddl_visibility"
	targets := cn0.ServiceID() + "," + cn1.ServiceID()
	var activation string
	require.NoError(t, db0.QueryRowContext(ctx,
		"select mo_ctl('cn', 'SetProtocolVersion', ?)", targets+":44").Scan(&activation))
	require.Contains(t, activation, cn0.ServiceID()+":44")
	require.Contains(t, activation, cn1.ServiceID()+":44")
	execIssue27743SQL(t, ctx, db0, "drop database if exists `"+database+"`")
	execIssue27743SQL(t, ctx, db0, "create database `"+database+"`")
	defer func() { _, _ = db0.ExecContext(context.Background(), "drop database if exists `"+database+"`") }()

	require.True(t, fault.Enable())
	defer fault.Disable()
	faultPointRemoved := false
	defer func() {
		if !faultPointRemoved {
			_, _ = fault.RemoveFaultPoint(context.Background(), cnservice.DDLVisibilitySyncCommitFaultPoint)
		}
	}()
	require.NoError(t, fault.AddFaultPoint(ctx,
		cnservice.DDLVisibilitySyncCommitFaultPoint, "1:1::", "return", 0, "expected", false))
	_, err = db0.ExecContext(ctx,
		"create table `"+database+"`.`must_fail_sync` (id int primary key)")
	require.ErrorContains(t, err, "injected DDL visibility sync commit error")
	_, err = fault.RemoveFaultPoint(ctx, cnservice.DDLVisibilitySyncCommitFaultPoint)
	require.NoError(t, err)
	faultPointRemoved = true

	execIssue27743SQL(t, ctx, db0,
		"create table `"+database+"`.`t` (id int primary key, payload varchar(32))")
	var count int
	require.NoError(t, db1.QueryRowContext(ctx,
		"select count(*) from `"+database+"`.`t`").Scan(&count))
	require.Zero(t, count)
}

func openIssue27743DB(t *testing.T, port int64) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s", port))
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func execIssue27743SQL(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoError(t, err)
}
