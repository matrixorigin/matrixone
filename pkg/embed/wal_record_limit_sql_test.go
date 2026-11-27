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
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOversizeWALCommitRollsBackOverMySQL(t *testing.T) {
	// WAL admission is configured when TN starts, so this public-path check
	// needs its own small cluster instead of the shared default-limit fixture.
	const walLimit = 2 << 20
	c, err := StartTestCluster(WithCNCount(1), WithPreStart(func(svc ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_TN {
			svc.Adjust(func(cfg *ServiceConfig) {
				cfg.TN_please_use_getTNServiceConfig.LogService.MaxWALRecordSize = walLimit
			})
		}
	}))
	if c != nil {
		t.Cleanup(func() { assert.NoError(t, c.Close()) })
	}
	require.NoError(t, err)

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	const database = "wal_record_limit_sql"
	_, err = conn.ExecContext(ctx, "create database "+database)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, cleanupErr := conn.ExecContext(cleanupCtx, "drop database if exists "+database)
		assert.NoError(t, cleanupErr)
	})
	// CN data inserts are written to S3 objects and do not put their payload
	// into TN WAL. Catalog bootstrap needs more than 1 MiB, so this 2 MiB
	// startup limit and one wide table exercise record admission with a small
	// number of SQL statements.
	const columns = 384
	comments := make([]byte, columns*512)
	state := uint64(1)
	for i := range comments {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		comments[i] = byte(state)
	}
	defs := make([]string, 0, columns+1)
	defs = append(defs, "id int primary key")
	for i := range columns {
		defs = append(defs, fmt.Sprintf("c%d int comment '%s'", i,
			hex.EncodeToString(comments[i*512:(i+1)*512])))
	}
	_, err = conn.ExecContext(ctx, "begin")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table "+database+".before_rejection (id int)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "create table "+database+".oversize_schema ("+strings.Join(defs, ",")+")")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "commit")
	require.ErrorContains(t, err, "WAL record is too large")

	var count int
	tableCount := "select count(*) from mo_catalog.mo_tables where reldatabase = '" + database + "'"
	require.NoError(t, db.QueryRowContext(ctx, tableCount).Scan(&count))
	require.Zero(t, count, "the failed transaction must leave no committed tables")
	_, err = conn.ExecContext(ctx, "begin")
	require.NoError(t, err, "the same client must be able to start another transaction")
	_, err = conn.ExecContext(ctx, "create table "+database+".after_rejection (id int)")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "commit")
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, tableCount).Scan(&count))
	require.Equal(t, 1, count)
}
