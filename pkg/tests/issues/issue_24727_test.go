// Copyright 2021 - 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue24727MultiRowPreparedInsertUUIDStrings(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		address := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)

		for _, interpolateParams := range []bool{false, true} {
			t.Run("interpolateParams="+strconv.FormatBool(interpolateParams), func(t *testing.T) {
				database := strings.ToLower(strings.ReplaceAll(testutils.GetDatabaseName(t), "/", "_"))
				root, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(%s)/?timeout=5s&readTimeout=15s&writeTimeout=15s", address))
				require.NoError(t, err)
				require.NoError(t, root.PingContext(ctx))
				require.NoError(t, execIssue24727(ctx, root, "create database `"+database+"`"))
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
					defer cleanupCancel()
					_ = execIssue24727(cleanupCtx, root, "drop database if exists `"+database+"`")
					_ = root.Close()
				})

				db, err := sql.Open("mysql", fmt.Sprintf(
					"dump:111@tcp(%s)/%s?interpolateParams=%t&timeout=5s&readTimeout=15s&writeTimeout=15s",
					address, database, interpolateParams))
				require.NoError(t, err)
				defer db.Close()
				require.NoError(t, db.PingContext(ctx))
				require.NoError(t, execIssue24727(ctx, db, `create table volume_files (
					id bigint auto_increment primary key,
					volume_id bigint not null,
					file_id varchar(36) not null,
					file_name varchar(255) not null,
					file_path varchar(1024) not null default '',
					created_by varchar(64) not null,
					updated_by varchar(64) not null,
					unique key uk_volume_file (volume_id, file_id))`))

				for _, mode := range []string{"db-exec", "tx-exec", "explicit-prepare"} {
					require.NoError(t, execIssue24727(ctx, db, "delete from volume_files"))
					require.NoError(t, executeIssue24727Insert(ctx, db, mode))
					assertIssue24727Rows(t, ctx, db)
				}

				// The same UUID text without a parameter must remain a rejected SQL
				// expression, and must not leave a partially inserted row behind.
				require.NoError(t, execIssue24727(ctx, db, "delete from volume_files"))
				_, err = db.ExecContext(ctx, `insert into volume_files
					(volume_id,file_id,file_name,file_path,created_by,updated_by)
					values (180001,ad027c8b-1111-4111-8111-111111111111,'bad.txt','','user1','user1')`)
				require.Error(t, err)
				var count int
				require.NoError(t, db.QueryRowContext(ctx, "select count(*) from volume_files").Scan(&count))
				require.Zero(t, count)
			})
		}
	})
}

func executeIssue24727Insert(ctx context.Context, db *sql.DB, mode string) error {
	const insert = `insert into volume_files
		(volume_id,file_id,file_name,file_path,created_by,updated_by)
		values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)`
	args := []any{
		int64(180001), "ad027c8b-1111-4111-8111-111111111111", "a.txt", "", "user1", "user1",
		int64(180001), "b4027c8b-2222-4222-8222-222222222222", "b.txt", "", "user1", "user1",
	}
	switch mode {
	case "db-exec":
		_, err := db.ExecContext(ctx, insert, args...)
		return err
	case "tx-exec":
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, args...); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	case "explicit-prepare":
		stmt, err := db.PrepareContext(ctx, insert)
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, args...)
		return err
	default:
		return fmt.Errorf("unknown issue 24727 insert mode %q", mode)
	}
}

func assertIssue24727Rows(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select volume_id,file_id,file_name,file_path,created_by,updated_by from volume_files order by file_id")
	require.NoError(t, err)
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var volumeID int64
		var fileID, fileName, filePath, createdBy, updatedBy string
		require.NoError(t, rows.Scan(&volumeID, &fileID, &fileName, &filePath, &createdBy, &updatedBy))
		actual = append(actual, fmt.Sprintf("%d|%s|%s|%s|%s|%s", volumeID, fileID, fileName, filePath, createdBy, updatedBy))
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{
		"180001|ad027c8b-1111-4111-8111-111111111111|a.txt||user1|user1",
		"180001|b4027c8b-2222-4222-8222-222222222222|b.txt||user1|user1",
	}, actual)
}

func execIssue24727(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, statement string) error {
	_, err := db.ExecContext(ctx, statement)
	return err
}
