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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestLeftJoinAntiRewritePreservesNullMarkerEvaluation(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		const database = "left_join_anti_marker"
		execSQLMaybe(t, ctx, db, "drop database if exists "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "create database "+database)
		execSQLRequire(t, ctx, db, "use "+database)
		execSQLRequire(t, ctx, db, "create table left_rows (id int primary key)")
		execSQLRequire(t, ctx, db, "create table right_rows (id int primary key)")
		execSQLRequire(t, ctx, db, "insert into left_rows values (1)")

		var count int
		err = db.QueryRowContext(ctx, `select count(*)
			from left_rows l left join right_rows r on l.id = r.id
			where coalesce(r.id, 0) is null`).Scan(&count)
		require.NoError(t, err)
		require.Zero(t, count,
			"a non-NULL fallback must continue to reject the NULL-extended row")

		err = db.QueryRowContext(ctx, `select count(*)
			from left_rows l left join right_rows r on l.id = r.id
			where json_object(r.id, 1) is null`).Scan(&count)
		require.Error(t, err)
		var mysqlErr *mysqlDriver.MySQLError
		require.True(t, errors.As(err, &mysqlErr), "expected MySQL error, got %T: %v", err, err)
		require.Equal(t, moerr.ErrInvalidInput, mysqlErr.Number)
	})
}
