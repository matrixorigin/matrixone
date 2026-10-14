// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentViewAuthoringIsRejected(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "user_var_assignment_29909"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		require.NoError(t, db.PingContext(ctx))
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create view v as select 17 as x")
		require.NoError(t, err)

		for _, statement := range []string{
			"create or replace view v as select @view_var := 1 as x",
			"alter view v as select @view_var := 2 as x",
		} {
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "view definitions is not supported")

			var value int
			require.NoError(t, db.QueryRowContext(ctx, "select x from v").Scan(&value))
			require.Equal(t, 17, value)
		}
	})
}

func TestUserVariableAssignmentNestedReadBeforeWriteIsRejected(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, db.PingContext(ctx))

		for _, statement := range []string{
			"select @b as prior, @a := @b := @b + 1 as current",
			"select @B as prior, @a := @b := @B + 1 as current",
		} {
			_, err = db.ExecContext(ctx, "set @b = 0")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, statement)
			require.ErrorContains(t, err, "reading a user variable before its assignment projection")

			var retained int
			require.NoError(t, db.QueryRowContext(ctx, "select @b").Scan(&retained))
			require.Equal(t, 0, retained)

			var outer sql.NullString
			require.NoError(t, db.QueryRowContext(ctx, "select @a").Scan(&outer))
			require.False(t, outer.Valid, "the rejected admission path must not publish the outer assignment")
		}
	})
}
