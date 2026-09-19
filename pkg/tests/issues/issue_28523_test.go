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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue28523NumericCompatibilityOverBinaryPreparedStatement checks the
// actual COM_STMT path: changing sql_mode must re-evaluate an already-prepared
// expression, and MATRIXONE_NATIVE must remain strict when both mode bits exist.
func TestIssue28523NumericCompatibilityOverBinaryPreparedStatement(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		// sql_mode and prepared-statement state are connection-local.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		_, err = db.ExecContext(ctx, "SET SESSION sql_mode = ''")
		require.NoError(t, err)
		stmt, err := db.PrepareContext(ctx, "SELECT ABS(?)")
		require.NoError(t, err)
		defer stmt.Close()

		query := func(value string) (float64, error) {
			var got float64
			err := stmt.QueryRowContext(ctx, value).Scan(&got)
			return got, err
		}
		_, err = query("1.5tail")
		require.Error(t, err, "empty sql_mode must use strict complete-token parsing")
		require.Contains(t, err.Error(), "invalid numeric string")

		_, err = db.ExecContext(ctx, "SET SESSION sql_mode = 'MYSQL_NUMERIC_COMPATIBILITY'")
		require.NoError(t, err)
		got, err := query("1.5tail")
		require.NoError(t, err)
		require.Equal(t, 1.5, got)
		got, err = query("  -1.25e2 ")
		require.NoError(t, err, "a complete signed exponent remains valid")
		require.Equal(t, 125.0, got)

		// Exercise both directions on the same cached COM_STMT handle.
		_, err = db.ExecContext(ctx, "SET SESSION sql_mode = ''")
		require.NoError(t, err)
		_, err = query("1.5tail")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "SET SESSION sql_mode = 'MYSQL_NUMERIC_COMPATIBILITY'")
		require.NoError(t, err)
		got, err = query("1.5tail")
		require.NoError(t, err)
		require.Equal(t, 1.5, got)

		_, err = db.ExecContext(ctx, "SET SESSION sql_mode = 'MATRIXONE_NATIVE,MYSQL_NUMERIC_COMPATIBILITY'")
		require.NoError(t, err)
		_, err = query("1.5tail")
		require.Error(t, err, "MATRIXONE_NATIVE must take precedence over compatibility")
		require.Contains(t, err.Error(), "invalid numeric string")
	})
}
