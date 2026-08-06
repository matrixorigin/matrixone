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
	"errors"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

func TestIssue25299RegexpRejectsBinaryCharset(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		assertCharacterSetMismatch := func(query string) {
			t.Helper()
			_, execErr := conn.ExecContext(ctx, query)
			require.Error(t, execErr)
			var mysqlErr *mysqlDriver.MySQLError
			require.True(t, errors.As(execErr, &mysqlErr), "expected MySQL protocol error, got %T: %v", execErr, execErr)
			require.Equal(t, uint16(moerr.ER_CHARACTER_SET_MISMATCH), mysqlErr.Number)
			require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
		}

		assertCharacterSetMismatch("select binary 'abc' regexp 'a'")
		assertCharacterSetMismatch("select regexp_instr('abc', binary 'a')")
		assertCharacterSetMismatch("select regexp_replace('abc', 'a', binary 'x')")
		assertCharacterSetMismatch("select cast(null as binary) regexp 'a'")

		var binaryMatched bool
		require.NoError(t, conn.QueryRowContext(ctx,
			"select _binary 'a' regexp _binary 'a'").Scan(&binaryMatched))
		require.True(t, binaryMatched)
		var nullBinaryMatched sql.NullBool
		require.NoError(t, conn.QueryRowContext(ctx,
			"select null regexp _binary 'a'").Scan(&nullBinaryMatched))
		require.False(t, nullBinaryMatched.Valid)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select 1 regexp _binary '1'").Scan(&binaryMatched))
		require.True(t, binaryMatched)
		var allBinaryReplace string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_replace(_binary 'abc', _binary 'a', _binary 'X')").Scan(&allBinaryReplace))
		require.Equal(t, "Xbc", allBinaryReplace)

		var instr int64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_instr('Cat', 'cat', 1, 1, 0, _binary 'i')").Scan(&instr))
		require.Equal(t, int64(1), instr)

		_, err = conn.ExecContext(ctx, "select regexp_replace('Cat', 'cat', 'X', 1, 0, 'x')")
		require.Error(t, err)
		var matchTypeErr *mysqlDriver.MySQLError
		require.True(t, errors.As(err, &matchTypeErr), "expected MySQL protocol error, got %T: %v", err, err)
		require.Equal(t, uint16(moerr.ER_WRONG_ARGUMENTS), matchTypeErr.Number)
		require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, matchTypeErr.SQLState)

		assertRegexpError := func(query string, code uint16) {
			t.Helper()
			_, execErr := conn.ExecContext(ctx, query)
			require.Error(t, execErr)
			var mysqlErr *mysqlDriver.MySQLError
			require.True(t, errors.As(execErr, &mysqlErr), "expected MySQL protocol error, got %T: %v", execErr, execErr)
			require.Equal(t, code, mysqlErr.Number)
			require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
		}
		assertRegexpError("select regexp_replace('a', '(a)', '$2')", moerr.ER_REGEXP_INDEX_OUTOFBOUNDS_ERROR)
		assertRegexpError("select regexp_replace('a', '(a)', '${1}')", moerr.ER_REGEXP_INVALID_CAPTURE_GROUP_NAME)
		assertRegexpError("select regexp_instr(null, '', 1, 1, 0, 'c')", moerr.ER_REGEXP_ILLEGAL_ARGUMENT)
		assertRegexpError("select regexp_replace(null, 'a', 'X', 1, 0, 'x')", moerr.ER_WRONG_ARGUMENTS)

		_, err = conn.ExecContext(ctx, "set @regexp_binary_param = binary 'abc'")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "prepare regexp_binary_stmt from 'select regexp_like(?, ''a'')'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare regexp_binary_stmt")

		var matched bool
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_binary_stmt using @regexp_binary_param").Scan(&matched))
		require.True(t, matched)
		_, err = conn.ExecContext(ctx, "set @regexp_binary_null = cast(null as binary)")
		require.NoError(t, err)
		var nullMatched sql.NullBool
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_binary_stmt using @regexp_binary_null").Scan(&nullMatched))
		require.False(t, nullMatched.Valid)

		_, err = conn.ExecContext(ctx,
			"prepare regexp_cast_stmt from 'select regexp_like(cast(? as char), ''a'')'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare regexp_cast_stmt")
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_cast_stmt using @regexp_binary_param").Scan(&matched))
		require.True(t, matched)

		_, err = conn.ExecContext(ctx, "set @regexp_text_param = 'abc'")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_binary_stmt using @regexp_text_param").Scan(&matched))
		require.True(t, matched)
	})
}
