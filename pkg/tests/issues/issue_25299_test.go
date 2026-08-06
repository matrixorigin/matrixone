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
		var matched bool
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
		assertRegexpErrorWithState := func(query string, code uint16, state [5]byte) {
			t.Helper()
			_, execErr := conn.ExecContext(ctx, query)
			require.Error(t, execErr)
			var mysqlErr *mysqlDriver.MySQLError
			require.True(t, errors.As(execErr, &mysqlErr), "expected MySQL protocol error, got %T: %v", execErr, execErr)
			require.Equal(t, code, mysqlErr.Number)
			require.Equal(t, state, mysqlErr.SQLState)
		}
		assertRegexpError("select regexp_replace('a', '(a)', '$2')", moerr.ER_REGEXP_INDEX_OUTOFBOUNDS_ERROR)
		assertRegexpError("select regexp_replace('a', '(a)', '${1}')", moerr.ER_REGEXP_INVALID_CAPTURE_GROUP_NAME)
		assertRegexpError("select regexp_instr(null, '', 1, 1, 0, 'c')", moerr.ER_REGEXP_ILLEGAL_ARGUMENT)
		assertRegexpError("select regexp_replace(null, 'a', 'X', 1, 0, 'x')", moerr.ER_WRONG_ARGUMENTS)
		assertRegexpError("select regexp_like('a', '*')", moerr.ER_REGEXP_RULE_SYNTAX)
		assertRegexpError("select regexp_like('a', '(')", moerr.ER_REGEXP_MISMATCHED_PAREN)
		assertRegexpError("select regexp_like('a', '[z-a]')", moerr.ER_REGEXP_INVALID_RANGE)
		assertRegexpError("select regexp_instr(null, 'a', 1, 1, -1, 'c')", moerr.ER_WRONG_ARGUMENTS)
		assertRegexpError("select regexp_instr(null, '*', 1, 1, -1, 'c')", moerr.ER_WRONG_ARGUMENTS)
		assertRegexpError("select regexp_replace(null, '', null)", moerr.ER_REGEXP_ILLEGAL_ARGUMENT)
		assertRegexpErrorWithState(
			"select regexp_substr(null, 'a', 0, 1, 'c')",
			moerr.ER_WRONG_PARAMETERS_TO_NATIVE_FCT, [5]byte{'4', '2', '0', '0', '0'})
		assertRegexpErrorWithState(
			"select regexp_replace(null, 'a', 'X', 0, 0, 'c')",
			moerr.ER_WRONG_PARAMETERS_TO_NATIVE_FCT, [5]byte{'4', '2', '0', '0', '0'})

		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_instr(_binary 0xc3a961, _binary 0x61)").Scan(&instr))
		require.Equal(t, int64(3), instr)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_like(_binary 0xff, _binary 0xfe)").Scan(&matched))
		require.False(t, matched)
		var binaryHex string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select hex(regexp_replace(_binary 0xc3a961, _binary 0x61, _binary 0x58))").Scan(&binaryHex))
		require.Equal(t, "C3A958", binaryHex)

		var emptyInstr int64
		var emptySubstr, endSubstr, endReplace string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_instr('', 'a*', 1, 1, 0, 'c'), "+
				"regexp_substr('', 'a*', 1, 1, 'c'), "+
				"regexp_substr('a', '$', 2, 1, 'c'), "+
				"regexp_replace('a', '$', 'X', 2, 0, 'c')").
			Scan(&emptyInstr, &emptySubstr, &endSubstr, &endReplace))
		require.Equal(t, int64(1), emptyInstr)
		require.Empty(t, emptySubstr)
		require.Empty(t, endSubstr)
		require.Equal(t, "aX", endReplace)

		var occurrenceReplacement string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_replace('Cat Dog Cat Dog Cat', 'Cat', 'Tiger', 1, 2)").
			Scan(&occurrenceReplacement))
		require.Equal(t, "Cat Dog Tiger Dog Cat", occurrenceReplacement)

		var substrCharset, replaceCharset, numericBinaryHex string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select charset(regexp_substr(123, _binary '.')), "+
				"charset(regexp_replace(123, _binary '.', _binary 0xff)), "+
				"hex(regexp_replace(123, _binary '.', _binary 0xff))").
			Scan(&substrCharset, &replaceCharset, &numericBinaryHex))
		require.Equal(t, "binary", substrCharset)
		require.Equal(t, "binary", replaceCharset)
		require.Equal(t, "FFFFFF", numericBinaryHex)

		maskedRows, err := conn.QueryContext(ctx,
			"select id, id = 2 and regexp_like('a', pat) "+
				"from (values row(1, '*'), row(2, 'a')) t(id, pat) order by id")
		require.NoError(t, err)
		defer maskedRows.Close()
		for expectedID := int64(1); expectedID <= 2; expectedID++ {
			require.True(t, maskedRows.Next())
			var id int64
			var value bool
			require.NoError(t, maskedRows.Scan(&id, &value))
			require.Equal(t, expectedID, id)
			require.Equal(t, expectedID == 2, value)
		}
		require.NoError(t, maskedRows.Err())

		var operatorCR, likeCR bool
		require.NoError(t, conn.QueryRowContext(ctx,
			"select '\r' regexp '.', regexp_like('\r', '.')").Scan(&operatorCR, &likeCR))
		require.False(t, operatorCR)
		require.False(t, likeCR)
		var digit, word, whitespace, alpha bool
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_like('١', '\\\\d'), "+
				"regexp_like('中', '\\\\w'), "+
				"regexp_like(convert(char(194,160) using utf8mb4), '\\\\s'), "+
				"regexp_like('中', '[[:alpha:]]')").Scan(&digit, &word, &whitespace, &alpha))
		require.True(t, digit)
		require.True(t, word)
		require.True(t, whitespace)
		require.True(t, alpha)

		var unixDot bool
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_like('\r', '.', 'u')").Scan(&unixDot))
		require.True(t, unixDot)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_instr('a\r', '$', 1, 1, 0, 'c')").Scan(&instr))
		require.Equal(t, int64(2), instr)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_instr('a\r\n', '$', 1, 1, 0, 'u')").Scan(&instr))
		require.Equal(t, int64(3), instr)

		var replacement string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_replace('ab', '\\\\bb', 'X', 2, 0, 'c')").Scan(&replacement))
		require.Equal(t, "ab", replacement)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select regexp_replace('a', '(a)', convert(char(92,36,49) using utf8mb4))").Scan(&replacement))
		require.Equal(t, "$1", replacement)
		assertRegexpError(
			"select regexp_like(concat(repeat('a', 30), 'b'), '(a|aa)+$')", moerr.ER_REGEXP_TIME_OUT)

		_, err = conn.ExecContext(ctx, "set @regexp_binary_param = _binary 0xc3a961")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "prepare regexp_binary_stmt from 'select regexp_like(?, ''a'')'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare regexp_binary_stmt")

		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_binary_stmt using @regexp_binary_param").Scan(&matched))
		require.True(t, matched)

		_, err = conn.ExecContext(ctx,
			"prepare regexp_binary_position_stmt from "+
				"'select regexp_instr(?,''a'',1,1,0,''c''), "+
				"hex(regexp_substr(?,''.'',1,1,''c'')), "+
				"hex(regexp_replace(?,''.'',''X'',2,1,''c''))'")
		require.NoError(t, err)
		defer conn.ExecContext(context.Background(), "deallocate prepare regexp_binary_position_stmt")
		var preparedInstr int64
		var preparedSubstrHex, preparedReplaceHex string
		require.NoError(t, conn.QueryRowContext(ctx,
			"execute regexp_binary_position_stmt using "+
				"@regexp_binary_param,@regexp_binary_param,@regexp_binary_param").
			Scan(&preparedInstr, &preparedSubstrHex, &preparedReplaceHex))
		require.Equal(t, int64(3), preparedInstr)
		require.Equal(t, "C383", preparedSubstrHex)
		require.Equal(t, "C3835861", preparedReplaceHex)
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

		protocolStmt, err := conn.PrepareContext(ctx, "select regexp_like(?, 'a')")
		require.NoError(t, err)
		defer protocolStmt.Close()
		require.NoError(t, protocolStmt.QueryRowContext(ctx, []byte("abc")).Scan(&matched))
		require.True(t, matched)
		var protocolNull sql.NullBool
		require.NoError(t, protocolStmt.QueryRowContext(ctx, []byte(nil)).Scan(&protocolNull))
		require.False(t, protocolNull.Valid)

		protocolCastStmt, err := conn.PrepareContext(ctx, "select regexp_like(cast(? as char), 'a')")
		require.NoError(t, err)
		defer protocolCastStmt.Close()
		require.NoError(t, protocolCastStmt.QueryRowContext(ctx, []byte("abc")).Scan(&matched))
		require.True(t, matched)
	})
}
