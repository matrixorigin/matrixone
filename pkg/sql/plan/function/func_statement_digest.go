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

package function

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	digest "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql/mysql_digest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const defaultMaxDigestLength = digest.DefaultMaxDigestLength

func statementDigestReturnType(_ []types.Type) types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func statementDigestSQLMode(proc *process.Process) (string, digest.SQLMode, error) {
	if proc == nil || proc.Base == nil || proc.GetSessionInfo() == nil {
		return "", 0, nil
	}

	sqlMode := proc.GetSessionInfo().SqlMode
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		value, err := resolve("sql_mode", true, false)
		if err != nil {
			return "", 0, err
		}
		if resolved, ok := value.(string); ok {
			// A remote/background resolver can contain an empty compiled
			// default. Preserve the coordinator snapshot in that case.
			if resolved != "" || proc.Base.IsFrontend || sqlMode == "" {
				sqlMode = resolved
			}
		}
	}
	if sqlMode == process.EmptySqlModeSentinel {
		sqlMode = ""
	}
	flags := mysql.ParseSQLModeFlags(sqlMode)
	var digestMode digest.SQLMode
	if flags.Has(mysql.SQLModeNoBackslashEscapes) {
		digestMode |= digest.ModeNoBackslashEscapes
	}
	if flags.Has(mysql.SQLModeANSIQuotes) {
		digestMode |= digest.ModeANSIQuotes
	}
	return mysql.SessionSQLModeForParser(sqlMode), digestMode, nil
}

func statementDigestMaxLength(proc *process.Process) (int, error) {
	if proc == nil || proc.Base == nil {
		return defaultMaxDigestLength, nil
	}
	// Remote processes have no session resolver. Preserve the coordinator's
	// snapshot, including an explicit zero, and keep it on subsequent forwards
	// even if a background resolver exposes its compiled default.
	if proc.Base.SessionInfo.MaxDigestLengthSet &&
		(!proc.Base.IsFrontend || proc.GetResolveVariableFunc() == nil) {
		return checkedStatementDigestMaxLength(proc.Base.SessionInfo.MaxDigestLength)
	}
	if proc.GetResolveVariableFunc() == nil {
		return defaultMaxDigestLength, nil
	}
	value, err := proc.GetResolveVariableFunc()("max_digest_length", true, true)
	if err != nil {
		return 0, err
	}
	var maxLength int64
	switch resolved := value.(type) {
	case int64:
		maxLength = resolved
	case uint64:
		if resolved > 1048576 {
			return 0, moerr.NewInternalErrorNoCtxf("max_digest_length is out of range: %d", resolved)
		}
		maxLength = int64(resolved)
	case int:
		maxLength = int64(resolved)
	case nil:
		return defaultMaxDigestLength, nil
	default:
		return 0, moerr.NewInternalErrorNoCtxf("unexpected max_digest_length type %T", value)
	}
	return checkedStatementDigestMaxLength(maxLength)
}

func checkedStatementDigestMaxLength(maxLength int64) (int, error) {
	if maxLength < 0 || maxLength > 1048576 {
		return 0, moerr.NewInternalErrorNoCtxf("max_digest_length is out of range: %d", maxLength)
	}
	return int(maxLength), nil
}

// StatementDigest returns the MySQL 8.x token digest of a SQL statement.
// MatrixOne's parser validates the statement because a token lexer alone cannot
// reject syntactically invalid SQL as MySQL's function does.
func StatementDigest(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	parserMode, digestMode, err := statementDigestSQLMode(proc)
	if err != nil {
		return err
	}
	maxDigestLength, err := statementDigestMaxLength(proc)
	if err != nil {
		return err
	}
	digester := digest.NewDigester(digest.Options{
		SQLMode:         digestMode,
		MaxDigestLength: &maxDigestLength,
	})

	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(input []byte) ([]byte, error) {
		sql := string(input)
		value, digestErr := digester.Digest(sql)
		stmt, err := mysql.ParseOneWithSQLMode(ctx, sql, 0, parserMode)
		if err != nil {
			// MySQL accepts a nonblank input made entirely of ordinary comments
			// and hashes its empty token stream. MatrixOne's ParseOne reports no
			// statement for that input, so distinguish it from empty/whitespace
			// input using the independent digest lexer result.
			if digestErr == nil && value.Text == "" && strings.TrimSpace(sql) != "" {
				return []byte(value.Hash), nil
			}
			return nil, err
		}
		stmt.Free()

		if digestErr != nil {
			return nil, digestErr
		}
		return []byte(value.Hash), nil
	}, selectList)
}
