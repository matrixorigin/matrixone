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
	"crypto/sha256"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func statementDigestReturnType(_ []types.Type) types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func statementDigestSQLMode(proc *process.Process) (string, error) {
	sqlMode, err := process.ResolveSQLMode(proc)
	if err != nil {
		return "", err
	}
	if sqlMode == process.EmptySqlModeSentinel {
		return "", nil
	}
	return sqlMode, nil
}

// statementDigestDeparse returns the existing MySQL-dialect AST rendering used
// by MatrixOne's SQL formatter. It does not claim MySQL STATEMENT_DIGEST
// compatibility; the deparsed bytes are this build's statement-hash contract.
func statementDigestDeparse(stmt tree.Statement) (formatted string, err error) {
	if stmt == nil {
		return "", moerr.NewNotSupportedNoCtx("cannot format a nil statement for STATEMENT_DIGEST")
	}
	defer func() {
		if recover() != nil {
			formatted = ""
			err = moerr.NewInternalErrorNoCtx("AST formatter failed during STATEMENT_DIGEST")
		}
	}()

	formatted = tree.StringWithOpts(
		stmt,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
		tree.WithCanonicalUserVariableNames(),
	)
	if formatted == "" {
		return "", moerr.NewNotSupportedNoCtx("AST formatter produced empty output for STATEMENT_DIGEST")
	}
	return formatted, nil
}

func statementDigestValue(ctx context.Context, sql string, sqlMode string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	statements, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, sql, 0, sqlMode)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, stmt := range statements {
			if stmt != nil {
				stmt.Free()
			}
		}
	}()

	if len(statements) != 1 {
		return nil, moerr.NewParseError(ctx, "STATEMENT_DIGEST requires exactly one statement")
	}
	if _, empty := statements[0].(*tree.EmptyStmt); empty {
		return nil, moerr.NewParseError(ctx, "STATEMENT_DIGEST requires a non-empty statement")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	formatted, err := statementDigestDeparse(statements[0])
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(formatted))
	encoded := make([]byte, hex.EncodedLen(len(hash)))
	hex.Encode(encoded, hash[:])
	return encoded, nil
}

// StatementDigest hashes the MySQL-dialect rendering of one MatrixOne-parsed
// statement. This is a build-scoped MatrixOne statement hash, not MySQL's
// normalized-token STATEMENT_DIGEST. Whitespace, comments, and equivalent
// formatting and case-insensitive user-variable names normalize through the
// AST formatter; literal values and distinct variable/identifier names remain
// part of the hash.
func StatementDigest(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	// The generic unary helper's constant fast path evaluates its argument even
	// for a zero-row batch. Preserve the no-row contract before that path can
	// resolve session state or read row zero.
	if length == 0 {
		return nil
	}

	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	var (
		sqlMode         string
		sqlModeResolved bool
		sqlModeErr      error
	)

	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(input []byte) ([]byte, error) {
		// Resolve mode only for an active, non-NULL input row. SQL expressions
		// skipped by NULL propagation or short-circuit masks must not surface
		// errors from a session-variable resolver they never evaluate.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !sqlModeResolved {
			sqlMode, sqlModeErr = statementDigestSQLMode(proc)
			sqlModeResolved = true
		}
		if sqlModeErr != nil {
			return nil, sqlModeErr
		}
		return statementDigestValue(ctx, string(input), sqlMode)
	}, selectList)
}
