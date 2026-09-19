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
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// Parsing a statement allocates a Go AST outside the query mpool. Keep the
	// input bounded so the AST, its source copy, and formatted output have a
	// bounded per-row lifetime and cannot grow with an arbitrary TEXT/BLOB value.
	maxStatementHashInputBytes = 1 << 20
	// The MySQL yacc parser grows its stack dynamically and AST formatting is
	// recursive. Bound lexical complexity and delimiter nesting before parsing.
	maxStatementHashTokenCount = 1 << 14
	maxStatementHashNesting    = 512
	// The formatter can add quoting and separators, but its output remains
	// proportional to the input AST. This bounds formatted bytes, not total Go
	// heap capacity or the AST itself.
	maxStatementHashFormattedBytes = 4 << 20
)

func statementHashReturnType(_ []types.Type) types.Type {
	typ := types.T_varchar.ToType()
	typ.Width = 64
	return typ
}

func statementHashSQLMode(proc *process.Process) (string, error) {
	sqlMode, err := process.ResolveSQLMode(proc)
	if err != nil {
		return "", err
	}
	if sqlMode == process.EmptySqlModeSentinel {
		return "", nil
	}
	return sqlMode, nil
}

// statementHashDeparse returns the existing MySQL-dialect AST rendering used
// by MatrixOne's SQL formatter. MO_STATEMENT_HASH is a MatrixOne-native
// contract, distinct from MySQL's normalized-token STATEMENT_DIGEST.
func statementHashDeparse(stmt tree.Statement) (formatted string, err error) {
	if stmt == nil {
		return "", moerr.NewNotSupportedNoCtx("cannot format a nil statement for MO_STATEMENT_HASH")
	}
	defer func() {
		if recover() != nil {
			formatted = ""
			err = moerr.NewInternalErrorNoCtx("AST formatter failed during MO_STATEMENT_HASH")
		}
	}()

	fmtCtx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
		tree.WithCanonicalUserVariableNames(),
		tree.WithMaxOutputBytes(maxStatementHashFormattedBytes),
	)
	stmt.Format(fmtCtx)
	if fmtCtx.OutputLimitExceeded() {
		return "", moerr.NewInvalidInputNoCtxf(
			"MO_STATEMENT_HASH formatted statement exceeds the maximum of %d bytes",
			maxStatementHashFormattedBytes,
		)
	}
	formatted = fmtCtx.String()
	if formatted == "" {
		return "", moerr.NewNotSupportedNoCtx("AST formatter produced empty output for MO_STATEMENT_HASH")
	}
	if len(formatted) > maxStatementHashFormattedBytes {
		return "", moerr.NewInvalidInputNoCtxf(
			"MO_STATEMENT_HASH formatted statement exceeds the maximum of %d bytes",
			maxStatementHashFormattedBytes,
		)
	}
	return formatted, nil
}

func statementHashValue(ctx context.Context, sql string, sqlMode string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(sql) > maxStatementHashInputBytes {
		return nil, statementHashInputTooLargeError(len(sql))
	}
	if err := validateStatementHashInputComplexity(ctx, sql, sqlMode); err != nil {
		return nil, err
	}
	statements, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, sql, 0, sqlMode)
	defer func() {
		for _, stmt := range statements {
			if stmt != nil {
				stmt.Free()
			}
		}
	}()
	if err != nil {
		return nil, err
	}

	if len(statements) != 1 {
		return nil, moerr.NewParseError(ctx, "MO_STATEMENT_HASH requires exactly one statement")
	}
	if _, empty := statements[0].(*tree.EmptyStmt); empty {
		return nil, moerr.NewParseError(ctx, "MO_STATEMENT_HASH requires a non-empty statement")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	formatted, err := statementHashDeparse(statements[0])
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

func statementHashInputTooLargeError(size int) error {
	return moerr.NewInvalidInputNoCtxf(
		"MO_STATEMENT_HASH input is %d bytes; maximum is %d bytes",
		size,
		maxStatementHashInputBytes,
	)
}

func validateStatementHashInputComplexity(ctx context.Context, sql, sqlMode string) error {
	scanner := mysqlparser.NewScannerWithSQLMode(
		dialect.MYSQL,
		sql,
		mysqlparser.ParseSQLModeFlags(sqlMode),
	)
	defer mysqlparser.PutScanner(scanner)

	tokenCount := 0
	nesting := 0
	for {
		if tokenCount&0xff == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		token, _ := scanner.Scan()
		if token == 0 || token == mysqlparser.LEX_ERROR {
			// Leave lexical failures to the parser so its normal error and source
			// location are preserved.
			return nil
		}
		tokenCount++
		if tokenCount > maxStatementHashTokenCount {
			return moerr.NewInvalidInputNoCtxf(
				"MO_STATEMENT_HASH statement exceeds the maximum of %d SQL tokens",
				maxStatementHashTokenCount,
			)
		}
		switch token {
		case int('('), int('['), int('{'):
			nesting++
			if nesting > maxStatementHashNesting {
				return moerr.NewInvalidInputNoCtxf(
					"MO_STATEMENT_HASH statement exceeds the maximum nesting depth of %d",
					maxStatementHashNesting,
				)
			}
		case int(')'), int(']'), int('}'):
			if nesting > 0 {
				nesting--
			}
		}
	}
}

// StatementHash hashes the MatrixOne MySQL-dialect rendering of one parsed
// statement. It is a build-scoped MatrixOne-native hash, not MySQL's
// normalized-token STATEMENT_DIGEST. Whitespace, comments, and equivalent
// formatting and case-insensitive user-variable names normalize through the
// AST formatter; literal values and distinct variable/identifier names remain
// part of the hash.
func StatementHash(
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
		// Enforce the input budget before consulting session state or copying the
		// bytes into a string for the parser.
		if len(input) > maxStatementHashInputBytes {
			return nil, statementHashInputTooLargeError(len(input))
		}
		if proc != nil && proc.Base != nil {
			if err := proc.ValidateStatementHashBuildCommitID(); err != nil {
				return nil, err
			}
		}
		if !sqlModeResolved {
			sqlMode, sqlModeErr = statementHashSQLMode(proc)
			sqlModeResolved = true
		}
		if sqlModeErr != nil {
			return nil, sqlModeErr
		}
		return statementHashValue(ctx, string(input), sqlMode)
	}, selectList)
}
