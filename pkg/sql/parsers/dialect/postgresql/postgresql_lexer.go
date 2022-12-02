// Copyright 2021 Matrix Origin
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

package postgresql

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Parse(ctx context.Context, sql string) ([]tree.Statement, error) {
	lexer := NewLexer(dialect.POSTGRESQL, sql)
	if yyParse(lexer) != 0 {
		return nil, lexer.scanner.LastError
	}
	return lexer.stmts, nil
}

func ParseOne(ctx context.Context, sql string) (tree.Statement, error) {
	lexer := NewLexer(dialect.POSTGRESQL, sql)
	if yyParse(lexer) != 0 {
		return nil, lexer.scanner.LastError
	}
	if len(lexer.stmts) != 1 {
		return nil, moerr.NewInternalError(ctx, "syntax Error, or too many sql to parse")
	}
	return lexer.stmts[0], nil
}

type Lexer struct {
	scanner *Scanner
	stmts   []tree.Statement
}

func NewLexer(dialectType dialect.DialectType, sql string) *Lexer {
	return &Lexer{
		scanner: NewScanner(dialectType, sql),
	}
}

func (l *Lexer) Lex(lval *yySymType) int {
	typ, str := l.scanner.Scan()
	l.scanner.LastToken = str

	switch typ {
	case INTEGRAL:
		return l.toInt(lval, str)
	case FLOAT:
		return l.toFloat(lval, str)
	case HEX:
		return l.toHex(lval, str)
	case HEXNUM:
		return l.toHexNum(lval, str)
	case BIT_LITERAL:
		return l.toBit(lval, str)
	}

	lval.str = str
	return typ
}

func (l *Lexer) Error(err string) {
	l.scanner.LastError = PositionedErr{Err: err, Pos: l.scanner.Pos + 1, Near: l.scanner.LastToken}
}

func (l *Lexer) AppendStmt(stmt tree.Statement) {
	l.stmts = append(l.stmts, stmt)
}

func (l *Lexer) toInt(lval *yySymType, str string) int {
	ival, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		l.scanner.LastError = err
	}
	lval.item = ival
	return INTEGRAL
}

func (l *Lexer) toFloat(lval *yySymType, str string) int {
	fval, err := strconv.ParseFloat(str, 64)
	if err != nil {
		l.scanner.LastError = err
	}
	lval.item = fval
	return FLOAT
}

func (l *Lexer) toHex(lval *yySymType, str string) int {
	return HEX
}

func (l *Lexer) toHexNum(lval *yySymType, str string) int {
	return HEXNUM
}

func (l *Lexer) toBit(lval *yySymType, str string) int {
	return BIT_LITERAL
}
